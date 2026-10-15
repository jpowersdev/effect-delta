# Yjs and its ecosystem

> Phase 1 research notes. Read from source on 2026-10-15: `yjs` 13.6.33 (stable, `v13` branch)
> and `@y/y` 14.0.0-rc.29 (`main`, release candidate). Behavior is v13 unless marked v14.
> "(src)" = verified in source. "(derived)" = our reasoning from source. "(unverified)" = not
> confirmed from a primary source.

## 1. Overview

Yjs (Kevin Jahns) is a CRDT library that exposes *shared types* (Text, Array, Map, Xml\*)
inside a `Y.Doc`. At its core is **one list CRDT**, a modified YATA. Maps, rich text, and
XML are all encoded as lists of `Item`s (INTERNALS.md).

- **Inserts are op-based.** Each element has a unique ID plus fixed left/right origins.
- **Deletes are state-based.** They form a set of deleted ID ranges and don't consume clock
  values.
- **Sync is state-vector based.** "Here is the highest clock I have per client" is answered
  with "everything above that, plus my whole delete set."
- **The core has no network or storage.** *Providers* (y-websocket, y-webrtc, y-indexeddb,
  Hocuspocus, y-sweet, y/hub) handle those. `y-protocols` defines the sync and awareness
  messages.
- **Rich text** uses the Quill Delta shape for changes and views. Formatting is stored as
  in-band marker items.
- **Ports:** Yrs (Rust, binary-compatible; ywasm, pycrdt, …). Bindings exist for
  ProseMirror/Tiptap, Lexical, Quill, CodeMirror, Monaco, and Slate.

**v14 (RC) is a redesign** (src):

- One generic `Y.Node` (name + attrs + children/text) replaces the class hierarchy.
- A schema-aware `lib0/delta` drives `toDelta`/`applyDelta` and a new `'delta'` event.
- `IdSet`/`IdMap` replace `DeleteSet`.
- Attribution renderers support blame and suggestions.
- clientIDs become 53-bit.
- The README still shows v13-style APIs in places, so treat v14 as in flux.

## 2. Glossary

| Term | Meaning in Yjs | Synonyms / clashes elsewhere |
|---|---|---|
| `Y.Doc` | Replica: clientID, guid, named root types, struct store | Automerge `Doc`, Loro `LoroDoc`; one doc = one sync unit |
| Shared type (`AbstractType`; v14 `Y.Node`) | Y.Text/Array/Map/Xml\*. Each has a list part and a map part | "container" (Loro), "object" (Automerge) |
| Root type | A type named in `doc.share`. Its kind is set by the local `get(name, Ctor)`, not by the wire (src) | — |
| Item | A run of content from one client: ID, origins, neighbors, parent, content | "struct", Yrs "Block", "op", "span" |
| clientID | Random ID per Doc *instance*: v13 32-bit (src; INTERNALS' "53-bit" is outdated), v14 53-bit | actor ID (Automerge), peer ID (Loro), site ID. **Not a user ID** |
| clock | Per-client counter, +1 per inserted element. Deletes don't advance it | Docs call the ID a "Lamport timestamp", but it never advances on receive. It is a sequence number / "dot" |
| ID `(client, clock)` | Identity of one inserted element | Automerge `counter@actor` |
| origin / rightOrigin | IDs of the left/right neighbors at insert time. Immutable | Fugue leftOrigin/rightOrigin. **Clash:** `transaction.origin` is unrelated |
| parentSub | The map key an Item lives under | — |
| StructStore | `Map<client, Array<Item\|GC>>`, contiguous, in clock order | op log |
| State vector | `Map<client, nextExpectedClock>` | Version vector, but "not used to track causality" (README). Automerge heads / Loro frontiers differ |
| Delete set (v14 `IdSet`) | Per-client sorted `[clock, len]` ranges | tombstone set |
| Tombstone / GC / Skip struct | Deleted Item with its content dropped / ID range fully discarded (parent deleted) / gap placeholder in merged updates | — |
| Update | Binary structs + delete set, in v1 or v2 encoding | patch, changeset, Automerge "change"; also a sync message name |
| Pending structs / `pendingDs` | Data buffered until missing dependencies arrive | causal buffer |
| Transaction | Batch → one round of events, one update | **Not ACID**: no isolation, no rollback |
| Delta | Quill-shaped insert/retain/delete + attributes | Clashes with delta-state CRDTs and Effect `Differ` patches |
| Format item (`ContentFormat`) | Inline `key=value` marker where formatting starts or ends | Peritext/ProseMirror "mark" (stored out of band) |
| Embed | Non-text, length-1 item in Y.Text | Quill embed |
| RelativePosition (+ `assoc`) | Position anchored to an ID; `assoc` picks the right (≥0) or left (<0) character | Yrs `StickyIndex`/`Assoc`, Automerge/Loro `Cursor`, "gravity" |
| Snapshot | `(delete set, state vector)`: names a version, holds no data | Elsewhere "snapshot" means full serialized state |
| Subdocument | Nested Y.Doc referenced by guid, loaded lazily | — |
| Awareness | Ephemeral per-client JSON state with heartbeats | presence |
| Provider | Network or storage adapter for a doc | connector |
| SyncStep1 / SyncStep2 / Update | Send state vector / reply with missing diff / incremental push | handshake |
| Search marker | Cached index→item lookups, ≤80 per type (src) | INTERNALS calls it a "skip list"; it isn't one |
| StackItem | Undo entry: insertions IdSet + deletions IdSet + `meta` | undo group |

## 3. How it works

### 3.1 Data model

**Root types.** Updates don't record a root type's kind. Data that arrives before anyone
calls `getText('x')` sits in a generic `AbstractType`, and is re-cast on the first typed
access. A conflicting second definition throws (src `Doc.get`). Root types can't be deleted
(FAQ).

**Item** (src) holds:

- `id`, `origin`, `rightOrigin`, `left`/`right` (current neighbors), `parent`, `parentSub`,
  `content`, `redone`
- an `info` bitfield: keep / countable / deleted / marker

**Content kinds:** String, Any, JSON, Binary, Embed, Format, Type (nested shared type),
Doc, Deleted. Format and Deleted content don't count toward length or index.

**Run-length.** Contiguous inserts from one client merge into one Item, provided
`right.origin == left.lastId`, `rightOrigin` matches, and the deleted flag and content kind
match. Items split on demand. On the B4 trace, 260k ops become 10,971 Items at about 88
bytes each excluding content (Jahns blog).

**StructStore.** Per-client arrays in clock order with no gaps; ID lookup is a binary
search. Document order lives in the linked lists. Index→item lookup goes through up to 80
cached search markers per type, not a tree.

**Text offsets** are UTF-16 code units. Splitting an Item inside a surrogate pair turns
both halves into U+FFFD (src, issue #248). Yrs offers `OffsetKind::{Bytes, Utf16}`.

### 3.2 Ordering concurrent inserts (YATA as implemented)

**YATA paper rules:**

1. Origin links must not cross.
2. The order is transitive.
3. Same origin → order by creator ID.

The worst case is O(C²) in conflicting inserts.

**Yjs changes.** Yjs adds `rightOrigin` "when many concurrent inserts happen after the same
character" (INTERNALS). `Item.integrate` scans the conflict zone between `left` and
`right`:

- If an item has the same origin and a lower clientID, the new item goes after it.
- If it has the same origin and the same rightOrigin, the scan stops.
- If its origin lies inside the already-scanned zone, the scan continues.
- Otherwise the scan stops.

Among siblings with identical origins, **the lower clientID comes first** (src).

**Maps** (derived). Each map key is its own YATA list, and the rightmost entry is the
value. So when two clients concurrently `set` the same key, **the higher clientID wins**.
This is not wall-clock last-writer-wins.

**Correctness.** `lean-yjs` proves the algorithm in Lean, and found errors in the YATA
paper's pseudocode (v14 README).

**Interleaving** (Fugue paper):

- Yjs is proven not to interleave forward insertions.
- Backward runs can interleave, but only across multiple replica IDs, e.g. a tab reload
  that issues a fresh clientID. The paper's repro prints `axb`.
- Gentle's "YjsMod" is conjectured to equal FugueMax (maximally non-interleaving).

### 3.3 Deletes, tombstones, GC

A delete sets a bit and adds the range to `transaction.deleteSet`. Nothing records who
deleted or when, and the clock doesn't move (INTERNALS). Delete sets are computed from the
store when needed rather than stored.

**GC** (on by default) runs after each transaction (src):

- A deleted Item that isn't marked `keep` has its content replaced by `ContentDeleted(len)`.
  Its **skeleton (ID, origins, position) stays forever**.
- The children of a deleted nested type become `GC` structs, which leave the tree.

So metadata only ever grows. The paper's time-based tombstone removal is *not* what Yjs
does. Versioning requires `gc: false` (`createDocFromSnapshot` throws otherwise).

### 3.4 Transactions and events

`doc.transact(fn, origin)` groups changes. Mutations outside a transaction get one
automatically, and nested calls join the outer one. There is **no rollback**: cleanup runs
in `finally`.

**A transaction records:** `beforeState`, `afterState`, `deleteSet`, `changed`,
`changedParentTypes`, `origin`, `local`, `meta`.

**Cleanup order** (src):

1. `observe`
2. `observeDeep`, shallowest path first
3. `afterTransaction`
4. Format cleanup
5. GC + struct merging
6. clientID collision check
7. `update` (encoded update + origin)
8. `subdocs`
9. `afterAllTransactions`

Writes made inside an observer are queued as the next transaction.

**YEvent** exposes `target`, `currentTarget`, `path`,
`changes {added, deleted, keys, delta}`, and
`keys: Map<key, {action, oldValue}>`. It is computed lazily: "You must not compute changes
after the event-handler fired." Text deltas use the Quill shape.

Providers use `transaction.origin` to suppress echoes, and UndoManager uses it for scoping.

### 3.5 Updates and encoding

**Shape.** An update is per-client runs `[count, client, startClock, structs…]` followed by
a delete set. Updates are "commutative, associative, and idempotent" (docs).

**Item encoding.** An info byte (content ref + three flags) comes first, then the origins.
The parent is written **only if there are no origins**, because otherwise it is inferred
from the neighbors. Content comes last.

**Two encodings:**

- **V1** is row-oriented varints.
- **V2** is columnar, with separate RLE/diff streams for client, clocks, info, strings,
  parent info, type refs, lengths, and keys (src). Not all providers support it.

**Diff.** `encodeStateAsUpdate(doc, sv)` writes each client's structs from `sv[client]`
onward, plus the **whole delete set** (state vectors don't cover deletes), plus pending
data. Higher client IDs go first, which "heavily improves the conflict algorithm" (src).

**Without a Doc:** `mergeUpdates` (stream-merges, inserting `Skip` for gaps), `diffUpdate`,
`encodeStateVectorFromUpdate`, `convertUpdateFormatV1ToV2`, `obfuscateUpdate`. Merging
"doesn't garbage-collect deleted content."

**Missing dependencies** (src). Integration stalls on a struct whose clock is ahead of
local state, or whose origin/rightOrigin/parent is unknown. The rest of that client's
structs go to `pendingStructs {missing, update}`, and deletes for unknown IDs go to
`pendingDs`. Each later `applyUpdate` retries. Pending data is forwarded by
`encodeStateAsUpdate` but excluded from the state vector.

So transports needn't be causal, but one lost update from client X blocks all of X's later
ops.

### 3.6 Sync and awareness (y-protocols PROTOCOL.md)

**Sync messages:**

| Message | Encoding |
|---|---|
| `SyncStep1` | `varUint(0) • varBuffer(stateVector)` |
| `SyncStep2` | `varUint(1) • varBuffer(encodeStateAsUpdate(doc, sv))` |
| `Update` | `varUint(2) • varBuffer(update)` |

**Handshake.** Peer-to-peer: both sides send step 1. Client-server: the client starts; the
server replies with step 2 and then its own step 1, and "SHOULD NOT initiate."

**Outer framing:** 0 = sync, 1 = awareness. y-websocket adds 2 = auth and
3 = queryAwareness. Hocuspocus prefixes every message with the document name (so many docs
share one socket), and adds SyncStatus acks, Stateless, and Ping/Pong.

**Read-only** is enforced by the first two bytes: accept `00 00`, reject `00 01` and
`00 02`.

**Acks.** The base protocol has none. y-websocket v4 tracks `unconfirmedUpdates` by content
ID and clears them when the server echoes the content, which drives a green/yellow/red
`sync-status`.

**Awareness** maps `clientID → (state | null, clock, lastUpdated)`:

- An entry applies only if its clock is greater; `null` means offline.
- Entries time out after 30 s; clients renew every 15 s.
- Peers can't remove your entry: if they try, you bump your clock and re-assert.
- Encoding: `varUint(n) • (clientID, clock, json(state))*`.
- Payloads are unauthenticated.
- `change` fires only on real changes; `update` also fires on heartbeats.

### 3.7 Rich text

**Quill Delta itself:**

- An array of ops: `insert` (a string, or an embed object of length 1),
  `retain(n, attrs?)`, `delete(n)`.
- An attribute set to `null` removes that format.
- The same structure describes a document (inserts only) or a change.
- Canonical form: compact (adjacent ops with equal attributes merged), no redundant
  attributes, insert before an adjacent delete.
- Line formats attach to `'\n'`, and documents end with `'\n'`.
- Library operations: `compose`, `transform(priority)`, `transformPosition`, `diff`,
  `invert`.
- It is an OT format. Yjs borrows only its shape.

**Y.Text API:** `insert(i, str, attrs?)`, `insertEmbed`, `format(i, len, attrs)`,
`toDelta(snapshot?, prevSnapshot?)`, `applyDelta`.

**Storage** (src):

- `format` inserts `ContentFormat(key, value)` before the range and
  `(key, previousValue | null)` after it, and deletes format items inside the range.
- Reading is a left-to-right scan that tracks the current attributes.
- An insert without attributes inherits the attributes at its position.
- Embeds are JSON or nested types.
- With snapshots, `toDelta` emits `ychange` attributes for diff views.

**Anomaly.** Concurrent overlapping bolds can end early. Peritext says of Yjs: "it suffers
from the anomaly shown in this section." ProseMirror-style editors use `Y.XmlFragment`, not
`Y.Text`.

**v14** deltas are nested and schema-checked, with separate `format` and `attribution`
dimensions and an OT-style `rebase`.

### 3.8 Positions

`RelativePosition {type | tname, item: ID | null, assoc}` (src):

- It references the ID of the character on its right (`assoc ≥ 0`) or left (`assoc < 0`).
  End-of-type positions reference the type itself.
- It resolves to `AbsolutePosition {type, index}` by walking left, which is O(n). It
  returns `null` if the anchor isn't known locally yet.
- It survives concurrent inserts and the deletion of its anchor.
- Pass `followUndoneDeletions = false` for shared positions, because only the local user can
  follow `redone` links (#638).
- Cursors usually travel in awareness as relative positions.

### 3.9 UndoManager

Constructor: `new UndoManager(scope, {captureTimeout: 500, trackedOrigins: Set([null]),
deleteFilter, captureTransaction})`.

**Recording.** When a transaction touches the scope and its origin is in
`trackedOrigins`, the manager pushes `StackItem {insertions, deletions, meta}`:

- Changes within `captureTimeout` merge into the same StackItem.
- Deleted items get the `keep` flag so GC can't drop their content.

**Undo** deletes the insertions and re-creates the deletions as **new items with new
IDs**, setting `old.redone`. It is selective: remote changes stay. `meta` is where apps
keep selections. Undo across docs needs y-utility.

### 3.10 Snapshots, versions, subdocuments

**Versions.** An item is visible in a snapshot when `clock < sv[client] && !ds.has(id)`.
The docs advise iterating the current doc rather than restoring a copy. `PermanentUserData`
maps clientIDs to users for blame. v14 uses `IdMap` attributions for Google-Docs-style
history.

**Subdocuments** are stored as `ContentDoc` (guid + options):

- Each stays empty until `load()` is called, or loads automatically with `autoLoad`.
- The `subdocs` event (`{added, removed, loaded}`) tells providers what else to sync.
- "Not all providers support subdocuments yet."

### 3.11 Providers and servers

**What a provider does.** It subscribes to `update` and sends what it gets. It applies
incoming data with itself as the origin, so nothing echoes back, and runs the handshake on
connect. Providers can be stacked because updates are idempotent.

| Project | Architecture |
|---|---|
| y-indexeddb | Appends each update to IndexedDB. At 500 entries, compacts them into one state update (src) |
| y-websocket | Reconnecting client with cross-tab BroadcastChannel. Its server keeps an in-memory doc per room, with pluggable persistence and an HTTP callback |
| Hocuspocus | Authoritative in-memory Y.Doc per document name. Hooks: `onAuthenticate`, `onLoadDocument`, `onChange`, `onStoreDocument` (debounce 2 s, max 10 s), `beforeHandleMessage`, … Read-only connections reject writes. Persists the full state. The Redis extension does pub/sub plus a Redlock on store |
| y-sweet (Rust/yrs) | KV snapshot in S3 or on disk, checkpointed every 10 s. One process owns a doc. Client tokens are `Full` or `ReadOnly` |
| y/hub (AGPL/commercial) | Holds no doc in memory after the initial sync. One Redis stream per doc. Workers persist to S3/Postgres. Auth plugin |

**Relay vs authority.** The protocol is symmetric, so a server is just another peer.
"Authority" means it keeps the canonical copy and gates access. It cannot reject one update
and keep the client consistent, because the client keeps the change and its later ops
depend on it (derived).

## 4. Strengths / weaknesses / known anomalies

**Strengths**

- Fast and compact. On B4: 160 kB saved, 63 ms load, 3.3 MB memory (Fugue paper).
- Commutative, idempotent updates that can be merged without loading a doc.
- Tiny transport-agnostic sync.
- Mature extras: positions, selective undo, awareness, many bindings, a Rust port.
- Verified ordering.

**Weaknesses and anomalies**

- **Single-client-clock model:**
  - A new clientID per session (FAQ) means state vectors grow with every session ever.
  - A gap in one client's sequence stalls all of that client's later ops.
  - Every sync ships the full delete set (derived).
- **clientID collisions:**
  - v13 uses a random uint32. That is about 1% collision risk at 10k sessions per doc and
    about 39% at 65k (derived, birthday bound).
  - Collisions are detected only after the fact.
  - The result is corruption "without a way to recover" (FAQ). v14 and Yrs 0.28 use 53-bit
    IDs.
- **Map conflicts are decided by clientID order**, not time (derived).
- **Rich text:** Peritext's overlap anomaly. No per-mark expand rules at range boundaries
  (unverified).
- **Interleaving:** backward runs across multiple replica IDs can interleave.
- **UTF-16 offsets**, and split surrogate pairs become U+FFFD.
- **No schema:** values are `any`, root kinds are declared locally, nothing is validated on
  apply, and TypeScript generics are unchecked. v14 adds lib0 schemas.
- **Ergonomics:**
  - A type can't be moved or re-inserted, and can't be read before it is integrated.
  - Inserted JSON must not be mutated.
  - Events are lazy and only valid inside the handler.
  - Observers are synchronous and can re-enter.
  - Two loaded copies of Yjs break `instanceof` checks (#438).
  - There is no list *move* in v13 (unverified for v14).
- **History:** skeletons are kept forever, GC and snapshots are mutually exclusive, and
  transactions don't roll back.

## 5. Relevance to us

**Borrow**

1. **Run-length insert ops plus an ID-range delete set.** Make `IdSet`/`IdMap` first-class
   Schema values with merge/diff/intersect, as v14 does.
2. **The three-message sync and its algebra.** Idempotent, commutative updates with doc-less
   merge/diff fit `Rpc`/`Socket`, dumb relays, and compaction.
3. **A causal buffer inside the document**, so transports stay simple.
4. **Post-commit change values as deltas** via `Stream`/`PubSub`. v14's schema'd nested
   deltas are close to what `Schema` + `Differ` give us.
5. **Origin tags** for echo suppression and undo scope. Also `assoc` positions, undo stacks
   built from ID sets, content-ID acks, and append-then-compact persistence for
   `KeyValueStore`.

**Avoid**

1. Untyped roots and `any` values.
2. Class hierarchies with `instanceof` dispatch. Use tagged data.
3. Synchronous re-entrant observers and short-lived lazy events.
4. A "transaction" without rollback. Build changes in an Effect, then commit atomically.
5. 32-bit per-session IDs, clientID-ordered map LWW, inline format markers, and UTF-16-only
   offsets.
6. Overloading "origin."
7. GC that silently rules out versioning. Make retention an explicit policy.

**Open questions**

- **List algorithm.** YATA as Yjs implements it, or FugueMax/YjsMod (same metadata,
  stronger non-interleaving)?
- **Replica IDs.** Width, generation, persistence/leases, collision detection.
- **Deletes and clocks.** Should deletes advance the clock, so diffs don't ship the whole
  delete set?
- **Text index units.** Code points, UTF-16, or graphemes?
- **Formatting.** Inline markers, or Peritext-style marks?
- **Server validation.** How can a server validate or reject updates without permanent
  divergence?
- **History.** Compaction policy and snapshot strategy.
- **Delta shape.** Quill, `Differ` patches, or nested schema'd deltas? And can a
  Schema-defined wire format still allow a v2-style columnar layout later?

## 6. Sources

- Yjs repo (v13 branch, main/v14): https://github.com/yjs/yjs — `INTERNALS.md`, `src/structs/Item.js`,
  `src/utils/{StructStore,DeleteSet,Transaction,encoding,updates,UpdateEncoder,RelativePosition,UndoManager,Snapshot,Doc,YEvent}.js`,
  `src/types/YText.js`; v14 `src/ynode.js`, `src/utils/ids.js`, `attributing-content.md`
- Yjs docs: https://docs.yjs.dev/api/document-updates, https://docs.yjs.dev/api/subdocuments,
  https://docs.yjs.dev/api/faq, https://docs.yjs.dev/api/delta-format,
  https://docs.yjs.dev/api/shared-types/y.text, https://docs.yjs.dev/getting-started/working-with-shared-types
- y-protocols: https://github.com/yjs/y-protocols (`PROTOCOL.md`, `src/sync.js`, `src/awareness.js`)
- YATA paper (GROUP 2016, doi:10.1145/2957276.2957310): https://www.researchgate.net/publication/310212186
  (PDF: https://files.gitter.im/y-js/yjs/yCYx/GROUP2016-_6_.pdf)
- Jahns, "Are CRDTs suitable for shared editing?": https://blog.kevinjahns.de/are-crdts-suitable-for-shared-editing/
- Weidner & Kleppmann, "The Art of the Fugue": https://arxiv.org/abs/2305.00583
- Peritext: https://www.inkandswitch.com/peritext/
- Gentle, "5000x faster CRDTs": https://josephg.com/blog/crdts-go-brrr/
- Quill Delta: https://github.com/slab/delta, https://quilljs.com/docs/delta, https://quilljs.com/guides/designing-the-delta-format
- lib0 delta: https://github.com/dmonad/lib0 (`src/delta/readme.md`)
- lean-yjs: https://github.com/iasakura/lean-yjs
- y-websocket: https://github.com/yjs/y-websocket, https://github.com/yjs/y-websocket-server
- y-indexeddb: https://github.com/yjs/y-indexeddb
- y/hub: https://github.com/yjs/yhub
- Hocuspocus: https://github.com/ueberdosis/hocuspocus
- y-sweet: https://github.com/jamsocket/y-sweet
- Yrs / ywasm: https://github.com/y-crdt/y-crdt
