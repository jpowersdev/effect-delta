# CRDT libraries other than Yjs, with a focus on text

Phase 1 research notes, compiled 2026-10-15. Sources are repo snapshots (commit dates in §6) plus the maintainers' docs, posts, and papers. Anything marked "(unverified)" could not be confirmed from a primary source.

## 1. Overview

Two families of collaborative text algorithms show up:

- **ID-per-character sequence CRDTs.** Every character gets a unique ID, and deleted characters stay behind as tombstones. Inserts reference neighbouring IDs. Ordering rules differ: RGA (Automerge, json-joy), Fugue/FugueMax (Loro, Collabs, list-positions), YATA (Yjs), and Lamport-ordered fragments (Zed, cola).
- **Event-graph replay (Eg-walker).** The library stores the *original* index-based edits plus a causal DAG. On merge it builds a temporary CRDT state only for the concurrent region, then throws it away (Diamond Types; Loro is "inspired by" it).

The libraries also differ along five independent axes, which drive our vocabulary:

1. Version identity: hash *heads*, op-ID *frontiers*, or version vectors.
2. Wire unit: change, update, patch, or message.
3. App-facing diff: path patches, Quill deltas, typed events, or none.
4. Rich text: Peritext-family marks with an "expand" behaviour at boundaries.
5. Stable positions: cursors anchored to character IDs, with a left/right bias.

| Library | Runtime | License | Version / status |
|---|---|---|---|
| Automerge | Rust core + WASM/JS | MIT | JS 3.5.0; 3.0 shipped Jul 2025 |
| Loro | Rust + WASM/JS | MIT | 1.16.4; format stable since 1.0 (Oct 2024) |
| Diamond Types | Rust (+WASM) | ISC | plain text; crates.io release out of date |
| json-joy | TypeScript | **AGPL-3.0-only** (`json-joy`, `peritext` packages) | v18.30, active |
| Collabs | TypeScript | Apache-2.0 | 0.13.4; last commit Mar 2025 |

## 2. Per-library notes

### 2.1 Automerge (2.x → 3.x)

- **Model.** A *document* is a JSON-like tree (maps, lists, text, counters, scalars) that carries its full history. In 3.x every JS `string` is collaborative text. `ImmutableString` (formerly `RawString`) holds non-collaborative strings, and the old `Text` class is gone.
- **IDs.**
  - The `actorId` is random bytes, usually 128-bit. Op IDs are written `counter@actor` and act as Lamport timestamps: order by counter, break ties by actor.
  - A **change** groups ops: `{actor, seq, startOp, time, message, deps, hash}`. `seq` numbers each actor's changes, `deps` are the parent change hashes, and `hash` is a SHA-256 of the encoded change.
  - **Heads** are the hashes of changes with no successors, so history is a hash DAG.
  - 3.5 adds an optional opaque `author`, separate from `actor`.
- **Merge.**
  - Maps use last-writer-wins (LWW) by op ID, but keep the losing values (`getConflicts`).
  - Lists and text use **RGA**: an insert references the element it follows, and concurrent inserts are ordered by op ID. The docs admit text typed backwards can interleave.
  - Counters sum. Marks follow Peritext.
- **App-facing changes.** `Patch[]`, addressed by `path`. Actions are `put | del | splice | insert | inc | mark | unmark | conflict`, for example `{action:"splice", path:["text",5], value:"hi"}`. They arrive through `patchCallback`, the `DocHandle` `"change"` event, and `diff(doc, fromHeads, toHeads)`.
- **Sync protocol.**
  - Works per document and per peer, needs a reliable in-order stream, and is based on Kleppmann & Howard (2020).
  - Each peer keeps an explicit sync `State` (shared heads, their heads, their `have`, sent hashes, in-flight flag); part of it can be persisted.
  - A message is `{heads, need, have:[{lastSync, bloom}], changes}`. The Bloom filter uses 10 bits per entry and 7 probes (about 1% false positives); explicit `need` requests fill the gaps.
  - V2 messages ship the whole doc when more than a third is needed. Peers loop `generateSyncMessage` / `receiveSyncMessage` until both return null.
- **Automerge Repo (2.x).**
  - A `Repo` takes pluggable network adapters (WebSocket, MessageChannel, BroadcastChannel) and storage adapters (IndexedDB, NodeFS).
  - A **`DocHandle`** is a "live" doc with `change`, `changeAt(heads)`, `view(heads)`, `diff`, `history`, and `broadcast`, and has load states from `LOADING` through `READY` and `UNAVAILABLE`.
  - URLs look like `automerge:<bs58check docId>[/<path>][#<heads>]`.
  - Storage keys are `[docId, "incremental"|"snapshot", hash|heads]`, and adapters must support range load/remove. Compaction deletes only the chunks it loaded, so it is safe without locks.
  - A `Presence` module handles ephemeral state.
- **Storage and history.**
  - The binary format is columnar, run-length encoded, and DEFLATE-compressed, built from document and change chunks. API: `save`, `saveIncremental`, `loadIncremental`.
  - 3.0 stays compressed in memory: over 10× less memory, with a *Moby Dick* paste going from 700 MB to 1.3 MB.
  - Time travel: `view(heads)` is a read-only past version, and `changeAt(heads)` edits from one (a branch).
- **Cursors.** `getCursor(doc, path, index, move?)` returns an opaque string, and `getCursorPosition` resolves it. There are `"start"`/`"end"` sentinels, and `move: before|after` picks where the cursor lands if its character is deleted.
- **Undo.** None built in (the source only has transaction rollback).
- **Rich text.**
  - Marks are `{name, scalar value, expand: before|after|both|none}`.
  - **Block markers** are maps embedded inline in the string, edited with `splitBlock`, `updateBlock`, and `joinBlock`.
  - `spans()` returns text and block entries. `updateSpans` diffs block structure, but not marks yet.
  - A published schema covers ProseMirror interop.
- **Text units.** Indexes are UTF-16 code units in JS. The Rust core also supports code points, UTF-8, and grapheme clusters.
- **Typing and ergonomics.** A TypeScript generic `Doc<T>` over proxies, with no runtime schema. The functional `A.change(doc, d => …)` style makes maps feel like plain JS, but text needs path-based free functions (`A.splice(d, ["text"], 0, 0, "hi")`).
- **Maturity.** The most mature non-Yjs option and actively developed.

### 2.2 Loro

- **Model.**
  - A `LoroDoc` holds **containers**: `LoroText`, `LoroList`, `LoroMovableList`, `LoroMap`, `LoroTree`, `LoroCounter`.
  - Root containers are looked up by name. A child container's ID derives from the op that created it, so concurrent `setContainer` on the same key makes two containers and LWW hides one. **Mergeable containers** (`ensureMergeableText(key)`, Jun 2026) fix this by keying identity on the key.
  - Internally Loro separates the **OpLog** (history DAG) from the **DocState** (current state). A relay can keep only the OpLog.
- **IDs.**
  - `PeerID` is a random u64 per doc instance. Op IDs are `{peer, counter}`, one counter per character, and each change also gets a Lamport timestamp.
  - A **change** is a run of ops from one peer; consecutive commits auto-merge into one.
  - Peer IDs must never be reused without first loading that peer's prior state.
- **Merge.**
  - Text and lists use **Fugue** (maximal non-interleaving).
  - `MovableList` adds Kleppmann's list-move algorithm, `Tree` is a movable tree, and `Map` is LWW.
  - The design is "inspired by Eg-walker, not a strict implementation": ops record indices, merges replay only the concurrent history, and state keeps no deleted IDs.
- **Versions.** Convertible with `frontiersToVV` and `vvToFrontiers`:
  - A **version vector** (`version()`) maps each peer to a counter, and is what sync uses.
  - **Frontiers** are the op IDs at the DAG tips, usually one or two. They suit checkpoints but can't be interpreted without the history.
- **Import/export.**
  - `export({mode})` modes: `update` (from a version vector), `updates-in-range`, `snapshot` (OpLog plus state), and `shallow-snapshot` (state at a frontier plus later history only). Shallow snapshots work like a git shallow clone: they can redact content, but block sync with peers concurrent to the cut.
  - `import` returns `{success, pending}` counter ranges, where `pending` means causally blocked.
- **Sync.**
  - The core has no handshake: peers exchange version vectors, call `export({mode:"update", from})`, then stream `subscribeLocalUpdates`.
  - The **Loro Protocol** (Oct 2025) multiplexes rooms over one WebSocket, tagged by magic bytes: `%LOR` (doc), `%EPH` (ephemeral), `%ELO` (encrypted), `%YJS` (Yjs). It fragments messages under 256 KiB.
- **Time travel.** `checkout(frontiers)` gives a read-only *detached* state, and `attach()` returns to latest. Also available: `forkAt`, `revertTo`, and `diff` / `applyDiff`.
- **Events.**
  - Shape: `LoroEventBatch {by: local|import|checkout, origin?, events:[{target, path, diff}], from, to}`.
  - Diffs depend on container type. Text gets Quill-style deltas (insert, retain, delete, plus attributes); lists get list deltas, maps `{updated}`, trees create/move/delete, counters `{increment}`.
  - Events fire on `commit()` (synchronously since 1.8), and Loro auto-commits before import and export. Transactions only group events: no rollback.
- **Cursors.** `text.getCursor(pos, side ∈ {-1,0,1})` returns a serializable `Cursor`. `doc.getCursorPos` returns `{offset, side, update?}`, where `update` is a refreshed cursor to store, because a deleted anchor has to be resolved by replay. JS offsets are UTF-16.
- **Undo.** `UndoManager`: local-only (never undoes remote ops) and single-peer, using OT-style transforms internally. Options include `mergeInterval` and `excludeOriginPrefixes`. It stores cursors in each undo item and keeps them updated, and groups steps with `groupStart` / `groupEnd`.
- **Rich text.**
  - Loro wrote its own Peritext-compliant algorithm, because original Peritext's op-ID anchors don't survive Eg-walker-style replay.
  - Each mark is a zero-width start/end pair of *style anchors* (op ID, key/value, Lamport, expand). For each key, the highest Lamport wins.
  - Expand behaviour is configured per key, e.g. `configTextStyle({bold:{expand:"after"}})`.
  - Loro passes the Peritext paper's test cases.
- **Ephemeral.** `EphemeralStore` is a timestamped LWW key-value store with a timeout.
- **Typing.** `LoroDoc<T>` types the root containers. **loro-mirror** (Sep 2025) adds a declarative schema that maps the doc to immutable app state in both directions.
- **Maturity.** Company-backed and very active, with ProseMirror and CodeMirror bindings. The WASM bundle is about 970 KB gzipped.

### 2.3 Diamond Types (Seph Gentle)

- **Model.** Plain text only; JSON types live on the `more_types` branch.
  - **OpLog:** append-only original ops plus the causal graph.
  - **Branch:** an in-memory `(version, content)` pair, not a git branch.
- **IDs.** `(agent, seq)`, where *each* inserted or deleted character consumes a `seq`; runs are run-length encoded. Locally, ops are numbered by "local version" (LV) integers that can't be shared with peers. The docs advise a fresh agent per session.
- **Merge (Eg-walker).**
  - Ops are stored as written: "insert `k` at 12, parents [X, Y]".
  - To merge, the algorithm walks the event graph from the common ancestor. It builds a temporary CRDT state over just the concurrent region (YjsMod/FugueMax order, which the source says give identical results), emits *transformed* positional ops, and discards the temporary state.
  - Paper claims (EuroSys 2025): about an order of magnitude less steady-state memory than CRDTs, much faster loading, and much faster than OT on long-running branches. Worst-case merges are comparable to CRDTs.
- **App-facing changes, sync, storage.** "xf" (transformed ops since a version) are OT-compatible positional ops. `get_patch_since(version)` returns bytes and `merge_bytes` applies them. There is no network layer.
- **Gaps.** No cursor API found (unverified), and no undo, rich text, or schema.
- **Maturity.** Low-level API, with docs that say they are out of date. Best treated as a performance reference; `eg-walker-reference` is a small TypeScript port.

### 2.4 json-joy JSON CRDT (Vadim Dalecky)

- **Model.** A tree of 7 node types, each its own CRDT:
  - `con`: immutable constant.
  - `val`: LWW reference to another node.
  - `obj`: LWW map.
  - `vec`: LWW tuple.
  - `str`: RGA text.
  - `bin`: RGA bytes.
  - `arr`: RGA list of references.

  Extensions compose nodes: multi-value register, counter, Quill Delta, and Peritext.
- **IDs.** A logical timestamp `(sid, time)`, where `sid` is a random 53-bit session ID; a `Timespan` adds a length for runs. **Server clock mode** gives every client sid 1 and lets the server order ops; it encodes smaller, but ops are no longer idempotent.
- **Merge.** Block-wise RGA (characters stored as runs) for sequences; LWW for the rest.
- **Patch format.**
  - A **Patch** is a list of ops with consecutive timestamps, and is both the change unit and the wire format.
  - Ops: `new_*`, `ins_*`, `upd_arr`, `del` (by timespans), `nop`. Example: `ins_str(obj, after=charId, "hello")`.
  - Codecs: binary, compact JSON, verbose. `model.applyPatch()` is the only way state changes.
- **Events.** Model-, API-, and node-level notifications (`onPatch`, `onChanges`, `onViewChanges`), batched per microtask and carrying no diff payload. Views are cached and identity-preserving, which suits React.
- **Storage and sync.**
  - Model codecs: structural, indexed, and **sidecar** (a plain JSON view plus a separate metadata blob).
  - `Log` holds a start model plus patches, and `Log.undo(patch)` builds the inverse patch.
  - `json-crdt-server` provides WebSocket RPC, LevelDB storage, presence, and history. `json-crdt-repo` is an offline-first browser client.
- **Rich text (Peritext).**
  - Text is an RGA `str`, and slices live in an `arr`: `vec(header, startId, endId, type, data?)`.
  - The header packs a *stacking* mode: `Marker` (block split), `One` (e.g. bold), `Many` (e.g. links or comments), `Erase`, `Cursor`, or `Atomic`.
  - It also packs each endpoint's anchor (Before/After a character), which replaces an expand enum.
  - Slices are split into saved, shared-unpersisted, and local-only (own selection). Bindings exist for ProseMirror, Quill, Slate, CodeMirror, and Monaco. The docs still say rich text is "being worked on."
- **Typing.** A schema builder (`s.obj({title: s.str("")})`) passed to `Model.create(schema)` gives a typed view and API. This is the closest prior art to a doc shape driven by Effect `Schema`.
- **Maturity.** Broad, single maintainer, and active. The AGPL license means ideas only, no code.

### 2.5 Collabs and list-positions (Matthew Weidner)

- **Model.** A document is a **`CRuntime`** plus named, composable **Collabs**: `doc.registerCollab("text", init => new CText(init))`. Subclasses of `CObject` nest Collabs into typed, reusable models. Built-ins include `CText`, `CRichText`, `CList` (with move), `CValueMap`, `CSet`, `CVar`, and `CCounter`.
- **IDs and merge.**
  - Each transaction is identified by a causal dot `(senderID, counter)`, and the doc tracks a vector clock.
  - Op-based `send` / `receive` and state-based `save` / `load` can be mixed freely. Messages wait for their causal dependencies, and duplicates are ignored.
  - Lists use Tree-Fugue. Rich text follows Peritext semantics, but stores span endpoints in a separate list.
- **Events.** Typed per-Collab events: `CText` emits `Insert` / `Delete` with `{index, values}`, and `CRichText` adds `Format`. The doc emits `"Change"` and `"Update"` (bytes for providers).
- **Sync.** Providers for WebSocket, IndexedDB, localStorage, and tab sync.
- **Positions.**
  - `Position` is an opaque, immutable string per list entry; `Cursors` represent gaps between entries.
  - **list-positions** (2024) generalises this: a list is an ordered map `Position → value`, with `Position = {bunchID, innerIndex}`. Self-contained `AbsPosition`s and lexicographic strings are also available. Fugue runs underneath.
- **Undo, typing, maturity.** No undo. The code is the schema: registrations must match on every replica. The project is academic and effectively in maintenance.
- **Server-ordered alternative.** Weidner's 2025 post "Collaborative Text Editing without CRDTs or OT": a UUID per character, "insert after ID" ops applied literally by the server, and client-side *server reconciliation* (undo pending edits, apply remote ones, redo pending).

### 2.6 Other notable work (2024–2026)

- **cola** (Rust): a plain-text CRDT that stores only metadata while the app owns the buffer. `inserted(offset, len)` returns an op, and `integrate_insertion` returns the offset to apply. Early ops wait in a backlog; stable positions are `Anchor`s.
- **Zed** (Dec 2022): immutable insertion fragments, `(insertion_id, offset)` anchors, deletions carrying version vectors, and Lamport ordering. An **undo map** (op ID → count; odd means undone) gives CRDT-native undo in any order.
- **Ink & Switch Patchwork** (2024–): "universal version control" (history, diffs, edit groups, branches) on Automerge.
- **Keyhive** (pre-alpha): end-to-end encrypted groups and capabilities. Its sync work is now **Subduction**, the successor to Beelay. Automerge 3.5's experimental fragment/commit APIs look related (unverified).

## 3. Comparison table

| | Automerge | Loro | Diamond Types | json-joy | Collabs |
|---|---|---|---|---|---|
| Text algorithm | RGA | Fugue, Eg-walker-style log | Eg-walker | block-wise RGA | Tree-Fugue |
| Op ID | `counter@actor` | `(peer, counter)` + Lamport | `(agent, seq)`; local LV | `(sid, time)` | `(replica, counter)` |
| Version | heads (hashes) | frontiers + version vector | frontier of LVs | vector clock | vector clock |
| Wire unit | change; sync message | update bytes | patch bytes | Patch | message / saved state |
| App diff | path `Patch[]` | events + Quill delta | transformed ops | events only | typed events |
| Sync | Bloom-filter heads exchange | version-vector diff; Loro Protocol | — | server RPC | providers |
| Storage | columnar chunks | snapshot / shallow / update | binary oplog | 3 codecs | saved state |
| Time travel | `view`, `changeAt` | `checkout`, `forkAt` | branch anywhere | `Log` replay | — |
| Cursors | string + `move` | `Cursor` + `Side` | — | `Point(id, anchor)` | `Position` |
| Undo | — | local `UndoManager` | — | `Log.undo` | — |
| Rich text | marks + blocks | style anchors | — | stacked slices | `CRichText` |
| Typing | TS generic | typed roots; loro-mirror | — | `s.*` schema | classes |
| Presence | `Presence` | `EphemeralStore` | — | server | — |
| Maturity | high | high | research | active | maintenance |

## 4. Vocabulary clashes

| Term | Meanings | Note for us |
|---|---|---|
| **patch** | Automerge: view diff handed to the app. json-joy: *the op/wire unit*. Diamond Types: encoded ops. Effect `Differ`: composable value diff. | Most overloaded term; never use it for the op unit. |
| **change** | Automerge: hashed atomic commit. Loro: run of ops from one peer that auto-merges. Collabs: a UI event. | |
| **update** | Loro: export bytes. Yjs: structs plus delete set. Collabs: a message *or* a saved state. | |
| **delta / diff** | Loro and Yjs: Quill insert/retain/delete. Automerge `diff()` returns `Patch[]`. | The de-facto word for text diffs. |
| **event** | Loro: a diff batch. json-joy: a notification with no payload. Effect `eventlog`: a persisted domain event (unverified for v4). | Clashes with Effect. |
| **transaction / commit** | Automerge: `change()` callback. Loro: event grouper, no rollback. Collabs: one microtask of ops. Effect: STM / SQL. | Avoid "transaction". |
| **doc / container / shared type / node / Collab** | Automerge objects; Loro containers; Yjs shared types; json-joy nodes; Collabs Collabs; Diamond Types OpLog + Branch. | |
| **heads / frontiers / state vector / version vector** | Automerge heads = tip *hashes*. Loro frontiers = tip *op IDs*. Loro `version()` returns a version vector. Yjs state vector = client → clock. Diamond Types = local integers. | One word for tips, one for vectors. |
| **actor / peer / agent / session / replica** | Automerge actor (op author); Loro `PeerID` (op author); automerge-repo `PeerId` (*network* peer); Diamond Types agent; json-joy session; Collabs replica. Automerge `author` ≠ actor. | Separate author from network identity. |
| **seq / counter / time** | Automerge `seq` counts changes and `counter` counts ops. Diamond Types `seq` counts characters. json-joy uses `time`. | |
| **cursor / position / anchor / point** | Automerge and Loro Cursor = ID plus bias. Collabs `Position` = an entry; Collabs Cursor = a gap. json-joy "Cursor" = a selection slice. Yjs RelativePosition. Zed and cola Anchor. | Separate entry ID from gap-with-bias. |
| **mark / style / format / slice** | Automerge mark; Loro style; Collabs format; json-joy slice; Quill attributes. Expand: `before/after/both/none` (Automerge, Loro) vs endpoint anchors (json-joy). | |
| **snapshot / compaction / shallow** | Automerge-repo snapshot = compacted chunk *with* history, while the Automerge glossary's "compaction" *drops* history. Loro snapshot = history + state; shallow = trimmed. Yjs snapshot = a version marker, not data. | Dangerous clash. |
| **branch / fork / checkout / view** | Diamond Types branch = in-memory state. Loro fork = new doc; Loro checkout = detached state. Automerge `view`. Patchwork branch = user-facing alternate version. | |
| **chunk** | Automerge storage chunk; json-joy run of characters; Effect `Chunk` (immutable array). | |
| **schema** | json-joy `s.*`; loro-mirror; Collabs registrations; Effect `Schema`. | Ours should *be* Effect `Schema`. |

## 5. Relevance to us

### Borrow

1. **Keep the op log separate from materialized state** (Loro, Diamond Types). The log fits `eventlog` / `KeyValueStore`; state is derived; relays keep only the log.
2. **Op IDs as a Schema struct `{replica, counter}` with run-length ranges**, and optional author identity kept separate.
3. **Two version shapes:** frontiers (compact; for URLs and checkpoints) and version vectors (for sync), with conversions.
4. **Sync as a pure per-peer state machine** (Automerge's explicit `SyncState` with `generate` / `receive`). Transports become `Socket`/`Rpc` adapters. Start with version-vector diffing; hash heads with Bloom filters are a later integrity upgrade.
5. **Quill-style deltas for text**, tagged with `target`, `path`, `origin`, `by`, and `from`/`to`, emitted through `Stream`/`PubSub` deterministically and synchronously.
6. **Fugue / FugueMax ordering**, the consensus choice; RGA interleaves text typed backwards.
7. **Cursors** as Schema-encodable IDs plus a side/bias, with refresh on resolve. Store them in undo items.
8. **A mark registry with a per-type `expand`**, declared in the Schema. Inline block markers. Consider json-joy-style stacking for comments and links.
9. **Local-only undo, filtered by origin.**
10. **Concurrency-safe storage keys** (`[docId, kind, hash]` plus range scans), and causal-pending results surfaced as typed values.

### Avoid

- Reusing "patch", "snapshot", "transaction", or "chunk", given what Effect already means by them.
- Mutable-proxy APIs.
- Hidden auto-commits and microtask-batched events.
- Concurrent child-container creation hiding data.
- Monolithic designs (Weidner's critique). Keep ordering, ops, and transport separable.
- Copying AGPL code.

### Open questions

1. Tombstone-based Fugue state, Eg-walker replay, or both as strategies?
2. Hash-addressed or counter-addressed versions?
3. A first-class server-ordered mode (json-joy server clock, server reconciliation)?
4. Default text index unit: UTF-16 or graphemes?
5. Rich-text anchors inline or in a separate list? Are blocks in v1?
6. Can one Effect `Schema` compile to container types, the way `s.*` and loro-mirror do?
7. Is `Differ` the right abstraction for app-facing diffs?
8. What is our equivalent of a shallow snapshot, and how does it interact with stale peers?
9. Does `KeyValueStore` support prefix/range scans (unverified)?

## 6. Sources

Repo snapshots, shallow-cloned 2026-10-15 (last commit date in parentheses):
- https://github.com/automerge/automerge (2026-10-14), https://github.com/automerge/automerge-repo (2026-10-08), https://github.com/automerge/automerge.github.io (2026-09-04)
- https://github.com/loro-dev/loro (2026-10-09), https://github.com/loro-dev/loro-docs (2026-10-11)
- https://github.com/josephg/diamond-types (2026-09-02), https://github.com/josephg/eg-walker-reference
- https://github.com/streamich/json-joy (2026-09-08)
- https://github.com/composablesys/collabs (2025-03-25), https://github.com/mweidner037/list-positions (2024-08-26)
- https://github.com/noib3/cola (2026-01-13), https://github.com/inkandswitch/keyhive (2026-10-13)

Docs, posts, and papers:
- https://automerge.org/docs/reference/concepts/, https://automerge.org/docs/reference/under-the-hood/merge-rules/, https://automerge.org/docs/reference/documents/rich-text/, https://automerge.org/docs/reference/repositories/dochandles/
- https://automerge.org/blog/automerge-3/, https://automerge.org/automerge-binary-format-spec/
- https://arxiv.org/abs/2012.00472 (Kleppmann & Howard, Byzantine eventual consistency / sync)
- https://loro.dev/docs (frontiers, version vector, container, OpLog/DocState, Eg-walker, text, cursor, event, sync, encoding, ephemeral, undo)
- https://loro.dev/blog/loro-richtext, https://loro.dev/blog/loro-protocol, https://loro.dev/blog/loro-mirror, https://loro.dev/blog/mergeable-containers
- https://arxiv.org/abs/2409.14252 (Eg-walker), https://arxiv.org/abs/2305.00583 (Fugue), https://josephg.com/blog/crdts-go-brrr/
- https://jsonjoy.com/specs/json-crdt, https://jsonjoy.com/specs/json-crdt-patch
- https://collabs.readthedocs.io/ (documents, updates, algorithms, undo/redo)
- https://mattweidner.com/2025/05/21/text-without-crdts.html, https://mattweidner.com/2024/04/29/list-positions.html
- https://www.inkandswitch.com/peritext/, https://zed.dev/blog/crdts
- https://www.inkandswitch.com/patchwork/notebook/2024-version-control/08/, https://www.inkandswitch.com/keyhive/notebook/, https://nomad.foo/blog/cola
