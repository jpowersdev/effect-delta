# Vocabulary

Phase 1 output: one name per concept, what it means, what other systems call it, and which Effect concept it
builds on. Research behind it lives in [`research/`](./research).

**Status: decided** (2026-10-15). The six naming decisions below were accepted as recommended.

## How we chose names

1. **Effect's existing meaning wins.** If Effect already uses a word, we use it the same way or not at all.
   That's what makes upstreaming a rename instead of a redesign.
2. **One word per concept, one concept per word.** The ecosystem uses "patch", "delta", "update", "change"
   and "snapshot" for different things in every library. We pin each one down.
3. **Plain words over academic ones** when they're equally precise: "change" rather than "event", "anchor"
   rather than "relative position". The academic term is listed so readers can find the papers.
4. **Consumers upgrade with a find-and-replace.** If this lands in Effect, an app changes
   `effect-delta` to `effect/delta` in its imports and nothing else. So every exported name, type and
   error tag must already be the one core would use. **The library name never appears in anything
   encoded**: not in wire messages, not in stored documents, not in tags. That way data written before the
   switch still loads after it, and replicas on either side of the upgrade can still sync.

## The model in one paragraph

A **document** is shared by many **replicas**, one per device or session. Each has a **replica id**. When
an app edits a replica's text, the edit is recorded as a **change**: one or more **ops** (insert or delete)
with an id, plus the **parents** it was made on top of. All the changes a replica knows form its
**history**, a graph whose newest changes are its **frontier**. A **version vector** summarizes what it has
seen from each replica. To sync, two replicas exchange version vectors, and each sends a **delta**: the
changes the other lacks. Receiving a delta **combines** it into the history. Combining is order-independent
and safe to repeat, so every replica that has received the same changes shows the same text. Where
concurrent changes inserted at the same place, the **strategy** decides their order. Apps observe each
change to the text as a **patch**. They keep stable positions as **anchors**, and are told about
**overlaps**: places where two replicas edited the same stretch of text without seeing each other's edit.

## Core terms

### Documents and replicas

| Term                                     | Meaning                                                                                                                                                                                                   | Elsewhere                                                                                                                         | Builds on                                                                                  |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| **Document**                             | The shared thing being edited, identified by a **document id**. Its shape is a `Schema`: one text, or fields ([`documents.md`](./documents.md)).                                                                                             | Yjs `Y.Doc`, Automerge doc, Loro `LoroDoc`; Liveblocks/Hocuspocus "room"                                                          | `Schema`                                                                                   |
| **Replica**                              | One copy of a document in one place (a tab, a device, a server) that accepts edits immediately and syncs later.                                                                                           | Yjs Doc instance; Automerge/Loro doc instance; docustore "copy"                                                                   | `SubscriptionRef` holds its state; `Stream` emits values                                   |
| **Replica id**                           | Identifies the stream of changes one replica writes. Unique per replica, never reused. A replica is not the same as a user or a network connection.                                                       | Yjs clientID, Automerge actor, Loro PeerID, Eg-walker/Diamond Types agent, json-joy session id, "site"                            | `Crypto` / `Random` to generate; `Schema.brand` to type                                    |
| **Author** (later)                       | The person or agent a change was made for. Optional, separate from the replica id.                                                                                                                        | Automerge 3.5 `author`; Yjs `PermanentUserData`                                                                                   | —                                                                                          |
| **Peer**                                 | The other side of a sync connection, from one replica's point of view. Network identity, not authorship.                                                                                                  | automerge-repo `PeerId` (the network sense). **Not** Loro's PeerID, which is an author                                            | —                                                                                          |

### Changes and history

| Term                         | Meaning                                                                                                                                                                                                                                               | Elsewhere                                                                                                                                    | Builds on                                     |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **Op** | One primitive edit: **Insert** (text at a place) or **Remove** (a stretch), "remove" as in Effect's collections. Later also **Mark**. How a place is named (index or character id) depends on the strategy. | Yjs Item/struct, Automerge op, json-joy op, Eg-walker op; Quill/ShareDB "delete" | `Schema.TaggedUnion` |
| **Change**                   | The ops one replica committed together, plus its **change id** and its **parents**. The unit that is stored, sent and never modified. Each inserted or deleted character consumes one sequence number, so a change covers a range of ids.              | Automerge change, Loro change, Eg-walker/Diamond Types "event" (run-length), Yjs "update" (roughly)                                         | `Schema.Class`; `Equal`/`Hash`                |
| **Change id**                | `(replica id, seq)`. The id of a change's first character. Character `k` of the change is `(replica id, seq + k)`.                                                                                                                                    | Automerge `counter@actor`, Loro `{peer, counter}`, Yjs `ID(client, clock)`, Eg-walker `(agent, seq)`, the academic **dot**                   | `Schema.Struct`, `Order`                      |
| **Seq**                      | A replica's own counter. Strictly increasing, one per character it inserts or deletes.                                                                                                                                                                | Yjs clock, Automerge counter, Diamond Types seq, json-joy time                                                                               | —                                             |
| **Parents**                  | The frontier a change was made on top of: the changes the replica had seen when it made this one.                                                                                                                                                     | Automerge `deps`, Eg-walker `parents`                                                                                                        | —                                             |
| **History**                  | Every change a replica knows, as a graph (each change points at its parents). Append-only. The current text is derived from it.                                                                                                                       | Eg-walker event graph, Loro OpLog, Diamond Types OpLog, Automerge change graph, Yjs StructStore (roughly)                                    | Plain data; `Graph` for tooling, maybe        |
| **Frontier**                 | The changes in a history that nothing else builds on yet (the tips). Names a version exactly and compactly, but can only be read with the history at hand.                                                                                            | Loro frontiers, Automerge heads (hashes), Eg-walker frontier, git "heads"                                                                    | —                                             |
| **Version vector**           | For each replica id, how many of its seqs a replica has seen. Answers "what am I missing?" without the history. Compares as before, after, equal or concurrent.                                                                                        | Yjs state vector, Loro `version()`, vector clock; docustore "version"                                                                        | `Record.makeReducerUnion(Combiner.max)`       |
| **Concurrent**               | Two changes are concurrent when neither's replica had seen the other when it was made.                                                                                                                                                                | Lamport "∥", "happened-before" (→) for the opposite                                                                                          | —                                             |

### Syncing

| Term                 | Meaning                                                                                                                                                                                                                                    | Elsewhere                                                                                                                         | Builds on                                                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **Delta**            | A set of changes sent from one replica to another, usually "everything you lack since your version vector". Deltas **combine**: commutative, associative and idempotent. A whole history is just the delta since the empty version.         | Yjs update, Loro update/export, Automerge sync message (roughly), delta-state CRDTs (Almeida et al.); docustore `Delta`           | `Combiner` / `Reducer` (the join); `Schema`, encoded as JSON for now                  |
| **Combine** | Join two deltas or histories into one: the CRDT merge, commutative and idempotent. (`TextPatch.compose` is different: sequential, like `Differ.combine`.) | "merge", "join", `applyUpdate`, `import` | `Combiner.combine`, `Reducer.combineAll` |
| **Receive**          | A replica taking in a delta: combine it into the history, update the text, and report what changed (patches and overlaps).                                                                                                                 | Yjs `applyUpdate`, Loro `import`, Automerge `applyChanges`/`receiveSyncMessage`                                                   | `SubscriptionRef.modify`                                                                  |
| **Pending**          | Changes received before their parents. They're held until the parents arrive, so transports needn't preserve order.                                                                                                                        | Yjs pendingStructs, Loro `import().pending`, causal buffer                                                                        | —                                                                                         |
| **Unconfirmed** | My own changes that the server I sync with hasn't acknowledged yet. Under an authority they may still be rejected. Not the same as pending. | y-websocket `unconfirmedUpdates`, Replicache pending mutations, prosemirror-collab unconfirmed steps | — |
| **Sync protocol**    | The messages two replicas exchange about one document. Both sides say what they have (a version vector), each sends the delta the other lacks, then they stream new deltas as they happen, optionally acknowledged. As an `RpcGroup`: `Subscribe` (what I have → a stream of what I lack, then live deltas) and `Push` (my changes → acknowledged, or rejected by an authority). The same protocol serves peers, relays and authorities. Many documents share one connection. | y-protocols SyncStep1/SyncStep2/Update; Automerge sync; Loro Protocol (rooms); Hocuspocus (doc-name prefix)                       | `RpcGroup` with `Schema` payloads, as eventlog does                      |
| **Peer state**       | What one replica remembers about a peer: the version vector it last confirmed and what's in flight. Persisting it makes reconnecting cheap.                                                                                                 | Automerge `SyncState`, eventlog per-`RemoteId` cursor, BEC `StoreHeads`                                                           | `KeyValueStore`                                                                           |
| **Transport** | What carries the sync protocol between two replicas. The protocol is an `RpcGroup`, so any Rpc protocol works: WebSocket, HTTP, a socket, a worker, or in-memory for tests. | Yjs provider (network half), automerge-repo network adapter | `RpcClient`/`RpcServer` protocols over `Socket`; provided as `Layer`s |
| **History store** | Where a replica or server keeps history (and snapshots) between sessions. The service is `HistoryStore`, named like `KeyValueStore` and `RateLimiterStore`. | Yjs provider (persistence half), automerge-repo storage adapter | `KeyValueStore`; provided as `Layer`s |
| **Relay**            | A server that passes deltas between replicas and usually stores them, but never rejects or reorders anything. Just another peer that persists. The default server role.                                                                    | y-websocket server, y-sweet, Hocuspocus (mostly), Automerge Repo sync server, "cloud peer" (local-first essay)                    | the `Relay` service (`layer`, `layerRemote`, `layerHandlers`) |
| **Envelope** (later) | The fields of a delta kept readable when its content is end-to-end encrypted: document id, replica id, seq range, parents. Enough for a relay that can't read the text to still answer "everything since version V".                      | Secsync, Evolu, Jazz session counters                                                                                            | `Schema`                                                                                  |
| **Authority**        | A server whose order is final: it can validate, reorder or reject changes. Clients' unconfirmed changes are **pending confirmation** until it acknowledges them.                                                                             | Google Docs/ShareDB server, prosemirror-collab authority, Replicache server; docustore `Replica.consistent`                        | the `Authority` service; replicas made with `ReplicatedText.makeConfirmed` require it |
| **Snapshot**         | The document state at one version, encoded so a replica can load it without replaying all of history. Folding stored deltas into a snapshot loses nothing. For now every replica keeps all history (see `strategies.md`). | Loro snapshot/shallow snapshot, Automerge saved doc, Automerge Repo "compaction" (lossless). **Not** Yjs snapshot (a version marker) | `Schema`; `KeyValueStore`                                                                 |
| **Prune**            | Drop history older than a version once no concurrent change can still arrive behind it. Lossy: a replica that was offline since before the cut can no longer sync by delta.                                                                 | GC, causal-stability purging, Loro shallow snapshot, eventlog `groupCompaction` (lossy, by key)                                  | —                                                                                         |
| **Presence** (later) | Short-lived per-replica state that isn't part of the document: who's here, where their cursor is. Expires without heartbeats.                                                                                                             | Yjs awareness, Loro EphemeralStore, Automerge Repo Presence, Liveblocks presence                                                  | `Schema`; `Schedule` for heartbeats                                                       |

### Text and positions

| Term                    | Meaning                                                                                                                                                                                                                                       | Elsewhere                                                                                                     | Builds on                                         |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| **Text** | A collaborative string: the first replicated type. The module is `ReplicatedText` (named like `PersistedQueue`; plain `Text` would shadow the DOM global). | Y.Text, LoroText, Automerge string (3.x), Collabs CText | — |
| **Index**               | A position in the current text as a number, counted in the **text unit**.                                                                                                                                                                     | offset, pos                                                                                                   | —                                                 |
| **Text unit**           | What one index step counts: UTF-16 code units, as in JavaScript strings (decided in `strategies.md`).                                                                                                                                | Yjs/Automerge-JS/Loro-JS: UTF-16; text-unicode: code points                                                   | —                                                 |
| **Patch** | How a text changed, as a sequence of retain, insert and remove, in the Quill shape. Apply it to the old text to get the new one. What apps observe and what editors speak. Sequential, not mergeable. The module is `TextPatch`, named like `JsonPatch`. | Yjs/Quill/Loro **"delta"**, Automerge splice patches, OT ops (ot.js, ShareDB text) | **`Differ<string, TextPatch>`**: same meaning as Effect's `Differ` and `JsonPatch` |
| **Anchor**              | A position that follows the text through edits and merges. It's attached to one character's id and sticks to one **side** of it. A pair of anchors makes a stable range (a comment, a selection).                                            | Yjs RelativePosition (+ `assoc`), Yrs StickyIndex, Automerge/Loro Cursor, Zed/cola Anchor; docustore Anchor (a range) | `Schema`                                          |
| **Side**                | Which way an anchor leans: `before` or `after` its character. This decides where text typed exactly at the anchor goes.                                                                                                                       | Yjs `assoc`, Loro `Side`, Automerge `move`, "gravity", "bias"                                                  | `Schema.Literals`                                 |
| **Overlap**             | A stretch of merged text where changes from two sides that hadn't seen each other touch. Every replica converges anyway; an overlap says where a person might want to look ("24 hours" → "812 hours").                                       | Nothing equivalent for text that we know of; Automerge `getConflicts` for map keys; docustore `Overlap`       | `Schema.TaggedUnion`                              |
| **Mark** (later)        | Formatting attached to a range by anchors, with an **expand** rule saying whether text typed at its edges joins it.                                                                                                                           | Peritext mark, Automerge mark, Loro style, json-joy slice, Quill attributes, Yjs format item                   | `Schema` registry of mark types                   |

### Ordering internals

These mostly stay inside `internal/`, but the docs and tests use them.

| Term                        | Meaning                                                                                                                                                                                      | Elsewhere                                                       |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| **Strategy**                | The algorithm that orders concurrent inserts and decides what's stored. Candidates are below.                                                                                                | —                                                               |
| **Left / right origin**     | The ids of the characters to the left and right of an insert when it was made. Fixed forever. Only ever in this algorithmic sense, never "who caused this change" (see Words we avoid).      | Fugue, YATA, Yjs `origin`/`rightOrigin`                         |
| **Tombstone**               | A deleted character kept because others may still position things relative to it.                                                                                                           | Yjs deleted item, ⊥                                             |
| **Run**                     | Consecutive characters one replica inserted together, stored as one entry. The main way to keep size per character small.                                                                   | Yjs Item, Yrs Block, json-joy chunk, Diamond Types span         |
| **Interleaving**            | Two passages typed concurrently at the same spot coming out mixed together character by character. A correctness bug we design out.                                                         | Kleppmann et al. 2019                                           |
| **Critical version**        | A version that every change is either before or after, with nothing concurrent across it. Eg-walker can drop its merge state there; it's also where pruning is safe.                         | Eg-walker                                                       |

### Strategies

Decided in phase 2; details in [`strategies.md`](./strategies.md). Plain names, with the published
algorithm named in the docs.

| Name          | One line                                                                                                                                                      | Algorithm                                       |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| **tree**      | Every character has a permanent id and a place in a tree next to the character it was typed beside. Proven maximally non-interleaving. The default.           | FugueMax (Weidner & Kleppmann 2025)             |
| **replay**    | Store the original index-based ops and their parents; keep only plain text in memory; on merge, replay just the concurrent part through a temporary tree.     | Eg-walker (Gentle & Kleppmann 2025)             |
| **authority** | Tree changes, plus a server that accepts or refuses each one. Clients' own changes are pending until confirmed.                                              | Server reconciliation (Replicache, prosemirror-collab, Weidner 2025) |
| **relay**     | Not a merge strategy: the other server role. Passes changes along and stores them, never refuses.                                                            | —                                               |

## How this lines up with Effect

| Our concept                       | Effect concept                                                                       | Relationship                                                                                       |
| --------------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| Combining deltas                  | `Combiner` / `Reducer`                                                               | Our merge **is** a `Combiner` that is also commutative and idempotent. We add law tests.            |
| Patch                             | `Differ`, `JsonPatch`                                                                | Same meaning: sequential, apply-to-old-value. `Text` provides a `Differ<string, Patch>`.           |
| Replica state                     | `SubscriptionRef`                                                                    | Holds it; `changes` is a `Stream` of values, as `SubscriptionRef.changes` is.                       |
| Changes, deltas, version vectors  | `Schema`                                                                             | Every wire and storage type is a Schema. JSON for now; a binary format later, with its own version.               |
| Documents with text fields        | `Schema` + `SchemaAST` annotations                                                   | Mark fields as text with an annotation; derive merge rules (a `toCombiner`-style deriver).          |
| Transport | `RpcGroup`, `RpcClient`, `RpcServer`, `Layer` | The sync protocol is an `RpcGroup`; servers are layers mounted in `RpcServer`. |
| History store | `KeyValueStore`, `Layer` | Default persistence. |
| Logical time, ids                 | `Clock`, `Random`, `Crypto`                                                          | Physical time only from `Clock`; ids from `Crypto`/`Random`, so `TestClock` and seeds make tests deterministic. |
| Convergence tests                 | `Arbitrary`, `@effect/vitest` `it.prop` / `it.effect.prop`                           | Generated concurrent histories, delivered in random orders, must converge.                         |
| Sync machinery                    | `eventlog`                                                                           | Borrow its pieces: Schema messages over `RpcGroup`, chunked frames, retry `Schedule`s, the optional Ed25519 handshake, read/write authorization hooks. Not its model: it's client → server, server-ordered, and resolves by key. |
| Transport interface               | LiveStore's sync backend (built on Effect)                                           | Prior art for the shape: pull as a `Stream`, push with typed errors, `isConnected` as a `SubscriptionRef`. |

## Words we avoid

| Word                                   | Why                                                                                                                         | What we say instead                                                           |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| "delta" for a text diff                | It's our sync unit. Yjs/Quill/Loro mean a text diff, which we call a patch.                                                  | **Patch**                                                                     |
| "patch" for the sync unit              | Effect's `Differ`/`JsonPatch` mean a sequential diff; json-joy/Automerge use it for other things.                           | **Delta** (sync), **Change** (stored)                                         |
| "event"                                | Effect `eventlog` owns `Event`, and Effect uses "event" loosely elsewhere. Eg-walker's "event" is our change.                | **Change**                                                                    |
| "transaction", "journal", "commit"     | Effect STM owns `Transaction` and `Journal`; SQL owns transactions. Yjs/Loro "transactions" don't roll back, which misleads. | "the ops committed together" form a **change**; local batching is an API question |
| "merge" (as a function name)           | Effect uses `merge` for concurrency (`Stream.merge`, `Layer.merge`).                                                        | **combine** (algebra), **receive** (replica)                                  |
| "sync" (as a function name)            | `Effect.sync` lifts a thunk. A `Replica.sync` reads wrong in Effect.                                                        | API phase; e.g. `connect`, `exchange`. "Sync protocol" as a noun is fine.     |
| "replicate"                            | Effect's `replicate` means "repeat n times".                                                                                | "copy", "send"                                                                |
| "version" alone                        | `effect/Version` is the package version; `TxRef.version` is STM.                                                            | **Version vector**, **frontier**                                              |
| "clock" for logical time               | Effect's `Clock` is wall/monotonic time.                                                                                    | **Seq**; if we need logical timestamps later, `Lamport` or a named stamp     |
| "conflict"                             | CRDT merges don't fail, and `eventlog` uses "conflicts" for something else (same-key entries handed to app code).           | **Overlap** (report), **concurrent** (relation)                               |
| "compaction", "compact"                | `Chunk.compact` / `HashMap.compact` drop `None`s; `eventlog` has its own compaction.                                        | **Prune**                                                                     |
| "chunk"                                | Effect's `Chunk` is an immutable array.                                                                                     | **Run**                                                                       |
| "origin" for provenance                | We use it for the algorithm's left/right origin. Yjs reuses it for "who caused this transaction".                           | API phase; e.g. `source: "local" \| "remote"`                                 |
| "session", "client", "actor", "site"   | Each means something different per system (author, connection, tab, key). "Session" alone has four meanings.                | **Replica** / **replica id** (author), **peer** (connection)                  |
| "room"                                 | A server-side grouping (fan-out, presence, auth), usually one per document.                                                  | **Document** (and document id for multiplexing)                               |
| "store" for a document | `eventlog`'s `StoreId` is a document-like unit. (Effect does name storage services `XStore`, and so do we: `HistoryStore`.) | **Document** |
| "provider"                             | Clashes with Effect's `provide` / `Layer` vocabulary.                                                                        | **Transport**, **storage**, provided as layers                                |
| "awareness"                            | Fine, but "presence" is more common and plainer.                                                                            | **Presence**                                                                  |
| "cursor" for a stable position         | A cursor is UI. `Graph` also uses "cursor" for traversal.                                                                   | **Anchor** (+ **side**)                                                       |
| "`*Ref`" names                         | In Effect, a `*Ref` is a mutable cell.                                                                                      | Only if ours really is one                                                    |

## Translation table

| Ours            | Yjs                       | Automerge              | Loro                  | Eg-walker / Diamond Types |
| --------------- | ------------------------- | ---------------------- | --------------------- | ------------------------- |
| Replica         | Y.Doc instance            | doc instance           | LoroDoc instance      | branch + oplog            |
| Replica id      | clientID                  | actor                  | PeerID                | agent                     |
| Change          | (item runs in an update)  | change                 | change                | event (run)               |
| Change id       | ID (client, clock)        | counter@actor          | ID {peer, counter}    | (agent, seq)              |
| History         | struct store              | change graph           | OpLog                 | event graph / oplog       |
| Frontier        | —                         | heads (hashes)         | frontiers             | frontier                  |
| Version vector  | state vector              | —                      | version vector        | —                         |
| Delta           | update                    | sync message / changes | update                | patch bytes               |
| Patch           | delta                     | patch (splice)         | delta (text diff)     | xf ops                    |
| Anchor + side   | RelativePosition + assoc  | cursor + move          | Cursor + Side         | —                         |
| Presence        | awareness                 | presence               | ephemeral store       | —                         |
| Tombstone       | deleted item              | deleted op             | (none in state)       | (none in state)           |

## Decisions

Accepted 2026-10-15, as recommended.

1. **Library and subpath name.** `effect-delta` → `effect/delta`. *Recommend keeping it.* "Delta" names the
   central thing (the mergeable unit of change), and the subpath is free. Alternatives: `effect/crdt`
   (precise but jargon) or `effect/collab` (vague).
2. **"Change" for the stored, replicated unit** (rather than "op", "event" or "update"). *Recommend
   "change"*, with "op" for the inserts and deletes inside it.
3. **Which of two "sets of changes" gets called a delta.** Both fit the word. One is what replicas send
   each other ("Alice's changes 5–9 and Bob's change 3"): it can be combined with other deltas in any
   order. The other is what an editor applies to show new text ("keep 6 characters, insert `big `, keep the
   rest"): it only makes sense applied in order to one particular old text. Yjs, Quill and Loro call the
   second one a delta. *Recommend* calling the first a **delta** and the second a **patch**, because Effect
   already calls apply-in-order diffs patches (`Differ`, `JsonPatch`). The cost is a translation note for
   people coming from Yjs, whose `toDelta()` returns what we'd call a patch.
4. **"Anchor" and "side"** rather than "cursor"/"relative position" and "assoc"/"bias". *Recommend.*
5. **"Overlap"** as the name of the merge report, not "conflict". *Recommend.*
6. **"Presence"** rather than "awareness". *Recommend*, and it's deferred anyway.

## Carried into phase 2

Resolved in [`strategies.md`](./strategies.md) (text unit, replica ids, history, authority, runs, undo,
formatting). Still open for the API phase: multi-tab replica ids, and how we relate to `eventlog` (a
sibling protocol that reuses its pieces, or deltas carried as eventlog entries).
