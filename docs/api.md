# API

Terms are defined in [`concepts.md`](./concepts.md). The signatures here are simplified; each module's JSDoc has the
details. Runnable programs are in [`examples/`](../examples).

## Conventions

- **A replica is a handle you pass to module functions**, like `SubscriptionRef` or `Queue`:
  `ReplicatedText.insert(notes, 0, "Hi")`. Functions with two or more arguments also work in `.pipe`.
- **The strategy is an option** (`strategy: "tree" | "replay"`). Code that edits, observes or syncs a text doesn't change
  with it.
- **What a replica syncs through is a service**, with an in-process layer (for servers and tests) and a remote layer (over
  RPC). The same client code runs against a server or in one process.
- **The sync protocol is an `RpcGroup`**, so it runs over any `RpcClient`/`RpcServer` protocol.
- **Everything that crosses a boundary is a `Schema`:** deltas, version vectors, anchors, patches, overlaps, RPC payloads
  and errors.

## Modules

| Module           | What it's for                                                                                  |
| ---------------- | ---------------------------------------------------------------------------------------------- |
| `ReplicatedText` | A collaborative text replica: create, edit, read, subscribe, anchor.                           |
| `TextPatch`      | How a string changed, in Quill's shape. Editor bindings speak this.                            |
| `Replica`        | What every replica shares: ids, version, delta, receive, exchange, overlaps.                   |
| `Delta`          | What syncs: a set of changes.                                                                  |
| `VersionVector`  | What a replica has seen, per replica id.                                                       |
| `Sync`           | The protocol, the `Authorization` service, connections and their status, and protocol errors.  |
| `Relay`          | A server that passes changes along and stores them.                                            |
| `Authority`      | A server that accepts or rejects each change.                                                  |
| `HistoryStore`   | Where replicas and servers keep history, with memory and `KeyValueStore` layers, and `persist`. |
| `Presence`       | Who's in a document, and their selections.                                                     |

## ReplicatedText

```ts
interface ReplicatedText extends Replica.Replica<string> {}
interface Confirmed extends ReplicatedText { readonly connection: Sync.Connection }

interface Options {
  readonly strategy?: "tree" | "replay" | undefined  // default "tree"
  readonly replica?: Replica.ReplicaId | undefined   // default: a fresh random id
}

// A replica that syncs with anything: peers (Replica.exchange) and relays (Relay.connect).
// It requires Crypto unless you give it a replica id.
declare const make: (options?: Options) => Effect<ReplicatedText, never, Crypto>

// A replica whose changes an authority confirms or rejects. It syncs through that Authority for the life
// of the scope, and with nothing else.
declare const makeConfirmed: (options: {
  readonly document: Replica.DocumentId
  readonly replica?: Replica.ReplicaId | undefined
  readonly retry?: Schedule | undefined
}) => Effect<Confirmed, never, Authority | Crypto | Scope>

// Reading
declare const get: (self: ReplicatedText) => Effect<string>
declare const changes: (self: ReplicatedText) => Stream<string> // current text, then each new one
declare const subscribe: (self: ReplicatedText) => Effect<Subscription, never, Scope>

interface Subscription {
  readonly text: string        // the text at the moment of subscribing
  readonly edits: Stream<Edit> // every edit applied after it, none missed
}

type Edit =
  | { readonly _tag: "Local"; readonly patch: TextPatch }
  | { readonly _tag: "Remote"; readonly patch: TextPatch; readonly overlaps: ReadonlyArray<Replica.Overlap> }
  | { readonly _tag: "Rejected"; readonly patch: TextPatch; readonly message: string } // rolls back my edit

// Editing: each call is one change.
declare const insert: (self: ReplicatedText, index: number, text: string) => Effect<void, ReplicatedTextError>
declare const remove: (self: ReplicatedText, index: number, length: number) => Effect<void, ReplicatedTextError>
declare const applyPatch: (self: ReplicatedText, patch: TextPatch) => Effect<void, ReplicatedTextError>
declare const set: (self: ReplicatedText, text: string) => Effect<void>
declare const update: (self: ReplicatedText, f: (text: string) => string) => Effect<void>
declare const modify: <B>(self: ReplicatedText, f: (text: string) => readonly [B, TextPatch]) => Effect<B, ReplicatedTextError>

// Stable positions
type Side = "before" | "after"
interface AnchorRange { readonly start: Anchor; readonly end: Anchor }
interface Range { readonly start: number; readonly end: number } // [start, end), UTF-16 code units

declare const anchor: (self: ReplicatedText, index: number, side?: Side) => Effect<Anchor, ReplicatedTextError>
declare const locate: (self: ReplicatedText, anchor: Anchor) => Effect<Option<number>> // None: not received here yet, or rolled back
declare const anchorRange: (self: ReplicatedText, range: Range) => Effect<AnchorRange, ReplicatedTextError>
declare const locateRange: (self: ReplicatedText, range: AnchorRange) => Effect<Option<Range>> // also None once all of it is deleted

// Who typed what: consecutive stretches of the current text, each with the replica that inserted it
declare const authorship: (self: ReplicatedText) => Effect<ReadonlyArray<{ readonly range: Range; readonly replica: Replica.ReplicaId }>>
```

`ReplicatedTextError` has a `reason` of `OutOfBounds` or `SplitsSurrogatePair`.

**`make` or `makeConfirmed`.** A `make` replica can sync with several things at once, so connecting is explicit. A
`makeConfirmed` replica has exactly one path, its authority. It connects on creation, and `Replica.exchange` and
`Replica.receive` from anything else fail with `AuthorityOnly`.

**`set` and `update`** are for code that holds whole strings, such as find-and-replace or form fields. They turn the new
string into one replacement: the shortest stretch between the longest common prefix and suffix, without splitting an emoji.
This decides how concurrent edits merge. Changing "24 hours" to "8 hours" on one replica and to "12 hours" on another gives
"812 hours". Editors should use `insert`, `remove` and `applyPatch`, which say exactly what the user did.

**`modify`** applies a patch worked out from the current text, with no edit or merge landing in between.

**Editor bindings** call `subscribe`, load `text` into the editor, then apply `Remote` and `Rejected` patches and skip
`Local` ones. If the editor has keystrokes it hasn't handed to the replica yet, move the remote patch past them with
`TextPatch.transform`.

**`Crypto`** comes from platform layers such as `NodeServices.layer`. Tests can use `TestCrypto.layer(seed)` from
`effect/testing`.

## TextPatch

```ts
type TextPatchOperation =
  | { readonly _tag: "Retain"; readonly length: number }
  | { readonly _tag: "Insert"; readonly text: string }
  | { readonly _tag: "Remove"; readonly length: number }
type TextPatch = ReadonlyArray<TextPatchOperation>

declare const TextPatch: Schema.Codec<TextPatch, ReadonlyArray<QuillOperation>> // encodes to { retain } | { insert } | { delete }

declare const empty: TextPatch
declare const insert: (index: number, text: string) => TextPatch
declare const remove: (index: number, length: number) => TextPatch
declare const get: (before: string, after: string) => TextPatch                 // one replacement, as ReplicatedText.set
declare const apply: (patch: TextPatch, before: string) => string
declare const compose: (first: TextPatch, second: TextPatch) => TextPatch       // apply first, then second
declare const transform: (patch: TextPatch, over: TextPatch, side: Side) => TextPatch // for editor bindings
declare const mapIndex: (patch: TextPatch, index: number, side: Side) => number // move a cursor past a patch
declare const Differ: Differ<string, TextPatch>
```

`compose` is sequential. To merge concurrent changes, use `Delta.combine`.

## Replica

```ts
interface Replica<out A> extends Pipeable {
  readonly strategy: "tree" | "replay"
}

declare const ReplicaId: Schema.brand<Schema.String, "ReplicaId"> // never empty
declare const DocumentId: Schema.brand<Schema.String, "DocumentId">

declare const id: (self: Replica<unknown>) => Effect<ReplicaId> // the id new changes are written under
declare const version: (self: Replica<unknown>) => Effect<VersionVector>
declare const delta: (self: Replica<unknown>, since: VersionVector) => Effect<Delta>
declare const receive: (self: Replica<unknown>, delta: Delta) => Effect<ReadonlyArray<Overlap>, DeltaError>
declare const exchange: (self: Replica<unknown>, that: Replica<unknown>) => Effect<ReadonlyArray<Overlap>, DeltaError> // self's overlaps

interface Overlap {
  readonly _tag: "Text"
  readonly range: ReplicatedText.Range         // where it is in the merged text, now
  readonly anchors: ReplicatedText.AnchorRange // follows it from here on
  readonly replicas: ReadonlyArray<ReplicaId>
}
```

`receive` is idempotent and order-independent. Changes that arrive before what they build on wait inside the replica.

`DeltaError` has a `reason`:

- `StrategyMismatch`: the delta came from the other strategy.
- `Malformed`: the delta isn't valid.
- `ReplicaIdReused`: the replica received a change under its own id that it didn't make. It's caught only once one copy has
  written more under that id than the other.
- `AuthorityOnly`: a confirmed replica was asked to exchange with something other than its authority.

`id` is an Effect because a confirmed replica switches to a new id after a rejection.

## Delta and VersionVector

```ts
declare const Delta: Schema.Codec<Delta, Encoded>
declare const empty: Delta
declare const combine: (self: Delta, that: Delta) => Delta
declare const ReducerUnion: Reducer<Delta>
declare const isEmpty: (self: Delta) => boolean
declare const version: (self: Delta) => VersionVector

type VersionVector = { readonly [replica: string]: number }
declare const VersionVector: Schema.Codec<VersionVector>
declare const empty: VersionVector
declare const get: (self: VersionVector, replica: string) => number
declare const combine: (self: VersionVector, that: VersionVector) => VersionVector // pointwise max
declare const ReducerMax: Reducer<VersionVector>
declare const compare: (self: VersionVector, that: VersionVector) => "before" | "after" | "equal" | "concurrent"
```

`Delta.combine` is commutative, associative and idempotent. If two deltas disagree about the same change (a reused or forged
replica id), it keeps one deterministically and `Replica.receive` reports `ReplicaIdReused`.

## Sync

```ts
// The protocol, shared by relays and authorities. Mount it in an RpcServer.
declare const Rpcs: RpcGroup<Subscribe | Push>

// Who may connect and what they may do. Servers check it on every request.
class Authorization extends Context.Service<Authorization, {
  readonly identify: (headers: Headers) => Effect<string, Forbidden> // who is asking, as a stable user id
  readonly authorize: (request: {
    readonly user: string
    readonly document: DocumentId
    readonly access: "read" | "write"
  }) => Effect<void, Forbidden>
}>()("effect-delta/Sync/Authorization") {}
declare const layerAuthorizationAllowAll: Layer<Authorization> // development and tests only

// Who the current changes are for. RPC handlers set it from `identify`; in-process code can provide it.
declare const CurrentUser: Context.Reference<string>

interface Connection {
  readonly status: Stream<Status>          // Connecting | Syncing | Synced | Offline { error }
  readonly unconfirmed: Stream<number>     // my changes the other side hasn't acknowledged: now, then each change
  readonly sync: Effect<void, SyncError>   // waits until I hold everything the server has now
  readonly await: Effect<never, SyncError> // fails if the connection gives up
}
```

`SyncError` has a `reason` of `Forbidden`, `ProtocolViolation`, `Disconnected`, `Unavailable` or a `DeltaError`.

A connection subscribes with what the replica has seen and applies the deltas it gets back, then pushes the replica's own
changes the server lacks, and each new one. On `Disconnected` or `Unavailable` it retries with `retry` and resubscribes from
its current version, so nothing is lost.

For peer-to-peer, one peer runs a relay (with `HistoryStore.layerMemory`) and serves it with `Relay.layerHandlers`, and the
other uses `Relay.layerRemote`.

## Relay and Authority

Each is one service with three layers:

| Layer           | What it is                                                                                                  |
| --------------- | ----------------------------------------------------------------------------------------------------------- |
| `layer`         | The real thing, in this process: on a server, or given straight to replicas in tests and single-process apps. |
| `layerRemote`   | The same service on a client, reaching a server over RPC.                                                   |
| `layerHandlers` | Serves the in-process service over RPC, checking each request with `Sync.Authorization`.                    |

```ts
// Relay
declare const layer: Layer<Relay, never, HistoryStore>
declare const layerRemote: Layer<Relay, never, RpcClient.Protocol>
declare const layerHandlers: Layer<Rpc.ToHandler<Sync.Rpcs>, never, Relay | Sync.Authorization>

// Syncs a replica through the Relay for the life of the scope.
declare const connect: (
  replica: Replica<unknown>,
  options: { readonly document: DocumentId; readonly retry?: Schedule | undefined }
) => Effect<Sync.Connection, never, Relay | Scope>

// Authority: the same shape, with a check.
declare const layer: <R>(options: {
  readonly check: (proposal: Proposal) => Effect<void, Rejected, R>
}) => Layer<Authority, never, HistoryStore | R>
declare const layerRemote: Layer<Authority, never, RpcClient.Protocol>
declare const layerHandlers: Layer<Rpc.ToHandler<Sync.Rpcs>, never, Authority | Sync.Authorization>

interface Proposal {
  readonly document: DocumentId
  readonly user: string     // Sync.CurrentUser
  readonly patch: TextPatch // what the change does to the current text
  readonly before: string
  readonly after: string
}
```

**Who may write what.** `identify` decides who each request is from, from its headers, so cookies and tokens work. `authorize`
decides read and write access per document. A replica id belongs to the first user who writes with it, and a push with
changes under an id owned by someone else is `Forbidden`.

**Rejection.** An authority checks a push's changes in order and accepts the longest acceptable prefix. The confirmed replica
then drops the rejected change and every unconfirmed change after it, emits a `Rejected` edit with the patch that rolls the
text back, and switches to a new replica id.

**Servers save before they share.** A pushed change is checked, stored, then sent to subscribers. If storing fails, the push
fails with `Unavailable`, every subscription to that document ends with it, and the document is loaded again from the store.
A change that builds on something the server doesn't have yet waits on a relay. An authority can't check it, so it refuses
the push as `Malformed`.

A server takes each document's strategy from what it has stored, or else from the first replica to subscribe or push, and
refuses the other strategy.

## HistoryStore

```ts
class HistoryStore extends Context.Service<HistoryStore, {
  readonly load: (document: DocumentId) => Effect<Option<Stored>, HistoryStoreError>
  readonly append: (document: DocumentId, delta: Delta) => Effect<void, HistoryStoreError>
  readonly setUnconfirmed: (document: DocumentId, delta: Delta) => Effect<void, HistoryStoreError>
  readonly setSnapshot: (document: DocumentId, snapshot: Snapshot) => Effect<void, HistoryStoreError>
  readonly addWriter: (document: DocumentId, replica: ReplicaId, owner: string) => Effect<void, HistoryStoreError>
}>()("effect-delta/HistoryStore") {}

interface Stored {
  readonly history: Delta
  readonly unconfirmed: Delta
  readonly writers: { readonly [replica: string]: string } // replica id → its owner (servers) or "" (replicas)
  readonly snapshot: Option<Snapshot>
}

interface Snapshot {
  readonly version: VersionVector // the changes it's the text of
  readonly text: string
}

declare const layerMemory: Layer<HistoryStore>
declare const layerKeyValueStore: (options: {
  readonly prefix: string
  readonly foldAfter?: number | undefined // default 64
}) => Layer<HistoryStore, never, KeyValueStore>

// Loads saved history into `replica`, then saves every change for the life of the scope.
declare const persist: (replica: Replica<unknown>, document: DocumentId) => Effect<Persisted, DeltaError | HistoryStoreError, HistoryStore | Scope>
interface Persisted { readonly await: Effect<never, HistoryStoreError> }
```

`persist` saves once more when its scope closes, so the last edits aren't lost. For `replay`, it also writes a snapshot then,
and after every 4,096 characters of history. Loading takes the text from the snapshot and merges only the changes saved after
it. A confirmed replica's unconfirmed changes are stored separately and rewritten after a rejection, so a rejected change
never comes back on reload.

`layerKeyValueStore` puts everything under your `prefix`. Each document can have one writer per store, so two tabs can't
persist the same document to the same store.

## Presence

```ts
interface State { readonly selection: Option<AnchorRange>; readonly data: Schema.Json }
interface Peer { readonly session: number; readonly user: string; readonly state: State }

declare const Rpcs: RpcGroup<Join | Set>
declare const layer: Layer<Presence>
declare const layerRemote: Layer<Presence, never, RpcClient.Protocol>
declare const layerHandlers: Layer<Rpc.ToHandler<Presence.Rpcs>, never, Presence | Sync.Authorization>

// Joins a document for the life of the scope.
declare const join: (options: {
  readonly document: DocumentId
  readonly retry?: Schedule | undefined
}) => Effect<Joined, never, Presence | Scope>

interface Joined {
  readonly peers: Stream<ReadonlyArray<Peer>>
  readonly set: (state: State) => Effect<void>
}
```

Presence is never stored or merged. It lives on the server while a client stays connected. Selections are anchors, so a peer
finds one in its own text with `ReplicatedText.locateRange`, which is `None` until it has received those characters.

## Encoding

Wire and storage formats are JSON (`RpcSerialization.layerNdjson` or `layerJson`). Every RPC payload and stored record carries
a format version, and nothing encoded contains the library's name.

## Limits

- **Replay merges cost time in proportion to the text.**
- **Replay anchors are slow on long histories.** `anchor`, `locate` and overlaps made only of removed text replay the whole
  history, once per change to the text, and the result is cached until the next one.
- **The tree is one node per character,** and typing builds deep chains, so tree edits cost time in proportion to their depth.

## Not built

- Schema-derived documents with several fields
- A symmetric peer-to-peer mesh
- Undo and formatting marks
- Pruning history
- Binary encoding
- WebRTC and BroadcastChannel transports
- Sharing one replica id across tabs
- End-to-end encryption
- Confirmed replicas for the `replay` strategy
