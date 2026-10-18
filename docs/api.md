# API

Phase 3 output: the public API, starting from how it reads in use. Terms are from
[`vocabulary.md`](./vocabulary.md); strategies from [`strategies.md`](./strategies.md). Signatures are
sketches in Effect 4 style. The build phase will refine them, but anything that changes what a user writes
comes back here first.

**Status: decided** (2026-10-15), after a review against the Effect 4 source and two rounds of feedback:
- the text module is `ReplicatedText`
- an authority is a service in the requirements channel, with its own constructor (`makeConfirmed`)

Building the network half changed a few details. They're listed under
[Changed while building](#changed-while-building), and the sketches below show the result.

## Principles

1. **Ref-style handles, module functions.** A replica is an opaque value you pass to functions, like
   `SubscriptionRef` or `Queue`: `ReplicatedText.insert(notes, 0, "Hi")`. Functions with two or more
   arguments are `dual`, so they also work in `.pipe`.
2. **Named like Effect's own variants.** Effect puts what's special first (`SubscriptionRef`,
   `PersistedQueue`), so a text that's replicated is `ReplicatedText`. It also avoids shadowing the DOM's
   global `Text`.
3. **One API across strategies.** The strategy is an option (`strategy: "tree" | "replay"`), as
   `RateLimiter` takes `algorithm: "fixed-window" | "token-bucket"`. Code that edits, observes or syncs a
   text doesn't change with it.
4. **Capabilities are services.** What a replica syncs through is a service in the requirements channel,
   with an in-process layer (for servers and tests) and a remote layer (over RPC).
   - A replica that answers to an authority requires `Authority`.
   - Connecting a replica to a relay requires `Relay`.
   - Persisting requires `HistoryStore`.
5. **Effect's existing pieces do the plumbing.**
   - **Sync protocol:** an `RpcGroup`, so it runs over any `RpcClient`/`RpcServer` protocol (WebSocket,
     HTTP, socket, worker), with `RpcSerialization` for framing. Who may connect is a service
     (`Sync.Authorization`), as `eventlog` does it with `EventLogServerAuthorization`.
   - **Storage:** `KeyValueStore`.
   - **State:** `SubscriptionRef`.
   - **Observation:** `Stream`.
   - **Ids:** `Crypto`.
   - **Merging:** `Combiner`/`Reducer`.
   - **Patches:** `Differ`, named like `JsonPatch`.
   - **Server-side documents:** loaded on demand with `RcMap`.
6. **Everything that crosses a boundary is a `Schema`:** deltas, version vectors, anchors, patches,
   overlaps, RPC payloads and errors.
7. **Upgrading is a find-and-replace.** Nothing encoded contains the library name. Tags are plain words,
   storage keys use a prefix the app chooses, and the wire and storage formats carry their own format
   version, independent of Effect's.
8. **Small public surface.** Ten modules, one service each where there's a service. Strategy internals
   stay in `internal/`. Every public module is `@stability unstable` until upstreamed.

## Examples

One per strategy, as they'll appear in `examples/`. Each shows a different part of the API.

### Tree: offline meeting notes

Covers editing, in-process sync, and overlaps.

```ts
/**
 * Two laptops edit the same meeting notes offline, then reconnect. Every edit
 * survives, and the merge points at the sentence they both rewrote.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Effect, Option } from "effect"
import { Replica, ReplicatedText } from "effect-delta"

const program = Effect.gen(function*() {
  const alice = yield* ReplicatedText.make()
  yield* ReplicatedText.insert(alice, 0, "Launch is Friday. Sessions last 24 hours.")

  // Bob's laptop gets a copy before they both go offline.
  const bob = yield* ReplicatedText.make()
  yield* Replica.exchange(alice, bob)

  // Offline, Alice adds a heading. Both change the session length.
  yield* ReplicatedText.insert(alice, 0, "# Decisions\n")
  yield* ReplicatedText.update(alice, (text) => text.replace("24 hours", "8 hours"))
  yield* ReplicatedText.update(bob, (text) => text.replace("24 hours", "12 hours"))

  // Back online.
  const overlaps = yield* Replica.exchange(alice, bob)

  yield* Console.log(yield* ReplicatedText.get(alice))
  // # Decisions
  // Launch is Friday. Sessions last 812 hours.    (or 128; the same on both laptops)

  for (const overlap of overlaps) {
    if (overlap._tag !== "Text") continue
    const range = yield* ReplicatedText.locateRange(alice, overlap.anchors)
    yield* Console.log("Both changed:", Option.getOrUndefined(range))
  }
})

program.pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain)
```

### Authority: reviewers can only comment

Covers the `Authority` service, a server, the RPC protocol, and a rejected edit.

```ts
/**
 * A spec server that accepts edits only from editors. A reviewer's edit is
 * rejected and rolled back on their screen, and never reaches anyone else.
 */
import { NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { Effect, Layer, Option, Stream } from "effect"
import { Headers, HttpRouter } from "effect/http"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import { Authority, HistoryStore, Replica, ReplicatedText, Sync } from "effect-delta"
import { createServer } from "node:http"

// Who's connecting, from the session token their app sent when the socket opened.
// `sessions` is the app's own.
const SignedIn = Layer.succeed(Sync.Authorization)({
  identify: (headers) =>
    Headers.get(headers, "authorization").pipe(
      Option.flatMap((value) => Option.fromNullishOr(sessions.get(value.replace(/^Bearer /, "")))),
      Option.match({ onNone: () => Effect.fail(new Sync.Forbidden({ message: "Sign in first" })), onSome: Effect.succeed })
    ),
  authorize: () => Effect.void // anyone signed in may open specs
})

// The authority: the rule lives here. `roles` is the app's own. Specs are texts, so a change to a
// document with fields is rejected too.
const EditorsOnly = Authority.layer({
  check: ({ _tag, user }) =>
    _tag !== "Text"
      ? Effect.fail(new Authority.Rejected({ message: "Specs are texts" }))
      : roles.get(user) === "editor"
      ? Effect.void
      : Effect.fail(new Authority.Rejected({ message: "Reviewers can comment but not edit" }))
}).pipe(Layer.provide(HistoryStore.layerMemory))

// Server: serve the authority over RPC.
const ServerLive = HttpRouter.serve(
  RpcServer.layer(Sync.Rpcs).pipe(
    Layer.provide(Authority.layerHandlers),
    Layer.provide([EditorsOnly, SignedIn]),
    Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/sync" }))
  )
).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: 3000 })),
  Layer.provide(RpcSerialization.layerNdjson)
)

// Client: a text that answers to an authority. Its type says so:
// Effect<ReplicatedText.Confirmed, never, Authority | Crypto | Scope>
const reviewer = Effect.gen(function*() {
  const spec = yield* ReplicatedText.makeConfirmed({ document: Replica.DocumentId.make("spec-42") })
  const { edits } = yield* ReplicatedText.subscribe(spec)

  yield* ReplicatedText.insert(spec, 0, "LGTM ")

  const rejected = yield* edits.pipe(
    Stream.filter((edit) => edit._tag === "Rejected"),
    Stream.runHead
  )
  // Some({ _tag: "Rejected", message: "Reviewers can comment but not edit", patch: <removes "LGTM "> })
})

// In production the authority is on a server.
const AuthorityRemote = Authority.layerRemote.pipe(
  Layer.provide(RpcClient.layerProtocolSocket()),
  Layer.provide(NodeSocket.layerWebSocket("wss://specs.example.com/sync", { headers: { authorization: `Bearer ${token}` } })),
  Layer.provide(RpcSerialization.layerNdjson)
)

reviewer.pipe(Effect.scoped, Effect.provide(AuthorityRemote) /* plus the platform's Crypto */)
```

The same client code runs in one process, with no server or sockets, by giving it the authority directly.
This is how the tests run:

```ts
reviewer.pipe(
  Effect.scoped,
  Effect.provideService(Sync.CurrentUser, "ben"),
  Effect.provide(EditorsOnly)
)
```

`examples/spec-review.ts` is the runnable version: the server and two people's apps in one process.

### Replay: a long manuscript, reopened

Covers the replay strategy, persistence, and a long offline stretch.

```ts
/**
 * A writer keeps a long manuscript on their laptop. Reopening the app loads
 * the text without rebuilding its history, and a co-author's week of edits
 * made elsewhere merges in quickly.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Effect, Layer } from "effect"
import { KeyValueStore } from "effect/persistence"
import { HistoryStore, Replica, ReplicatedText } from "effect-delta"

const manuscript = Replica.DocumentId.make("manuscript")

const program = Effect.gen(function*() {
  const laptop = yield* ReplicatedText.make({ strategy: "replay" })
  yield* HistoryStore.persist(laptop, manuscript) // loads what's saved, then saves every change
  yield* Console.log("Loaded", (yield* ReplicatedText.get(laptop)).length, "characters")

  const coauthor = yield* ReplicatedText.make({ strategy: "replay" })
  // ...a week of edits on the co-author's machine...
  yield* Replica.exchange(laptop, coauthor)
})

const StoreLive = HistoryStore.layerKeyValueStore({ prefix: "manuscripts" }).pipe(
  Layer.provide(KeyValueStore.layerFileSystem("./data"))
)

program.pipe(
  Effect.scoped,
  Effect.provide(StoreLive),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain
)
```

`examples/manuscript.ts` is the runnable version: forty chapters typed a sentence at a time, then a week
apart. It reopens 115,000 characters in about 25 ms and merges the co-author's week in about 65 ms.

## Modules

| Module           | What it's for                                                                                                 |
| ---------------- | ------------------------------------------------------------------------------------------------------------- |
| `ReplicatedText` | A collaborative text replica: create, edit, read, subscribe, anchor.                                          |
| `ReplicatedDocument` | A replica whose value has a `Schema`, merged by rules from the schema ([`documents.md`](./documents.md)). |
| `TextPatch`      | How a string changed, in the Quill shape. Named and shaped like `JsonPatch`, and a `Differ<string, TextPatch>`. Editor bindings speak this. |
| `Replica`        | What every replica shares, so sync and storage work for texts and documents alike: ids, version, delta, receive, exchange, overlaps. |
| `Delta`          | What syncs: a set of changes. Combines with `Combiner`/`Reducer`.                                            |
| `VersionVector`  | What a replica has seen, per replica id.                                                                     |
| `Sync`           | The protocol as an `RpcGroup`, the `Authorization` service, connections and their status, `CurrentUser`, protocol errors. |
| `Relay`          | The relay service: passes changes along and stores them. In-process, remote and RPC-handler layers, and `connect`. |
| `Authority`      | The authority service: accepts or rejects each change. In-process, remote and RPC-handler layers.             |
| `HistoryStore`   | Where replicas and servers keep history: a service, with memory and `KeyValueStore` layers, and `persist`.    |

Internal: the tree and replay implementations, change envelopes, frontiers, replica-id ownership, and
encoding details.

## Module sketches

### ReplicatedText

```ts
// A text the functions below edit, watch and anchor: a ReplicatedText, or a document's text field
// (ReplicatedDocument.text, documents.md), so one editor binding works for both.
interface Editable extends Pipeable, Inspectable {}
interface ReplicatedText extends Replica.Replica<string>, Editable {}
interface Confirmed extends ReplicatedText { readonly connection: Sync.Connection }

interface Options {
  readonly strategy?: "tree" | "replay" | undefined  // default "tree"
  readonly replica?: Replica.ReplicaId | undefined   // default: a fresh random id
}

// A replica that syncs with anything: peers (Replica.exchange) and relays (Relay.connect).
declare const make: (options?: Options) => Effect<ReplicatedText, never, Crypto>

// A replica whose changes an authority confirms or rejects. It syncs through that Authority for the life
// of the scope, and with nothing else. Tree-only for now, so there's no strategy option.
declare const makeConfirmed: (options: {
  readonly document: Replica.DocumentId
  readonly replica?: Replica.ReplicaId | undefined
  readonly retry?: Schedule | undefined
}) => Effect<Confirmed, never, Authority | Crypto | Scope>

// Reading
declare const get: (self: Editable) => Effect<string>
declare const changes: (self: Editable) => Stream<string> // current text, then each new one (like SubscriptionRef.changes)
declare const subscribe: (self: Editable) => Effect<Subscription, never, Scope>

interface Subscription {
  readonly text: string        // the text at the moment of subscribing
  readonly edits: Stream<Edit> // every edit applied after it, none missed
}

type Edit =
  | { readonly _tag: "Local"; readonly patch: TextPatch }  // bindings skip the ones they made
  | { readonly _tag: "Remote"; readonly patch: TextPatch; readonly overlaps: ReadonlyArray<Replica.Overlap> }
  | { readonly _tag: "Rejected"; readonly patch: TextPatch; readonly message: string } // rolls back my edit

// Editing: each call is one change. All are dual.
declare const insert: (self: Editable, index: number, text: string) => Effect<void, ReplicatedTextError>
declare const remove: (self: Editable, index: number, length: number) => Effect<void, ReplicatedTextError>
declare const applyPatch: (self: Editable, patch: TextPatch) => Effect<void, ReplicatedTextError> // several edits as one change
declare const set: (self: Editable, text: string) => Effect<void>                                  // one replacement, see below
declare const update: (self: Editable, f: (text: string) => string) => Effect<void>

// Stable positions
type Side = "before" | "after" // which way an anchor leans: where text typed exactly at it goes
interface Anchor { /* opaque; Schema-encodable */ }
interface AnchorRange { readonly start: Anchor; readonly end: Anchor }
interface Range { readonly start: number; readonly end: number } // [start, end), UTF-16 code units

declare const anchor: (self: Editable, index: number, side?: Side) => Effect<Anchor, ReplicatedTextError>
declare const locate: (self: Editable, anchor: Anchor) => Effect<Option<number>>       // None: not received here yet, or rolled back
declare const anchorRange: (self: Editable, range: Range) => Effect<AnchorRange, ReplicatedTextError>
declare const locateRange: (self: Editable, range: AnchorRange) => Effect<Option<Range>> // also None once all of it is deleted

class ReplicatedTextError extends Schema.TaggedError<ReplicatedTextError>()("ReplicatedTextError", {
  reason: Schema.Union([OutOfBounds, SplitsSurrogatePair])
}) {}
```

**Two constructors, because they sync differently.**
- A `make` replica can sync with several things at once: direct exchanges and any number of relays. So
  connecting is explicit (`Relay.connect`).
- A `makeConfirmed` replica has exactly one path, its authority, so that path is its requirement. It
  connects on creation, and `Replica.exchange` and `Replica.receive` from anything else fail with
  `AuthorityOnly`. That means a change that might be rejected never reaches another replica.

**`set` and `update`** are for apps that hold whole strings: agents doing find-and-replace, form fields. They
turn the new string into **one replacement**: the shortest stretch between the longest common prefix and
suffix, moved to code-point boundaries so they never split an emoji. That's a contract, because it decides
how concurrent edits merge (it's why the example gives "812 hours"). `TextPatch.get` uses the same rule.
Editors should use `insert`, `remove` and `applyPatch`. Those say exactly what the user did, while a diff
has to guess: in `"aa"` → `"aaa"`, which `a` was typed?

**Editor bindings** call `subscribe`, load `text` into the editor, then apply every patch except the
`Local` ones they made, which is what prevents echo loops. Other `Local` patches, such as a document
`update` that changed a text field, are applied like the rest. If the editor has keystrokes it hasn't
handed to the replica yet, the binding moves the remote patch past them with `TextPatch.transform`.

**`make` needs `Crypto`** for random replica ids. Platform layers (`NodeServices.layer` and the browser
equivalent) provide it, and tests use `TestCrypto.layer(seed)` for deterministic ids. A failure to get
random bytes is a defect, not a typed error.

### TextPatch

Shaped and named like `JsonPatch` (`JsonPatchOperation`, `JsonPatch`, `get`, `apply`).

```ts
type TextPatchOperation =
  | { readonly _tag: "Retain"; readonly length: number }
  | { readonly _tag: "Insert"; readonly text: string }
  | { readonly _tag: "Remove"; readonly length: number }
type TextPatch = ReadonlyArray<TextPatchOperation>

declare const TextPatch: Schema.Codec<TextPatch, QuillDelta> // encodes to Quill's { retain } | { insert } | { delete }

declare const empty: TextPatch
declare const get: (before: string, after: string) => TextPatch                      // one replacement, as ReplicatedText.set
declare const apply: (patch: TextPatch, before: string) => string                     // dual
declare const compose: (first: TextPatch, second: TextPatch) => TextPatch             // apply first, then second
declare const transform: (patch: TextPatch, over: TextPatch, side: ReplicatedText.Side) => TextPatch // for editor bindings
declare const mapIndex: (patch: TextPatch, index: number, side: ReplicatedText.Side) => number      // move a cursor past a patch
declare const Differ: Differ<string, TextPatch>                                       // get / compose / apply
```

`compose` is sequential, as `Differ.combine` is. It isn't the CRDT join; that's `Delta.combine`.

### Replica

```ts
interface Replica<out A> extends Pipeable {
  readonly strategy: "tree" | "replay"
}

declare const ReplicaId: Schema.brand<Schema.String, "ReplicaId"> // never empty, nor __proto__
declare const DocumentId: Schema.brand<Schema.String, "DocumentId">
type Path = ReadonlyArray<string> // where a part of a document is: ["comments", "c1", "text"]; [] is the root

declare const id: (self: Replica<unknown>) => Effect<ReplicaId> // the id new changes are written under
declare const version: (self: Replica<unknown>) => Effect<VersionVector>
declare const delta: (self: Replica<unknown>, since: VersionVector) => Effect<Delta>                           // dual
declare const receive: (self: Replica<unknown>, delta: Delta) => Effect<ReadonlyArray<Overlap>, DeltaError>  // dual
declare const exchange: (self: Replica<unknown>, that: Replica<unknown>) => Effect<ReadonlyArray<Overlap>, DeltaError> // dual; self's overlaps

type Overlap =
  | {
    readonly _tag: "Text"
    readonly path: Path                           // [] for a replicated text
    readonly range: ReplicatedText.Range          // where it is in the merged text, now
    readonly anchors: ReplicatedText.AnchorRange  // follows it from here on
    readonly replicas: ReadonlyArray<ReplicaId>
  }
  // Documents only (documents.md): both wrote different values here, both placed this list item (adding or
  // moving it), or one removed it while the other wrote it; `kept` is whose write shows.
  | { readonly _tag: "Value"; readonly path: Path; readonly replicas: ReadonlyArray<ReplicaId>; readonly kept: ReplicaId }
  // One removed this entry, item or optional field while the other changed something inside it, or moved it.
  // It stays removed, by a removal made concurrently with that change.
  | { readonly _tag: "Removed"; readonly path: Path; readonly replicas: ReadonlyArray<ReplicaId> }

class DeltaError extends Schema.TaggedError<DeltaError>()("DeltaError", {
  reason: Schema.Union([StrategyMismatch, Malformed, ReplicaIdReused, AuthorityOnly])
}) {}
```

**`receive` is idempotent and order-independent.** Changes that arrive before what they build on wait
inside the replica (pending), so it never fails for that.

**`DeltaError` reasons:**
- `ReplicaIdReused`: the replica received a change under its own id that it didn't make. That's the one
  mistake that silently corrupts history in Yjs and Loro, so we catch it.
- `AuthorityOnly`: a confirmed replica was asked to exchange with something other than its authority.

**`id` is an Effect, not a field,** because a confirmed replica switches to a new id after a rejection.

### Delta and VersionVector

```ts
interface Delta { /* opaque set of changes; Schema-encodable */ }
declare const Delta: Schema.Codec<Delta, DeltaEncoded>
declare const empty: Delta
declare const combine: (self: Delta, that: Delta) => Delta // dual
declare const ReducerUnion: Reducer<Delta>
declare const isEmpty: (self: Delta) => boolean

type VersionVector = { readonly [replica: string]: number }
declare const VersionVector: Schema.Codec<VersionVector, Record<string, number>>
declare const empty: VersionVector
declare const combine: (self: VersionVector, that: VersionVector) => VersionVector // dual; pointwise max
declare const ReducerMax: Reducer<VersionVector>
declare const compare: (self: VersionVector, that: VersionVector) => "before" | "after" | "equal" | "concurrent"
```

`Delta.combine` is commutative, associative and idempotent for deltas made by honest replicas. If two
deltas disagree about the same change id (a reused or forged id), `combine` keeps one deterministically and
`Replica.receive` reports `ReplicaIdReused`.

### Sync

```ts
// The protocol, shared by relays and authorities. Apps mount it in their RpcServer.
declare const Rpcs: RpcGroup<Subscribe | Push>
//   Subscribe { format, document, strategy, version, authority: boolean }
//     → Stream<Update { delta, version }>: first what the caller lacks, then each new delta as it arrives
//   Push { format, document, delta }
//     → Accepted { confirmed: VersionVector } | Rejected { confirmed, message }

// What a relay and an authority both provide.
interface Server {
  readonly subscribe: (request: Subscribe) => Stream<Update, SyncError>
  readonly push: (request: Push) => Effect<Ack, SyncError>
}

// Who may connect and what they may do. Servers check it on every request.
class Authorization extends Context.Service<Authorization, {
  readonly identify: (headers: Headers) => Effect<string, Forbidden>  // who is asking, as a stable user id
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
  readonly await: Effect<never, SyncError> // fails if the connection gives up (retries exhausted, forbidden, protocol error)
}

class SyncError extends Schema.TaggedError<SyncError>()("SyncError", {
  reason: Schema.Union([Forbidden, ProtocolViolation, Disconnected, Unavailable, DeltaError])
}) {}
```

**How a connection works.** It `Subscribe`s with what the replica has seen and applies the stream of deltas
it gets back. Once caught up it `Push`es the replica's own changes the server lacks, then each new one. On a
`Disconnected` or `Unavailable` error it retries with `retry` and resubscribes from its current version, so
nothing is lost. The server's first update says what it has, so a server that lost unsaved changes gets
them again. Peer-to-peer in the first release means one peer runs a relay (with `HistoryStore.layerMemory`)
and serves it with `Relay.layerHandlers`, and the other uses `Relay.layerRemote`. A symmetric mesh comes
later.

### Relay and Authority

Each is one service with three layers:

| Layer                          | What it is                                                                                                       |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `layer`                        | The real thing, in this process. On a server, or given straight to replicas in tests and single-process apps. Documents are loaded from the `HistoryStore` when first asked for and released when unused (`RcMap`). |
| `layerRemote`                  | The same service on a client, reaching a server over RPC (`RpcClient.Protocol`). One connection carries every document. |
| `layerHandlers(rpcs, options)` | Serves the in-process service over RPC, adding per-connection security (below).                                 |

```ts
// Relay: passes changes along and stores them; never rejects.
class Relay extends Context.Service<Relay, Sync.Server>()("effect-delta/Relay") {}
declare const layer: Layer<Relay, never, HistoryStore>
declare const layerRemote: Layer<Relay, never, RpcClient.Protocol>
declare const layerHandlers: Layer<Rpc.ToHandler<Sync.Rpcs>, never, Relay | Sync.Authorization>

// Syncs a replica through the Relay for the life of the scope.
declare const connect: (
  replica: Replica<unknown>,
  options: { readonly document: DocumentId; readonly retry?: Schedule | undefined }
) => Effect<Sync.Connection, never, Relay | Scope> // dual

// Authority: the same shape; its pushes can be answered with Rejected.
class Authority extends Context.Service<Authority, Sync.Server>()("effect-delta/Authority") {}
declare const layer: <R>(options: {
  readonly check: (proposal: Proposal) => Effect<void, Rejected, R>
}) => Layer<Authority, never, HistoryStore | R>
declare const layerRemote: Layer<Authority, never, RpcClient.Protocol>
declare const layerHandlers: Layer<Rpc.ToHandler<Sync.Rpcs>, never, Authority | Sync.Authorization>

type Proposal = TextProposal | DocumentProposal

interface TextProposal {
  readonly _tag: "Text"
  readonly document: DocumentId
  readonly user: string     // Sync.CurrentUser
  readonly patch: TextPatch // what the change does to the current text
  readonly before: string
  readonly after: string
}

// A change to a ReplicatedDocument. The authority has no schema, so the document is JSON; a check reads
// it with ReplicatedDocument.readProposal (documents.md).
interface DocumentProposal {
  readonly _tag: "Document"
  readonly document: DocumentId
  readonly user: string
  readonly before: Schema.Json
  readonly after: Schema.Json
  // The paths in each whose latest write says they exist (entries, list items and optional fields added,
  // and values), which JSON alone can't say
  readonly exists: { readonly before: ReadonlyArray<Replica.Path>; readonly after: ReadonlyArray<Replica.Path> }
  readonly paths: ReadonlyArray<Replica.Path> // where the change wrote
  readonly delta: Delta                       // the change itself
}

class Rejected extends Schema.TaggedError<Rejected>()("Rejected", { message: Schema.String }) {}
```

**Who may write what.** The handler layers add the security that matters across a network, from the
`Sync.Authorization` service:
- `identify` decides who each request is from, from its headers (the HTTP request or WebSocket upgrade, so
  cookies and tokens work). Handlers set `Sync.CurrentUser` to it, so a `check` sees the user.
- `authorize` decides read and write access per document.
- A replica id belongs to the first user who writes with it, and a push with changes under an id owned by
  someone else is `Forbidden`. So a client can't forge another user's edits. A connection uploads only
  its own changes; other devices upload theirs.

**Relays trust their clients; put an authority in front of untrusted ones.** A relay refuses what's
malformed or stamped far ahead of its clock, so a forged change can't crash it, stop it, or leave the
replicas it serves showing different values. Past that, it takes a client's changes as they come. A client
that forges well-formed changes, claiming to have seen changes it hadn't, can make overlaps misleading or
make relays that heard different pushes disagree about one. An authority checks every change before anyone
sees it, so it's the boundary for clients you don't trust. Peers that exchange directly trust each other
the same way.

**Rejection.** The authority checks a push's changes in order and accepts the longest acceptable prefix,
answering `Rejected` with what it accepted and the reason.
The confirmed replica:
1. drops the rejected change and every unconfirmed change after it, since each was made on top of the last;
2. emits a `Rejected` edit with the patch that rolls the text back;
3. switches to a new replica id.

Switching ids means sequence numbers never repeat and never leave a gap, so the authority's version vector
keeps moving. A subscription says whether it expects an authority, and a mismatch either way is a
`ProtocolViolation`.

**Servers save before they share.** A pushed change is checked, stored, then sent to subscribers. If
storing fails, the push fails with `Unavailable`, every subscription to that document ends with it, and the
document is loaded again from the store, so nothing in memory gets ahead of what's saved. Changes that
build on something the server doesn't have yet (a peer's change that peer hasn't uploaded) wait on a relay,
which stores them and sends them to subscribers once it has checked them; an authority can't check them, so
it refuses the push as `Malformed`.

### HistoryStore

```ts
class HistoryStore extends Context.Service<HistoryStore, {
  readonly load: (document: DocumentId) => Effect<Option<Stored>, HistoryStoreError>
  readonly append: (document: DocumentId, delta: Delta) => Effect<void, HistoryStoreError>       // confirmed history; append-only
  readonly setUnconfirmed: (document: DocumentId, delta: Delta) => Effect<void, HistoryStoreError> // confirmed replicas; rewritten
  readonly setSnapshot: (document: DocumentId, snapshot: Snapshot) => Effect<void, HistoryStoreError>  // replay; replaced
  readonly addWriter: (document: DocumentId, replica: ReplicaId, owner: string) => Effect<void, HistoryStoreError>
}>()("effect-delta/HistoryStore") {}

interface Stored {
  readonly history: Delta
  readonly unconfirmed: Delta
  readonly writers: { readonly [replica: string]: string } // replica id → its owner (servers) or "" (replicas)
  readonly snapshot: Option<Snapshot>
}

interface Snapshot {
  readonly version: VersionVector // the changes they're the texts of
  readonly texts: ReadonlyArray<{ readonly path: Replica.Path; readonly text: string }> // [] for a ReplicatedText
}

declare const layerMemory: Layer<HistoryStore>
declare const layerKeyValueStore: (options: {
  readonly prefix: string
  readonly foldAfter?: number | undefined // default 64
}) => Layer<HistoryStore, never, KeyValueStore>

// Loads saved history into `replica`, then saves every change for the life of the scope.
declare const persist: (replica: Replica<unknown>, document: DocumentId) => Effect<Persisted, HistoryStoreError, HistoryStore | Scope>
interface Persisted { readonly await: Effect<never, HistoryStoreError> }
```

**The store stays simple.** `persist` knows the strategy, so it decides when to write a snapshot. For
replay, it writes one when its scope closes and after every 4,096 sequence numbers of history in between
(one per character, and one per document op). Loading records the history without replaying it, takes the
texts from the snapshot, and merges only the changes saved after it; a snapshot that doesn't match the
history is ignored. `writers` lets a replica upload changes it made under earlier ids, and lets servers
remember who owns each id. When its scope closes,
`persist` saves once more, so the last edits aren't lost. A confirmed replica's
unconfirmed changes are stored separately and rewritten after a rejection, so a rejected change never
comes back on reload.

**The `KeyValueStore` layer** puts everything under the app's `prefix`, never the library name. It writes
appended deltas under numbered keys and folds them into the snapshot past a size threshold, because
`KeyValueStore` has no range scans. One writer per document per store: two tabs persisting the same
document to the same store isn't supported yet (sharing a replica across tabs is later).

## Encoding

- **JSON for the wire and storage in the first release** (`RpcSerialization.layerNdjson` or `layerJson`).
  It's readable, stable and debuggable. `SchemaBinary` is unstable in Effect, and its fingerprints change
  between Effect releases (LiveStore hit this). A compact binary format is a later, deliberate step with
  its own version.
- **Tags are plain words** (`"Insert"`, `"Remove"`, `"Rejected"`), and RPC tags are module-scoped like
  eventlog's (`"Sync.Subscribe"`). Schema identifiers and service keys may say `effect-delta/...`, because
  they're never encoded.
- **Every RPC payload and stored record carries a format version.**

## Decisions

All accepted 2026-10-15.

1. **A shared `Replica` interface**, with `ReplicatedText` as the first kind. Sync, storage and servers
   work over any replica, so documents later add a kind rather than a second sync stack.
2. **The text module is `ReplicatedText`**, following `PersistedQueue`, `SubscriptionRef` and friends, and
   avoiding the DOM's global `Text`.
3. **The strategy is an option on `make`** (`strategy: "tree" | "replay"`, default `"tree"`).
4. **Authority is a requirement, with its own constructor.** `makeConfirmed` requires an `Authority`
   service; `Relay` and `HistoryStore` are services the same way. Each has in-process and remote layers, so
   the same client code runs against a server in production and in one process in tests.
5. **"Remove" instead of "delete"** for the operation and the function, because Effect's collections say
   remove (`HashMap.remove`, `Record.remove`, JsonPatch's `"remove"`). The patch still encodes to Quill's
   `{ delete: n }` for editor interop.
6. **`changes` for values and `subscribe` for edits.** `changes` matches `SubscriptionRef.changes`.
   `subscribe` returns the current text and every edit after it, with none missed.
7. **The protocol is an `RpcGroup`,** like `eventlog`'s. Rpc handles framing, many documents per
   connection, retries and auth middleware, over WebSocket, HTTP, sockets or workers. Peer-to-peer in the
   first release means one peer serves a relay.
8. **`HistoryStore`** for the persistence service, following `KeyValueStore` and `RateLimiterStore`.
9. **Servers authorize, and replica ids have owners.** The `Sync.Authorization` service has `identify` and
   `authorize`; an authority's `check` sees the user through `Sync.CurrentUser`.
10. **Keep changes, frontiers and the strategy states internal** in the first release. `Delta` is opaque
    but encodable.
11. **`make` needs `Crypto`**, and a failure to get random bytes is a defect. Tests use
    `TestCrypto.layer(seed)`.
12. **Errors are one `TaggedError` per module with a tagged `reason`.** Errors carrying a human-readable
    string call the field `message`. We say "rejected", never "refused", for what an authority turns down;
    what's malformed, or a schema `make` can't take, is refused. Bad indexes are typed errors, not
    defects.
13. **After a rejection, a confirmed replica continues under a new replica id.**
14. **`set`/`update` make one replacement** (common prefix and suffix, aligned to code points), as a stated
    contract. A multi-region diff can come later as a separate function.
15. **`TextPatch` mirrors `JsonPatch`**, and instances use PascalCase (`TextPatch.Differ`,
    `Delta.ReducerUnion`), as `Number.ReducerSum` does.
16. **Relays trust their clients; authorities are the boundary for untrusted ones** (accepted 2026-10-17).
    Relays refuse what's malformed, so forged changes can't crash them or split replicas, but they don't
    check what a well-formed change claims.

## Changed while building

Found while building the network half and the replay strategy (2026-10-15). None changes how a replica is
edited or read.

1. **`Sync.connect` became `Relay.connect`.** `Sync` holds only the protocol, which every other module
   imports, so it can't import `Relay` without a cycle. It reads well too: you connect through a relay.
2. **Authorization is a service, `Sync.Authorization`, not options on `layerHandlers`.** That's how
   `eventlog` does it (`EventLogServerAuthorization`). `identify` gets the request's headers, so apps don't
   need their own `RpcMiddleware`, and `layerHandlers` is a plain layer.
3. **A rejected push is an answer, not an error.** `Push` answers `Accepted { confirmed }` or
   `Rejected { confirmed, message }`, because the replica needs to know what was accepted before the
   rejected change.
4. **`Subscribe` says whether it expects an authority** (`authority: boolean`), instead of listing its
   writers. Servers learn writers from pushes.
5. **`Connection.unconfirmed` is a `Stream<number>`,** like `status`, so an app can show "Saving…" and wait
   for 0 before closing a document. As an `Effect` it couldn't be watched.
6. **`SyncError` has an `Unavailable` reason,** for a server that can't load or save a document.
   Connections retry it, like `Disconnected`.
7. **`HistoryStore` has `addWriter`,** so replicas and servers remember which replica ids were written
   under, and by whom.
8. **Replica ids can't be empty.** The replay strategy's temporary tree uses the empty id for characters
   whose history it skips.
9. **A server learns each document's strategy** from the first text op it stores, or else from the first
   replica to subscribe, and refuses the other strategy (`ProtocolViolation` when subscribing,
   `StrategyMismatch` when pushing). Authorities are tree-only for now, as planned.

## Known limits

Measured on a 150,000-character text with 2,600 changes (`scratchpad` benchmark, not committed):

| | tree | replay |
| --- | --- | --- |
| Typing 2,000 edits | 150 ms | 12 ms |
| First copy to a new replica | 160 ms | 19 ms |
| Merging a week of concurrent edits (600) | 80 ms | 110 ms |
| One concurrent keystroke | 20 ms | 80 ms |

- **Replay merges cost time in proportion to the text.** A merge starts from stand-ins for the text at the
  last change both sides share, one per character. Grouping stand-ins into runs would make that cost
  nearly constant.
- **Replay anchors replay the whole history,** once per change to the text (cached until the next one).
  `anchor`, `locate` and overlaps made only of removed text are slow on long histories. They could start
  from the latest change the anchored character's history shares, as merges do.
- **The tree is one node per character,** and typing builds deep chains, so tree edits cost time in
  proportion to the depth, and one long insert or removal costs its length times the depth (a whole-text
  `update` that changes text far apart is one long replacement). Runs and a balanced layout are the usual
  fix.
- **A reused replica id is caught** only once one copy has written more under it than the other (see
  `ReplicaIdReused`).

## Not in the first release

- A symmetric peer-to-peer mesh
- Undo
- Formatting marks
- Presence
- Pruning history
- Binary encoding
- WebRTC and BroadcastChannel transports
- Sharing one replica id across tabs
- Time travel and history views
- End-to-end encryption
- Confirmed replicas for the replay strategy
