/**
 * The `Sync` module is the protocol replicas sync over, shared by both server
 * roles (`Relay` and `Authority`): an `RpcGroup` with two calls.
 *
 * - `Subscribe` sends what a replica has seen and streams back what it lacks,
 *   then every new change as it arrives.
 * - `Push` sends a replica's own changes, and is answered with what the server
 *   now has, or (from an authority) with a rejection.
 *
 * Because it's an `RpcGroup`, the protocol runs over any `RpcClient` and
 * `RpcServer` protocol (WebSocket, HTTP, sockets, workers), one connection
 * carries any number of documents, and apps add their own `RpcMiddleware`. Who
 * may read and write each document is decided by the `Authorization` service.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type * as Headers from "effect/http/Headers"
import * as Layer from "effect/Layer"
import * as Rpc from "effect/rpc/Rpc"
import * as RpcGroup from "effect/rpc/RpcGroup"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"
import * as Delta from "./Delta.ts"
import * as Replica from "./Replica.ts"
import * as VersionVector from "./VersionVector.ts"

/**
 * The protocol's own version, sent with every request. It changes only when the
 * wire format does, independently of the library's and Effect's versions.
 *
 * @stability unstable
 * @category constants
 * @since 0.1.0
 */
export const FORMAT = 1 as const

/**
 * Changes sent from a server to a replica, and what the server has seen once
 * they're included. A relay's `version` also counts changes it keeps that wait
 * for what they build on, which it sends once that's there, so their authors
 * don't push them again. An authority's counts only what it accepted: a
 * confirmed replica takes it as confirmed, as it does a push's answer.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export class Update extends Schema.Class<Update>("effect-delta/Sync/Update")({
  delta: Delta.Delta,
  version: VersionVector.VersionVector
}) {}

/**
 * The server stored the pushed changes and now has seen `confirmed`.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export class Accepted extends Schema.TaggedClass<Accepted>("effect-delta/Sync/Accepted")("Accepted", {
  confirmed: VersionVector.VersionVector
}) {}

/**
 * An authority accepted the pushed changes up to `confirmed` and rejected the
 * next one, for the reason in `message`. Everything after it was dropped too,
 * because it was made on top of the rejected change.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export class PushRejected extends Schema.TaggedClass<PushRejected>("effect-delta/Sync/PushRejected")("Rejected", {
  confirmed: VersionVector.VersionVector,
  message: Schema.String
}) {}

/**
 * What a push is answered with.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Ack = Schema.Union([Accepted, PushRejected])

/**
 * What a push is answered with.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Ack = Accepted | PushRejected

/**
 * The connection's user may not read or write this document.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class Forbidden extends Schema.TaggedError<Forbidden>()("Forbidden", {
  message: Schema.String
}) {}

/**
 * The two sides disagree about the protocol: a format version, a strategy, or
 * whether the server is an authority.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class ProtocolViolation extends Schema.TaggedError<ProtocolViolation>()("ProtocolViolation", {
  message: Schema.String
}) {}

/**
 * The connection to the server was lost. Connections retry these.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class Disconnected extends Schema.TaggedError<Disconnected>()("Disconnected", {
  cause: Schema.Defect()
}) {}

/**
 * The server couldn't load or save the document. Connections retry these.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class Unavailable extends Schema.TaggedError<Unavailable>()("Unavailable", {
  message: Schema.String
}) {}

/**
 * Syncing failed. The `reason` says why: only `Disconnected` and `Unavailable`
 * are worth retrying.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class SyncError extends Schema.TaggedError<SyncError>()("SyncError", {
  reason: Schema.Union([Forbidden, ProtocolViolation, Disconnected, Unavailable, Replica.DeltaError])
}) {}

/**
 * Subscribes to a document: streams back the changes a replica at `version`
 * lacks, then each new change, once what it builds on is there. `authority`
 * says whether the replica expects to answer to an authority, so the server
 * can refuse a mismatch.
 *
 * @stability unstable
 * @category protocols
 * @since 0.1.0
 */
export class Subscribe extends Rpc.make("Sync.Subscribe", {
  payload: {
    format: Schema.Literal(FORMAT),
    document: Replica.DocumentId,
    strategy: Schema.Literals(["tree", "replay"]),
    version: VersionVector.VersionVector,
    authority: Schema.Boolean
  },
  success: Update,
  error: SyncError,
  stream: true
}) {}

/**
 * Pushes a replica's own changes to a document.
 *
 * @stability unstable
 * @category protocols
 * @since 0.1.0
 */
export class Push extends Rpc.make("Sync.Push", {
  payload: {
    format: Schema.Literal(FORMAT),
    document: Replica.DocumentId,
    delta: Delta.Delta
  },
  success: Ack,
  error: SyncError
}) {}

/**
 * The sync protocol. Mount it in an `RpcServer`, with `Relay.layerHandlers` or
 * `Authority.layerHandlers`, and add any `RpcMiddleware` the app needs.
 *
 * @stability unstable
 * @category protocols
 * @since 0.1.0
 */
export class Rpcs extends RpcGroup.make(Subscribe, Push) {}

/**
 * A subscribe request, as a relay or authority service receives it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type SubscribeRequest = Rpc.Payload<typeof Subscribe>

/**
 * A push request, as a relay or authority service receives it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type PushRequest = Rpc.Payload<typeof Push>

/**
 * What a relay and an authority both provide: subscribing to a document and
 * pushing changes to it. The in-process layers implement it directly; the
 * remote layers implement it over `Rpcs`.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Server {
  readonly subscribe: (request: SubscribeRequest) => Stream.Stream<Update, SyncError>
  readonly push: (request: PushRequest) => Effect.Effect<Ack, SyncError>
}

/**
 * Who the current changes are for, as a stable user id. RPC handlers set it
 * from `Authorization.identify`; in-process code can provide it directly.
 *
 * @stability unstable
 * @category references
 * @since 0.1.0
 */
export const CurrentUser: Context.Reference<string> = Context.Reference<string>("effect-delta/Sync/CurrentUser", {
  defaultValue: () => "local"
})

/**
 * Decides who is connecting and what they may do. Servers check it on every
 * request: `identify` turns the request's headers (from the HTTP request or
 * WebSocket upgrade) into a stable user id, and `authorize` decides read and
 * write access per document.
 *
 * **Details**
 *
 * A replica id belongs to the first user who writes with it, and a push with
 * changes under an id that belongs to someone else is `Forbidden`, so a client
 * can't forge another user's edits.
 *
 * @stability unstable
 * @category services
 * @since 0.1.0
 */
export class Authorization extends Context.Service<Authorization, {
  readonly identify: (headers: Headers.Headers) => Effect.Effect<string, Forbidden>
  readonly authorize: (request: {
    readonly user: string
    readonly document: Replica.DocumentId
    readonly access: "read" | "write"
  }) => Effect.Effect<void, Forbidden>
}>()("effect-delta/Sync/Authorization") {}

/**
 * An `Authorization` that lets everyone read and write everything, as one user.
 * For development and tests only.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerAuthorizationAllowAll: Layer.Layer<Authorization> = Layer.succeed(Authorization)({
  identify: () => Effect.succeed("anonymous"),
  authorize: () => Effect.void
})

/**
 * Where a connection is up to.
 *
 * **Details**
 *
 * - `Connecting`: opening the connection.
 * - `Syncing`: connected, catching up.
 * - `Synced`: caught up; new changes flow both ways as they happen.
 * - `Offline`: the connection was lost, with the error. It retries after
 *   `Disconnected` and `Unavailable`; any other error has ended it (see
 *   `Connection.await`).
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Status =
  | { readonly _tag: "Connecting" }
  | { readonly _tag: "Syncing" }
  | { readonly _tag: "Synced" }
  | { readonly _tag: "Offline"; readonly error: SyncError }

/**
 * A replica's live connection to a relay or authority.
 *
 * **Details**
 *
 * - `status`: where the connection is up to, then each change of it.
 * - `unconfirmed`: how many of the replica's own changes the server hasn't
 *   acknowledged yet, then each new count, leaving out any that wait on a
 *   change the replica refused. An app can show it ("Saving…"), or wait for 0
 *   before closing a document.
 * - `await`: fails if the connection gives up: retries exhausted, forbidden,
 *   a protocol error, or a change either side refused (a `DeltaError`). It
 *   never succeeds; the connection lasts as long as its scope.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Connection {
  readonly status: Stream.Stream<Status>
  readonly unconfirmed: Stream.Stream<number>
  readonly await: Effect.Effect<never, SyncError>
}
