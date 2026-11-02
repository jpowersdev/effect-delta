/**
 * A relay is a server that passes changes between replicas and keeps them:
 * every change pushed to a document goes to the `HistoryStore` and then to
 * every replica subscribed to it. A relay never rejects a change; for a server
 * that decides which changes count, see `Authority`.
 *
 * `Relay` is a service with three layers:
 *
 * - `layer` runs a relay in this process: on a server, or given straight to
 *   replicas in tests and single-process apps.
 * - `layerRemote` reaches a relay on a server, over any `RpcClient` protocol.
 *   One connection carries every document.
 * - `layerHandlers` serves a relay over `Sync.Rpcs`, checking each request with
 *   `Sync.Authorization`.
 *
 * `connect` syncs a replica through whichever one is provided. Peer-to-peer
 * means one peer runs a relay and serves it, and the others connect to it.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import { dual } from "effect/Function"
import * as Layer from "effect/Layer"
import type * as Rpc from "effect/rpc/Rpc"
import type * as RpcClient from "effect/rpc/RpcClient"
import type * as RpcGroup from "effect/rpc/RpcGroup"
import type * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import type * as HistoryStore from "./HistoryStore.ts"
import * as InternalConnection from "./internal/connection.ts"
import * as InternalServer from "./internal/server.ts"
import type * as Replica from "./Replica.ts"
import type * as Sync from "./Sync.ts"

/**
 * A relay: subscribing to a document, and pushing changes to it.
 *
 * @stability unstable
 * @category services
 * @since 0.1.0
 */
export class Relay extends Context.Service<Relay, Sync.Server>()("effect-delta/Relay") {}

/**
 * A relay in this process, keeping documents in the `HistoryStore`. Each
 * document is loaded when first asked for, and released a minute after its
 * last use.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layer: Layer.Layer<Relay, never, HistoryStore.HistoryStore> = Layer.effect(Relay)(
  InternalServer.make({ check: undefined })
)

/**
 * A relay on a server, reached over `Sync.Rpcs`. Provide an
 * `RpcClient.Protocol`, such as `RpcClient.layerProtocolSocket`.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerRemote: Layer.Layer<Relay, never, RpcClient.Protocol> = Layer.effect(Relay)(InternalServer.remote)

/**
 * Serves the `Relay` over `Sync.Rpcs`. Each request is identified and
 * authorized with `Sync.Authorization`, and a push may only carry changes
 * under replica ids that belong to its user.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerHandlers: Layer.Layer<
  Rpc.ToHandler<RpcGroup.Rpcs<typeof Sync.Rpcs>>,
  never,
  Relay | Sync.Authorization
> = InternalServer.layerHandlers(Relay)

/**
 * Options for `connect`.
 *
 * **Details**
 *
 * - `document`: which document on the relay the replica is a copy of.
 * - `retry`: how to retry a lost connection. The default backs off from 200
 *   milliseconds to every 10 seconds, indefinitely.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface ConnectOptions {
  readonly document: Replica.DocumentId
  readonly retry?: Schedule.Schedule<unknown, Sync.SyncError> | undefined
}

/**
 * Syncs a replica through the `Relay` for the life of the scope.
 *
 * **Details**
 *
 * The connection subscribes with what the replica has seen and merges every
 * change that comes back. It pushes the replica's own changes: what the relay
 * lacks once caught up, then each new one. After a lost connection it retries
 * and resubscribes from where the replica is, so nothing is lost. A replica can
 * connect to any number of relays and still exchange with peers directly.
 *
 * **Example** (Two devices syncing through a relay)
 *
 * ```ts import.meta.vitest
 * import { Effect, Layer, Option, Stream } from "effect"
 * import { HistoryStore, Relay, Replica, ReplicatedText } from "effect-delta"
 *
 * const notes = Replica.DocumentId.make("notes")
 *
 * const program = Effect.gen(function*() {
 *   const laptop = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
 *   const phone = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("phone") })
 *   yield* Relay.connect(laptop, { document: notes })
 *   yield* Relay.connect(phone, { document: notes })
 *
 *   yield* ReplicatedText.insert(laptop, 0, "Hello")
 *   return yield* ReplicatedText.changes(phone).pipe(Stream.filter((text) => text === "Hello"), Stream.runHead)
 * })
 *
 * const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))
 * await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(RelayLive))) // => Option.some("Hello")
 * ```
 *
 * @stability unstable
 * @category connecting
 * @since 0.1.0
 */
export const connect: {
  (options: ConnectOptions): (
    replica: Replica.Replica<unknown>
  ) => Effect.Effect<Sync.Connection, never, Relay | Scope.Scope>
  (
    replica: Replica.Replica<unknown>,
    options: ConnectOptions
  ): Effect.Effect<Sync.Connection, never, Relay | Scope.Scope>
} = dual(
  2,
  (replica: Replica.Replica<unknown>, options: ConnectOptions) =>
    Relay.use((relay) =>
      InternalConnection.connect(replica, relay, {
        document: options.document,
        authority: false,
        retry: options.retry
      })
    )
)
