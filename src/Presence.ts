/**
 * Presence is who else has a document open, and where: cursors, selections and
 * anything else that matters only while someone is here. Unlike a text's changes,
 * it's never stored or merged. It lives on the server for as long as a client stays
 * connected, and everyone's view of it is simply the latest.
 *
 * Positions are anchors, so they mean the same thing to every replica. A peer finds
 * a selection in its own text with `ReplicatedText.locateRange`, which is `None`
 * until it has received the characters the selection is on.
 *
 * `Presence` is a service with three layers, like `Relay` and `Authority`: `layer`
 * runs in this process, `layerRemote` reaches a server over any `RpcClient` protocol,
 * and `layerHandlers` serves `Rpcs`, checking each request with `Sync.Authorization`.
 *
 * **Example** (Seeing another person's selection)
 *
 * ```ts import.meta.vitest
 * import { Presence, Replica, Sync } from "@jpowersdev/effect-delta"
 * import { Effect, Option, Stream } from "effect"
 *
 * const notes = Replica.DocumentId.make("notes")
 *
 * const program = Effect.gen(function*() {
 *   const ada = yield* Presence.join({
 *     document: notes
 *   }).pipe(Effect.provideService(Sync.CurrentUser, "ada"))
 *   const ben = yield* Presence.join({
 *     document: notes
 *   }).pipe(Effect.provideService(Sync.CurrentUser, "ben"))
 *   yield* ada.set({
 *     selection: Option.none(),
 *     data: {
 *       name: "Ada"
 *     }
 *   })
 *   const seen = yield* ben.peers.pipe(
 *     Stream.filter((peers) => peers.some((peer) => peer.state.data !== null)),
 *     Stream.runHead
 *   )
 *   return Option.getOrThrow(seen).map((peer) => peer.user)
 * })
 *
 * await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(Presence.layer))) // => ["ada"]
 * ```
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Ref from "effect/Ref"
import * as Rpc from "effect/rpc/Rpc"
import * as RpcClient from "effect/rpc/RpcClient"
import type * as RpcClientError from "effect/rpc/RpcClientError"
import * as RpcGroup from "effect/rpc/RpcGroup"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Replica from "./Replica.ts"
import * as ReplicatedText from "./ReplicatedText.ts"
import * as Sync from "./Sync.ts"

/**
 * What a client says about itself: its selection, if it has one, and whatever else
 * an app wants others to see, such as a name or a color. A caret is a selection whose
 * anchors are at the same place.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const State = Schema.Struct({
  selection: Schema.OptionFromNullOr(ReplicatedText.AnchorRange),
  data: Schema.Json
})

/**
 * What a client says about itself.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type State = typeof State.Type

/**
 * One connection to a document. A person with two tabs open is two peers with the
 * same `user`, which the server sets, so a client can't claim to be someone else.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Peer = Schema.Struct({
  session: Schema.Int,
  user: Schema.String,
  state: State
})

/**
 * One connection to a document.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Peer = typeof Peer.Type

/**
 * Who's in a document now, and which of them is the connection receiving this.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Update = Schema.Struct({
  session: Schema.Int,
  peers: Schema.Array(Peer)
})

/**
 * Who's in a document now.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Update = typeof Update.Type

/**
 * Joins a document: streams who's in it, now and after each change, and removes this
 * connection from it when the stream ends.
 *
 * @stability unstable
 * @category protocols
 * @since 0.1.0
 */
export class Join extends Rpc.make("Presence.Join", {
  payload: { document: Replica.DocumentId },
  success: Update,
  error: Sync.SyncError,
  stream: true
}) {}

/**
 * Says what this connection's state is now.
 *
 * @stability unstable
 * @category protocols
 * @since 0.1.0
 */
export class Set extends Rpc.make("Presence.Set", {
  payload: { document: Replica.DocumentId, session: Schema.Int, state: State },
  error: Sync.SyncError
}) {}

/**
 * The presence protocol. Mount it in an `RpcServer` with `layerHandlers`, merged with
 * `Sync.Rpcs` to serve both from one place. A client opens a connection for each.
 *
 * @stability unstable
 * @category protocols
 * @since 0.1.0
 */
export class Rpcs extends RpcGroup.make(Join, Set) {}

/**
 * Joining a document and saying what your state is.
 *
 * @stability unstable
 * @category services
 * @since 0.1.0
 */
export class Presence extends Context.Service<Presence, {
  readonly join: (request: { readonly document: Replica.DocumentId }) => Stream.Stream<Update, Sync.SyncError>
  readonly set: (request: {
    readonly document: Replica.DocumentId
    readonly session: number
    readonly state: State
  }) => Effect.Effect<void, Sync.SyncError>
}>()("effect-delta/Presence") {}

const initial: State = { selection: Option.none(), data: null }

/**
 * Presence in this process: on a server, or given straight to clients in tests.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layer: Layer.Layer<Presence> = Layer.effect(Presence)(Effect.gen(function*() {
  const documents = yield* RcMap.make({
    lookup: (_: Replica.DocumentId) => SubscriptionRef.make<ReadonlyArray<Peer>>([]),
    idleTimeToLive: 0
  })
  let sessions = 0
  return Presence.of({
    join: ({ document }) =>
      Stream.unwrap(Effect.gen(function*() {
        const user = yield* Sync.CurrentUser
        const session = ++sessions
        const peers = yield* RcMap.get(documents, document)
        yield* Effect.acquireRelease(
          SubscriptionRef.update(peers, (all) => [...all, { session, user, state: initial }]),
          () => SubscriptionRef.update(peers, (all) => all.filter((peer) => peer.session !== session))
        )
        return Stream.map(SubscriptionRef.changes(peers), (all) => ({ session, peers: all }))
      })),
    set: ({ document, session, state }) =>
      Effect.gen(function*() {
        const user = yield* Sync.CurrentUser
        const peers = yield* RcMap.get(documents, document)
        const mine = (yield* SubscriptionRef.get(peers)).find((peer) => peer.session === session)
        // Whoever's connection this was has gone, and this was on its way.
        if (mine === undefined) return
        if (mine.user !== user) {
          return yield* new Sync.SyncError({
            reason: new Sync.Forbidden({ message: `Session ${session} belongs to another user` })
          })
        }
        yield* SubscriptionRef.update(
          peers,
          (all) => all.map((peer) => peer.session === session ? { ...peer, state } : peer)
        )
      }).pipe(Effect.scoped)
  })
}))

// A transport failure is a lost connection, which `join` retries.
const fromClient = (error: Sync.SyncError | RpcClientError.RpcClientError): Sync.SyncError =>
  error._tag === "SyncError" ? error : new Sync.SyncError({ reason: new Sync.Disconnected({ cause: error }) })

/**
 * Presence on a server, reached over `Rpcs`. Provide an `RpcClient.Protocol`, such as
 * `RpcClient.layerProtocolSocket`.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerRemote: Layer.Layer<Presence, never, RpcClient.Protocol> = Layer.effect(Presence)(
  Effect.gen(function*() {
    const client = yield* RpcClient.make(Rpcs)
    return Presence.of({
      join: (request) => client["Presence.Join"](request).pipe(Stream.mapError(fromClient)),
      set: (request) => client["Presence.Set"](request).pipe(Effect.mapError(fromClient))
    })
  })
)

/**
 * Serves `Presence` over `Rpcs`. Each request is identified and authorized for reading
 * with `Sync.Authorization`: someone who can see a document can be seen in it.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerHandlers: Layer.Layer<
  Rpc.ToHandler<RpcGroup.Rpcs<typeof Rpcs>>,
  never,
  Presence | Sync.Authorization
> = Rpcs.toLayer(Effect.gen(function*() {
  const presence = yield* Presence
  const authorization = yield* Sync.Authorization
  const enter = (headers: Parameters<Sync.Authorization["Service"]["identify"]>[0], document: Replica.DocumentId) =>
    authorization.identify(headers).pipe(
      Effect.tap((user) => authorization.authorize({ user, document, access: "read" })),
      Effect.mapError((reason) => new Sync.SyncError({ reason }))
    )
  return Rpcs.of({
    "Presence.Join": (request, { headers }) =>
      Stream.unwrap(Effect.map(
        enter(headers, request.document),
        (user) => Stream.provideService(presence.join(request), Sync.CurrentUser, user)
      )),
    "Presence.Set": (request, { headers }) =>
      Effect.flatMap(
        enter(headers, request.document),
        (user) => Effect.provideService(presence.set(request), Sync.CurrentUser, user)
      )
  })
}))

/**
 * A client in a document.
 *
 * **Details**
 *
 * - `peers`: everyone else who's here now, then after each change.
 * - `set`: says what this client's state is now. It's sent again after a reconnect,
 *   and doesn't fail: presence that doesn't arrive is simply out of date.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Joined {
  readonly peers: Stream.Stream<ReadonlyArray<Peer>>
  readonly set: (state: State) => Effect.Effect<void>
}

// Back off as a connection's sync does.
const defaultRetry = Schedule.min([Schedule.exponential("200 millis", 1.5), Schedule.spaced("10 seconds")])

/**
 * Joins a document for the life of the scope. A lost connection is retried, and others
 * see this client leave and come back.
 *
 * @stability unstable
 * @category connecting
 * @since 0.1.0
 */
export const join = Effect.fnUntraced(function*(options: {
  readonly document: Replica.DocumentId
  readonly retry?: Schedule.Schedule<unknown, Sync.SyncError> | undefined
}): Effect.fn.Return<Joined, never, Presence | Scope.Scope> {
  const presence = yield* Presence
  const user = yield* Sync.CurrentUser
  const { document } = options
  const peers = yield* SubscriptionRef.make<ReadonlyArray<Peer>>([])
  const session = yield* Ref.make(Option.none<number>())
  const latest = yield* Ref.make(Option.none<State>())

  const send = Effect.gen(function*() {
    const current = yield* Ref.get(session)
    const state = yield* Ref.get(latest)
    if (Option.isNone(current) || Option.isNone(state)) return
    yield* presence.set({ document, session: current.value, state: state.value })
  }).pipe(Effect.provideService(Sync.CurrentUser, user))

  yield* presence.join({ document }).pipe(
    Stream.runForEach((update) =>
      Effect.gen(function*() {
        const changed = Option.getOrUndefined(yield* Ref.get(session)) !== update.session
        yield* Ref.set(session, Option.some(update.session))
        yield* SubscriptionRef.set(peers, update.peers.filter((peer) => peer.session !== update.session))
        if (changed) yield* send
      })
    ),
    Effect.tapError(() => SubscriptionRef.set(peers, [])),
    Effect.retry({
      schedule: options.retry ?? defaultRetry,
      while: (error) => error.reason._tag === "Disconnected" || error.reason._tag === "Unavailable"
    }),
    Effect.ignore,
    Effect.forkScoped
  )

  return {
    peers: SubscriptionRef.changes(peers),
    set: (state) => Effect.andThen(Ref.set(latest, Option.some(state)), Effect.ignore(send))
  }
})
