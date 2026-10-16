import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as PubSub from "effect/PubSub"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Delta from "../Delta.ts"
import type * as Replica from "../Replica.ts"
import * as Sync from "../Sync.ts"
import * as VersionVector from "../VersionVector.ts"
import type * as InternalDelta from "./delta.ts"
import * as InternalReplica from "./replica.ts"

// A replica's connection to the relay or authority it syncs through. It
// subscribes with what the replica has seen, applies whatever comes back, and
// pushes the replica's own changes the server lacks: once after catching up,
// and again after every change. On a lost connection it waits, retries
// and resubscribes from where the replica is now, so nothing is lost.

// Back off from 200ms up to every 10s, as eventlog's remotes do.
const defaultRetry = Schedule.min([Schedule.exponential("200 millis", 1.5), Schedule.spaced("10 seconds")])

/** @internal */
export const connect = Effect.fnUntraced(function*(
  replica: Replica.Replica<unknown>,
  server: Sync.Server,
  options: {
    readonly document: Replica.DocumentId
    readonly authority: boolean
    readonly retry?: Schedule.Schedule<unknown, Sync.SyncError> | undefined
  }
): Effect.fn.Return<Sync.Connection, never, Scope.Scope> {
  const core = InternalReplica.core(replica)
  const status = yield* SubscriptionRef.make<Sync.Status>({ _tag: "Connecting" })
  // What the server is known to have, from its updates and acknowledgements.
  let serverVersion = VersionVector.empty
  // Told whenever `serverVersion` moves.
  const acknowledged = yield* PubSub.unbounded<void>()
  const setServerVersion = (version: VersionVector.VersionVector) =>
    Effect.suspend(() => {
      serverVersion = version
      return PubSub.publish(acknowledged, undefined)
    })
  const pushing = yield* Semaphore.make(1)

  const push = pushing.withPermit(Effect.gen(function*() {
    const delta = yield* core.outgoing(serverVersion)
    if (Delta.isEmpty(delta)) return
    const ack = yield* server.push({ format: Sync.FORMAT, document: options.document, delta })
    if (core.confirmed !== undefined) {
      yield* ack._tag === "Accepted"
        ? core.confirmed.acknowledge(ack.confirmed)
        : core.confirmed.reject(ack.confirmed, ack.message)
    }
    yield* setServerVersion(VersionVector.combine(serverVersion, ack.confirmed))
  }))

  const session = Effect.gen(function*() {
    yield* SubscriptionRef.set(status, { _tag: "Syncing" })
    const changes = yield* core.changed
    const updates = server.subscribe({
      format: Sync.FORMAT,
      document: options.document,
      strategy: replica.strategy,
      version: yield* core.version,
      authority: options.authority
    })
    let caughtUp = false
    const pull = Stream.runForEach(updates, (update) =>
      Effect.gen(function*() {
        yield* core.accept(update.delta).pipe(
          Effect.mapError((reason) => new Sync.SyncError({ reason }))
        )
        if (caughtUp) {
          yield* setServerVersion(VersionVector.combine(serverVersion, update.version))
        } else {
          // The first update says what the server has now. It can have less
          // than before, if it lost changes it hadn't saved, and then they're
          // pushed again.
          yield* setServerVersion(update.version)
          caughtUp = true
          yield* push
          yield* SubscriptionRef.set(status, { _tag: "Synced" })
        }
      }))
    // Changes made while catching up go out with the first push.
    const pushChanges = Stream.runForEach(changes, () => caughtUp ? push : Effect.void)
    yield* Effect.all([pull, pushChanges], { concurrency: "unbounded", discard: true })
  }).pipe(Effect.scoped)

  const run = session.pipe(
    Effect.tapError((error) => SubscriptionRef.set(status, { _tag: "Offline", error })),
    Effect.retry({
      schedule: options.retry ?? defaultRetry,
      while: (error) => error.reason._tag === "Disconnected" || error.reason._tag === "Unavailable"
    }),
    Effect.flatMap(() => Effect.never)
  )
  const fiber = yield* Effect.forkScoped(run)

  const count = Effect.suspend(() => core.outgoing(serverVersion)).pipe(
    Effect.map((delta) => (delta as InternalDelta.DeltaImpl).changes.size)
  )
  // Counted afresh for each subscriber, so it's never behind an edit just
  // made, then again after every change and acknowledgement.
  const unconfirmed = Stream.unwrap(Effect.gen(function*() {
    const changes = yield* core.changed
    const acks = Stream.fromSubscription(yield* PubSub.subscribe(acknowledged))
    return Stream.concat(Stream.succeed(undefined), Stream.merge(changes, acks)).pipe(
      Stream.mapEffect(() => count),
      Stream.changes
    )
  }))

  return {
    status: SubscriptionRef.changes(status),
    unconfirmed,
    await: Fiber.join(fiber)
  }
})
