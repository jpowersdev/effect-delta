import type * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import type * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as PubSub from "effect/PubSub"
import * as RcMap from "effect/RcMap"
import * as Result from "effect/Result"
import type * as Rpc from "effect/rpc/Rpc"
import * as RpcClient from "effect/rpc/RpcClient"
import type * as RpcClientError from "effect/rpc/RpcClientError"
import type * as RpcGroup from "effect/rpc/RpcGroup"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type * as Authority from "../Authority.ts"
import * as HistoryStore from "../HistoryStore.ts"
import * as Replica from "../Replica.ts"
import * as Sync from "../Sync.ts"
import type * as VersionVector from "../VersionVector.ts"
import { type Change, key } from "./change.ts"
import * as InternalDelta from "./delta.ts"
import * as History from "./history.ts"
import * as TreeReplica from "./treeReplica.ts"
import * as TreeText from "./treeText.ts"

// The core shared by relays and authorities. Each document is held in memory
// while it's in use: loaded from the history store when first asked for, and
// released a minute after its last use. Pushed changes are checked, stored and
// only then sent to subscribers, so no subscriber sees a change the store
// doesn't have.

type TreeChange = Change<TreeText.Op>

interface Document {
  readonly id: Replica.DocumentId
  readonly lock: Semaphore.Semaphore
  text: TreeText.State
  history: History.History<TreeText.Op>
  /** Who each replica id belongs to: the first user who wrote with it. */
  readonly owners: Map<string, string>
  readonly updates: PubSub.PubSub<Sync.Update>
  /**
   * Fails if saving fails. That ends every subscription, and the document is
   * loaded again from the store, so nothing in memory gets ahead of it.
   */
  readonly failed: Deferred.Deferred<never, Sync.SyncError>
}

/**
 * Decides whether an authority accepts a change.
 *
 * @internal
 */
export type Check = (proposal: Authority.Proposal) => Effect.Effect<void, Authority.Rejected>

/**
 * Makes a relay (without `check`) or an authority (with it).
 *
 * @internal
 */
export const make = Effect.fnUntraced(function*(options: {
  readonly check: Check | undefined
}): Effect.fn.Return<Sync.Server, never, HistoryStore.HistoryStore | Scope.Scope> {
  const store = yield* HistoryStore.HistoryStore
  const check = options.check
  const isAuthority = check !== undefined
  const documents = yield* RcMap.make({
    lookup: (id: Replica.DocumentId) => load(store, id),
    idleTimeToLive: "1 minute"
  })

  const open = Effect.fnUntraced(function*(id: Replica.DocumentId) {
    const document = yield* RcMap.get(documents, id)
    if (yield* Deferred.isDone(document.failed)) {
      return yield* syncError(new Sync.Unavailable({ message: `Document ${id} is reloading` }))
    }
    return document
  })

  const subscribe = (request: Sync.SubscribeRequest): Stream.Stream<Sync.Update, Sync.SyncError> =>
    Stream.unwrap(Effect.gen(function*() {
      if (request.strategy !== "tree") {
        return yield* syncError(new Sync.ProtocolViolation({ message: "This server only has the tree strategy" }))
      }
      if (request.authority !== isAuthority) {
        return yield* syncError(
          new Sync.ProtocolViolation({
            message: isAuthority
              ? "This server is an authority: connect with ReplicatedText.makeConfirmed"
              : "This server is a relay, not an authority"
          })
        )
      }
      const document = yield* open(request.document)
      return yield* document.lock.withPermit(Effect.gen(function*() {
        // Subscribed under the lock, so no update lands between the first one
        // and the rest.
        const subscription = yield* PubSub.subscribe(document.updates)
        const first = new Sync.Update({
          delta: InternalDelta.fromChanges(History.since(document.history, request.version)),
          version: held(document.history)
        })
        return Stream.concat(Stream.succeed(first), Stream.fromSubscription(subscription)).pipe(
          Stream.interruptWhen(Deferred.await(document.failed))
        )
      }))
    }))

  const push = (request: Sync.PushRequest): Effect.Effect<Sync.Ack, Sync.SyncError> =>
    Effect.gen(function*() {
      const user = yield* Sync.CurrentUser
      const document = yield* open(request.document)
      return yield* document.lock.withPermit(Effect.gen(function*() {
        const incoming = [...(request.delta as InternalDelta.DeltaImpl).changes.values()]
        for (const change of incoming) {
          const owner = document.owners.get(change.replica)
          if (owner !== undefined && owner !== user) {
            return yield* syncError(
              new Sync.Forbidden({ message: `Replica id ${change.replica} belongs to another user` })
            )
          }
        }

        // Changes that are new here wait as pending until what they build on
        // is here, then they're checked and applied in causal order.
        const fresh: Array<TreeChange> = []
        for (const change of incoming) {
          const existing = document.history.changes.get(key(change)) ?? document.history.pending.get(key(change))
          if (existing !== undefined && !Equal.equals(existing, change)) {
            return yield* syncError(
              new Replica.DeltaError({
                reason: new Replica.ReplicaIdReused({ replica: Replica.ReplicaId.make(change.replica) })
              })
            )
          }
          if (existing === undefined && !History.has(document.history, change)) fresh.push(change)
        }
        const pushed = new Set(fresh.map(key))
        for (const change of fresh) document.history.pending.set(key(change), change)

        let rejected: string | undefined
        let malformed: string | undefined
        for (const change of History.takeReady(document.history)) {
          const problem = TreeReplica.check(document.text, change)
          if (problem !== undefined) {
            // A relay drops a bad change that was waiting from an earlier push
            // and carries on; the push that brought it has already been
            // answered.
            if (pushed.has(key(change))) malformed ??= problem
            if (isAuthority) break
            continue
          }
          const before = document.text.text
          const { patch } = TreeReplica.integrate(document.text, document.history, change)
          if (check === undefined) continue
          const verdict = yield* Effect.result(
            check({ document: document.id, user, patch, before, after: document.text.text })
          )
          if (Result.isFailure(verdict)) {
            // Start over without it. An authority keeps nothing pending, so
            // nothing else is lost.
            const rebuilt = TreeReplica.rebuild([...document.history.changes.values()].filter((c) => c !== change))
            document.text = rebuilt.text
            document.history = rebuilt.history
            rejected = verdict.failure.message
            break
          }
        }

        // A relay keeps changes that wait for something they build on; an
        // authority can't check those, so it drops them.
        for (const change of fresh) {
          if (document.history.pending.has(key(change)) && (isAuthority || malformed !== undefined)) {
            document.history.pending.delete(key(change))
            if (isAuthority && rejected === undefined) {
              malformed ??= `Change ${key(change)} builds on changes the authority doesn't have`
            }
          }
        }

        const kept = fresh.filter((change) =>
          document.history.changes.has(key(change)) || document.history.pending.has(key(change))
        )
        if (kept.length > 0) {
          yield* save(store, document, kept, user).pipe(
            Effect.tapError((error) =>
              Effect.andThen(
                Deferred.fail(document.failed, error),
                RcMap.invalidate(documents, document.id)
              )
            )
          )
          yield* PubSub.publish(
            document.updates,
            new Sync.Update({ delta: InternalDelta.fromChanges(kept), version: held(document.history) })
          )
        }

        if (malformed !== undefined) {
          return yield* syncError(new Replica.DeltaError({ reason: new Replica.Malformed({ message: malformed }) }))
        }
        const confirmed = held(document.history)
        return rejected === undefined
          ? new Sync.Accepted({ confirmed })
          : new Sync.PushRejected({ confirmed, message: rejected })
      }))
    }).pipe(Effect.scoped)

  return { subscribe, push }
})

const load = Effect.fnUntraced(function*(store: HistoryStore.HistoryStore["Service"], id: Replica.DocumentId) {
  const stored = yield* store.load(id).pipe(Effect.mapError(unavailable))
  const updates = yield* PubSub.unbounded<Sync.Update>()
  yield* Effect.addFinalizer(() => PubSub.shutdown(updates))
  const document: Document = {
    id,
    lock: yield* Semaphore.make(1),
    text: TreeText.make(),
    history: History.make(),
    owners: new Map(),
    updates,
    failed: yield* Deferred.make<never, Sync.SyncError>()
  }
  if (Option.isSome(stored)) {
    for (const [replica, owner] of Object.entries(stored.value.writers)) document.owners.set(replica, owner)
    const { history, text } = document
    for (const change of (stored.value.history as InternalDelta.DeltaImpl).changes.values()) {
      history.pending.set(key(change), change)
    }
    for (const change of History.takeReady(history)) {
      // Applied changes were checked before they were stored. One that was
      // stored while it waited, and turns out to be bad, is dropped.
      if (TreeReplica.check(text, change) === undefined) TreeReplica.integrate(text, history, change)
    }
  }
  return document
})

// Stores changes, and claims their replica ids for `user`.
const save = Effect.fnUntraced(function*(
  store: HistoryStore.HistoryStore["Service"],
  document: Document,
  changes: ReadonlyArray<TreeChange>,
  user: string
) {
  yield* store.append(document.id, InternalDelta.fromChanges(changes)).pipe(Effect.mapError(unavailable))
  for (const replica of new Set(changes.map((change) => change.replica))) {
    if (document.owners.has(replica)) continue
    yield* store.addWriter(document.id, Replica.ReplicaId.make(replica), user).pipe(Effect.mapError(unavailable))
    document.owners.set(replica, user)
  }
})

// What a document holds: its version vector, moved past changes that wait for
// something they build on, so their authors don't push them again.
const held = (history: History.History<TreeText.Op>): VersionVector.VersionVector => {
  const version: Record<string, number> = { ...history.version }
  const waiting = [...history.pending.values()].sort((a, b) => a.seq - b.seq)
  for (const change of waiting) {
    if ((version[change.replica] ?? 0) === change.seq) version[change.replica] = change.seq + change.length
  }
  return version
}

const syncError = (reason: Sync.SyncError["reason"]) => Effect.fail(new Sync.SyncError({ reason }))

const unavailable = (error: HistoryStore.HistoryStoreError) =>
  new Sync.SyncError({ reason: new Sync.Unavailable({ message: error.message }) })

/**
 * Serves a relay or authority over `Sync.Rpcs`. Every request is identified
 * and authorized with `Sync.Authorization`, and runs as its user.
 *
 * @internal
 */
export const layerHandlers = <I>(
  service: Context.Key<I, Sync.Server>
): Layer.Layer<Rpc.ToHandler<RpcGroup.Rpcs<typeof Sync.Rpcs>>, never, I | Sync.Authorization> =>
  Sync.Rpcs.toLayer(Effect.gen(function*() {
    const server = yield* service
    const authorization = yield* Sync.Authorization
    const enter = (
      headers: Parameters<Sync.Authorization["Service"]["identify"]>[0],
      document: Replica.DocumentId,
      access: "read" | "write"
    ) =>
      authorization.identify(headers).pipe(
        Effect.tap((user) => authorization.authorize({ user, document, access })),
        Effect.mapError((reason) => new Sync.SyncError({ reason }))
      )
    return Sync.Rpcs.of({
      "Sync.Subscribe": (request, { headers }) =>
        Stream.unwrap(Effect.map(
          enter(headers, request.document, "read"),
          (user) => Stream.provideService(server.subscribe(request), Sync.CurrentUser, user)
        )),
      "Sync.Push": (request, { headers }) =>
        Effect.flatMap(
          enter(headers, request.document, "write"),
          (user) => Effect.provideService(server.push(request), Sync.CurrentUser, user)
        )
    })
  }))

/**
 * A relay or authority on a server, reached over `Sync.Rpcs`.
 *
 * @internal
 */
export const remote: Effect.Effect<Sync.Server, never, RpcClient.Protocol | Scope.Scope> = Effect.gen(function*() {
  const client = yield* RpcClient.make(Sync.Rpcs)
  return {
    subscribe: (request) => client["Sync.Subscribe"](request).pipe(Stream.mapError(fromClient)),
    push: (request) => client["Sync.Push"](request).pipe(Effect.mapError(fromClient))
  }
})

// A transport failure is a lost connection, which connections retry.
const fromClient = (error: Sync.SyncError | RpcClientError.RpcClientError): Sync.SyncError =>
  error._tag === "SyncError" ? error : new Sync.SyncError({ reason: new Sync.Disconnected({ cause: error }) })
