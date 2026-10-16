import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import type * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
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
import * as TextPatch from "../TextPatch.ts"
import type * as VersionVector from "../VersionVector.ts"
import { key } from "./change.ts"
import * as InternalDelta from "./delta.ts"
import * as InternalDocument from "./document.ts"
import * as History from "./history.ts"
import * as Path from "./path.ts"

// The core shared by relays and authorities. Each document is held in memory
// while it's in use: loaded from the history store when first asked for, and
// released a minute after its last use. Pushed changes are checked, stored and
// only then sent to subscribers, so no subscriber sees a change the store
// doesn't have. A server has no schema: changes say what they do, so it
// applies them as they are.

interface Document {
  readonly id: Replica.DocumentId
  readonly lock: Semaphore.Semaphore
  /**
   * Its strategy is undefined until it's known: from what's stored, or else
   * from the first replica to subscribe or push a text op.
   */
  state: InternalDocument.Document
  /** Who each replica id belongs to: the first user who wrote with it. */
  readonly owners: Map<string, string>
  readonly updates: PubSub.PubSub<Sync.Update>
  /**
   * Fails if a push stops while saving, failed or interrupted. That ends
   * every subscription, and the document is loaded again from the store, so
   * nothing in memory gets ahead of it.
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
    yield* ensureServing(document)
    return document
  })

  const subscribe = (request: Sync.SubscribeRequest): Stream.Stream<Sync.Update, Sync.SyncError> =>
    Stream.unwrap(Effect.gen(function*() {
      if (isAuthority && request.strategy !== "tree") {
        return yield* syncError(
          new Sync.ProtocolViolation({ message: "Authorities only have the tree strategy so far" })
        )
      }
      if (request.authority !== isAuthority) {
        return yield* syncError(
          new Sync.ProtocolViolation({
            message: isAuthority
              ? "This server is an authority: connect with makeConfirmed"
              : "This server is a relay, not an authority"
          })
        )
      }
      const document = yield* open(request.document)
      return yield* document.lock.withPermit(Effect.gen(function*() {
        const { history, strategy } = document.state
        if (strategy !== undefined && strategy !== request.strategy) {
          return yield* syncError(
            new Sync.ProtocolViolation({ message: `Document ${document.id} uses the ${strategy} strategy` })
          )
        }
        document.state.strategy = request.strategy
        // Subscribed under the lock, so no update lands between the first one
        // and the rest.
        const subscription = yield* PubSub.subscribe(document.updates)
        // Only what's been checked and applied: changes that wait for what
        // they build on are sent once they've joined the document.
        const first = new Sync.Update({
          delta: InternalDelta.fromChanges(
            History.since(history, request.version).filter((change) => history.changes.has(key(change)))
          ),
          version: held(history)
        })
        return Stream.concat(Stream.succeed(first), Stream.fromSubscription(subscription)).pipe(
          Stream.interruptWhen(Deferred.await(document.failed))
        )
      }))
    }))

  // Stops serving a document, so it's loaded again from the store, which has
  // everything any subscriber has seen. Only the push holding its lock calls
  // this, and pushes after it change nothing, so the id still names this copy.
  const reload = (document: Document, error: Sync.SyncError) =>
    Effect.uninterruptible(
      Effect.andThen(Deferred.fail(document.failed, error), RcMap.invalidate(documents, document.id))
    )

  const push = (request: Sync.PushRequest): Effect.Effect<Sync.Ack, Sync.SyncError> =>
    Effect.gen(function*() {
      const user = yield* Sync.CurrentUser
      const document = yield* open(request.document)
      // Until its changes are saved and sent, the document holds changes the
      // store doesn't. Stopped before saving them, by an interruption or a
      // check that dies, it drops them, which nobody else has seen. Stopped
      // while saving or sending them, the store may have them and subscribers
      // don't, so the document starts over from the store.
      let unsaved: ReadonlySet<string> | undefined
      let saving = false
      return yield* Effect.gen(function*() {
        // It may have failed while this push waited.
        yield* ensureServing(document)
        const delta = request.delta as InternalDelta.DeltaImpl
        const incoming = [...delta.changes.values()]
        for (const change of incoming) {
          const owner = document.owners.get(change.replica)
          if (owner !== undefined && owner !== user) {
            return yield* syncError(
              new Sync.Forbidden({ message: `Replica id ${change.replica} belongs to another user` })
            )
          }
        }

        const { state } = document
        if (incoming.length === 0) return new Sync.Accepted({ confirmed: held(state.history) })
        // A document's strategy is its first text op's. It's learned from
        // the changes that are kept, below.
        const strategy = state.strategy ?? (isAuthority ? "tree" : InternalDocument.strategyOf(incoming))
        for (const change of incoming) {
          if (!InternalDocument.fitsStrategy(change, strategy)) {
            return yield* syncError(
              new Replica.DeltaError({ reason: new Replica.StrategyMismatch({ expected: strategy ?? "tree" }) })
            )
          }
        }
        // Write times follow the clock, so one far ahead of this server's is a
        // broken clock or a forgery, and everyone who saw it couldn't write
        // later than it.
        const now = yield* Clock.currentTimeMillis
        const early = incoming.find((change) => change.time > now + maxAhead)
        if (early !== undefined) {
          return yield* syncError(
            new Replica.DeltaError({
              reason: new Replica.Malformed({ message: `Change ${key(early)} is stamped too far in the future` })
            })
          )
        }

        // Changes that are new here wait as pending until what they build on
        // is here, then they're checked and applied in causal order, as on
        // replicas.
        const { fresh, refused, reused } = InternalDocument.arrive(state, incoming)
        if (reused !== undefined) {
          return yield* syncError(
            new Replica.DeltaError({ reason: new Replica.ReplicaIdReused({ replica: Replica.ReplicaId.make(reused) }) })
          )
        }
        unsaved = new Set(fresh.map(key))
        // What this push may apply: these, and changes that waited for them.
        const waiting = [...state.history.pending.values()]

        let rejected: string | undefined
        let malformed = refused
        if (check === undefined) {
          // A relay drops a bad change and carries on. If the bad change came
          // only with an earlier push, that push has already been answered.
          for (const problem of InternalDocument.integrateReady(state, { overlaps: false }).problems) {
            if (delta.changes.has(problem.key)) malformed ??= problem.message
          }
        } else {
          for (const change of History.takeReady(state.history)) {
            // A change all at the root is a replicated text's.
            const isText = change.ops.every((op) => Path.isRoot(InternalDocument.pathOf(op)))
            const before = isText ? InternalDocument.textAt(state, Path.root) : jsonOf(state)
            // Each path holds one kind of thing, so the JSON a check reads is
            // never ambiguous. A change that breaks that is rejected, as a
            // check rejects, so the replica that made it rolls it back; but
            // one that's malformed is refused as that, as replicas refuse it.
            const unkept = InternalDocument.checkPaths(state, change)
            const { problem, texts } = InternalDocument.integrate(state, [change])
            if (problem !== undefined) {
              malformed = problem
              break
            }
            if (unkept !== undefined) {
              document.state = without(state, (c) => c === change)
              rejected = unkept
              break
            }
            const verdict = yield* Effect.result(check(
              isText
                ? {
                  _tag: "Text",
                  document: document.id,
                  user,
                  patch: texts.get(Path.key(Path.root)) ?? TextPatch.empty,
                  before: before as string,
                  after: InternalDocument.textAt(state, Path.root)
                }
                : {
                  _tag: "Document",
                  document: document.id,
                  user,
                  ...proposed(before as ReturnType<typeof jsonOf>, jsonOf(state)),
                  paths: pathsOf(change),
                  delta: InternalDelta.fromChanges([change])
                }
            ))
            if (Result.isFailure(verdict)) {
              document.state = without(state, (c) => c === change)
              rejected = verdict.failure.message
              break
            }
          }
        }
        const { history } = document.state

        // A relay keeps changes that wait for something they build on, as a
        // replica does, whatever else the push brought; an authority can't
        // check those, so it drops them.
        for (const change of fresh) {
          if (isAuthority && history.pending.has(key(change))) {
            history.pending.delete(key(change))
            if (rejected === undefined) {
              malformed ??= `Change ${key(change)} builds on changes the authority doesn't have`
            }
          }
        }

        const kept = fresh.filter((change) => history.changes.has(key(change)) || history.pending.has(key(change)))
        // Subscribers are sent what's been checked and applied, whoever
        // pushed it: a change that waits might turn out bad.
        const applied = waiting.filter((change) => history.changes.has(key(change)))
        // Applying a text op sets it, but kept changes that wait count too:
        // the store has them, so a reload would learn it from them.
        document.state.strategy ??= InternalDocument.strategyOf(kept)
        if (kept.length > 0) {
          saving = true
          yield* save(store, document, kept, user)
          if (applied.length > 0) {
            yield* PubSub.publish(
              document.updates,
              new Sync.Update({ delta: InternalDelta.fromChanges(applied), version: held(history) })
            )
          }
        }
        unsaved = undefined
        saving = false

        if (malformed !== undefined) {
          return yield* syncError(new Replica.DeltaError({ reason: new Replica.Malformed({ message: malformed }) }))
        }
        const confirmed = held(history)
        return rejected === undefined
          ? new Sync.Accepted({ confirmed })
          : new Sync.PushRejected({ confirmed, message: rejected })
      }).pipe(
        // Under the lock, so no other push sees what this one left. Only an
        // authority's check can stop it before it saves.
        Effect.onExit((exit) => {
          if (Exit.isSuccess(exit)) return Effect.void
          // Subscribers are told why: the store's error, or that it stopped.
          if (saving) return reload(document, Option.getOrElse(Cause.findErrorOption(exit.cause), stoppedSaving))
          if (unsaved !== undefined) {
            const dropped = unsaved
            document.state = without(document.state, (change) => dropped.has(key(change)))
          }
          return Effect.void
        }),
        document.lock.withPermit
      )
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
    state: InternalDocument.make(undefined),
    owners: new Map(),
    updates,
    failed: yield* Deferred.make<never, Sync.SyncError>()
  }
  if (Option.isSome(stored)) {
    for (const [replica, owner] of Object.entries(stored.value.writers)) document.owners.set(replica, owner)
    const changes = [...(stored.value.history as InternalDelta.DeltaImpl).changes.values()]
    const { state } = document
    state.strategy = InternalDocument.strategyOf(changes)
    for (const change of changes) state.history.pending.set(key(change), change)
    // Applied changes were checked before they were stored. One that was
    // stored while it waited, and turns out to be bad, is dropped.
    InternalDocument.integrateReady(state, { overlaps: false })
  }
  return document
})

// An authority's document without some of its changes, started over from the
// rest. An authority keeps nothing pending, so nothing else is lost.
const without = (
  state: InternalDocument.Document,
  dropped: (change: InternalDocument.AnyChange) => boolean
): InternalDocument.Document =>
  InternalDocument.rebuild(state.strategy, [...state.history.changes.values()].filter((change) => !dropped(change)))

// A document as a proposal has it: its JSON, and the paths in the JSON of the
// parts that come and go and exist.
const jsonOf = (state: InternalDocument.Document) => {
  const exists: Array<Replica.Path> = []
  return { json: InternalDocument.toJson(state, exists), exists }
}

const proposed = (before: ReturnType<typeof jsonOf>, after: ReturnType<typeof jsonOf>) => ({
  before: before.json,
  after: after.json,
  exists: { before: before.exists, after: after.exists }
})

// The paths a change wrote to, in order.
const pathsOf = (change: InternalDocument.AnyChange): ReadonlyArray<Replica.Path> => {
  const paths = new Map<string, Replica.Path>()
  for (const op of change.ops) {
    const path = InternalDocument.pathOf(op)
    paths.set(Path.key(path), path)
  }
  return [...paths.values()]
}

// Stores changes, and claims their replica ids for `user`.
const save = Effect.fnUntraced(function*(
  store: HistoryStore.HistoryStore["Service"],
  document: Document,
  changes: ReadonlyArray<InternalDocument.AnyChange>,
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
const held = (history: History.History<unknown> | undefined): VersionVector.VersionVector => {
  if (history === undefined) return {}
  const version: Record<string, number> = { ...history.version }
  const waiting = [...history.pending.values()].sort((a, b) => a.seq - b.seq)
  for (const change of waiting) {
    if ((version[change.replica] ?? 0) === change.seq) version[change.replica] = change.seq + change.length
  }
  return version
}

const syncError = (reason: Sync.SyncError["reason"]) => Effect.fail(new Sync.SyncError({ reason }))

const stoppedSaving = () =>
  new Sync.SyncError({ reason: new Sync.Unavailable({ message: "A push stopped while saving" }) })

// Fails once a document has stopped being served, while it's reloaded.
const ensureServing = (document: Document): Effect.Effect<void, Sync.SyncError> =>
  Effect.flatMap(
    Deferred.isDone(document.failed),
    (failed) =>
      failed ? syncError(new Sync.Unavailable({ message: `Document ${document.id} is reloading` })) : Effect.void
  )

// How far ahead of a server's clock a change's write time may be.
const maxAhead = 24 * 60 * 60 * 1000

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
