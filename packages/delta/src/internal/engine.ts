import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import type * as Result from "effect/Result"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Replica from "../Replica.ts"
import * as TextPatch from "../TextPatch.ts"
import * as VersionVector from "../VersionVector.ts"
import { idKey, key, lastId } from "./change.ts"
import * as InternalDelta from "./delta.ts"
import * as InternalDocument from "./document.ts"
import * as History from "./history.ts"
import * as Path from "./path.ts"
import type * as InternalReplica from "./replica.ts"
import * as InternalText from "./text.ts"

// The replica behind both ReplicatedText and ReplicatedDocument: a document's
// state, the replica id it writes under, a lock that serializes everything,
// and what it tells subscribers. A model says how to read the state as a
// value. It keeps every change it can apply, including ones its schema has
// no place for: those don't show, nor do their overlaps, and they're passed
// on.

/** @internal */
export interface Model {
  /** The value, worked out from the state. */
  readonly read: (document: InternalDocument.Document) => unknown
  /** Whether the value shows the part an overlap is at. */
  readonly shows: (overlap: InternalDocument.Overlap) => boolean
  /** How the value reads what's written at each path, for overlaps. */
  readonly reading: InternalDocument.Reading
}

/**
 * What subscribers are told after every change.
 *
 * @internal
 */
export interface Event {
  readonly _tag: "Local" | "Remote" | "Rejected"
  /** The value after it. */
  readonly value: unknown
  /** How each text it edited changed, by path key; for a rejection, each it rolled back edits to. */
  readonly texts: ReadonlyMap<string, TextPatch.TextPatch>
  readonly overlaps: ReadonlyArray<Replica.Overlap>
  readonly message: string
}

/** @internal */
export interface Engine {
  readonly strategy: Replica.Strategy
  readonly lock: Semaphore.Semaphore
  readonly state: State
  readonly model: Model
  readonly value: SubscriptionRef.SubscriptionRef<unknown>
  readonly events: PubSub.PubSub<Event>
  readonly core: InternalReplica.Core
}

/** @internal */
export interface State {
  replica: Replica.ReplicaId
  nextSeq: number
  /** Every replica id this replica has written under, including ones restored from storage. */
  readonly writers: Set<string>
  document: InternalDocument.Document
  /** Only for replicas that answer to an authority. */
  readonly confirmed: ConfirmedState | undefined
}

/** @internal */
export interface ConfirmedState {
  /** What the authority has acknowledged. */
  version: VersionVector.VersionVector
  /** Where the next replica id comes from after a rejection. */
  readonly freshId: Effect.Effect<Replica.ReplicaId>
}

/** @internal */
export const randomReplicaId = Effect.gen(function*() {
  const crypto = yield* Crypto.Crypto
  return Replica.ReplicaId.make(yield* Effect.orDie(crypto.randomUUIDv4()))
})

/** @internal */
export const make = Effect.fnUntraced(function*(options: {
  readonly strategy: Replica.Strategy
  readonly replica: Replica.ReplicaId
  readonly confirmed: ConfirmedState | undefined
  readonly model: Model
}) {
  const state: State = {
    replica: options.replica,
    nextSeq: 0,
    writers: new Set([options.replica]),
    document: InternalDocument.make(options.strategy),
    confirmed: options.confirmed
  }
  const engine = {
    strategy: options.strategy,
    lock: yield* Semaphore.make(1),
    state,
    model: options.model,
    value: yield* SubscriptionRef.make(options.model.read(state.document)),
    events: yield* PubSub.unbounded<Event>()
  } as Omit<Engine, "core"> as Engine
  return Object.assign(engine, { core: makeCore(engine) })
})

/**
 * Runs `effect` holding the lock, and without interruption once it has it.
 * Everything under the lock is short, and an edit or merge stopped between
 * changing the state and publishing it would leave the replica sending a
 * change it doesn't show.
 *
 * @internal
 */
export const locked = <A, E, R>(engine: Engine, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  engine.lock.withPermit(Effect.uninterruptible(effect))

// Tells subscribers about a change, with the value it leads to.
const publish = (engine: Engine, event: Omit<Event, "value">): Effect.Effect<void> =>
  Effect.suspend(() => {
    const value = engine.model.read(engine.state.document)
    return Effect.andThen(SubscriptionRef.set(engine.value, value), PubSub.publish(engine.events, { ...event, value }))
  })

// How each text at `paths` changed from one state of the document to another.
const textsBetween = (
  before: InternalDocument.Document,
  after: InternalDocument.Document,
  paths: Iterable<Path.Path>
): ReadonlyMap<string, TextPatch.TextPatch> => {
  const texts = new Map<string, TextPatch.TextPatch>()
  for (const path of paths) {
    texts.set(
      Path.key(path),
      TextPatch.get(InternalDocument.textAt(before, path), InternalDocument.textAt(after, path))
    )
  }
  return texts
}

/**
 * Makes a local edit as one change, with the intents `plan` works out from the
 * document as it is. Nothing happens if they change nothing.
 *
 * @internal
 */
export const edit = <E>(
  engine: Engine,
  plan: (document: InternalDocument.Document) => Result.Result<ReadonlyArray<InternalDocument.Intent>, E>
): Effect.Effect<void, E> =>
  locked(
    engine,
    Effect.gen(function*() {
      const { state } = engine
      const intents = yield* Effect.fromResult(plan(state.document))
      if (intents.length === 0) return
      // Later than every change this replica has seen, even if its clock is
      // behind theirs, and whole, as a change carries it.
      const now = Math.floor(yield* Clock.currentTimeMillis)
      const edited = InternalDocument.edit(state.document, {
        replica: state.replica,
        seq: state.nextSeq,
        time: Math.max(now, state.document.history.time + 1)
      }, intents)
      if (edited === undefined) return
      state.nextSeq += edited.change.length
      yield* publish(engine, { _tag: "Local", texts: edited.texts, overlaps: [], message: "" })
    })
  )

// Syncing ------------------------------------------------------------------

const makeCore = (engine: Engine): InternalReplica.Core => {
  const { state } = engine
  const history = () => state.document.history
  const subscribeAll = Effect.map(
    PubSub.subscribe(engine.events),
    (subscription) => Stream.map(Stream.fromSubscription(subscription), () => undefined)
  )
  return {
    id: Effect.sync(() => state.replica),
    version: locked(engine, Effect.sync(() => ({ ...history().version }))),
    delta: (since) =>
      locked(
        engine,
        Effect.sync(() =>
          InternalDelta.fromChanges(History.since(history(), since).filter((change) => !isUnconfirmed(state, change)))
        )
      ),
    receive: (delta) =>
      state.confirmed !== undefined
        ? Effect.fail(deltaError(new Replica.AuthorityOnly()))
        : locked(engine, Effect.suspend(() => receive(engine, delta as InternalDelta.DeltaImpl, { overlaps: true }))),
    accept: (delta) =>
      locked(engine, Effect.suspend(() => receive(engine, delta as InternalDelta.DeltaImpl, { overlaps: true }))),
    outgoing: (since) =>
      locked(
        engine,
        Effect.sync(() =>
          InternalDelta.fromChanges(
            // A change of its own waits only on one it refused, as it loaded,
            // which no server would take either.
            History.since(history(), since).filter((change) =>
              state.writers.has(change.replica) && history().changes.has(key(change))
            )
          )
        )
      ),
    changed: subscribeAll,
    toStore: (since) =>
      locked(
        engine,
        Effect.sync(() => {
          const unconfirmed = [...history().changes.values()].filter((change) => isUnconfirmed(state, change))
          const version: Record<string, number> = { ...history().version }
          for (const change of unconfirmed) version[change.replica] = Math.min(version[change.replica]!, change.seq)
          return {
            history: InternalDelta.fromChanges(
              History.since(history(), since).filter((change) => !isUnconfirmed(state, change))
            ),
            unconfirmed: InternalDelta.fromChanges(unconfirmed),
            writers: [...state.writers].map((writer) => Replica.ReplicaId.make(writer)),
            version,
            texts: InternalDocument.texts(state.document)
          }
        })
      ),
    restore: (stored) => {
      // A change saved while it waited, which turned out to be bad, is
      // dropped again, as a relay drops it when it loads.
      const restore = (delta: InternalDelta.DeltaImpl, options: { readonly overlaps: boolean }) =>
        receive(engine, delta, options).pipe(
          Effect.catchReason("DeltaError", "Malformed", () => Effect.void),
          Effect.asVoid
        )
      return locked(
        engine,
        Effect.suspend(() => {
          const all = InternalDelta.combine(stored.history, stored.unconfirmed)
          // Refused as receiving them would be, before anything changes here,
          // and though a snapshot skips receiving them.
          const changes = [...all.changes.values()]
          const reused = InternalDocument.reusing(state.document.history, changes)
          const refused = refusal(engine, changes) ?? (reused === undefined ? undefined : reusedId(reused.replica))
          if (refused !== undefined) return Effect.fail(refused)
          for (const writer of stored.writers) state.writers.add(writer)
          // A store's history holds only what the authority had confirmed;
          // the rest is unconfirmed.
          if (state.confirmed !== undefined) {
            state.confirmed.version = VersionVector.combine(state.confirmed.version, InternalDelta.held(stored.history))
          }
          const { snapshot } = stored
          const empty = history().changes.size === 0 && history().pending.size === 0
          if (snapshot !== undefined && empty) {
            // Start from the snapshot's texts where the strategy can, and
            // merge only what came after it.
            const inSnapshot = (change: InternalDocument.AnyChange) =>
              change.seq + change.length <= VersionVector.get(snapshot.version, change.replica)
            const loaded = InternalDocument.load(
              engine.strategy,
              changes.filter(inSnapshot),
              snapshot.version,
              snapshot.texts
            )
            if (loaded !== undefined) {
              const texts = textsBetween(
                state.document,
                loaded,
                [...InternalDocument.texts(state.document), ...InternalDocument.texts(loaded)].map(({ path }) => path)
              )
              state.document = loaded
              const after = InternalDelta.fromChanges(changes.filter((change) => !inSnapshot(change)))
              // Loaded, as the snapshot's were, so they don't meet them.
              return Effect.andThen(
                publish(engine, { _tag: "Remote", texts, overlaps: [], message: "" }),
                restore(after, { overlaps: false })
              )
            }
          }
          return restore(all, { overlaps: true })
        })
      )
    },
    confirmed: state.confirmed === undefined ? undefined : {
      acknowledge: (version) =>
        locked(
          engine,
          Effect.sync(() => {
            state.confirmed!.version = VersionVector.combine(state.confirmed!.version, version)
          })
        ),
      reject: (version, message) =>
        locked(
          engine,
          Effect.gen(function*() {
            const confirmed = state.confirmed!
            // A new id, so sequence numbers are never reused and never skip.
            // Taken first, so if it can't be, nothing has changed.
            const replica = yield* confirmed.freshId
            // What the authority has, even if it once acknowledged more and
            // lost it: what it lacks is rolled back, not pushed again.
            confirmed.version = version
            const before = state.document
            // Everything the authority doesn't have was made on top of the
            // rejected change, so it all goes. That includes others' changes
            // made on top of this replica's: an authority that lost what it
            // accepted had sent it on.
            const { changes, pending } = history()
            const gone = new Set<string>()
            const dropped: Array<InternalDocument.AnyChange> = []
            const kept: Array<InternalDocument.AnyChange> = []
            // In the order they were applied, so parents come first.
            for (const change of changes.values()) {
              if (isUnconfirmed(state, change) || change.parents.some((parent) => gone.has(idKey(parent)))) {
                gone.add(idKey(lastId(change)))
                dropped.push(change)
              } else {
                kept.push(change)
              }
            }
            state.document = InternalDocument.rebuild(engine.strategy, kept)
            for (const change of pending.values()) history().pending.set(key(change), change)
            state.replica = replica
            state.nextSeq = 0
            state.writers.add(state.replica)
            yield* publish(engine, {
              _tag: "Rejected",
              // Only the texts it rolls back edits to: a rejection elsewhere
              // in a document leaves a text alone.
              texts: textsBetween(
                before,
                state.document,
                dropped.flatMap(({ ops }) => ops.filter(InternalText.isOp).map(InternalDocument.pathOf))
              ),
              overlaps: [],
              message
            })
          })
        )
    }
  }
}

// A confirmed replica's own changes that its authority hasn't acknowledged.
const isUnconfirmed = (state: State, change: InternalDocument.AnyChange): boolean =>
  state.confirmed !== undefined && state.writers.has(change.replica) &&
  change.seq + change.length > VersionVector.get(state.confirmed.version, change.replica)

const receive = (
  engine: Engine,
  delta: InternalDelta.DeltaImpl,
  options: { readonly overlaps: boolean }
): Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError> => {
  const { state } = engine
  // Checked before anything changes, so a bad delta leaves the replica as it was.
  const refused = refusal(engine, delta.changes.values())
  if (refused !== undefined) return Effect.fail(refused)
  const arrived = InternalDocument.arrive(state.document, [...delta.changes.values()])
  if (arrived.reused !== undefined) return Effect.fail(reusedId(arrived.reused))

  // A bad change is dropped, and the rest are applied without it.
  const integrated = InternalDocument.integrateReady(state.document, {
    overlaps: options.overlaps ? engine.model.reading : undefined
  })
  const overlaps = integrated.overlaps.filter(engine.model.shows).map(toOverlap)
  const published = integrated.applied === 0
    ? Effect.void
    : publish(engine, { _tag: "Remote", texts: integrated.texts, overlaps, message: "" })
  // A bad change that waited on these, if it came only with an earlier delta,
  // was answered then: these are fine, as a relay would say. One refused as
  // it arrived comes first, as servers say too.
  const problem = arrived.refused ?? integrated.problems.find(({ key }) => delta.changes.has(key))?.message
  if (problem === undefined) return Effect.as(published, overlaps)
  return Effect.andThen(published, Effect.fail(deltaError(new Replica.Malformed({ message: problem }))))
}

// Why the replica refuses changes whole, before taking any: one has another
// strategy, or is under its own replica id but not its own, made by another
// copy of it.
const refusal = (engine: Engine, changes: Iterable<InternalDocument.AnyChange>): Replica.DeltaError | undefined => {
  const { state } = engine
  for (const change of changes) {
    if (!InternalDocument.fitsStrategy(change, engine.strategy)) {
      return deltaError(new Replica.StrategyMismatch({ expected: engine.strategy }))
    }
    if (change.replica === state.replica && History.get(state.document.history, key(change)) === undefined) {
      return reusedId(change.replica)
    }
  }
  return undefined
}

const reusedId = (replica: string) =>
  deltaError(new Replica.ReplicaIdReused({ replica: Replica.ReplicaId.make(replica) }))

const toOverlap = (overlap: InternalDocument.Overlap): Replica.Overlap => {
  const replicas = overlap.replicas.map((replica) => Replica.ReplicaId.make(replica))
  switch (overlap._tag) {
    case "Text":
      return {
        _tag: "Text",
        path: overlap.path,
        range: { start: overlap.start, end: overlap.end },
        anchors: overlap.anchors,
        replicas
      }
    case "Value":
      return { _tag: "Value", path: overlap.path, replicas, kept: Replica.ReplicaId.make(overlap.kept) }
    case "Removed":
      return { _tag: "Removed", path: overlap.path, replicas }
  }
}

const deltaError = (reason: Replica.DeltaError["reason"]) => new Replica.DeltaError({ reason })
