import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as PubSub from "effect/PubSub"
import type * as Result from "effect/Result"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Replica from "../Replica.ts"
import * as TextPatch from "../TextPatch.ts"
import * as VersionVector from "../VersionVector.ts"
import { key } from "./change.ts"
import * as InternalDelta from "./delta.ts"
import * as InternalDocument from "./document.ts"
import * as History from "./history.ts"
import * as Path from "./path.ts"
import type * as InternalReplica from "./replica.ts"

// The replica behind both ReplicatedText and ReplicatedDocument: a document's
// state, the replica id it writes under, a lock that serializes everything,
// and what it tells subscribers. A model says how to read the state as a
// value and which changes fit.

/** @internal */
export interface Model {
  /** The value, worked out from the state. */
  readonly read: (document: InternalDocument.Document) => unknown
  /** What's wrong with a change, beyond what every document checks. */
  readonly check: (change: InternalDocument.AnyChange) => string | undefined
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
  /** How each text changed, by path key. */
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

/** @internal */
export const locked = <A, E, R>(engine: Engine, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  engine.lock.withPermit(effect)

// Tells subscribers about a change, with the value it leads to.
const publish = (engine: Engine, event: Omit<Event, "value">): Effect.Effect<void> =>
  Effect.suspend(() => {
    const value = engine.model.read(engine.state.document)
    return Effect.andThen(SubscriptionRef.set(engine.value, value), PubSub.publish(engine.events, { ...event, value }))
  })

// How each text changed from one state of the document to another.
const textsBetween = (
  before: InternalDocument.Document,
  after: InternalDocument.Document
): ReadonlyMap<string, TextPatch.TextPatch> => {
  const texts = new Map<string, TextPatch.TextPatch>()
  for (const { path } of [...InternalDocument.texts(before), ...InternalDocument.texts(after)]) {
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
      // behind theirs.
      const now = yield* Clock.currentTimeMillis
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
        : locked(engine, Effect.suspend(() => receive(engine, delta as InternalDelta.DeltaImpl))),
    accept: (delta) => locked(engine, Effect.suspend(() => receive(engine, delta as InternalDelta.DeltaImpl))),
    outgoing: (since) =>
      locked(
        engine,
        Effect.sync(() =>
          InternalDelta.fromChanges(
            History.since(history(), since).filter((change) => state.writers.has(change.replica))
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
    restore: (stored) =>
      locked(
        engine,
        Effect.suspend(() => {
          for (const writer of stored.writers) state.writers.add(writer)
          const all = InternalDelta.combine(stored.history, stored.unconfirmed)
          const { snapshot } = stored
          const empty = history().changes.size === 0 && history().pending.size === 0
          if (snapshot !== undefined && empty) {
            // Start from the snapshot's texts where the strategy can, and
            // merge only what came after it.
            const inSnapshot = (change: InternalDocument.AnyChange) =>
              change.seq + change.length <= VersionVector.get(snapshot.version, change.replica)
            const changes = [...all.changes.values()]
            const loaded = InternalDocument.load(
              engine.strategy,
              changes.filter(inSnapshot),
              snapshot.version,
              snapshot.texts
            )
            if (loaded !== undefined) {
              const texts = textsBetween(state.document, loaded)
              state.document = loaded
              const after = InternalDelta.fromChanges(changes.filter((change) => !inSnapshot(change)))
              return publish(engine, { _tag: "Remote", texts, overlaps: [], message: "" }).pipe(
                Effect.andThen(receive(engine, after)),
                Effect.asVoid
              )
            }
          }
          return Effect.asVoid(receive(engine, all))
        })
      ),
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
            confirmed.version = VersionVector.combine(confirmed.version, version)
            const before = state.document
            // Everything the authority doesn't have was made on top of the
            // rejected change, and nobody else has seen it, so it all goes.
            const { changes, pending } = history()
            state.document = InternalDocument.rebuild(
              engine.strategy,
              [...changes.values()].filter((change) => !isUnconfirmed(state, change))
            )
            for (const change of pending.values()) history().pending.set(key(change), change)
            // A new id, so sequence numbers are never reused and never skip.
            state.replica = yield* confirmed.freshId
            state.nextSeq = 0
            state.writers.add(state.replica)
            yield* publish(engine, {
              _tag: "Rejected",
              texts: textsBetween(before, state.document),
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
  delta: InternalDelta.DeltaImpl
): Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError> => {
  const { state } = engine
  const { history } = state.document
  // Checked before anything changes, so a bad delta leaves the replica as it was.
  for (const change of delta.changes.values()) {
    if (!InternalDocument.fitsStrategy(change, engine.strategy)) {
      return Effect.fail(deltaError(new Replica.StrategyMismatch({ expected: engine.strategy })))
    }
    const existing = history.changes.get(key(change)) ?? history.pending.get(key(change))
    if (
      (change.replica === state.replica && existing === undefined) ||
      (existing !== undefined && !Equal.equals(existing, change))
    ) {
      return Effect.fail(deltaError(new Replica.ReplicaIdReused({ replica: Replica.ReplicaId.make(change.replica) })))
    }
  }
  for (const change of delta.changes.values()) {
    if (!History.has(history, change)) history.pending.set(key(change), change)
  }

  const ready = History.takeReady(history)
  if (ready.length === 0) return Effect.succeed([])
  const integrated = InternalDocument.integrate(state.document, ready, { overlaps: true, check: engine.model.check })
  const overlaps = integrated.overlaps.map(toOverlap)
  const published = integrated.applied === 0
    ? Effect.void
    : publish(engine, { _tag: "Remote", texts: integrated.texts, overlaps, message: "" })
  if (integrated.problem === undefined) return Effect.as(published, overlaps)
  // The bad change is dropped; whatever was ready behind it keeps waiting.
  const [, ...behind] = ready.filter((change) => !history.changes.has(key(change)))
  for (const change of behind) history.pending.set(key(change), change)
  return Effect.andThen(published, Effect.fail(deltaError(new Replica.Malformed({ message: integrated.problem }))))
}

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
