/**
 * A replicated text is a string that many replicas edit at once: tabs,
 * devices, servers. Every replica accepts edits immediately, without waiting on
 * the network, and replicas that have received the same changes show the same
 * text, whatever order the changes arrived in.
 *
 * Edit it with `insert`, `remove` and `applyPatch` (what editors use), or with
 * `set` and `update` (for apps that hold whole strings). Observe it with
 * `changes` (each new value) or `subscribe` (each edit, as a `TextPatch`). Keep
 * stable positions with `anchor` and `locate`.
 *
 * Replicas exchange changes with `Replica.exchange`, or over a network with the
 * `Sync`, `Relay` and `Authority` modules.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as SecureRandom from "effect/crypto/SecureRandom"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import { dual } from "effect/Function"
import * as Option from "effect/Option"
import { hasProperty } from "effect/Predicate"
import * as PubSub from "effect/PubSub"
import type * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Authority from "./Authority.ts"
import * as InternalAnchor from "./internal/anchor.ts"
import { key } from "./internal/change.ts"
import * as InternalConnection from "./internal/connection.ts"
import * as InternalDelta from "./internal/delta.ts"
import * as History from "./internal/history.ts"
import * as InternalReplica from "./internal/replica.ts"
import * as InternalText from "./internal/text.ts"
import * as Replica from "./Replica.ts"
import type * as Sync from "./Sync.ts"
import * as TextPatch from "./TextPatch.ts"
import * as VersionVector from "./VersionVector.ts"

const TypeId = "~effect-delta/ReplicatedText"

// Implementation ----------------------------------------------------------

interface ReplicatedTextImpl extends ReplicatedText {
  readonly core: InternalReplica.Core
  readonly semaphore: Semaphore.Semaphore
  readonly state: State
  readonly textRef: SubscriptionRef.SubscriptionRef<string>
  readonly edits: PubSub.PubSub<Edit>
}

interface State {
  replica: Replica.ReplicaId
  nextSeq: number
  /** Every replica id this replica has written under, including ones restored from storage. */
  readonly writers: Set<string>
  /** The text and its history, under the replica's strategy. */
  text: InternalText.Text
  /** Only for replicas that answer to an authority. */
  readonly confirmed: ConfirmedState | undefined
}

interface ConfirmedState {
  /** What the authority has acknowledged. */
  version: VersionVector.VersionVector
  /** Where the next replica id comes from after a rejection. */
  readonly freshId: Effect.Effect<Replica.ReplicaId>
}

const Proto = {
  ...InternalReplica.Proto,
  [TypeId]: TypeId,
  toJSON(this: ReplicatedTextImpl) {
    return { _id: "ReplicatedText", strategy: this.strategy, text: this.state.text.value }
  }
}

const randomReplicaId = Effect.gen(function*() {
  const random = yield* SecureRandom.SecureRandom
  return Replica.makeId(yield* random.uuidV4())
})

const makeWith = Effect.fnUntraced(function*(options: {
  readonly strategy: Replica.Strategy
  readonly replica: Replica.ReplicaId
  readonly confirmed: ConfirmedState | undefined
}) {
  const state: State = {
    replica: options.replica,
    nextSeq: 0,
    writers: new Set([options.replica]),
    text: InternalText.make(options.strategy),
    confirmed: options.confirmed
  }
  const self: ReplicatedTextImpl = Object.assign(Object.create(Proto), {
    strategy: options.strategy,
    semaphore: yield* Semaphore.make(1),
    state,
    textRef: yield* SubscriptionRef.make(""),
    edits: yield* PubSub.unbounded<Edit>()
  })
  return Object.assign(self, { core: makeCore(self) })
})

const makeImpl = Effect.fnUntraced(function*(options?: Options) {
  const replica = options?.replica ?? (yield* randomReplicaId)
  return yield* makeWith({ strategy: options?.strategy ?? "tree", replica, confirmed: undefined }) as Effect.Effect<
    ReplicatedText
  >
})

const makeConfirmedImpl = Effect.fnUntraced(function*(options: ConfirmedOptions) {
  // Kept for later: a rejection moves the replica to a new id.
  const random = yield* SecureRandom.SecureRandom
  const freshId = Effect.provideService(randomReplicaId, SecureRandom.SecureRandom, random)
  const self = yield* makeWith({
    strategy: "tree",
    replica: options.replica ?? (yield* freshId),
    confirmed: { version: {}, freshId }
  })
  const connection = yield* InternalConnection.connect(self, yield* Authority.Authority, {
    room: options.room,
    authority: true,
    retry: options.retry
  })
  return Object.assign(self, { connection }) as Confirmed
})

const impl = (self: ReplicatedText): ReplicatedTextImpl => self as ReplicatedTextImpl

const locked = <A, E, R>(self: ReplicatedText, effect: Effect.Effect<A, E, R>) =>
  impl(self).semaphore.withPermit(effect)

// Publishes how the text changed, to `changes` and `subscribe`.
const publish = (self: ReplicatedTextImpl, edit: Edit): Effect.Effect<void> =>
  Effect.andThen(
    SubscriptionRef.set(self.textRef, self.state.text.value),
    PubSub.publish(self.edits, edit)
  )

// Reading ------------------------------------------------------------------

const getImpl = (self: ReplicatedText): Effect.Effect<string> =>
  locked(self, Effect.sync(() => impl(self).state.text.value))

const changesImpl = (self: ReplicatedText): Stream.Stream<string> => SubscriptionRef.changes(impl(self).textRef)

const subscribeImpl = (
  self: ReplicatedText
): Effect.Effect<Subscription, never, Scope.Scope> =>
  locked(
    self,
    Effect.gen(function*() {
      // Taken under the lock, so no edit lands between reading the text and
      // subscribing.
      const subscription = yield* PubSub.subscribe(impl(self).edits)
      return { text: impl(self).state.text.value, edits: Stream.fromSubscription(subscription) }
    })
  )

// Editing ------------------------------------------------------------------

// The editing steps below expect the caller to hold the lock, so reading the
// text and changing it happen with no edit or merge in between.
const applyLocked = (self: ReplicatedText, patch: TextPatch.TextPatch): Effect.Effect<void, ReplicatedTextError> =>
  Effect.suspend(() => {
    const s = impl(self).state
    const invalid = validate(s.text.value, patch)
    if (invalid !== undefined) return Effect.fail(invalid)
    const edited = s.text.edit(patch, { replica: s.replica, seq: s.nextSeq })
    if (edited === undefined) return Effect.void
    s.nextSeq += edited.change.length
    return publish(impl(self), { _tag: "Local", patch: edited.patch })
  })

// A replacement made by `TextPatch.get` stays inside the text and keeps
// surrogate pairs whole, so it can't fail.
const replaceLocked = (self: ReplicatedText, text: string): Effect.Effect<void> =>
  Effect.orDie(Effect.suspend(() => applyLocked(self, TextPatch.get(impl(self).state.text.value, text))))

const applyPatchImpl = (
  self: ReplicatedText,
  patch: TextPatch.TextPatch
): Effect.Effect<void, ReplicatedTextError> => locked(self, applyLocked(self, patch))

const setImpl = (self: ReplicatedText, text: string): Effect.Effect<void> => locked(self, replaceLocked(self, text))

const updateImpl = (self: ReplicatedText, f: (text: string) => string): Effect.Effect<void> =>
  locked(self, Effect.suspend(() => replaceLocked(self, f(impl(self).state.text.value))))

const modifyImpl = <B>(
  self: ReplicatedText,
  f: (text: string) => readonly [B, TextPatch.TextPatch]
): Effect.Effect<B, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const [result, patch] = f(impl(self).state.text.value)
      return Effect.as(applyLocked(self, patch), result)
    })
  )

// Checks a patch against the text it's for: it must stay inside the text and
// never cut a surrogate pair (half of an emoji) apart.
const validate = (text: string, patch: TextPatch.TextPatch): ReplicatedTextError | undefined => {
  let current = text
  let position = 0
  for (const operation of patch) {
    switch (operation._tag) {
      case "Retain":
        position += operation.length
        if (position > current.length) return outOfBounds(position, current.length)
        break
      case "Insert":
        if (splitsPair(current, position)) return splitsSurrogatePair(position)
        current = current.slice(0, position) + operation.text + current.slice(position)
        position += operation.text.length
        break
      case "Remove":
        if (position + operation.length > current.length) {
          return outOfBounds(position + operation.length, current.length)
        }
        if (splitsPair(current, position)) return splitsSurrogatePair(position)
        if (splitsPair(current, position + operation.length)) return splitsSurrogatePair(position + operation.length)
        current = current.slice(0, position) + current.slice(position + operation.length)
        break
    }
  }
  return undefined
}

const splitsPair = (text: string, index: number): boolean =>
  index > 0 && index < text.length &&
  isHighSurrogate(text.charCodeAt(index - 1)) && isLowSurrogate(text.charCodeAt(index))

const isHighSurrogate = (code: number): boolean => code >= 0xd800 && code <= 0xdbff
const isLowSurrogate = (code: number): boolean => code >= 0xdc00 && code <= 0xdfff

// Anchors ------------------------------------------------------------------

const anchorImpl = (
  self: ReplicatedText,
  index: number,
  side: TextPatch.Side
): Effect.Effect<Anchor, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const text = impl(self).state.text
      if (index < 0 || index > text.value.length) return Effect.fail(outOfBounds(index, text.value.length))
      if (splitsPair(text.value, index)) return Effect.fail(splitsSurrogatePair(index))
      return Effect.succeed(text.anchor(index, side))
    })
  )

const locateImpl = (self: ReplicatedText, anchor: Anchor): Effect.Effect<Option.Option<number>> =>
  locked(self, Effect.sync(() => impl(self).state.text.locate(anchor)))

const anchorRangeImpl = (
  self: ReplicatedText,
  range: Range
): Effect.Effect<AnchorRange, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const text = impl(self).state.text
      const length = text.value.length
      if (range.start < 0 || range.start > range.end) return Effect.fail(outOfBounds(range.start, length))
      if (range.end > length) return Effect.fail(outOfBounds(range.end, length))
      if (splitsPair(text.value, range.start)) return Effect.fail(splitsSurrogatePair(range.start))
      if (splitsPair(text.value, range.end)) return Effect.fail(splitsSurrogatePair(range.end))
      // Leaning inward, so text typed at either edge stays outside.
      return Effect.succeed({ start: text.anchor(range.start, "before"), end: text.anchor(range.end, "after") })
    })
  )

const locateRangeImpl = (
  self: ReplicatedText,
  range: AnchorRange
): Effect.Effect<Option.Option<Range>> =>
  locked(
    self,
    Effect.sync(() => {
      const text = impl(self).state.text
      const start = text.locate(range.start)
      const end = text.locate(range.end)
      if (Option.isNone(start) || Option.isNone(end) || end.value <= start.value) return Option.none()
      return Option.some({ start: start.value, end: end.value })
    })
  )

// Authorship ---------------------------------------------------------------

const authorshipImpl = (self: ReplicatedText): Effect.Effect<ReadonlyArray<Authorship>> =>
  locked(
    self,
    Effect.sync(() => {
      const authored: Array<Authorship> = []
      let start = 0
      for (const run of impl(self).state.text.authorship()) {
        authored.push({ range: { start, end: start + run.length }, replica: Replica.makeId(run.replica) })
        start += run.length
      }
      return authored
    })
  )

// Syncing ------------------------------------------------------------------

const makeCore = (self: ReplicatedTextImpl): InternalReplica.Core => {
  const { state } = self
  const subscribeAll = Effect.map(
    PubSub.subscribe(self.edits),
    (subscription) => Stream.map(Stream.fromSubscription(subscription), () => undefined)
  )
  return {
    id: Effect.sync(() => state.replica),
    version: locked(self, Effect.sync(() => ({ ...state.text.history.version }))),
    delta: (since) =>
      locked(
        self,
        Effect.sync(() =>
          InternalDelta.fromChanges(
            History.since(state.text.history, since).filter((change) => !isUnconfirmed(state, change))
          )
        )
      ),
    receive: (delta) =>
      state.confirmed !== undefined
        ? Effect.fail(deltaError(new Replica.AuthorityOnly()))
        : locked(self, Effect.suspend(() => receive(self, delta as InternalDelta.DeltaImpl))),
    accept: (delta) => locked(self, Effect.suspend(() => receive(self, delta as InternalDelta.DeltaImpl))),
    outgoing: (since) =>
      locked(
        self,
        Effect.sync(() =>
          InternalDelta.fromChanges(
            History.since(state.text.history, since).filter((change) => state.writers.has(change.replica))
          )
        )
      ),
    changed: subscribeAll,
    toStore: (since) =>
      locked(
        self,
        Effect.sync(() => {
          const unconfirmed = [...state.text.history.changes.values()].filter((change) => isUnconfirmed(state, change))
          const version: Record<string, number> = { ...state.text.history.version }
          for (const change of unconfirmed) version[change.replica] = Math.min(version[change.replica]!, change.seq)
          return {
            history: InternalDelta.fromChanges(
              History.since(state.text.history, since).filter((change) => !isUnconfirmed(state, change))
            ),
            unconfirmed: InternalDelta.fromChanges(unconfirmed),
            writers: [...state.writers].map((writer) => Replica.makeId(writer)),
            version,
            text: state.text.value
          }
        })
      ),
    restore: (stored) =>
      locked(
        self,
        Effect.suspend(() => {
          for (const writer of stored.writers) state.writers.add(writer)
          const all = InternalDelta.combine(stored.history, stored.unconfirmed)
          const { snapshot } = stored
          if (snapshot !== undefined) {
            // Start from the snapshot's text where the strategy can, and merge
            // only what came after it.
            const inSnapshot = (change: InternalText.AnyChange) =>
              change.seq + change.length <= VersionVector.get(snapshot.version, change.replica)
            const changes = [...all.changes.values()]
            if (state.text.load(changes.filter(inSnapshot), snapshot.version, snapshot.text)) {
              const after = InternalDelta.fromChanges(changes.filter((change) => !inSnapshot(change)))
              return publish(self, { _tag: "Remote", patch: TextPatch.insert(0, snapshot.text), overlaps: [] }).pipe(
                Effect.andThen(receive(self, after)),
                Effect.asVoid
              )
            }
          }
          return Effect.asVoid(receive(self, all))
        })
      ),
    confirmed: state.confirmed === undefined ? undefined : {
      acknowledge: (version) =>
        locked(
          self,
          Effect.sync(() => {
            state.confirmed!.version = VersionVector.combine(state.confirmed!.version, version)
          })
        ),
      reject: (version, message) =>
        locked(
          self,
          Effect.gen(function*() {
            const confirmed = state.confirmed!
            confirmed.version = VersionVector.combine(confirmed.version, version)
            const before = state.text.value
            // Everything the authority doesn't have was made on top of the
            // rejected change, and nobody else has seen it, so it all goes.
            const { changes, pending } = state.text.history
            state.text = InternalText.rebuild(
              state.text.strategy,
              [...changes.values()].filter((change) => !isUnconfirmed(state, change))
            )
            for (const change of pending.values()) state.text.history.pending.set(key(change), change)
            // A new id, so sequence numbers are never reused and never skip.
            state.replica = yield* confirmed.freshId
            state.nextSeq = 0
            state.writers.add(state.replica)
            yield* publish(self, { _tag: "Rejected", patch: TextPatch.get(before, state.text.value), message })
          })
        )
    }
  }
}

// A confirmed replica's own changes that its authority hasn't acknowledged.
const isUnconfirmed = (state: State, change: InternalText.AnyChange): boolean =>
  state.confirmed !== undefined && state.writers.has(change.replica) &&
  change.seq + change.length > VersionVector.get(state.confirmed.version, change.replica)

const receive = (
  self: ReplicatedTextImpl,
  delta: InternalDelta.DeltaImpl
): Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError> => {
  const { state } = self
  const { history } = state.text
  // Checked before anything changes, so a bad delta leaves the replica as it was.
  for (const change of delta.changes.values()) {
    if (InternalText.strategyOf(change) !== state.text.strategy) {
      return Effect.fail(deltaError(new Replica.StrategyMismatch({ expected: state.text.strategy })))
    }
    const existing = history.changes.get(key(change)) ?? history.pending.get(key(change))
    if (
      (change.replica === state.replica && existing === undefined) ||
      (existing !== undefined && !Equal.equals(existing, change))
    ) {
      return Effect.fail(deltaError(new Replica.ReplicaIdReused({ replica: Replica.makeId(change.replica) })))
    }
  }
  for (const change of delta.changes.values()) {
    if (!History.has(history, change)) history.pending.set(key(change), change)
  }

  const ready = History.takeReady(history)
  if (ready.length === 0) return Effect.succeed([])
  const { overlaps: found, patch, problem } = state.text.integrate(ready, { overlaps: true })
  const overlaps: ReadonlyArray<Replica.Overlap> = found.map((overlap) => ({
    _tag: "Text",
    range: { start: overlap.start, end: overlap.end },
    anchors: overlap.anchors,
    replicas: overlap.replicas.map((replica) => Replica.makeId(replica))
  }))
  const published = patch.length === 0 && overlaps.length === 0
    ? Effect.void
    : publish(self, { _tag: "Remote", patch, overlaps })
  if (problem === undefined) return Effect.as(published, overlaps)
  // The bad change is dropped; whatever was ready behind it keeps waiting.
  const [, ...behind] = ready.filter((change) => !history.changes.has(key(change)))
  for (const change of behind) history.pending.set(key(change), change)
  return Effect.andThen(published, Effect.fail(deltaError(new Replica.Malformed({ message: problem }))))
}

const deltaError = (reason: Replica.DeltaError["reason"]) => new Replica.DeltaError({ reason })

// Public API --------------------------------------------------------------

/**
 * A replica of a text.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface ReplicatedText extends Replica.Replica<string> {
  readonly [TypeId]: typeof TypeId
}

/**
 * Options for `make`.
 *
 * **Details**
 *
 * - `strategy`: how concurrent edits merge. `"tree"` (the default) or `"replay"`.
 * - `replica`: the replica id to write changes under. Leave it out to get a
 *   fresh random one, which is almost always right: a replica id must never be
 *   shared by two replicas.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Options {
  readonly strategy?: Replica.Strategy | undefined
  readonly replica?: Replica.ReplicaId | undefined
}

/**
 * Creates an empty replicated text.
 *
 * **Details**
 *
 * Without a `replica` option, a fresh random replica id comes from
 * `SecureRandom`, which `SecureRandom.layerWebCrypto` provides on Node.js,
 * browsers and Workers. With one, no `SecureRandom` is needed.
 *
 * **Example** (Creating and editing a text)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({
 *     replica: Replica.makeId("laptop")
 *   })
 *   yield* ReplicatedText.insert(notes, 0, "Hello")
 *   yield* ReplicatedText.insert(notes, 5, ", world")
 *   return yield* ReplicatedText.get(notes)
 * })
 *
 * await Effect.runPromise(program) // => "Hello, world"
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const make: {
  (options: Options & { readonly replica: Replica.ReplicaId }): Effect.Effect<ReplicatedText>
  (options?: Options): Effect.Effect<ReplicatedText, never, SecureRandom.SecureRandom>
} = makeImpl as any

/**
 * A replicated text that answers to an authority: a server that accepts or
 * rejects each of its changes. `connection` is its link to the authority.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Confirmed extends ReplicatedText {
  readonly connection: Sync.Connection
}

/**
 * Options for `makeConfirmed`.
 *
 * **Details**
 *
 * - `room`: which room on the authority this replica syncs in.
 * - `replica`: the replica id to start with. Leave it out to get a fresh
 *   random one.
 * - `retry`: how to retry a lost connection. The default backs off from 200
 *   milliseconds to every 10 seconds, indefinitely.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface ConfirmedOptions {
  readonly room: Replica.RoomId
  readonly replica?: Replica.ReplicaId | undefined
  readonly retry?: Schedule.Schedule<unknown, Sync.SyncError> | undefined
}

/**
 * Creates a replicated text whose changes an authority confirms or rejects. It
 * syncs through the `Authority` it requires, for the life of the scope, and
 * with nothing else.
 *
 * **Details**
 *
 * Edits apply immediately, as with any replica. They're unconfirmed until the
 * authority acknowledges them, and the replica never gives them to anything
 * else: `Replica.exchange` and `Replica.receive` fail with `AuthorityOnly`. When
 * the authority rejects a change, the replica drops it and every unconfirmed
 * change made after it, emits a `Rejected` edit that rolls the text back, and
 * continues under a new replica id.
 *
 * Give it `Authority.layerRemote` to reach an authority on a server, or
 * `Authority.layer` to run the authority in the same process (as tests do).
 *
 * **Example** (Rejecting a reviewer's edit)
 *
 * ```ts import.meta.vitest
 * import { Authority, HistoryStore, Replica, ReplicatedText, Sync } from "@jpowersdev/effect-delta"
 * import { Effect, Layer, Stream } from "effect"
 * import { SecureRandom } from "effect/crypto"
 *
 * const EditorsOnly = Authority.layer({
 *   check: ({ user }) =>
 *     user === "editor" ? Effect.void : Effect.fail(
 *       new Authority.Rejected({
 *         message: "Read only"
 *       })
 *     )
 * }).pipe(Layer.provide(HistoryStore.layerMemory))
 *
 * const program = Effect.gen(function*() {
 *   const spec = yield* ReplicatedText.makeConfirmed({
 *     room: Replica.makeRoom("spec")
 *   })
 *   const { edits } = yield* ReplicatedText.subscribe(spec)
 *   yield* ReplicatedText.insert(spec, 0, "LGTM")
 *   yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
 *   return yield* ReplicatedText.get(spec)
 * })
 *
 * await Effect.runPromise(program.pipe(
 *   Effect.scoped,
 *   Effect.provideService(Sync.CurrentUser, "reviewer"),
 *   Effect.provide([EditorsOnly, SecureRandom.layerWebCrypto])
 * )) // => ""
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const makeConfirmed: (
  options: ConfirmedOptions
) => Effect.Effect<Confirmed, never, Authority.Authority | SecureRandom.SecureRandom | Scope.Scope> = makeConfirmedImpl

/**
 * Returns the current text.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const get: (self: ReplicatedText) => Effect.Effect<string> = getImpl

/**
 * Returns a stream of the text: its current value, then each new value after a
 * local edit or a merge.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const changes: (self: ReplicatedText) => Stream.Stream<string> = changesImpl

/**
 * How the text changed, and why.
 *
 * **Details**
 *
 * - `Local`: an edit made on this replica. Editor bindings skip these, because
 *   the editor made them.
 * - `Remote`: changes from another replica merged in, with any overlaps the
 *   merge found.
 * - `Rejected`: an authority rejected this replica's edit; `patch` rolls it
 *   back.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Edit =
  | { readonly _tag: "Local"; readonly patch: TextPatch.TextPatch }
  | {
    readonly _tag: "Remote"
    readonly patch: TextPatch.TextPatch
    readonly overlaps: ReadonlyArray<Replica.Overlap>
  }
  | { readonly _tag: "Rejected"; readonly patch: TextPatch.TextPatch; readonly message: string }

/**
 * The text at the moment of subscribing, and every edit applied after it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Subscription {
  readonly text: string
  readonly edits: Stream.Stream<Edit>
}

/**
 * Subscribes to the text's edits for the life of the scope. No edit is missed
 * between reading `text` and the first of `edits`, which is what an editor
 * binding needs: load `text`, then apply each `Remote` and `Rejected` patch.
 *
 * **Example** (Watching edits)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect, Option, Stream } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({
 *     replica: Replica.makeId("laptop")
 *   })
 *   const { text, edits } = yield* ReplicatedText.subscribe(notes)
 *   yield* ReplicatedText.insert(notes, 0, "Hi")
 *   const first = yield* Stream.runHead(edits)
 *   return [text, first]
 * })
 *
 * const [text, first] = await Effect.runPromise(program.pipe(Effect.scoped))
 * text // => ""
 * first // => Option.some({ _tag: "Local", patch: [{ _tag: "Insert", text: "Hi" }] })
 * ```
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const subscribe: (self: ReplicatedText) => Effect.Effect<Subscription, never, Scope.Scope> = subscribeImpl

/**
 * The index was outside the text.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class OutOfBounds extends Schema.TaggedError<OutOfBounds>()("OutOfBounds", {
  index: Schema.Int,
  length: Schema.Int
}) {}

/**
 * The index fell between the two halves of a surrogate pair (half of an emoji).
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class SplitsSurrogatePair extends Schema.TaggedError<SplitsSurrogatePair>()("SplitsSurrogatePair", {
  index: Schema.Int
}) {}

/**
 * An edit or anchor couldn't be made. The `reason` says why.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class ReplicatedTextError extends Schema.TaggedError<ReplicatedTextError>()("ReplicatedTextError", {
  reason: Schema.Union([OutOfBounds, SplitsSurrogatePair])
}) {}

const outOfBounds = (index: number, length: number) =>
  new ReplicatedTextError({ reason: new OutOfBounds({ index, length }) })

const splitsSurrogatePair = (index: number) => new ReplicatedTextError({ reason: new SplitsSurrogatePair({ index }) })

/**
 * Inserts `text` at `index`, as one change.
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const insert: {
  (index: number, text: string): (self: ReplicatedText) => Effect.Effect<void, ReplicatedTextError>
  (self: ReplicatedText, index: number, text: string): Effect.Effect<void, ReplicatedTextError>
} = dual(
  3,
  (self: ReplicatedText, index: number, text: string) =>
    applyPatchImpl(self, [{ _tag: "Retain", length: index }, { _tag: "Insert", text }])
)

/**
 * Removes `length` characters starting at `index`, as one change.
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const remove: {
  (index: number, length: number): (self: ReplicatedText) => Effect.Effect<void, ReplicatedTextError>
  (self: ReplicatedText, index: number, length: number): Effect.Effect<void, ReplicatedTextError>
} = dual(
  3,
  (self: ReplicatedText, index: number, length: number) =>
    applyPatchImpl(self, [{ _tag: "Retain", length: index }, { _tag: "Remove", length }])
)

/**
 * Applies a patch, as one change. Editors that batch keystrokes, and editor
 * bindings that speak Quill deltas, use this.
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const applyPatch: {
  (patch: TextPatch.TextPatch): (self: ReplicatedText) => Effect.Effect<void, ReplicatedTextError>
  (self: ReplicatedText, patch: TextPatch.TextPatch): Effect.Effect<void, ReplicatedTextError>
} = dual(2, applyPatchImpl)

/**
 * Replaces the whole text with `text`, as one change.
 *
 * **Details**
 *
 * The new string becomes one replacement: the shortest stretch between the
 * longest common prefix and suffix of the old and new text, moved so it never
 * splits a surrogate pair. That rule decides how concurrent whole-string edits
 * merge, so it's part of the contract; `TextPatch.get` uses the same one.
 * Editors should use `insert`, `remove` and `applyPatch` instead, which say
 * exactly what the user did.
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const set: {
  (text: string): (self: ReplicatedText) => Effect.Effect<void>
  (self: ReplicatedText, text: string): Effect.Effect<void>
} = dual(2, setImpl)

/**
 * Replaces the text with `f` of the current text, as one change. The new
 * string becomes one replacement, as with `set`.
 *
 * **Example** (Find and replace)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({
 *     replica: Replica.makeId("laptop")
 *   })
 *   yield* ReplicatedText.set(notes, "Sessions last 24 hours.")
 *   yield* ReplicatedText.update(notes, (text) => text.replace("24", "8"))
 *   return yield* ReplicatedText.get(notes)
 * })
 *
 * await Effect.runPromise(program) // => "Sessions last 8 hours."
 * ```
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const update: {
  (f: (text: string) => string): (self: ReplicatedText) => Effect.Effect<void>
  (self: ReplicatedText, f: (text: string) => string): Effect.Effect<void>
} = dual(2, updateImpl)

/**
 * Applies a patch worked out from the current text, and returns `f`'s result.
 * No edit or merge lands between `f` reading the text and its patch applying,
 * so the patch always fits the text it was made for. Return
 * `TextPatch.empty` to leave the text as it is.
 *
 * **Details**
 *
 * Editor bindings use it to hand the replica keystrokes only while the replica
 * still has the text they were typed against. A find-and-replace uses it to
 * report a missing match without racing other replicas' edits; `TextPatch.get`
 * turns a new string into a patch, as `update` does.
 *
 * **Example** (A find and replace that reports a missing match)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText, TextPatch } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const replace = (from: string, to: string) => (text: string) =>
 *   text.includes(from)
 *     ? ["replaced", TextPatch.get(text, text.replace(from, to))] as const
 *     : ["not found", TextPatch.empty] as const
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({
 *     replica: Replica.makeId("laptop")
 *   })
 *   yield* ReplicatedText.set(notes, "Sessions last 24 hours.")
 *   const first = yield* ReplicatedText.modify(notes, replace("24", "8"))
 *   const second = yield* ReplicatedText.modify(notes, replace("24", "8"))
 *   return [first, second, yield* ReplicatedText.get(notes)]
 * })
 *
 * await Effect.runPromise(program) // => ["replaced", "not found", "Sessions last 8 hours."]
 * ```
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const modify: {
  <B>(
    f: (text: string) => readonly [B, TextPatch.TextPatch]
  ): (self: ReplicatedText) => Effect.Effect<B, ReplicatedTextError>
  <B>(
    self: ReplicatedText,
    f: (text: string) => readonly [B, TextPatch.TextPatch]
  ): Effect.Effect<B, ReplicatedTextError>
} = dual(2, modifyImpl)

/**
 * Which way an anchor leans: `"before"` its character (text typed exactly at
 * the anchor goes in front of it) or `"after"` it (text typed there goes
 * behind it).
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Side = TextPatch.Side

/**
 * A position that follows the text through edits and merges: the gap just
 * before or just after one character, or the very start or end of the text.
 * Store anchors (for example on a comment) and turn them back into indexes with
 * `locate`.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Anchor =
  | { readonly _tag: "Character"; readonly replica: string; readonly seq: number; readonly side: Side }
  | { readonly _tag: "Start" }
  | { readonly _tag: "End" }

/**
 * Schema for an anchor.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Anchor: Schema.Codec<Anchor> = InternalAnchor.Anchor

/**
 * A pair of anchors around a stretch of text.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface AnchorRange {
  readonly start: Anchor
  readonly end: Anchor
}

/**
 * Schema for an anchor range.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const AnchorRange: Schema.Codec<AnchorRange> = InternalAnchor.AnchorRange

/**
 * A stretch of the current text as indexes: from `start` up to, but not
 * including, `end`. Indexes count UTF-16 code units, as JavaScript strings do.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Range {
  readonly start: number
  readonly end: number
}

/**
 * Anchors the gap at `index`. With side `"before"` (the default) it leans on the
 * character after the gap; with `"after"`, on the character before it.
 *
 * **Example** (Following a position through an edit)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect, Option } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({
 *     replica: Replica.makeId("laptop")
 *   })
 *   yield* ReplicatedText.insert(notes, 0, "world")
 *   const anchor = yield* ReplicatedText.anchor(notes, 0)
 *   yield* ReplicatedText.insert(notes, 0, "Hello, ")
 *   return yield* ReplicatedText.locate(notes, anchor)
 * })
 *
 * await Effect.runPromise(program) // => Option.some(7)
 * ```
 *
 * @stability unstable
 * @category anchors
 * @since 0.1.0
 */
export const anchor: {
  (index: number, side?: Side): (self: ReplicatedText) => Effect.Effect<Anchor, ReplicatedTextError>
  (self: ReplicatedText, index: number, side?: Side): Effect.Effect<Anchor, ReplicatedTextError>
} = dual(
  (args) => isReplicatedText(args[0]),
  (self: ReplicatedText, index: number, side: Side = "before") => anchorImpl(self, index, side)
)

/**
 * Returns where an anchor is now. An anchor on a removed character sits where
 * the character was. None if the anchor's character hasn't reached this replica
 * yet, or was rolled back.
 *
 * @stability unstable
 * @category anchors
 * @since 0.1.0
 */
export const locate: {
  (anchor: Anchor): (self: ReplicatedText) => Effect.Effect<Option.Option<number>>
  (self: ReplicatedText, anchor: Anchor): Effect.Effect<Option.Option<number>>
} = dual(2, locateImpl)

/**
 * Anchors a stretch of text, so it can be found again after edits and merges.
 * Text typed at either edge stays outside it; text typed inside joins it.
 *
 * @stability unstable
 * @category anchors
 * @since 0.1.0
 */
export const anchorRange: {
  (range: Range): (self: ReplicatedText) => Effect.Effect<AnchorRange, ReplicatedTextError>
  (self: ReplicatedText, range: Range): Effect.Effect<AnchorRange, ReplicatedTextError>
} = dual(2, anchorRangeImpl)

/**
 * Returns where an anchored stretch is now. None once all of it has been
 * removed, or if its characters haven't reached this replica.
 *
 * @stability unstable
 * @category anchors
 * @since 0.1.0
 */
export const locateRange: {
  (range: AnchorRange): (self: ReplicatedText) => Effect.Effect<Option.Option<Range>>
  (self: ReplicatedText, range: AnchorRange): Effect.Effect<Option.Option<Range>>
} = dual(2, locateRangeImpl)

/**
 * A stretch of the current text and the replica whose change typed it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Authorship {
  readonly range: Range
  readonly replica: Replica.ReplicaId
}

/**
 * Returns who typed each part of the current text: consecutive stretches, in
 * text order, each with the replica that inserted it. Apps map replica ids to
 * the people and agents behind them to show who wrote what.
 *
 * **Example** (Telling two writers' text apart)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const owner = yield* ReplicatedText.make({
 *     replica: Replica.makeId("owner")
 *   })
 *   const agent = yield* ReplicatedText.make({
 *     replica: Replica.makeId("agent")
 *   })
 *   yield* ReplicatedText.insert(owner, 0, "Ship it Friday.")
 *   yield* Replica.exchange(owner, agent)
 *   yield* ReplicatedText.insert(agent, 14, " after the review")
 *   yield* Replica.exchange(owner, agent)
 *   const authored = yield* ReplicatedText.authorship(owner)
 *   return authored.map(({ range, replica }) => `${replica} ${range.start}-${range.end}`)
 * })
 *
 * await Effect.runPromise(program) // => ["owner 0-14", "agent 14-31", "owner 31-32"]
 * ```
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const authorship: (self: ReplicatedText) => Effect.Effect<ReadonlyArray<Authorship>> = authorshipImpl

/**
 * Returns `true` when a value is a replicated text.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isReplicatedText = (u: unknown): u is ReplicatedText => hasProperty(u, TypeId)
