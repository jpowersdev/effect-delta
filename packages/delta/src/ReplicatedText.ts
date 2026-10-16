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
import * as Crypto from "effect/Crypto"
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
import { type Change, key } from "./internal/change.ts"
import * as InternalConnection from "./internal/connection.ts"
import * as InternalDelta from "./internal/delta.ts"
import * as History from "./internal/history.ts"
import * as InternalReplica from "./internal/replica.ts"
import * as TreeReplica from "./internal/treeReplica.ts"
import * as TreeText from "./internal/treeText.ts"
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
  text: TreeText.State
  history: History.History<TreeText.Op>
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
    return { _id: "ReplicatedText", strategy: this.strategy, text: this.state.text.text }
  }
}

const randomReplicaId = Effect.gen(function*() {
  const crypto = yield* Crypto.Crypto
  return Replica.ReplicaId.make(yield* Effect.orDie(crypto.randomUUIDv4()))
})

const makeWith = Effect.fnUntraced(function*(options: {
  readonly strategy: Replica.Strategy
  readonly replica: Replica.ReplicaId
  readonly confirmed: ConfirmedState | undefined
}) {
  if (options.strategy === "replay") return yield* Effect.die(new Error("The replay strategy isn't built yet"))
  const state: State = {
    replica: options.replica,
    nextSeq: 0,
    writers: new Set([options.replica]),
    text: TreeText.make(),
    history: History.make(),
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
  const crypto = yield* Crypto.Crypto
  const freshId = Effect.provideService(randomReplicaId, Crypto.Crypto, crypto)
  const self = yield* makeWith({
    strategy: "tree",
    replica: options.replica ?? (yield* freshId),
    confirmed: { version: {}, freshId }
  })
  const connection = yield* InternalConnection.connect(self, yield* Authority.Authority, {
    document: options.document,
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
    SubscriptionRef.set(self.textRef, self.state.text.text),
    PubSub.publish(self.edits, edit)
  )

// Reading ------------------------------------------------------------------

const getImpl = (self: ReplicatedText): Effect.Effect<string> =>
  locked(self, Effect.sync(() => impl(self).state.text.text))

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
      return { text: impl(self).state.text.text, edits: Stream.fromSubscription(subscription) }
    })
  )

// Editing ------------------------------------------------------------------

const applyPatchImpl = (
  self: ReplicatedText,
  patch: TextPatch.TextPatch
): Effect.Effect<void, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const s = impl(self).state
      const invalid = validate(s.text.text, patch)
      if (invalid !== undefined) return Effect.fail(invalid)
      if (patch.length === 0) return Effect.void
      const info: TreeText.Info = { replica: s.replica, seq: s.nextSeq, length: 0, past: { ...s.history.version } }
      const ops: Array<TreeText.Op> = []
      let applied = TextPatch.empty
      let position = 0
      for (const operation of patch) {
        switch (operation._tag) {
          case "Retain":
            position += operation.length
            break
          case "Insert": {
            const first = { replica: s.replica, seq: info.seq + info.length }
            const [op, done] = TreeText.insert(s.text, position, operation.text, first, info)
            ops.push(op)
            applied = TextPatch.compose(applied, done)
            info.length += operation.text.length
            position += operation.text.length
            break
          }
          case "Remove": {
            const [op, done] = TreeText.remove(s.text, position, operation.length, info)
            ops.push(op)
            applied = TextPatch.compose(applied, done)
            info.length += operation.length
            break
          }
        }
      }
      if (ops.length === 0) return Effect.void
      const change: Change<TreeText.Op> = {
        replica: s.replica,
        seq: info.seq,
        length: info.length,
        parents: s.history.frontier,
        ops
      }
      History.record(s.history, change, info.past)
      s.nextSeq += info.length
      return publish(impl(self), { _tag: "Local", patch: applied })
    })
  )

const setImpl = (self: ReplicatedText, text: string): Effect.Effect<void> =>
  Effect.orDie(
    locked(
      self,
      Effect.suspend(() => Effect.succeed(TextPatch.get(impl(self).state.text.text, text)))
    ).pipe(Effect.flatMap((patch) => applyPatchImpl(self, patch)))
  )

const updateImpl = (self: ReplicatedText, f: (text: string) => string): Effect.Effect<void> =>
  Effect.flatMap(getImpl(self), (text) => setImpl(self, f(text)))

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
      if (index < 0 || index > text.text.length) return Effect.fail(outOfBounds(index, text.text.length))
      if (splitsPair(text.text, index)) return Effect.fail(splitsSurrogatePair(index))
      return Effect.succeed(TreeText.anchor(text, index, side))
    })
  )

const locateImpl = (self: ReplicatedText, anchor: Anchor): Effect.Effect<Option.Option<number>> =>
  locked(self, Effect.sync(() => TreeText.locate(impl(self).state.text, anchor)))

const anchorRangeImpl = (
  self: ReplicatedText,
  range: Range
): Effect.Effect<AnchorRange, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const text = impl(self).state.text
      const length = text.text.length
      if (range.start < 0 || range.start > range.end) return Effect.fail(outOfBounds(range.start, length))
      if (range.end > length) return Effect.fail(outOfBounds(range.end, length))
      if (splitsPair(text.text, range.start)) return Effect.fail(splitsSurrogatePair(range.start))
      if (splitsPair(text.text, range.end)) return Effect.fail(splitsSurrogatePair(range.end))
      // Leaning inward, so text typed at either edge stays outside.
      return Effect.succeed({
        start: TreeText.anchor(text, range.start, "before"),
        end: TreeText.anchor(text, range.end, "after")
      })
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
      const start = TreeText.locate(text, range.start)
      const end = TreeText.locate(text, range.end)
      if (Option.isNone(start) || Option.isNone(end) || end.value <= start.value) return Option.none()
      return Option.some({ start: start.value, end: end.value })
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
    version: locked(self, Effect.sync(() => ({ ...state.history.version }))),
    delta: (since) =>
      locked(
        self,
        Effect.sync(() =>
          InternalDelta.fromChanges(
            History.since(state.history, since).filter((change) => !isUnconfirmed(state, change))
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
            History.since(state.history, since).filter((change) => state.writers.has(change.replica))
          )
        )
      ),
    changed: subscribeAll,
    toStore: (since) =>
      locked(
        self,
        Effect.sync(() => {
          const unconfirmed = [...state.history.changes.values()].filter((change) => isUnconfirmed(state, change))
          const version: Record<string, number> = { ...state.history.version }
          for (const change of unconfirmed) version[change.replica] = Math.min(version[change.replica]!, change.seq)
          return {
            history: InternalDelta.fromChanges(
              History.since(state.history, since).filter((change) => !isUnconfirmed(state, change))
            ),
            unconfirmed: InternalDelta.fromChanges(unconfirmed),
            writers: [...state.writers].map((writer) => Replica.ReplicaId.make(writer)),
            version
          }
        })
      ),
    restore: (stored) =>
      locked(
        self,
        Effect.suspend(() => {
          for (const writer of stored.writers) state.writers.add(writer)
          return Effect.asVoid(
            receive(self, InternalDelta.combine(stored.history, stored.unconfirmed))
          )
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
            const before = state.text.text
            // Everything the authority doesn't have was made on top of the
            // rejected change, and nobody else has seen it, so it all goes.
            const pending = [...state.history.pending.values()]
            rebuild(state, [...state.history.changes.values()].filter((change) => !isUnconfirmed(state, change)))
            for (const change of pending) state.history.pending.set(key(change), change)
            // A new id, so sequence numbers are never reused and never skip.
            state.replica = yield* confirmed.freshId
            state.nextSeq = 0
            state.writers.add(state.replica)
            yield* publish(self, { _tag: "Rejected", patch: TextPatch.get(before, state.text.text), message })
          })
        )
    }
  }
}

// A confirmed replica's own changes that its authority hasn't acknowledged.
const isUnconfirmed = (state: State, change: Change<TreeText.Op>): boolean =>
  state.confirmed !== undefined && state.writers.has(change.replica) &&
  change.seq + change.length > VersionVector.get(state.confirmed.version, change.replica)

// Starts over from `changes`, in the order they were applied.
const rebuild = (state: State, changes: ReadonlyArray<Change<TreeText.Op>>): void => {
  const rebuilt = TreeReplica.rebuild(changes)
  state.text = rebuilt.text
  state.history = rebuilt.history
}

const receive = (
  self: ReplicatedTextImpl,
  delta: InternalDelta.DeltaImpl
): Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError> => {
  const { state } = self
  const { history, text } = state
  for (const change of delta.changes.values()) {
    const existing = history.changes.get(key(change)) ?? history.pending.get(key(change))
    if (change.replica === state.replica && existing === undefined) {
      return Effect.fail(deltaError(new Replica.ReplicaIdReused({ replica: Replica.ReplicaId.make(change.replica) })))
    }
    if (existing !== undefined && !Equal.equals(existing, change)) {
      return Effect.fail(deltaError(new Replica.ReplicaIdReused({ replica: Replica.ReplicaId.make(change.replica) })))
    }
    if (existing === undefined && !History.has(history, change)) history.pending.set(key(change), change)
  }

  const added = new Set<TreeText.Info>()
  let patch = TextPatch.empty
  const ready = History.takeReady(history)
  for (const [index, change] of ready.entries()) {
    const problem = TreeReplica.check(text, change)
    if (problem !== undefined) {
      // Whatever was waiting behind the bad change keeps waiting.
      for (const later of ready.slice(index + 1)) history.pending.set(key(later), later)
      return Effect.fail(deltaError(new Replica.Malformed({ message: problem })))
    }
    const applied = TreeReplica.integrate(text, history, change)
    patch = TextPatch.compose(patch, applied.patch)
    added.add(applied.info)
  }
  if (added.size === 0) return Effect.succeed([])

  const overlaps: ReadonlyArray<Replica.Overlap> = TreeText.overlaps(text, added).map((overlap) => ({
    _tag: "Text",
    range: { start: overlap.start, end: overlap.end },
    anchors: overlap.anchors,
    replicas: overlap.replicas.map((replica) => Replica.ReplicaId.make(replica))
  }))
  return Effect.as(publish(self, { _tag: "Remote", patch, overlaps }), overlaps)
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
 * Without a `replica` option, a fresh random replica id comes from `Crypto`,
 * which platform layers such as `NodeServices.layer` provide. With one, no
 * `Crypto` is needed; tests and examples use fixed ids to stay deterministic.
 *
 * **Example** (Creating and editing a text)
 *
 * ```ts import.meta.vitest
 * import { Effect } from "effect"
 * import { Replica, ReplicatedText } from "effect-delta"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
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
  (options?: Options): Effect.Effect<ReplicatedText, never, Crypto.Crypto>
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
 * - `document`: which document on the authority this is a replica of.
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
  readonly document: Replica.DocumentId
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
 * import { NodeServices } from "@effect/platform-node"
 * import { Effect, Layer, Stream } from "effect"
 * import { Authority, HistoryStore, Replica, ReplicatedText, Sync } from "effect-delta"
 *
 * const EditorsOnly = Authority.layer({
 *   check: ({ user }) =>
 *     user === "editor" ? Effect.void : Effect.fail(new Authority.Rejected({ message: "Read only" }))
 * }).pipe(Layer.provide(HistoryStore.layerMemory))
 *
 * const program = Effect.gen(function*() {
 *   const spec = yield* ReplicatedText.makeConfirmed({ document: Replica.DocumentId.make("spec") })
 *   const { edits } = yield* ReplicatedText.subscribe(spec)
 *   yield* ReplicatedText.insert(spec, 0, "LGTM")
 *   yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
 *   return yield* ReplicatedText.get(spec)
 * })
 *
 * await Effect.runPromise(program.pipe(
 *   Effect.scoped,
 *   Effect.provideService(Sync.CurrentUser, "reviewer"),
 *   Effect.provide([EditorsOnly, NodeServices.layer])
 * )) // => ""
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const makeConfirmed: (
  options: ConfirmedOptions
) => Effect.Effect<Confirmed, never, Authority.Authority | Crypto.Crypto | Scope.Scope> = makeConfirmedImpl

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
 * import { Effect, Option, Stream } from "effect"
 * import { Replica, ReplicatedText } from "effect-delta"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
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
 * import { Effect } from "effect"
 * import { Replica, ReplicatedText } from "effect-delta"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
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
 * import { Effect, Option } from "effect"
 * import { Replica, ReplicatedText } from "effect-delta"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
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
 * Returns `true` when a value is a replicated text.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isReplicatedText = (u: unknown): u is ReplicatedText => hasProperty(u, TypeId)
