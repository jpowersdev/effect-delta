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
 * Every function except the constructors takes an `Editable`: a replicated
 * text, or a text field of a `ReplicatedDocument`. So one editor binding works
 * for both.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import { dual } from "effect/Function"
import type * as Inspectable from "effect/Inspectable"
import * as Option from "effect/Option"
import type { Pipeable } from "effect/Pipeable"
import { hasProperty } from "effect/Predicate"
import * as PubSub from "effect/PubSub"
import * as Result from "effect/Result"
import type * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Authority from "./Authority.ts"
import * as InternalAnchor from "./internal/anchor.ts"
import * as InternalConnection from "./internal/connection.ts"
import * as InternalDocument from "./internal/document.ts"
import * as InternalEditable from "./internal/editable.ts"
import * as InternalEngine from "./internal/engine.ts"
import * as InternalPath from "./internal/path.ts"
import * as InternalReplica from "./internal/replica.ts"
import * as InternalText from "./internal/text.ts"
import type * as Replica from "./Replica.ts"
import type * as Sync from "./Sync.ts"
import * as TextPatch from "./TextPatch.ts"

const TypeId = "~effect-delta/ReplicatedText"

// Implementation ----------------------------------------------------------

interface ReplicatedTextImpl extends ReplicatedText, InternalEditable.EditableImpl {
  readonly core: InternalReplica.Core
}

const Proto = {
  ...InternalReplica.Proto,
  [TypeId]: TypeId,
  [InternalEditable.TypeId]: InternalEditable.TypeId,
  toJSON(this: ReplicatedTextImpl) {
    return { _id: "ReplicatedText", strategy: this.strategy, text: InternalEditable.text(this) }
  }
}

// A replicated text is a document whose only part is a text at the root.
const model: InternalEngine.Model = {
  read: (document) => InternalDocument.textAt(document, InternalPath.root),
  shows: (overlap) => overlap._tag === "Text" && InternalPath.isRoot(overlap.path),
  // It reads no values, its text is always there, and nothing in it moves.
  reading: { reads: () => false, differ: () => false, comesAndGoes: () => false, moves: () => false }
}

const makeWith = Effect.fnUntraced(function*(options: {
  readonly strategy: Replica.Strategy
  readonly replica: Replica.ReplicaId
  readonly confirmed: InternalEngine.ConfirmedState | undefined
}) {
  const engine = yield* InternalEngine.make({ ...options, model })
  const self: ReplicatedTextImpl = Object.assign(Object.create(Proto), {
    strategy: options.strategy,
    engine,
    path: InternalPath.root,
    core: engine.core
  })
  return self
})

const makeImpl = Effect.fnUntraced(function*(options?: Options) {
  const replica = options?.replica ?? (yield* InternalEngine.randomReplicaId)
  return yield* makeWith({ strategy: options?.strategy ?? "tree", replica, confirmed: undefined }) as Effect.Effect<
    ReplicatedText
  >
})

const makeConfirmedImpl = Effect.fnUntraced(function*(options: ConfirmedOptions) {
  // Kept for later: a rejection moves the replica to a new id.
  const crypto = yield* Crypto.Crypto
  const freshId = Effect.provideService(InternalEngine.randomReplicaId, Crypto.Crypto, crypto)
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

const impl = (self: Editable): InternalEditable.EditableImpl => self as InternalEditable.EditableImpl

const locked = <A, E, R>(self: Editable, effect: Effect.Effect<A, E, R>) =>
  InternalEngine.locked(impl(self).engine, effect)

// The text, or an empty one if nothing has been written to it yet.
const textOf = (self: Editable): InternalText.Text =>
  InternalDocument.slot(impl(self).engine.state.document, impl(self).path)?.text ??
    InternalText.make(impl(self).engine.strategy)

// Reading ------------------------------------------------------------------

const getImpl = (self: Editable): Effect.Effect<string> => locked(self, Effect.sync(() => InternalEditable.text(self)))

// Events that change this text, or say something about it.
const editsOf = (self: Editable, events: Stream.Stream<InternalEngine.Event>): Stream.Stream<Edit> => {
  const { path } = impl(self)
  const k = InternalPath.key(path)
  return events.pipe(
    Stream.map((event): Edit | undefined => {
      const patch = event.texts.get(k) ?? TextPatch.empty
      switch (event._tag) {
        case "Local":
          return patch.length === 0 ? undefined : { _tag: "Local", patch }
        case "Remote": {
          const overlaps = event.overlaps.filter((overlap) =>
            overlap._tag === "Text" && InternalPath.equals(overlap.path, path)
          )
          return patch.length === 0 && overlaps.length === 0 ? undefined : { _tag: "Remote", patch, overlaps }
        }
        // A rejection elsewhere in a document leaves this text alone.
        case "Rejected":
          return event.texts.has(k) ? { _tag: "Rejected", patch, message: event.message } : undefined
      }
    }),
    Stream.filter((edit): edit is Edit => edit !== undefined)
  )
}

// Each edit's patch turns the text before it into the text after it.
const changesImpl = (self: Editable): Stream.Stream<string> =>
  Stream.unwrap(
    Effect.map(subscribeImpl(self), ({ edits, text }) =>
      Stream.scan(edits, () => text, (current, edit) => TextPatch.apply(edit.patch, current)))
  )

const subscribeImpl = (self: Editable): Effect.Effect<Subscription, never, Scope.Scope> =>
  locked(
    self,
    Effect.gen(function*() {
      // Taken under the lock, so no edit lands between reading the text and
      // subscribing.
      const subscription = yield* PubSub.subscribe(impl(self).engine.events)
      return { text: InternalEditable.text(self), edits: editsOf(self, Stream.fromSubscription(subscription)) }
    })
  )

// Editing ------------------------------------------------------------------

const editText = <E>(
  self: Editable,
  patchOf: (text: string) => Result.Result<TextPatch.TextPatch, E>
): Effect.Effect<void, E> =>
  InternalEngine.edit(impl(self).engine, (document) => {
    const { path } = impl(self)
    return Result.map(
      patchOf(InternalDocument.textAt(document, path)),
      (patch): ReadonlyArray<InternalDocument.Intent> => [{ _tag: "Text", path, patch }]
    )
  })

const applyPatchImpl = (self: Editable, patch: TextPatch.TextPatch): Effect.Effect<void, ReplicatedTextError> =>
  editText(self, (text) => {
    const invalid = validate(text, patch)
    return invalid === undefined ? Result.succeed(patch) : Result.fail(invalid)
  })

const updateImpl = (self: Editable, f: (text: string) => string): Effect.Effect<void> =>
  editText(self, (current) => Result.succeed(TextPatch.get(current, f(current))))

const setImpl = (self: Editable, text: string): Effect.Effect<void> => updateImpl(self, () => text)

// Checks a patch against the text it's for: it must stay inside the text and
// never cut a surrogate pair (half of an emoji) apart.
const validate = (text: string, patch: TextPatch.TextPatch): ReplicatedTextError | undefined => {
  let current = text
  let position = 0
  for (const operation of patch) {
    // Counting backwards ends before where it starts, outside the text.
    if (operation._tag !== "Insert" && operation.length < 0) {
      return outOfBounds(position + operation.length, current.length)
    }
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
  self: Editable,
  index: number,
  side: TextPatch.Side
): Effect.Effect<Anchor, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const text = textOf(self)
      if (index < 0 || index > text.value.length) return Effect.fail(outOfBounds(index, text.value.length))
      if (splitsPair(text.value, index)) return Effect.fail(splitsSurrogatePair(index))
      return Effect.succeed(text.anchor(index, side))
    })
  )

const locateImpl = (self: Editable, anchor: Anchor): Effect.Effect<Option.Option<number>> =>
  locked(self, Effect.sync(() => textOf(self).locate(anchor)))

const anchorRangeImpl = (
  self: Editable,
  range: Range
): Effect.Effect<AnchorRange, ReplicatedTextError> =>
  locked(
    self,
    Effect.suspend(() => {
      const text = textOf(self)
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
  self: Editable,
  range: AnchorRange
): Effect.Effect<Option.Option<Range>> =>
  locked(
    self,
    Effect.sync(() => {
      const text = textOf(self)
      const start = text.locate(range.start)
      const end = text.locate(range.end)
      if (Option.isNone(start) || Option.isNone(end) || end.value <= start.value) return Option.none()
      return Option.some({ start: start.value, end: end.value })
    })
  )

// Public API --------------------------------------------------------------

/**
 * A replica of a text.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface ReplicatedText extends Replica.Replica<string>, Editable {
  readonly [TypeId]: typeof TypeId
}

// Must match the TypeId in internal/editable.ts.
const EditableTypeId = "~effect-delta/ReplicatedText/Editable"

/**
 * A text that can be edited, watched and anchored: a replicated text, or a
 * text field of a document (see `ReplicatedDocument.text`).
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Editable extends Pipeable, Inspectable.Inspectable {
  readonly [EditableTypeId]: typeof EditableTypeId
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
 *   check: ({ _tag, user }) =>
 *     _tag !== "Text"
 *       ? Effect.fail(new Authority.Rejected({ message: "Specs are texts" }))
 *       : user === "editor"
 *       ? Effect.void
 *       : Effect.fail(new Authority.Rejected({ message: "Read only" }))
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
export const get: (self: Editable) => Effect.Effect<string> = getImpl

/**
 * Returns a stream of the text: its current value, then each new value after a
 * local edit, a merge, or an authority's rejection rolling an edit back.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const changes: (self: Editable) => Stream.Stream<string> = changesImpl

/**
 * How the text changed, and why.
 *
 * **Details**
 *
 * - `Local`: an edit made on this replica. An editor binding skips the ones it
 *   made itself, and applies the rest, such as a `ReplicatedDocument.update`
 *   that changed a text field.
 * - `Remote`: changes from another replica merged in, with any overlaps the
 *   merge found.
 * - `Rejected`: an authority rejected this replica's edit; `patch` rolls it
 *   back. A text field hears of a rejection only if it rolls back edits to
 *   the field; `ReplicatedDocument.subscribe` hears of every one.
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
 * binding needs: load `text`, then apply each patch except the `Local` ones it
 * made itself.
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
export const subscribe: (self: Editable) => Effect.Effect<Subscription, never, Scope.Scope> = subscribeImpl

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
  (index: number, text: string): (self: Editable) => Effect.Effect<void, ReplicatedTextError>
  (self: Editable, index: number, text: string): Effect.Effect<void, ReplicatedTextError>
} = dual(
  3,
  (self: Editable, index: number, text: string) =>
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
  (index: number, length: number): (self: Editable) => Effect.Effect<void, ReplicatedTextError>
  (self: Editable, index: number, length: number): Effect.Effect<void, ReplicatedTextError>
} = dual(
  3,
  (self: Editable, index: number, length: number) =>
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
  (patch: TextPatch.TextPatch): (self: Editable) => Effect.Effect<void, ReplicatedTextError>
  (self: Editable, patch: TextPatch.TextPatch): Effect.Effect<void, ReplicatedTextError>
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
  (text: string): (self: Editable) => Effect.Effect<void>
  (self: Editable, text: string): Effect.Effect<void>
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
  (f: (text: string) => string): (self: Editable) => Effect.Effect<void>
  (self: Editable, f: (text: string) => string): Effect.Effect<void>
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
  (index: number, side?: Side): (self: Editable) => Effect.Effect<Anchor, ReplicatedTextError>
  (self: Editable, index: number, side?: Side): Effect.Effect<Anchor, ReplicatedTextError>
} = dual(
  (args) => InternalEditable.isEditable(args[0]),
  (self: Editable, index: number, side: Side = "before") => anchorImpl(self, index, side)
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
  (anchor: Anchor): (self: Editable) => Effect.Effect<Option.Option<number>>
  (self: Editable, anchor: Anchor): Effect.Effect<Option.Option<number>>
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
  (range: Range): (self: Editable) => Effect.Effect<AnchorRange, ReplicatedTextError>
  (self: Editable, range: Range): Effect.Effect<AnchorRange, ReplicatedTextError>
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
  (range: AnchorRange): (self: Editable) => Effect.Effect<Option.Option<Range>>
  (self: Editable, range: AnchorRange): Effect.Effect<Option.Option<Range>>
} = dual(2, locateRangeImpl)

/**
 * Returns `true` when a value is a replicated text.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isReplicatedText = (u: unknown): u is ReplicatedText => hasProperty(u, TypeId)

/**
 * Returns `true` when a value is an editable text: a replicated text, or a
 * document's text field.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isEditable: (u: unknown) => u is Editable = InternalEditable.isEditable
