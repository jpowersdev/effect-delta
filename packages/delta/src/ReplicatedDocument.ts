/**
 * A replicated document is a value of an ordinary `Schema` (a design doc with a
 * title, a body, a status and comments) that many replicas edit at once. Every
 * replica accepts edits immediately, and replicas that have received the same
 * changes show the same value.
 *
 * Its merge rules come from its schema. A struct merges field by field and a
 * record entry by entry. A `Text` merges character by character, a `Counter`
 * by adding, and a `List` item by item. Anything else (strings, numbers,
 * literals, unions, arrays, dates) takes the latest write, and the merge
 * reports an overlap when two replicas wrote different values.
 *
 * Edit it with `update` and `set`, which compare the new value with the old
 * part by part and write only what changed. Edit its text fields with the
 * `ReplicatedText` functions, through `text`, which is what editors use.
 * Observe it with `changes` (each new value) or `subscribe` (each edit).
 *
 * It's a replica like any other: `Replica.exchange`, `Relay.connect`,
 * `HistoryStore.persist` and authorities take it as it is, and servers don't
 * need its schema.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import { dual, identity } from "effect/Function"
import { hasProperty } from "effect/Predicate"
import * as PubSub from "effect/PubSub"
import * as Result from "effect/Result"
import type * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import type * as Types from "effect/Types"
import * as Authority from "./Authority.ts"
import * as InternalConnection from "./internal/connection.ts"
import type * as InternalDelta from "./internal/delta.ts"
import * as InternalEditable from "./internal/editable.ts"
import * as InternalEngine from "./internal/engine.ts"
import * as InternalPath from "./internal/path.ts"
import * as InternalReplica from "./internal/replica.ts"
import * as InternalShape from "./internal/shape.ts"
import type * as Replica from "./Replica.ts"
import type * as ReplicatedText from "./ReplicatedText.ts"
import type * as Sync from "./Sync.ts"

const TypeId = "~effect-delta/ReplicatedDocument"

// Implementation ----------------------------------------------------------

interface ReplicatedDocumentImpl extends ReplicatedDocument<unknown> {
  readonly core: InternalReplica.Core
  readonly engine: InternalEngine.Engine
  readonly validate: (value: unknown) => Result.Result<unknown, Schema.SchemaError>
  readonly shape: InternalShape.Shape
  readonly initial: unknown
  readonly memo: InternalShape.Memo
}

const Proto = {
  ...InternalReplica.Proto,
  [TypeId]: { _A: identity },
  toJSON(this: ReplicatedDocumentImpl) {
    return {
      _id: "ReplicatedDocument",
      strategy: this.strategy,
      value: this.engine.model.read(this.engine.state.document)
    }
  }
}

// Schemas are compiled once.
const shapes = new WeakMap<Schema.Top, InternalShape.Fields | InternalShape.Entries>()
const shapeOf = (schema: Schema.Top): InternalShape.Fields | InternalShape.Entries => {
  let shape = shapes.get(schema)
  if (shape === undefined) {
    shape = InternalShape.compile(schema)
    shapes.set(schema, shape)
  }
  return shape
}

// An initial value must fit its schema, and start as the merge rules say.
const checkInitial = (schema: Schema.Top, shape: InternalShape.Shape, initial: unknown): Effect.Effect<void> =>
  Effect.suspend(() => {
    const valid = Schema.decodeUnknownResult(Schema.toType(schema))(initial)
    if (Result.isFailure(valid)) {
      return Effect.die(new Error(`A document's initial value must fit its schema: ${valid.failure.message}`))
    }
    InternalShape.checkInitial(shape, initial)
    return Effect.void
  })

const makeWith = Effect.fnUntraced(function*(schema: Schema.Top, options: {
  readonly initial: unknown
  readonly strategy: Replica.Strategy
  readonly replica: Replica.ReplicaId
  readonly confirmed: InternalEngine.ConfirmedState | undefined
}) {
  const shape = shapeOf(schema)
  const validate = Schema.decodeUnknownResult(Schema.toType(schema))
  yield* checkInitial(schema, shape, options.initial)
  const memo: InternalShape.Memo = new Map()
  const engine = yield* InternalEngine.make({
    strategy: options.strategy,
    replica: options.replica,
    confirmed: options.confirmed,
    model: {
      read: (document) => InternalShape.read(shape, document, options.initial, memo),
      shows: (overlap) => InternalShape.shows(shape, overlap),
      reading: InternalShape.reading(shape)
    }
  })
  const self: ReplicatedDocumentImpl = Object.assign(Object.create(Proto), {
    strategy: options.strategy,
    engine,
    core: engine.core,
    validate,
    shape,
    initial: options.initial,
    memo
  })
  return self
})

const makeImpl = Effect.fnUntraced(function*(schema: Schema.Top, options: Options<unknown>) {
  const replica = options.replica ?? (yield* InternalEngine.randomReplicaId)
  return yield* makeWith(schema, {
    initial: options.initial,
    strategy: options.strategy ?? "tree",
    replica,
    confirmed: undefined
  })
})

const makeConfirmedImpl = Effect.fnUntraced(function*(schema: Schema.Top, options: ConfirmedOptions<unknown>) {
  // Kept for later: a rejection moves the replica to a new id.
  const crypto = yield* Crypto.Crypto
  const freshId = Effect.provideService(InternalEngine.randomReplicaId, Crypto.Crypto, crypto)
  const self = yield* makeWith(schema, {
    initial: options.initial,
    strategy: "tree",
    replica: options.replica ?? (yield* freshId),
    confirmed: { version: {}, freshId }
  })
  const connection = yield* InternalConnection.connect(self, yield* Authority.Authority, {
    document: options.document,
    authority: true,
    retry: options.retry
  })
  return Object.assign(self, { connection })
})

const impl = <A>(self: ReplicatedDocument<A>): ReplicatedDocumentImpl => self as unknown as ReplicatedDocumentImpl

const toEdit = (event: InternalEngine.Event): Edit<unknown> => {
  switch (event._tag) {
    case "Local":
      return { _tag: "Local", value: event.value }
    case "Remote":
      return { _tag: "Remote", value: event.value, overlaps: event.overlaps }
    case "Rejected":
      return { _tag: "Rejected", value: event.value, message: event.message }
  }
}

const updateImpl = <A>(self: ReplicatedDocument<A>, f: (value: A) => A) => {
  const { engine, initial, memo, shape, validate } = impl(self)
  return InternalEngine.edit(engine, (document) => {
    const value = f(engine.model.read(document) as A)
    return Result.flatMap(validate(value), () => InternalShape.plan(shape, document, value, initial, memo))
  })
}

// Public API --------------------------------------------------------------

/**
 * A replica of a document whose value is an `A`.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface ReplicatedDocument<in out A> extends Replica.Replica<A> {
  // Invariant, as SubscriptionRef is: it's read as an A, and written with one.
  readonly [TypeId]: { readonly _A: Types.Invariant<A> }
}

/**
 * A replicated document that answers to an authority. `connection` is its link
 * to the authority.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Confirmed<in out A> extends ReplicatedDocument<A> {
  readonly connection: Sync.Connection
}

/**
 * Options for `make`.
 *
 * **Details**
 *
 * - `initial`: what the document shows for parts nobody has written. It's
 *   never sent, so every replica must use the same one: define it next to the
 *   schema, never from local data. Texts, records and lists start empty in it,
 *   and optional fields start absent; to start with content, write it as a
 *   change.
 * - `strategy`: how its texts merge. `"tree"` (the default) or `"replay"`.
 * - `replica`: the replica id to write changes under. Leave it out to get a
 *   fresh random one.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Options<A> {
  readonly initial: A
  readonly strategy?: Replica.Strategy | undefined
  readonly replica?: Replica.ReplicaId | undefined
}

/**
 * Schema for a string that merges character by character, with the
 * document's strategy: a text. Edit it through `text`. Other strings take the
 * latest write.
 *
 * **Gotchas**
 *
 * A text can't have checks, such as a maximum length: merging edits that each
 * passed a check can break it. `make` refuses such a schema; put the rule in
 * an authority's check instead.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Text: Schema.String = Schema.String.annotate({ [InternalShape.RuleKey]: { _tag: "Text" } })

/**
 * Schema for a whole number that merges by adding, a counter: two concurrent
 * increments of 1 make 2. `update` writes the difference between the total
 * and the new value.
 *
 * **Details**
 *
 * Counters are whole numbers because fractions added in a different order on
 * each replica can round differently, and the replicas would disagree.
 * Increments that add up past the safe integers read as a nearby safe
 * integer, the same on every replica, and `update` leaves a counter that
 * reads as the value it's given alone. Like a text, a counter can't have
 * checks.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Counter: Schema.Int = Schema.Int.annotate({ [InternalShape.RuleKey]: { _tag: "Counter" } })

/**
 * The fields of `A` whose values are strings: what can identify a list's items.
 *
 * @stability unstable
 * @category utility types
 * @since 0.1.0
 */
export type KeyOf<A> = { readonly [K in keyof A]-?: A[K] extends string ? K : never }[keyof A] & string

/**
 * Creates a schema for an array that merges item by item: concurrent inserts,
 * removals, moves and edits all apply, and an item moved by two replicas at
 * once ends up in one place.
 *
 * **Details**
 *
 * With `key`, items are structs (or classes) identified by that field, which
 * the app sets, usually to a random id. The key must be a required string
 * that takes the latest write, not a `Text`; `make` refuses a list keyed by
 * one, as a defect. `update` compares the old and new
 * arrays by key: new keys are inserts, missing ones are removals, keys in a
 * different order are moves, and items in both merge their fields. A removal
 * wins over concurrent edits inside the item.
 *
 * Without `key`, items are values, compared by value: changing one counts as
 * removing it and inserting another.
 *
 * An array that isn't a `List` is a value, written whole.
 *
 * **Example** (A task board)
 *
 * ```ts import.meta.vitest
 * import { Schema } from "effect"
 * import { ReplicatedDocument } from "effect-delta"
 *
 * const Task = Schema.Struct({ id: Schema.String, title: ReplicatedDocument.Text, done: Schema.Boolean })
 * const Board = Schema.Struct({
 *   tasks: ReplicatedDocument.List(Task, { key: "id" }),
 *   tags: ReplicatedDocument.List(Schema.String)
 * })
 * ```
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const List: {
  <S extends Schema.Top>(item: S, options: { readonly key: KeyOf<S["Type"]> }): Schema.$Array<S>
  <S extends Schema.Top>(item: S): Schema.$Array<S>
} = <S extends Schema.Top>(item: S, options?: { readonly key: string }): Schema.$Array<S> =>
  Schema.Array(item).annotate({ [InternalShape.RuleKey]: { _tag: "List", key: options?.key } })

/**
 * Creates a document with the value `initial`.
 *
 * **Details**
 *
 * The schema's root must be a struct, a class or a record. A schema whose
 * merge rules can't hold, such as a check across a struct's fields or a text
 * inside a union, is refused with a defect, as is an `initial` that doesn't
 * fit it.
 *
 * Without a `replica` option, a fresh random replica id comes from `Crypto`,
 * which platform layers such as `NodeServices.layer` provide.
 *
 * **Example** (Two copies of a document)
 *
 * ```ts import.meta.vitest
 * import { Effect, Schema } from "effect"
 * import { Replica, ReplicatedDocument } from "effect-delta"
 *
 * const Note = Schema.Struct({ title: ReplicatedDocument.Text, pinned: Schema.Boolean })
 * const initial = { title: "", pinned: false }
 *
 * const program = Effect.gen(function*() {
 *   const laptop = yield* ReplicatedDocument.make(Note, { initial, replica: Replica.ReplicaId.make("laptop") })
 *   const phone = yield* ReplicatedDocument.make(Note, { initial, replica: Replica.ReplicaId.make("phone") })
 *   yield* ReplicatedDocument.update(laptop, (note) => ({ ...note, title: "Groceries" }))
 *   yield* ReplicatedDocument.update(phone, (note) => ({ ...note, pinned: true }))
 *   yield* Replica.exchange(laptop, phone)
 *   return yield* ReplicatedDocument.get(phone)
 * })
 *
 * await Effect.runPromise(program) // => { title: "Groceries", pinned: true }
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const make: {
  <A>(
    schema: Schema.Schema<A>,
    options: Options<NoInfer<A>> & { readonly replica: Replica.ReplicaId }
  ): Effect.Effect<ReplicatedDocument<A>>
  <A>(
    schema: Schema.Schema<A>,
    options: Options<NoInfer<A>>
  ): Effect.Effect<ReplicatedDocument<A>, never, Crypto.Crypto>
} = makeImpl as any

/**
 * Options for `makeConfirmed`.
 *
 * **Details**
 *
 * - `initial`: as for `make`.
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
export interface ConfirmedOptions<A> {
  readonly initial: A
  readonly document: Replica.DocumentId
  readonly replica?: Replica.ReplicaId | undefined
  readonly retry?: Schedule.Schedule<unknown, Sync.SyncError> | undefined
}

/**
 * Creates a document whose changes an authority confirms or rejects, with the
 * tree strategy. It syncs through the `Authority` it requires, for the life of
 * the scope, and with nothing else.
 *
 * **Details**
 *
 * Edits apply immediately and are unconfirmed until the authority
 * acknowledges them, and the replica never gives them to anything else:
 * `Replica.exchange` and `Replica.receive` fail with `AuthorityOnly`. When the
 * authority rejects a change, the replica drops it and every unconfirmed
 * change made after it, emits a `Rejected` edit with the value rolled back,
 * and continues under a new replica id. The authority's check reads its
 * changes with `ReplicatedDocument.readProposal`.
 *
 * **Example** (Rolling back a status only the owner may set)
 *
 * ```ts import.meta.vitest
 * import { NodeServices } from "@effect/platform-node"
 * import { Effect, Layer, Schema, Stream } from "effect"
 * import { Authority, HistoryStore, Replica, ReplicatedDocument, Sync } from "effect-delta"
 *
 * const Spec = Schema.Struct({ body: ReplicatedDocument.Text, status: Schema.Literals(["draft", "approved"]) })
 * const initial = { body: "", status: "draft" } as const
 *
 * const OwnerApproves = Authority.layer({
 *   check: (proposal) =>
 *     Effect.gen(function*() {
 *       if (proposal._tag === "Text") return yield* new Authority.Rejected({ message: "Specs aren't texts" })
 *       const { after } = yield* ReplicatedDocument.readProposal(proposal, Spec, { initial })
 *       if (after.status === "approved" && proposal.user !== "owner") {
 *         return yield* new Authority.Rejected({ message: "Only the owner can approve" })
 *       }
 *     })
 * }).pipe(Layer.provide(HistoryStore.layerMemory))
 *
 * const program = Effect.gen(function*() {
 *   const spec = yield* ReplicatedDocument.makeConfirmed(Spec, { initial, document: Replica.DocumentId.make("spec") })
 *   const { edits } = yield* ReplicatedDocument.subscribe(spec)
 *   yield* ReplicatedDocument.update(spec, (current) => ({ ...current, status: "approved" }))
 *   yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
 *   return yield* ReplicatedDocument.get(spec)
 * })
 *
 * await Effect.runPromise(program.pipe(
 *   Effect.scoped,
 *   Effect.provideService(Sync.CurrentUser, "reviewer"),
 *   Effect.provide([OwnerApproves, NodeServices.layer])
 * )) // => { body: "", status: "draft" }
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const makeConfirmed: <A>(
  schema: Schema.Schema<A>,
  options: ConfirmedOptions<NoInfer<A>>
) => Effect.Effect<Confirmed<A>, never, Authority.Authority | Crypto.Crypto | Scope.Scope> = makeConfirmedImpl as any

/**
 * Returns the document's value.
 *
 * **Details**
 *
 * Reading never fails. Parts nobody wrote come from `initial`. Parts this
 * schema doesn't describe, written by a newer version of it, are left out,
 * and so are entries, items and optional fields it can't read; a value whose
 * latest write it can't decode shows the latest write it can. An entry or
 * item missing a value field that has a constructor default shows the
 * default, so that default must be a constant for every replica to show the
 * same.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const get = <A>(self: ReplicatedDocument<A>): Effect.Effect<A> =>
  SubscriptionRef.get(impl(self).engine.value) as Effect.Effect<A>

/**
 * Returns a stream of the document's value: its current value, then each new
 * value after a local edit, a merge, or an authority's rejection rolling an
 * edit back.
 *
 * **Details**
 *
 * A new value shares every part that didn't change with the one before, so a
 * UI can skip re-rendering parts that are the same object as before.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const changes = <A>(self: ReplicatedDocument<A>): Stream.Stream<A> =>
  SubscriptionRef.changes(impl(self).engine.value) as Stream.Stream<A>

/**
 * How the document changed, and why.
 *
 * **Details**
 *
 * - `Local`: an edit made on this replica.
 * - `Remote`: changes from another replica merged in, with any overlaps the
 *   merge found.
 * - `Rejected`: an authority rejected this replica's edit; `value` is rolled
 *   back.
 *
 * Each has the value after it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Edit<A> =
  | { readonly _tag: "Local"; readonly value: A }
  | { readonly _tag: "Remote"; readonly value: A; readonly overlaps: ReadonlyArray<Replica.Overlap> }
  | { readonly _tag: "Rejected"; readonly value: A; readonly message: string }

/**
 * The value at the moment of subscribing, and every edit after it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Subscription<A> {
  readonly value: A
  readonly edits: Stream.Stream<Edit<A>>
}

/**
 * Subscribes to the document's edits for the life of the scope. No edit is
 * missed between reading `value` and the first of `edits`.
 *
 * **Example** (Watching edits)
 *
 * ```ts import.meta.vitest
 * import { Effect, Option, Schema, Stream } from "effect"
 * import { Replica, ReplicatedDocument } from "effect-delta"
 *
 * const Note = Schema.Struct({ pinned: Schema.Boolean })
 *
 * const program = Effect.gen(function*() {
 *   const note = yield* ReplicatedDocument.make(Note, {
 *     initial: { pinned: false },
 *     replica: Replica.ReplicaId.make("laptop")
 *   })
 *   const { edits, value } = yield* ReplicatedDocument.subscribe(note)
 *   yield* ReplicatedDocument.set(note, { pinned: true })
 *   const first = yield* Stream.runHead(edits)
 *   return [value, first]
 * })
 *
 * const [value, first] = await Effect.runPromise(program.pipe(Effect.scoped))
 * value // => { pinned: false }
 * first // => Option.some({ _tag: "Local", value: { pinned: true } })
 * ```
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const subscribe = <A>(self: ReplicatedDocument<A>): Effect.Effect<Subscription<A>, never, Scope.Scope> => {
  const { engine } = impl(self)
  return InternalEngine.locked(
    engine,
    Effect.gen(function*() {
      // Taken under the lock, so no edit lands between reading the value and
      // subscribing.
      const subscription = yield* PubSub.subscribe(engine.events)
      return {
        value: yield* SubscriptionRef.get(engine.value),
        edits: Stream.map(Stream.fromSubscription(subscription), toEdit)
      } as Subscription<A>
    })
  )
}

/**
 * Replaces the value with `f` of the current value, as one change.
 *
 * **Details**
 *
 * The new value is compared with the old part by part, and only what changed
 * is written: values that aren't equal (by `Schema.toEquivalence`), texts as
 * one replacement each (as `ReplicatedText.set` does), counters by the
 * difference, records by which keys appear and disappear (leaving out keys
 * the record doesn't take, as decoding does), and lists by which keys appear,
 * disappear and move. Fails with `Schema.SchemaError` if the new value doesn't
 * fit the schema (a class's parts must be its instances, as for its schema),
 * if two items of a list have the same key, or
 * if a value encodes to something JSON can't carry, or to JSON that reads
 * back as another value (as a union's members can).
 *
 * Editors should edit text fields through `text`, which says exactly what the
 * user did; `update` has to work it out.
 *
 * **Gotchas**
 *
 * `f` must return a new value rather than change the one it's given, which
 * is the one the document shows. A value written whole that's changed in
 * place (an array that isn't a `List`, pushed to, say) reads as unchanged,
 * so it isn't written, and only this replica shows the change.
 *
 * **Example** (Changing one field)
 *
 * ```ts import.meta.vitest
 * import { Effect, Schema } from "effect"
 * import { Replica, ReplicatedDocument } from "effect-delta"
 *
 * const Settings = Schema.Struct({ theme: Schema.Literals(["light", "dark"]), fontSize: Schema.Int })
 *
 * const program = Effect.gen(function*() {
 *   const settings = yield* ReplicatedDocument.make(Settings, {
 *     initial: { theme: "light", fontSize: 14 },
 *     replica: Replica.ReplicaId.make("laptop")
 *   })
 *   yield* ReplicatedDocument.update(settings, (current) => ({ ...current, theme: "dark" }))
 *   return yield* ReplicatedDocument.get(settings)
 * })
 *
 * await Effect.runPromise(program) // => { theme: "dark", fontSize: 14 }
 * ```
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const update: {
  <A>(f: (value: A) => A): (self: ReplicatedDocument<A>) => Effect.Effect<void, Schema.SchemaError>
  <A>(self: ReplicatedDocument<A>, f: NoInfer<(value: A) => A>): Effect.Effect<void, Schema.SchemaError>
} = dual(2, updateImpl)

/**
 * Replaces the value with `value`, as one change, writing only what changed
 * (see `update`).
 *
 * **Gotchas**
 *
 * As with `update`, give it a new value, not the one the document shows
 * changed in place: a value written whole that's changed in place isn't
 * written, and only this replica shows the change.
 *
 * @stability unstable
 * @category editing
 * @since 0.1.0
 */
export const set: {
  <A>(value: NoInfer<A>): (self: ReplicatedDocument<A>) => Effect.Effect<void, Schema.SchemaError>
  <A>(self: ReplicatedDocument<A>, value: NoInfer<A>): Effect.Effect<void, Schema.SchemaError>
} = dual(2, <A>(self: ReplicatedDocument<A>, value: A) => updateImpl(self, () => value))

/**
 * A path through a value's fields, record keys and list item keys that ends
 * at a string: where a text field can be. Paths are at most 16 steps long, so
 * that a recursive value, such as a `Schema.Json` field, has paths too.
 *
 * @stability unstable
 * @category utility types
 * @since 0.1.0
 */
export type TextPath<A> = TextPathWithin<A, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]>

// The text paths of `A` with at most as many steps as `Steps` has elements.
type TextPathWithin<A, Steps extends ReadonlyArray<0>> = A extends string ? readonly []
  : Steps extends readonly [0, ...infer Rest extends ReadonlyArray<0>] ?
    A extends ReadonlyArray<infer Item> ? readonly [string, ...TextPathWithin<Item, Rest>]
    : A extends ((...args: ReadonlyArray<any>) => any) | Date ? never
    : A extends object ? {
        readonly [K in keyof A & string]-?: readonly [K, ...TextPathWithin<Exclude<A[K], undefined>, Rest>]
      }[keyof A & string]
    : never
  : never

/**
 * Returns the text field at `path`, for the `ReplicatedText` functions:
 * `insert`, `remove`, `applyPatch`, `set`, `update`, `get`, `changes`,
 * `subscribe` and the anchor functions.
 *
 * **Details**
 *
 * Each edit through it is one change to the document. A text field in a
 * record entry or list item that doesn't exist, or was removed, still works:
 * its text is kept, and shows once the entry exists. An `update` that adds the
 * entry writes the text it's given over the kept text, so to keep it, give
 * the entry the handle's text (`ReplicatedText.get`). The path's type allows
 * any string in the value, but only a `Text` is a text field: a path to
 * another string throws, as a defect.
 *
 * **Example** (Editing a text field)
 *
 * ```ts import.meta.vitest
 * import { Effect, Schema } from "effect"
 * import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"
 *
 * const Post = Schema.Struct({ title: ReplicatedDocument.Text, body: ReplicatedDocument.Text })
 *
 * const program = Effect.gen(function*() {
 *   const post = yield* ReplicatedDocument.make(Post, {
 *     initial: { title: "", body: "" },
 *     replica: Replica.ReplicaId.make("laptop")
 *   })
 *   const body = ReplicatedDocument.text(post, ["body"])
 *   yield* ReplicatedText.insert(body, 0, "Hello")
 *   yield* ReplicatedText.insert(body, 5, ", world")
 *   return yield* ReplicatedDocument.get(post)
 * })
 *
 * await Effect.runPromise(program) // => { title: "", body: "Hello, world" }
 * ```
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const text: {
  <A>(path: TextPath<A>): (self: ReplicatedDocument<A>) => ReplicatedText.Editable
  <A>(self: ReplicatedDocument<A>, path: TextPath<A>): ReplicatedText.Editable
} = dual(2, <A>(self: ReplicatedDocument<A>, given: Replica.Path): ReplicatedText.Editable => {
  // Its own copy: the path goes into every change it makes.
  const path = Object.freeze([...given])
  const found = InternalShape.at(impl(self).shape, path)
  if (typeof found === "string" || found.shape._tag !== "Text") {
    throw new Error(`${JSON.stringify(path)} isn't a text field of the document`)
  }
  return InternalEditable.make(impl(self).engine, path)
})

/**
 * A proposal an authority received, read with the document's schema: the
 * value before and after the change, and where the change wrote.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Proposed<A> {
  readonly before: A
  readonly after: A
  readonly paths: ReadonlyArray<Replica.Path>
}

/**
 * Reads a document proposal with the app's schema and initial value, for an
 * authority's check, and rejects a change that doesn't fit the schema. The app
 * picks the schema from the document id, as it already decides who may open
 * which document.
 *
 * **Details**
 *
 * Replicas keep changes that don't fit their schema and don't show them. An
 * authority's check reads JSON, which would show them, so a client could hide
 * a write from the check (by writing over it with a value the schema can't
 * read, say) while every replica shows it. So `readProposal` fails with
 * `Authority.Rejected` when the change writes where the schema has no part,
 * writes a part with an op its rule doesn't take, writes a value the schema
 * can't read, gives a list item a key field that names another item, or adds
 * a list item without placing it in its list. A change that fits reads as
 * replicas will show it: entries, items and optional fields only where they
 * exist, and each value as its latest write.
 *
 * **Gotchas**
 *
 * Read proposals with the newest version of the schema, and upgrade the
 * authority before the apps that write to it: a change from a newer version
 * doesn't fit an older one. A schema or initial value that `make` refuses is a
 * defect here too.
 *
 * **Example** (Only the owner approves)
 *
 * ```ts import.meta.vitest
 * import { Effect, Layer, Schema } from "effect"
 * import { Authority, HistoryStore, ReplicatedDocument } from "effect-delta"
 *
 * const Spec = Schema.Struct({ body: ReplicatedDocument.Text, status: Schema.Literals(["draft", "approved"]) })
 * const initial = { body: "", status: "draft" } as const
 *
 * const OwnerApproves = Authority.layer({
 *   check: (proposal) =>
 *     Effect.gen(function*() {
 *       if (proposal._tag === "Text") return yield* new Authority.Rejected({ message: "Specs aren't texts" })
 *       const { after, before } = yield* ReplicatedDocument.readProposal(proposal, Spec, { initial })
 *       if (after.status === "approved" && before.status !== "approved" && proposal.user !== "owner") {
 *         return yield* new Authority.Rejected({ message: "Only the owner can approve" })
 *       }
 *     })
 * }).pipe(Layer.provide(HistoryStore.layerMemory))
 * ```
 *
 * @stability unstable
 * @category decoding
 * @since 0.1.0
 */
export const readProposal = <A>(
  proposal: Authority.DocumentProposal,
  schema: Schema.Schema<A>,
  options: { readonly initial: NoInfer<A> }
): Effect.Effect<Proposed<A>, Authority.Rejected> =>
  Effect.suspend(() => {
    const shape = shapeOf(schema)
    return Effect.as(checkInitial(schema, shape, options.initial), shape)
  }).pipe(Effect.flatMap((shape) => {
    for (const change of (proposal.delta as InternalDelta.DeltaImpl).changes.values()) {
      const misfit = InternalShape.misfit(shape, change)
      if (misfit !== undefined) return Effect.fail(new Authority.Rejected({ message: misfit }))
    }
    return Effect.succeed({
      before: InternalShape.readJson(shape, proposal.before, options.initial, existsIn(proposal.exists.before)) as A,
      after: InternalShape.readJson(shape, proposal.after, options.initial, existsIn(proposal.exists.after)) as A,
      paths: proposal.paths
    })
  }))

const existsIn = (paths: ReadonlyArray<Replica.Path>): (path: Replica.Path) => boolean => {
  const keys = new Set(paths.map(InternalPath.key))
  return (path) => keys.has(InternalPath.key(path))
}

/**
 * Returns `true` when a value is a replicated document.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isReplicatedDocument = (u: unknown): u is ReplicatedDocument<unknown> => hasProperty(u, TypeId)
