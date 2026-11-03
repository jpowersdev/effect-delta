/**
 * A delta is what replicas send each other: a set of changes, usually
 * everything one replica has that another lacks. Deltas combine by union, so
 * combining is commutative, associative and idempotent: deltas can arrive in
 * any order, any number of times, and a replica ends up with the same text.
 *
 * A delta is opaque. Get one from `Replica.delta`, send it (it has a JSON
 * codec), and give it to `Replica.receive`. A replica's whole history is just
 * the delta since the empty version vector.
 *
 * @stability unstable
 * @since 0.1.0
 */
import type * as Equal from "effect/Equal"
import { dual } from "effect/Function"
import type * as Inspectable from "effect/Inspectable"
import type { Pipeable } from "effect/Pipeable"
import * as Reducer from "effect/Reducer"
import * as Schema from "effect/Schema"
import * as SchemaTransformation from "effect/SchemaTransformation"
import * as internal from "./internal/delta.ts"
import type * as VersionVector from "./VersionVector.ts"

const TypeId = "~effect-delta/Delta"

/**
 * A set of changes sent between replicas.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Delta extends Pipeable, Equal.Equal, Inspectable.Inspectable {
  readonly [TypeId]: typeof TypeId
}

/**
 * The JSON form of a delta: a format version and the changes. The shape of a
 * change is an implementation detail; treat it as opaque.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Encoded {
  readonly format: number
  readonly changes: ReadonlyArray<unknown>
}

/**
 * Returns `true` when a value is a `Delta`.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isDelta: (u: unknown) => u is Delta = internal.isDelta

/**
 * Schema for a delta, encoded as JSON with its own format version.
 *
 * **Example** (Sending a delta as JSON)
 *
 * ```ts import.meta.vitest
 * import { Schema } from "effect"
 * import { Delta } from "@jpowersdev/effect-delta"
 *
 * const json = Schema.encodeSync(Delta.Delta)(Delta.empty)
 * json // => { format: 1, changes: [] }
 * Delta.isEmpty(Schema.decodeUnknownSync(Delta.Delta)(json)) // => true
 * ```
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Delta: Schema.Codec<Delta, Encoded> = internal.Encoded.pipe(
  Schema.decodeTo(
    Schema.declare(isDelta, { expected: "Delta" }),
    SchemaTransformation.transform<Delta, typeof internal.Encoded.Type>({
      decode: (encoded) => internal.fromChanges(encoded.changes),
      encode: (delta) => ({ format: internal.FORMAT, changes: [...(delta as internal.DeltaImpl).changes.values()] })
    })
  )
)

/**
 * The delta with no changes.
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const empty: Delta = internal.empty

/**
 * Returns `true` when a delta has no changes, so there's nothing worth sending.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isEmpty = (self: Delta): boolean => (self as internal.DeltaImpl).changes.size === 0

/**
 * Returns the version vector of a delta's changes: for each replica, how far
 * they reach. A replica has them all once its own version is at least this.
 *
 * **Example** (Checking whether a replica has a delta's changes)
 *
 * ```ts import.meta.vitest
 * import { Delta, Replica, ReplicatedText, VersionVector } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const notes = yield* ReplicatedText.make({
 *     replica: Replica.makeId("laptop")
 *   })
 *   yield* ReplicatedText.insert(notes, 0, "Hello")
 *   const delta = yield* Replica.delta(notes, VersionVector.empty)
 *   return VersionVector.compare(Delta.version(delta), yield* Replica.version(notes))
 * })
 *
 * await Effect.runPromise(program) // => "equal"
 * ```
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const version: (self: Delta) => VersionVector.VersionVector = internal.version

/**
 * Combines two deltas into one holding the changes of both.
 *
 * **Details**
 *
 * Combining is commutative, associative and idempotent for deltas made by
 * honest replicas. If two deltas disagree about the same change (a reused or
 * forged replica id), `combine` keeps one of them, the same one whichever way
 * round, and the replica that receives it reports the clash.
 *
 * @stability unstable
 * @category combining
 * @since 0.1.0
 */
export const combine: {
  (that: Delta): (self: Delta) => Delta
  (self: Delta, that: Delta): Delta
} = dual(2, internal.combine)

/**
 * A `Reducer` for deltas: `combine`, starting from `empty`.
 *
 * @stability unstable
 * @category instances
 * @since 0.1.0
 */
export const ReducerUnion: Reducer.Reducer<Delta> = Reducer.make<Delta>(internal.combine, empty)
