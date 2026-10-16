/**
 * Version vectors record what a replica has seen: for each replica id, how
 * many of that replica's sequence numbers it knows. Two replicas compare
 * version vectors to work out what the other is missing, and a replica sends
 * its version vector to ask for exactly that.
 *
 * Every inserted or removed character consumes one sequence number, so a
 * version vector also says how much of each replica's editing has been seen.
 * Version vectors combine by taking the larger count for each replica.
 *
 * @stability unstable
 * @since 0.1.0
 */
import { dual } from "effect/Function"
import * as Reducer from "effect/Reducer"
import * as Schema from "effect/Schema"
import { ReplicaName } from "./internal/change.ts"

/**
 * What a replica has seen: for each replica id, how many of its sequence
 * numbers. A replica that isn't listed has been seen not at all.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type VersionVector = { readonly [replica: string]: number }

/**
 * Schema for a version vector, encoded as a plain JSON object.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const VersionVector: Schema.Codec<VersionVector, { readonly [replica: string]: number }> = Schema.Record(
  ReplicaName,
  Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
)

/**
 * The version vector of a replica that has seen nothing.
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const empty: VersionVector = {}

/**
 * Returns how many of `replica`'s sequence numbers `self` has seen.
 *
 * **Example** (Reading a version vector)
 *
 * ```ts import.meta.vitest
 * import { VersionVector } from "effect-delta"
 *
 * VersionVector.get({ alice: 3 }, "alice") // => 3
 * VersionVector.get({ alice: 3 }, "bob") // => 0
 * ```
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const get: {
  (replica: string): (self: VersionVector) => number
  (self: VersionVector, replica: string): number
} = dual(2, (self: VersionVector, replica: string): number => Object.hasOwn(self, replica) ? self[replica]! : 0)

/**
 * Combines two version vectors into one that has seen everything either has,
 * by taking the larger count for each replica.
 *
 * **Example** (Combining what two replicas have seen)
 *
 * ```ts import.meta.vitest
 * import { VersionVector } from "effect-delta"
 *
 * VersionVector.combine({ alice: 3, bob: 1 }, { bob: 4 }) // => { alice: 3, bob: 4 }
 * ```
 *
 * @stability unstable
 * @category combining
 * @since 0.1.0
 */
export const combine: {
  (that: VersionVector): (self: VersionVector) => VersionVector
  (self: VersionVector, that: VersionVector): VersionVector
} = dual(2, (self: VersionVector, that: VersionVector): VersionVector => {
  const out: Record<string, number> = { ...self }
  for (const replica of Object.keys(that)) {
    out[replica] = Math.max(get(self, replica), that[replica]!)
  }
  return out
})

/**
 * A `Reducer` for version vectors: `combine`, starting from `empty`. It is
 * commutative and idempotent as well as associative.
 *
 * @stability unstable
 * @category instances
 * @since 0.1.0
 */
export const ReducerMax: Reducer.Reducer<VersionVector> = Reducer.make(combine, empty)

/**
 * How two version vectors relate: whether `self` has seen exactly the same as
 * `that`, a subset of it, a superset of it, or each has seen something the
 * other hasn't.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Comparison = "equal" | "before" | "after" | "concurrent"

/**
 * Compares two version vectors. `"before"` means `that` has seen everything
 * `self` has and more; `"concurrent"` means each has seen something the other
 * hasn't.
 *
 * **Example** (Comparing what two replicas have seen)
 *
 * ```ts import.meta.vitest
 * import { VersionVector } from "effect-delta"
 *
 * VersionVector.compare({ alice: 2 }, { alice: 3 }) // => "before"
 * VersionVector.compare({ alice: 3 }, { bob: 1 }) // => "concurrent"
 * ```
 *
 * @stability unstable
 * @category comparing
 * @since 0.1.0
 */
export const compare: {
  (that: VersionVector): (self: VersionVector) => Comparison
  (self: VersionVector, that: VersionVector): Comparison
} = dual(2, (self: VersionVector, that: VersionVector): Comparison => {
  let selfAhead = false
  let thatAhead = false
  for (const replica of new Set([...Object.keys(self), ...Object.keys(that)])) {
    const a = get(self, replica)
    const b = get(that, replica)
    if (a > b) selfAhead = true
    if (b > a) thatAhead = true
  }
  return selfAhead ? (thatAhead ? "concurrent" : "after") : (thatAhead ? "before" : "equal")
})
