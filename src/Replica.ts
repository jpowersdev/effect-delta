/**
 * The `Replica` module is what every replica shares, whatever it replicates
 * and whatever its strategy: an id, a version vector saying what it has seen,
 * and the ability to hand out deltas and take them in. Syncing and storage
 * work through these, so they work the same for every kind of replica.
 *
 * Two replicas in one process can be brought up to date with `exchange`. Across
 * a network, see the `Sync`, `Relay` and `Authority` modules.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Effect from "effect/Effect"
import { dual } from "effect/Function"
import type * as Inspectable from "effect/Inspectable"
import type { Pipeable } from "effect/Pipeable"
import * as Schema from "effect/Schema"
import type * as Types from "effect/Types"
import type * as Delta from "./Delta.ts"
import * as InternalAnchor from "./internal/anchor.ts"
import * as internal from "./internal/replica.ts"
import type * as ReplicatedText from "./ReplicatedText.ts"
import type * as VersionVector from "./VersionVector.ts"

const TypeId = "~effect-delta/Replica"

/**
 * How a replica merges concurrent edits. `"tree"` keeps every character, even
 * removed ones, in a tree that's always in memory. `"replay"` keeps the plain
 * text and its history, and replays the concurrent part of the history to
 * merge.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Strategy = "tree" | "replay"

/**
 * One copy of a shared value in one place (a tab, a device, a server) that accepts
 * edits immediately and syncs with other copies later.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Replica<out A> extends Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: { readonly _A: Types.Covariant<A> }
  readonly strategy: Strategy
}

/**
 * Returns `true` when a value is a replica of any kind.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isReplica: (u: unknown) => u is Replica<unknown> = internal.isReplica

/**
 * Identifies the stream of changes one replica writes. Unique per replica and
 * never reused. It isn't a user or a connection: one user's tabs and devices
 * are each their own replica.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const ReplicaId = Schema.String.check(Schema.isMinLength(1)).pipe(Schema.brand("ReplicaId"))

/**
 * A replica id: identifies the stream of changes one replica writes.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type ReplicaId = typeof ReplicaId.Type

/**
 * Identifies a room: where the replicas of one shared value meet. Sync and
 * storage use it; a replica itself doesn't need one.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const RoomId = Schema.NonEmptyString.pipe(Schema.brand("RoomId"))

/**
 * A room id: identifies where the replicas of one shared value meet.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type RoomId = typeof RoomId.Type

/**
 * Makes a room id from its name, which can't be empty.
 *
 * **Example** (Naming a room)
 *
 * ```ts import.meta.vitest
 * import { Replica } from "@jpowersdev/effect-delta"
 *
 * Replica.makeRoom("notes") // => "notes"
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const makeRoom = (name: string): RoomId => RoomId.make(name)

/**
 * Schema for an overlap: a stretch of merged text where changes from replicas
 * that hadn't seen each other's edits met.
 *
 * **Details**
 *
 * Every replica converges anyway. An overlap says where a person might want to
 * look, for example where "8 hours" and "12 hours" merged into "812 hours".
 * `range` is where the stretch is in the merged text right now (empty if both
 * sides removed all of it); `anchors` follow it from here on.
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const Overlap: Schema.Codec<Overlap, OverlapEncoded> = Schema.TaggedStruct("Text", {
  range: Schema.Struct({ start: Schema.Int, end: Schema.Int }),
  anchors: InternalAnchor.AnchorRange,
  replicas: Schema.Array(ReplicaId)
})

/**
 * A stretch of merged text where changes from replicas that hadn't seen each
 * other's edits met.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Overlap {
  readonly _tag: "Text"
  readonly range: ReplicatedText.Range
  readonly anchors: ReplicatedText.AnchorRange
  readonly replicas: ReadonlyArray<ReplicaId>
}

/**
 * The JSON form of an overlap.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface OverlapEncoded {
  readonly _tag: "Text"
  readonly range: ReplicatedText.Range
  readonly anchors: ReplicatedText.AnchorRange
  readonly replicas: ReadonlyArray<string>
}

/**
 * A delta held changes made with a different strategy than the replica's.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class StrategyMismatch extends Schema.TaggedError<StrategyMismatch>()("StrategyMismatch", {
  expected: Schema.Literals(["tree", "replay"])
}) {}

/**
 * A delta held a change that can't be applied: it refers to characters that
 * the changes it was made on top of don't have.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class Malformed extends Schema.TaggedError<Malformed>()("Malformed", {
  message: Schema.String
}) {}

/**
 * Two different changes used the same replica id and sequence numbers. That
 * happens when a replica id is reused, for example by two tabs restoring the
 * same saved replica, and it would silently corrupt history if it were
 * applied.
 *
 * **Gotchas**
 *
 * It's caught when one copy has written more under the id than the other has.
 * Two copies that wrote exactly as much have identical version vectors, so
 * neither sends the other anything, and they stay apart without noticing.
 * Never reuse a replica id.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class ReplicaIdReused extends Schema.TaggedError<ReplicaIdReused>()("ReplicaIdReused", {
  replica: ReplicaId
}) {}

/**
 * A replica that answers to an authority was asked to exchange changes with
 * something else. Its changes may still be rejected, so they must not spread.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class AuthorityOnly extends Schema.TaggedError<AuthorityOnly>()("AuthorityOnly", {}) {}

/**
 * A replica couldn't take in a delta. The `reason` says why.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class DeltaError extends Schema.TaggedError<DeltaError>()("DeltaError", {
  reason: Schema.Union([StrategyMismatch, Malformed, ReplicaIdReused, AuthorityOnly])
}) {}

/**
 * Returns the replica id new changes are written under. It's an effect because
 * a replica that answers to an authority moves to a new id after one of its
 * changes is rejected.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const id = (self: Replica<unknown>): Effect.Effect<ReplicaId> => (self as internal.ReplicaImpl).core.id

/**
 * Returns what the replica has seen.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const version = (self: Replica<unknown>): Effect.Effect<VersionVector.VersionVector> =>
  (self as internal.ReplicaImpl).core.version

/**
 * Returns the changes the replica has that a replica at `since` lacks.
 *
 * @stability unstable
 * @category getters
 * @since 0.1.0
 */
export const delta: {
  (since: VersionVector.VersionVector): (self: Replica<unknown>) => Effect.Effect<Delta.Delta>
  (self: Replica<unknown>, since: VersionVector.VersionVector): Effect.Effect<Delta.Delta>
} = dual(
  2,
  (self: Replica<unknown>, since: VersionVector.VersionVector): Effect.Effect<Delta.Delta> =>
    (self as internal.ReplicaImpl).core.delta(since)
)

/**
 * Takes in a delta from another replica and returns the overlaps the merge
 * found.
 *
 * **Details**
 *
 * Receiving is idempotent and order-independent: a delta can arrive more than
 * once and in any order relative to others. Changes that arrive before the
 * changes they were made on top of wait inside the replica until those arrive.
 *
 * @stability unstable
 * @category combinators
 * @since 0.1.0
 */
export const receive: {
  (delta: Delta.Delta): (self: Replica<unknown>) => Effect.Effect<ReadonlyArray<Overlap>, DeltaError>
  (self: Replica<unknown>, delta: Delta.Delta): Effect.Effect<ReadonlyArray<Overlap>, DeltaError>
} = dual(
  2,
  (self: Replica<unknown>, delta: Delta.Delta): Effect.Effect<ReadonlyArray<Overlap>, DeltaError> =>
    (self as internal.ReplicaImpl).core.receive(delta)
)

/**
 * Brings two replicas in the same process up to date with each other, each
 * sending only what the other lacks. Returns the overlaps `self` found.
 *
 * **Example** (Bringing two copies up to date)
 *
 * ```ts import.meta.vitest
 * import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const program = Effect.gen(function*() {
 *   const laptop = yield* ReplicatedText.make({
 *     replica: Replica.ReplicaId.make("laptop")
 *   })
 *   const phone = yield* ReplicatedText.make({
 *     replica: Replica.ReplicaId.make("phone")
 *   })
 *   yield* ReplicatedText.insert(laptop, 0, "Hello")
 *   yield* Replica.exchange(laptop, phone)
 *   return yield* ReplicatedText.get(phone)
 * })
 *
 * await Effect.runPromise(program) // => "Hello"
 * ```
 *
 * @stability unstable
 * @category combinators
 * @since 0.1.0
 */
export const exchange: {
  (that: Replica<unknown>): (self: Replica<unknown>) => Effect.Effect<ReadonlyArray<Overlap>, DeltaError>
  (self: Replica<unknown>, that: Replica<unknown>): Effect.Effect<ReadonlyArray<Overlap>, DeltaError>
} = dual(
  2,
  (self: Replica<unknown>, that: Replica<unknown>): Effect.Effect<ReadonlyArray<Overlap>, DeltaError> =>
    Effect.gen(function*() {
      const [mine, theirs] = yield* Effect.all([version(self), version(that)])
      const [toThat, toSelf] = yield* Effect.all([delta(self, theirs), delta(that, mine)])
      const overlaps = yield* receive(self, toSelf)
      yield* receive(that, toThat)
      return overlaps
    })
)
