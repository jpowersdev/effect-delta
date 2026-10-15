import type * as Effect from "effect/Effect"
import * as Inspectable from "effect/Inspectable"
import { pipeArguments } from "effect/Pipeable"
import { hasProperty } from "effect/Predicate"
import type * as Delta from "../Delta.ts"
import type * as Replica from "../Replica.ts"
import type * as VersionVector from "../VersionVector.ts"

// Must match the TypeId in Replica.ts.
const TypeId = "~effect-delta/Replica"

/**
 * What every kind of replica provides, whatever its strategy. The public
 * `Replica` functions call these.
 *
 * @internal
 */
export interface Core {
  readonly id: Effect.Effect<Replica.ReplicaId>
  readonly version: Effect.Effect<VersionVector.VersionVector>
  readonly delta: (since: VersionVector.VersionVector) => Effect.Effect<Delta.Delta>
  readonly receive: (delta: Delta.Delta) => Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError>
}

/** @internal */
export interface ReplicaImpl extends Replica.Replica<unknown> {
  readonly core: Core
}

/** @internal */
export const isReplica = (u: unknown): u is ReplicaImpl => hasProperty(u, TypeId)

/** @internal */
export const Proto: object = {
  ...Inspectable.BaseProto,
  [TypeId]: { _A: (_: never) => _ },
  pipe() {
    return pipeArguments(this, arguments)
  }
}
