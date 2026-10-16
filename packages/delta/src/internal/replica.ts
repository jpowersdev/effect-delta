import type * as Effect from "effect/Effect"
import * as Inspectable from "effect/Inspectable"
import { pipeArguments } from "effect/Pipeable"
import { hasProperty } from "effect/Predicate"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import type * as Delta from "../Delta.ts"
import type * as Replica from "../Replica.ts"
import type * as VersionVector from "../VersionVector.ts"

// Must match the TypeId in Replica.ts.
const TypeId = "~effect-delta/Replica"

/**
 * What a replica keeps in a history store.
 *
 * @internal
 */
export interface Stored {
  /** Changes the replica had applied (for a confirmed replica, only the confirmed ones). */
  readonly history: Delta.Delta
  /** A confirmed replica's own changes its authority hadn't acknowledged. */
  readonly unconfirmed: Delta.Delta
  /** Every replica id this document's replicas have written under here. */
  readonly writers: ReadonlyArray<Replica.ReplicaId>
}

/**
 * What every kind of replica provides, whatever its strategy. The public
 * `Replica` functions, sync and storage all go through these.
 *
 * @internal
 */
export interface Core {
  readonly id: Effect.Effect<Replica.ReplicaId>
  readonly version: Effect.Effect<VersionVector.VersionVector>
  /** Changes a replica at `since` lacks. A confirmed replica leaves out its unconfirmed ones. */
  readonly delta: (since: VersionVector.VersionVector) => Effect.Effect<Delta.Delta>
  /** Takes in a delta from a peer. A confirmed replica refuses. */
  readonly receive: (delta: Delta.Delta) => Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError>

  /** Takes in a delta from the relay or authority this replica syncs through. */
  readonly accept: (delta: Delta.Delta) => Effect.Effect<ReadonlyArray<Replica.Overlap>, Replica.DeltaError>
  /** This replica's own changes (under any id it has written with) that a replica at `since` lacks. */
  readonly outgoing: (since: VersionVector.VersionVector) => Effect.Effect<Delta.Delta>
  /** Notifies after every change, local or merged. */
  readonly changed: Effect.Effect<Stream.Stream<void>, never, Scope.Scope>
  /** What to save: changes since `since`, plus the unconfirmed ones and the writer ids. */
  readonly toStore: (since: VersionVector.VersionVector) => Effect.Effect<
    Stored & { readonly version: VersionVector.VersionVector }
  >
  /** Loads what a store had saved. */
  readonly restore: (stored: Stored) => Effect.Effect<void, Replica.DeltaError>
  /** For replicas made to answer to an authority. */
  readonly confirmed: Confirmed | undefined
}

/**
 * The extra hooks of a replica whose changes an authority confirms.
 *
 * @internal
 */
export interface Confirmed {
  /** The authority has everything up to `version`. */
  readonly acknowledge: (version: VersionVector.VersionVector) => Effect.Effect<void>
  /**
   * The authority accepted up to `version` and rejected the next change: drop
   * every own change it doesn't have, and continue under a new replica id.
   */
  readonly reject: (version: VersionVector.VersionVector, message: string) => Effect.Effect<void>
}

/** @internal */
export interface ReplicaImpl extends Replica.Replica<unknown> {
  readonly core: Core
}

/** @internal */
export const core = (replica: Replica.Replica<unknown>): Core => (replica as ReplicaImpl).core

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
