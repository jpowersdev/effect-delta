import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import * as Inspectable from "effect/Inspectable"
import { pipeArguments } from "effect/Pipeable"
import { hasProperty } from "effect/Predicate"
import * as Schema from "effect/Schema"
import type * as Delta from "../Delta.ts"
import { Change, key } from "./change.ts"
import * as ReplayText from "./replayText.ts"
import * as TreeText from "./treeText.ts"

/** @internal */
export const AnyChange = Change(Schema.Union([TreeText.Op, ReplayText.Op]))

/** @internal */
export type AnyChange = typeof AnyChange.Type

// Must match the TypeId in Delta.ts.
const TypeId = "~effect-delta/Delta"

/** @internal */
export interface DeltaImpl extends Delta.Delta {
  readonly [TypeId]: typeof TypeId
  readonly changes: ReadonlyMap<string, AnyChange>
}

const Proto = {
  ...Inspectable.BaseProto,
  [TypeId]: TypeId,
  pipe() {
    return pipeArguments(this, arguments)
  },
  toJSON(this: DeltaImpl) {
    return { _id: "Delta", changes: [...this.changes.values()] }
  },
  [Equal.symbol](this: DeltaImpl, that: unknown): boolean {
    if (!isDelta(that) || that.changes.size !== this.changes.size) return false
    for (const [k, change] of this.changes) {
      const other = that.changes.get(k)
      if (other === undefined || !Equal.equals(change, other)) return false
    }
    return true
  },
  [Hash.symbol](this: DeltaImpl): number {
    return Hash.string([...this.changes.keys()].sort().join(","))
  }
}

/** @internal */
export const isDelta = (u: unknown): u is DeltaImpl => hasProperty(u, TypeId)

/** @internal */
export const fromChanges = (changes: Iterable<AnyChange>): DeltaImpl => {
  const map = new Map<string, AnyChange>()
  for (const change of changes) map.set(key(change), change)
  return fromMap(map)
}

const fromMap = (changes: ReadonlyMap<string, AnyChange>): DeltaImpl => {
  const self = Object.create(Proto)
  self.changes = changes
  return self
}

/** @internal */
export const empty: DeltaImpl = fromMap(new Map())

/**
 * What a replica has seen once it has every change here.
 *
 * @internal
 */
export const version = (self: Delta.Delta): { readonly [replica: string]: number } => {
  const seen: Record<string, number> = {}
  for (const change of (self as DeltaImpl).changes.values()) {
    seen[change.replica] = Math.max(seen[change.replica] ?? 0, change.seq + change.length)
  }
  return seen
}

const encodeChange = Schema.encodeSync(AnyChange)

/** @internal */
export const combine = (self: Delta.Delta, that: Delta.Delta): DeltaImpl => {
  const a = (self as DeltaImpl).changes
  const b = (that as DeltaImpl).changes
  if (b.size === 0) return self as DeltaImpl
  if (a.size === 0) return that as DeltaImpl
  const merged = new Map(a)
  for (const [k, change] of b) {
    const existing = merged.get(k)
    // Honest replicas never make two different changes with one key. If a
    // reused or forged id did, keep the same one whichever way round the deltas
    // are combined; the replica that receives it reports the clash.
    if (existing === undefined || (!Equal.equals(existing, change) && canonical(change) < canonical(existing))) {
      merged.set(k, change)
    }
  }
  return fromMap(merged)
}

const canonical = (change: AnyChange): string => JSON.stringify(encodeChange(change))

/** @internal */
export const FORMAT = 1

/** @internal */
export const Encoded = Schema.Struct({
  format: Schema.Literal(FORMAT),
  changes: Schema.Array(AnyChange)
})
