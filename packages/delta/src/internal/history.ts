import type * as VersionVector from "../VersionVector.ts"
import { type Change, compareReplica, type Id, key, lastId } from "./change.ts"

// What a replica knows, whatever its strategy: every change it has applied
// (its history), the changes waiting for something they build on (pending),
// its version vector and its frontier. Changes are applied in causal order:
// a change only once everything it was made on top of has been applied.

/** @internal */
export interface History<Op> {
  readonly changes: Map<string, Change<Op>>
  /** Applied changes per replica, in sequence order. */
  readonly byReplica: Map<string, Array<Change<Op>>>
  /** What each applied change's author had seen when making it, by change key. */
  readonly pasts: Map<string, VersionVector.VersionVector>
  readonly pending: Map<string, Change<Op>>
  readonly version: Record<string, number>
  frontier: ReadonlyArray<Id>
}

/** @internal */
export const make = <Op>(): History<Op> => ({
  changes: new Map(),
  byReplica: new Map(),
  pasts: new Map(),
  pending: new Map(),
  version: {},
  frontier: []
})

/** @internal */
export const seen = (history: History<unknown>, replica: string): number =>
  Object.hasOwn(history.version, replica) ? history.version[replica]! : 0

/**
 * Whether the history already has `change`, applied or pending.
 *
 * @internal
 */
export const has = (history: History<unknown>, change: Change<unknown>): boolean => {
  const k = key(change)
  return history.changes.has(k) || history.pending.has(k) || change.seq + change.length <= seen(history, change.replica)
}

/**
 * What the author of a change made on top of `parents` had seen.
 *
 * @internal
 */
export const pastOf = (history: History<unknown>, parents: ReadonlyArray<Id>): VersionVector.VersionVector => {
  const past: Record<string, number> = {}
  for (const parent of parents) {
    const change = containing(history, parent)
    const before = history.pasts.get(key(change))!
    for (const replica of Object.keys(before)) past[replica] = Math.max(past[replica] ?? 0, before[replica]!)
    past[parent.replica] = Math.max(past[parent.replica] ?? 0, parent.seq + 1)
  }
  return past
}

/**
 * Records `change` as applied: it joins the history, the version vector moves
 * past it, and it replaces its parents in the frontier.
 *
 * @internal
 */
export const record = <Op>(history: History<Op>, change: Change<Op>, past: VersionVector.VersionVector): void => {
  const k = key(change)
  history.changes.set(k, change)
  history.pasts.set(k, past)
  history.pending.delete(k)
  let ofReplica = history.byReplica.get(change.replica)
  if (ofReplica === undefined) {
    ofReplica = []
    history.byReplica.set(change.replica, ofReplica)
  }
  ofReplica.push(change)
  history.version[change.replica] = change.seq + change.length
  history.frontier = [
    ...history.frontier.filter((tip) =>
      !change.parents.some((parent) => parent.replica === tip.replica && parent.seq === tip.seq)
    ),
    lastId(change)
  ].sort((a, b) => compareReplica(a.replica, b.replica) || a.seq - b.seq)
}

/**
 * Removes pending changes that can now be applied, in an order that respects
 * what each was made on top of. The order among independent changes doesn't
 * matter to the result, but it's deterministic anyway.
 *
 * @internal
 */
export const takeReady = <Op>(history: History<Op>): Array<Change<Op>> => {
  const ready: Array<Change<Op>> = []
  // Changes become ready as earlier ones are taken, so track what taking them
  // would have applied without touching the real version vector.
  const version: Record<string, number> = { ...history.version }
  const seenSoFar = (replica: string) => version[replica] ?? 0
  let progress = true
  while (progress) {
    progress = false
    const candidates = [...history.pending.values()].sort((a, b) =>
      compareReplica(a.replica, b.replica) || a.seq - b.seq
    )
    for (const change of candidates) {
      if (
        seenSoFar(change.replica) === change.seq &&
        change.parents.every((parent) => parent.seq < seenSoFar(parent.replica))
      ) {
        history.pending.delete(key(change))
        version[change.replica] = change.seq + change.length
        ready.push(change)
        progress = true
      }
    }
  }
  return ready
}

/**
 * Every change, applied or pending, that a replica at `since` lacks.
 *
 * @internal
 */
export const since = <Op>(history: History<Op>, since: VersionVector.VersionVector): Array<Change<Op>> => {
  const out: Array<Change<Op>> = []
  for (const [replica, changes] of history.byReplica) {
    const known = Object.hasOwn(since, replica) ? since[replica]! : 0
    for (const change of changes) {
      if (change.seq + change.length > known) out.push(change)
    }
  }
  for (const change of history.pending.values()) {
    const known = Object.hasOwn(since, change.replica) ? since[change.replica]! : 0
    if (change.seq + change.length > known) out.push(change)
  }
  return out
}

/**
 * Whether `id` is the last sequence number of an applied change.
 *
 * @internal
 */
export const endsAt = (history: History<unknown>, id: Id): boolean => {
  const change = find(history, id)
  return change !== undefined && change.seq + change.length - 1 === id.seq
}

// The applied change that consumed sequence number `id`.
const containing = <Op>(history: History<Op>, id: Id): Change<Op> => {
  const change = find(history, id)
  if (change === undefined) throw new Error(`No applied change contains ${id.seq}@${id.replica}`)
  return change
}

const find = <Op>(history: History<Op>, id: Id): Change<Op> | undefined => {
  const changes = history.byReplica.get(id.replica) ?? []
  let low = 0
  let high = changes.length - 1
  while (low <= high) {
    const middle = (low + high) >> 1
    const change = changes[middle]!
    if (id.seq < change.seq) high = middle - 1
    else if (id.seq >= change.seq + change.length) low = middle + 1
    else return change
  }
  return undefined
}
