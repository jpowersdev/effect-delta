import * as Schema from "effect/Schema"

// A change is the unit that is stored, sent and never modified: the ops one
// replica committed together, the sequence numbers they consume, the frontier
// they were made on top of, and when they were made. Every inserted or removed
// character consumes one sequence number, and so does every other op, so a
// change covers the range [seq, seq + length) of its replica's sequence
// numbers.

/**
 * A replica id as changes and version vectors carry it: never empty, and never
 * `__proto__`, which a plain object can't hold as a key.
 *
 * @internal
 */
export const ReplicaName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.makeFilter((name) => name !== "__proto__", { expected: "a replica id other than __proto__" })
)

/**
 * One sequence number of one replica: the identity of a character, and the
 * name of a version when it's the last sequence number of a change.
 *
 * @internal
 */
export interface Id {
  readonly replica: string
  readonly seq: number
}

/** @internal */
export const Id: Schema.Codec<Id> = Schema.Struct({
  replica: ReplicaName,
  seq: Schema.Int
})

/**
 * A run of consecutive sequence numbers from one replica.
 *
 * @internal
 */
export interface Span {
  readonly replica: string
  readonly seq: number
  readonly length: number
}

/** @internal */
export const Span: Schema.Codec<Span> = Schema.Struct({
  replica: ReplicaName,
  seq: Schema.Int,
  length: Schema.Int
})

/** @internal */
export interface Change<Op> {
  readonly replica: string
  readonly seq: number
  readonly length: number
  readonly parents: ReadonlyArray<Id>
  /**
   * The write time: milliseconds from `Clock`, moved later when needed so it's
   * later than every change the replica had seen.
   */
  readonly time: number
  readonly ops: ReadonlyArray<Op>
}

/** @internal */
export const Change = <Op extends Schema.Top>(op: Op) =>
  Schema.Struct({
    replica: ReplicaName,
    seq: Schema.Int,
    length: Schema.Int,
    parents: Schema.Array(Id),
    time: Schema.Int,
    ops: Schema.Array(op)
  })

/**
 * The key a change is stored under: its first sequence number. Two honest
 * replicas never produce different changes with the same key.
 *
 * @internal
 */
export const key = (change: Change<unknown>): string => `${change.seq}@${change.replica}`

/** @internal */
export const idKey = (id: Id): string => `${id.seq}@${id.replica}`

/** @internal */
export const lastId = (change: Change<unknown>): Id => ({
  replica: change.replica,
  seq: change.seq + change.length - 1
})

/**
 * Orders writes: by write time, then replica id, then sequence number. The
 * largest is the latest write.
 *
 * @internal
 */
export const compareWrites = (
  a: { readonly time: number; readonly replica: string; readonly seq: number },
  b: { readonly time: number; readonly replica: string; readonly seq: number }
): number => a.time - b.time || compareReplica(a.replica, b.replica) || a.seq - b.seq

/**
 * Orders replica ids. Concurrent inserts at the same spot are ordered by the
 * replicas that made them, so every replica must compare them the same way:
 * by UTF-16 code units, as JavaScript's `<` does.
 *
 * @internal
 */
export const compareReplica = (a: string, b: string): -1 | 0 | 1 => a < b ? -1 : a > b ? 1 : 0
