import { Schema } from "effect"
import { type Replica, ReplicatedDocument } from "effect-delta"
import type * as InternalDocument from "effect-delta/internal/document"
import { Fraction, Which } from "./sessions.ts"

// Changes made directly, as a buggy or malicious client could send them,
// rather than by a replica. Many are fine; the rest write where the schema has
// nothing, name characters and positions their author hadn't seen or that
// don't exist, edit outside the text, take no sequence numbers or fewer than
// none, start or name a parent before any change or in the middle of one, or
// are stamped no later than what they were made on top of.
//
// A forger makes them one at a time, each on top of earlier ones. The caller
// says whether each was kept: one that wasn't is forgotten, and its writer
// carries on under a new replica id, so later changes don't all wait on it.

const Sub = Schema.Struct({ id: Schema.String, note: Schema.optionalKey(ReplicatedDocument.Text) })
const Tag = Schema.Struct({ label: ReplicatedDocument.Text, n: ReplicatedDocument.Counter })
const Item = Schema.Struct({
  id: Schema.String,
  title: ReplicatedDocument.Text,
  done: Schema.Boolean,
  tags: Schema.Record(Schema.String, Tag),
  extra: Schema.optionalKey(Schema.Literals(["a", "b"])),
  subs: ReplicatedDocument.List(Sub, { key: "id" })
})

// Every merge rule, with records and lists inside list items.
export const Project = Schema.Struct({
  status: Schema.Literals(["draft", "done"]),
  title: ReplicatedDocument.Text,
  votes: ReplicatedDocument.Counter,
  notes: Schema.Record(Schema.String, Schema.Struct({ body: ReplicatedDocument.Text, pinned: Schema.Boolean })),
  due: Schema.optionalKey(Schema.Literals(["mon", "fri"])),
  meta: Schema.optionalKey(Schema.Struct({ x: ReplicatedDocument.Text, y: Schema.Boolean })),
  items: ReplicatedDocument.List(Item, { key: "id" }),
  vals: ReplicatedDocument.List(Schema.String)
})
export type Project = typeof Project.Type

export const initialProject: Project = { status: "draft", title: "", votes: 0, notes: {}, items: [], vals: [] }

type Path = ReadonlyArray<string>

const each = <A>(f: (item: string) => ReadonlyArray<A>): Array<A> => ["i1", "i2"].flatMap(f)

// Where ops write, by the rule that takes them.
const texts: ReadonlyArray<Path> = [
  ["title"],
  ["notes", "a", "body"],
  ["meta", "x"],
  ...each((i) => [["items", i, "title"], ["items", i, "tags", "t", "label"], ["items", i, "subs", "s", "note"]])
]
const items: ReadonlyArray<Path> = [
  ["vals", "v1"],
  ["vals", "v2"],
  ...each((i) => [["items", i], ["items", i, "subs", "s"]])
]
const values: ReadonlyArray<Path> = [
  ["status"],
  ["due"],
  ["notes", "a", "pinned"],
  ["meta", "y"],
  ["vals", "v1"],
  ["vals", "v2"],
  ...each((i) => [["items", i, "id"], ["items", i, "done"], ["items", i, "extra"], ["items", i, "subs", "s", "id"]])
]
const presences: ReadonlyArray<Path> = [
  ["notes", "a"],
  ["due"],
  ["meta"],
  ["meta", "x"],
  ...each((i) => [["items", i], ["items", i, "tags", "t"], ["items", i, "extra"], ["items", i, "subs", "s"]])
]
const counters: ReadonlyArray<Path> = [["votes"], ...each((i) => [["items", i, "tags", "t", "n"]])]
// And parts the schema doesn't have, the root among them.
const anywhere: ReadonlyArray<Path> = [
  ...texts,
  ...items,
  ...values,
  ...presences,
  ...counters,
  [],
  ["owner"],
  ["notes"],
  ["items"]
]

const json: ReadonlyArray<Schema.Json> = ["draft", "done", "i1", "i2", "s", "a", "mon", "x", true, false, 1, {}]

// Parts written the way a replica writes them, so lists and entries fill up:
// their ops, then for a list item, a position at the start of its list.
const blocks: ReadonlyArray<{ readonly ops: ReadonlyArray<InternalDocument.Op>; readonly item?: Path }> = [
  { ops: [{ _tag: "Add", path: ["notes", "a"] }, { _tag: "Set", path: ["notes", "a", "pinned"], value: false }] },
  { ops: [{ _tag: "Add", path: ["meta"] }, { _tag: "Set", path: ["meta", "y"], value: true }] },
  { ops: [{ _tag: "Set", path: ["vals", "v1"], value: "x" }], item: ["vals", "v1"] },
  ...each((i) =>
    [
      {
        ops: [
          { _tag: "Add", path: ["items", i] },
          { _tag: "Set", path: ["items", i, "id"], value: i },
          { _tag: "Set", path: ["items", i, "done"], value: false }
        ],
        item: ["items", i]
      },
      {
        ops: [
          { _tag: "Add", path: ["items", i, "subs", "s"] },
          { _tag: "Set", path: ["items", i, "subs", "s", "id"], value: "s" }
        ],
        item: ["items", i, "subs", "s"]
      },
      { ops: [{ _tag: "Add", path: ["items", i, "tags", "t"] }] }
    ] as const
  )
]

const between = (minimum: number, maximum: number) => Schema.Int.check(Schema.isBetween({ minimum, maximum }))

// A character or list position an op names, picked by `at`. By `kind`, mostly
// one its author had seen (or the start, if it had seen none); 3 is the start,
// 4 one it hadn't seen, and 5 one that doesn't exist.
const Ref = Schema.Struct({ at: Fraction, kind: between(0, 5) })

// Where an op writes: picked by `at` from the parts whose rule takes it, or
// with `stray` 0, from anywhere.
const Where = Schema.Struct({ at: Fraction, stray: between(0, 3) })

// How many characters a removal takes: 0 or fewer is malformed.
const Length = between(-1, 4)
// Where a replay op edits, which can be past the end of its text.
const Index = between(0, 6)

const OpCommand = Schema.Union([
  Schema.TaggedStruct("Write", {
    op: Schema.Literals(["Set", "Add", "Unset", "Increment"]),
    where: Where,
    value: Fraction,
    by: Schema.Literals([-1, 0, 1, 2, Number.MAX_SAFE_INTEGER])
  }),
  // With `swap`, a side that doesn't match whether there's a right origin.
  Schema.TaggedStruct("Place", { where: Where, parent: Ref, right: Schema.optionalKey(Ref), swap: Schema.Boolean }),
  // A tree `Insert`, or a replay `InsertAt` at `index`.
  Schema.TaggedStruct("Insert", {
    where: Where,
    text: Schema.Literals(["x", "yz", "abc", "de", "f", ""]),
    parent: Ref,
    right: Schema.optionalKey(Ref),
    index: Index
  }),
  // A tree `Remove` of a span from `target`, or a replay `RemoveAt` at `index`.
  Schema.TaggedStruct("Remove", { where: Where, target: Ref, length: Length, index: Index })
])

const ChangeCommand = Schema.Struct({
  writer: Which,
  // Whether it's made on top of its writer's last change, as a replica's is.
  own: Schema.Boolean,
  others: Schema.Array(Fraction).check(Schema.isMaxLength(2)),
  // Mostly nothing: 1 names the first of the others by its first id rather
  // than its last (in its middle, if it has more than one), 2 adds a parent
  // before any change, and 3 starts the change before any.
  quirk: between(0, 9),
  // Its write time after the latest of its parents': 0 or less is too early.
  later: between(-1, 9),
  // Parts written as a replica writes them, before the ops.
  block: Schema.optionalKey(Fraction),
  ops: Schema.Array(OpCommand).check(Schema.isMaxLength(3))
})

export const ForgedSession = Schema.Array(ChangeCommand).check(Schema.isMaxLength(30))
export const forgedRuns = { runs: 100, size: 100 }

type Id = { readonly replica: string; readonly seq: number }

interface Made {
  readonly change: InternalDocument.AnyChange
  /** The indexes of the changes it was made on top of, directly or not. */
  readonly past: ReadonlySet<number>
  /** By path, the characters it inserted into each text and the positions it placed in each list. */
  readonly ids: ReadonlyMap<string, ReadonlyArray<Id>>
}

const pathKey = (path: Path) => JSON.stringify(path)
const pick = <A>(choices: ReadonlyArray<A>, at: number): A | undefined =>
  choices.length === 0 ? undefined : choices[Math.floor(at * choices.length / 1001)]
const pathIn = (paths: ReadonlyArray<Path>, where: typeof Where.Type): Path =>
  pick(where.stray === 0 ? anywhere : paths, where.at)!

export const makeForger = (strategy: Replica.Strategy) => {
  const made: Array<Made> = []
  const names = ["w0", "w1", "w2"]
  const renamed = [0, 0, 0]
  const next = new Map<string, number>()
  const last = new Map<string, number>()
  let forged: Made | undefined

  const forge = (command: typeof ChangeCommand.Type): InternalDocument.AnyChange => {
    const replica = names[command.writer]!
    const own = last.get(replica)
    const others = command.others.flatMap((at) => pick(made.map((_, i) => i), at) ?? [])
    const parents = new Set([...(command.own && own !== undefined ? [own] : []), ...others])
    const past = new Set<number>()
    for (const parent of parents) {
      past.add(parent)
      for (const before of made[parent]!.past) past.add(before)
    }
    const seq = command.quirk === 3 ? -1 : next.get(replica) ?? 0
    const ids = new Map<string, Array<Id>>()
    const note = (path: Path, id: Id) => {
      const k = pathKey(path)
      ids.set(k, [...(ids.get(k) ?? []), id])
    }
    // The id a ref names among those at `path`.
    const resolve = (path: Path, ref: typeof Ref.Type): Id | null => {
      const k = pathKey(path)
      const from = (seen: boolean) => made.flatMap((other, i) => past.has(i) === seen ? other.ids.get(k) ?? [] : [])
      switch (ref.kind) {
        case 3:
          return null
        case 4:
          return pick(from(false), ref.at) ?? null
        case 5:
          return { replica, seq: seq + 1000 }
        default:
          return pick([...from(true), ...(ids.get(k) ?? [])], ref.at) ?? null
      }
    }

    const ops: Array<InternalDocument.Op> = []
    let at = seq
    const push = (op: InternalDocument.Op, length: number) => {
      ops.push(op)
      at += length
    }
    const place = (path: Path, parent: Id | null, right: Id | null | undefined, swap: boolean) => {
      note(path.slice(0, -1), { replica, seq: at })
      push(
        (right !== undefined) !== swap
          ? { _tag: "Place", path, parent, side: "R", rightOrigin: right ?? null }
          : { _tag: "Place", path, parent, side: "L" },
        1
      )
    }
    if (command.block !== undefined) {
      const block = pick(blocks, command.block)!
      for (const op of block.ops) push(op, 1)
      if (block.item !== undefined) place(block.item, null, undefined, false)
    }
    for (const op of command.ops) {
      switch (op._tag) {
        case "Write": {
          const path = pathIn(op.op === "Set" ? values : op.op === "Increment" ? counters : presences, op.where)
          push(
            op.op === "Set"
              ? { _tag: "Set", path, value: pick(json, op.value)! }
              : op.op === "Increment"
              ? { _tag: "Increment", path, by: op.by }
              : { _tag: op.op, path },
            1
          )
          break
        }
        case "Place": {
          const path = pathIn(items, op.where)
          const list = path.slice(0, -1)
          place(path, resolve(list, op.parent), op.right && resolve(list, op.right), op.swap)
          break
        }
        case "Insert": {
          const path = pathIn(texts, op.where)
          if (strategy === "replay") {
            push({ _tag: "InsertAt", path, index: op.index, text: op.text }, op.text.length)
            break
          }
          const parent = resolve(path, op.parent)
          const right = op.right && resolve(path, op.right)
          const first = at
          push(
            right === undefined
              ? { _tag: "Insert", path, text: op.text, parent, side: "L" }
              : { _tag: "Insert", path, text: op.text, parent, side: "R", rightOrigin: right },
            op.text.length
          )
          for (let i = 0; i < op.text.length; i++) note(path, { replica, seq: first + i })
          break
        }
        case "Remove": {
          const path = pathIn(texts, op.where)
          if (strategy === "replay") {
            push({ _tag: "RemoveAt", path, index: op.index, length: op.length }, op.length)
            break
          }
          const target = resolve(path, op.target)
          // With nothing to name, a removal of nothing.
          if (target === null && op.length > 0) break
          const targets = target === null ? [] : [{ ...target, length: op.length }]
          push({ _tag: "Remove", path, targets }, targets.reduce((sum, span) => sum + span.length, 0))
          break
        }
      }
    }

    const middle = command.quirk === 1 ? others[0] : undefined
    const change: InternalDocument.AnyChange = {
      replica,
      seq,
      length: at - seq,
      parents: [
        ...[...parents].map((i) => {
          const { change: parent } = made[i]!
          return { replica: parent.replica, seq: i === middle ? parent.seq : parent.seq + parent.length - 1 }
        }),
        ...(command.quirk === 2 ? [{ replica, seq: -1 }] : [])
      ],
      time: Math.max(-1, ...[...parents].map((i) => made[i]!.change.time)) + command.later,
      ops
    }
    forged = { change, past, ids }
    return change
  }

  // Says whether the change just forged was kept.
  const settle = (kept: boolean): void => {
    const { change } = forged!
    if (kept) {
      last.set(change.replica, made.length)
      next.set(change.replica, change.seq + change.length)
      made.push(forged!)
    } else {
      const writer = names.indexOf(change.replica)
      renamed[writer]!++
      names[writer] = `w${writer}-${renamed[writer]}`
    }
    forged = undefined
  }

  return { forge, settle }
}
