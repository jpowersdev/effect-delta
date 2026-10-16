import { Effect, Random, Schema, Struct } from "effect"
import { Delta, type Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"
import type * as InternalDocument from "effect-delta/internal/document"
import { Fraction, gapAt, itemAt, Which } from "./sessions.ts"

// Changes made directly, as a buggy or malicious client could send them,
// rather than by a replica. Many are fine; the rest write where the schema has
// nothing, name characters and positions their author hadn't seen or that
// don't exist, put them on a side that doesn't match their right origin or
// on the left of the start, edit outside the text, take no sequence numbers
// or fewer than none, start or name a parent before any change or in the
// middle of one, or are stamped no later than what they were made on top of,
// or, for peers that take changes directly, far ahead of any clock.
//
// A forger makes them one at a time, each on top of earlier ones, and sends
// them as the session says: most on their own, some together with the ones
// made after them, and some only after those, so they arrive late. Once
// they're sent, the caller says which were kept: later changes don't build on
// the rest, and their writers carry on under new replica ids, so later
// changes don't all wait on them.

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

// The project as an older version of its schema has it, before the optional
// fields were added.
export const OldProject = Schema.Struct({
  ...Struct.omit(Project.fields, ["due", "meta"]),
  items: ReplicatedDocument.List(Schema.Struct(Struct.omit(Item.fields, ["extra"])), { key: "id" })
})
export type OldProject = typeof OldProject.Type

/** The parts of a project an older replica shows. */
export const asOld = ({ due: _, meta: __, ...project }: Project): OldProject => ({
  ...project,
  items: project.items.map(({ extra: _, ...item }) => item)
})

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

// Which side of its parent an insert or position goes: mostly as a replica
// puts it, on the right when it names a right origin or has no parent and on
// the left otherwise. 2 is the other side, with a right origin that doesn't
// match it, and 3 the left, which is malformed without a parent.
const Side = between(0, 3)

type Placement =
  | { readonly parent: Id | null; readonly side: "L"; readonly rightOrigin?: Id | null }
  | { readonly parent: Id | null; readonly side: "R"; readonly rightOrigin?: Id | null }

const placement = (parent: Id | null, right: Id | null | undefined, side: number): Placement => {
  if (side === 3) return { parent, side: "L" }
  const onRight = right !== undefined || parent === null
  if (side === 2) return onRight ? { parent, side: "L", rightOrigin: right ?? null } : { parent, side: "R" }
  return onRight ? { parent, side: "R", rightOrigin: right ?? null } : { parent, side: "L" }
}

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
  Schema.TaggedStruct("Place", { where: Where, parent: Ref, right: Schema.optionalKey(Ref), side: Side }),
  // A tree `Insert`, or a replay `InsertAt` at `index`.
  Schema.TaggedStruct("Insert", {
    where: Where,
    text: Schema.Literals(["x", "yz", "abc", "de", "f", ""]),
    parent: Ref,
    right: Schema.optionalKey(Ref),
    side: Side,
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
  // Mostly absent: for peers that take changes directly, stamped far ahead, at
  // one of `farAhead`.
  ahead: Schema.optionalKey(between(0, 3)),
  // Parts written as a replica writes them, before the ops.
  block: Schema.optionalKey(Fraction),
  ops: Schema.Array(OpCommand).check(Schema.isMaxLength(3)),
  // How it's sent: 0 and 1 now, with any sent together with it; 2 together
  // with the next one; 3 after the next ones sent.
  send: between(0, 3)
})

export const ForgedSession = Schema.Array(ChangeCommand).check(Schema.isMaxLength(30))
export const forgedRuns = { runs: 100, size: 100 }

// Write times far ahead of the test clock: a few days, the latest a clock
// reads (a `Date`'s last millisecond), and the last safe integers, after which
// nothing could be stamped.
const farAhead = [3 * 24 * 60 * 60 * 1000, 8.64e15, Number.MAX_SAFE_INTEGER - 1, Number.MAX_SAFE_INTEGER]

type Id = { readonly replica: string; readonly seq: number }

interface Made {
  readonly change: InternalDocument.AnyChange
  /** The indexes of the changes it was made on top of, directly or not. */
  readonly past: ReadonlySet<number>
  /** By path, the characters it inserted into each text and the positions it placed in each list. */
  readonly ids: ReadonlyMap<string, ReadonlyArray<Id>>
  /** Whether it turned out not to be kept. */
  dropped: boolean
}

const pathKey = (path: Path) => JSON.stringify(path)
const pick = <A>(choices: ReadonlyArray<A>, at: number): A | undefined =>
  choices.length === 0 ? undefined : choices[Math.floor(at * choices.length / 1001)]
const pathIn = (paths: ReadonlyArray<Path>, where: typeof Where.Type): Path =>
  pick(where.stray === 0 ? anywhere : paths, where.at)!

const makeForger = (strategy: Replica.Strategy, options: { readonly ahead: boolean }) => {
  const made: Array<Made> = []
  const names = ["w0", "w1", "w2"]
  const renamed = [0, 0, 0]
  const next = new Map<string, number>()
  const last = new Map<string, number>()

  // Two different changes under one key can't both be kept anywhere, so a
  // writer whose next key is taken carries on under a new id.
  const used = new Set<string>()
  const rename = (writer: number) => {
    renamed[writer]!++
    names[writer] = `w${writer}-${renamed[writer]}`
  }

  const forge = (command: typeof ChangeCommand.Type): InternalDocument.AnyChange => {
    const seqOf = (replica: string) => command.quirk === 3 ? -1 : next.get(replica) ?? 0
    while (used.has(`${seqOf(names[command.writer]!)}@${names[command.writer]}`)) rename(command.writer)
    const replica = names[command.writer]!
    const own = last.get(replica)
    const others = command.others.flatMap((at) => pick(made.flatMap((other, i) => other.dropped ? [] : [i]), at) ?? [])
    const parents = new Set([...(command.own && own !== undefined ? [own] : []), ...others])
    const past = new Set<number>()
    for (const parent of parents) {
      past.add(parent)
      for (const before of made[parent]!.past) past.add(before)
    }
    const seq = seqOf(replica)
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
    const place = (path: Path, where: Placement) => {
      note(path.slice(0, -1), { replica, seq: at })
      push({ _tag: "Place", path, ...where }, 1)
    }
    if (command.block !== undefined) {
      const block = pick(blocks, command.block)!
      for (const op of block.ops) push(op, 1)
      if (block.item !== undefined) place(block.item, placement(null, undefined, 0))
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
          place(path, placement(resolve(list, op.parent), op.right && resolve(list, op.right), op.side))
          break
        }
        case "Insert": {
          const path = pathIn(texts, op.where)
          if (strategy === "replay") {
            push({ _tag: "InsertAt", path, index: op.index, text: op.text }, op.text.length)
            break
          }
          const first = at
          push({
            _tag: "Insert",
            path,
            text: op.text,
            ...placement(resolve(path, op.parent), op.right && resolve(path, op.right), op.side)
          }, op.text.length)
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
      // A safe integer, as a change sent as JSON has.
      time: Math.min(
        Number.MAX_SAFE_INTEGER,
        Math.max(
          Math.max(-1, ...[...parents].map((i) => made[i]!.change.time)) + command.later,
          options.ahead && command.ahead !== undefined ? farAhead[command.ahead]! : -Infinity
        )
      ),
      ops
    }
    used.add(`${seq}@${replica}`)
    last.set(replica, made.length)
    next.set(replica, change.seq + change.length)
    made.push({ change, past, ids, dropped: false })
    return change
  }

  // Says that a change wasn't kept.
  const drop = (change: InternalDocument.AnyChange): void => {
    const found = made.find((other) => other.change === change)!
    if (found.dropped) return
    found.dropped = true
    const writer = names.indexOf(change.replica)
    if (writer !== -1) rename(writer)
  }

  return { forge, drop }
}

/**
 * Forges a session's changes and sends them as it says. `send` sends changes
 * together, and says which of those sent so far were kept. With `ahead`, some
 * are stamped far ahead, as servers refuse. Returns every change, in the order
 * they were made.
 */
export const forgeSession = <E, R>(
  strategy: Replica.Strategy,
  session: typeof ForgedSession.Type,
  send: (
    changes: ReadonlyArray<InternalDocument.AnyChange>
  ) => Effect.Effect<(change: InternalDocument.AnyChange) => boolean, E, R>,
  options: { readonly ahead: boolean } = { ahead: false }
): Effect.Effect<ReadonlyArray<InternalDocument.AnyChange>, E, R> =>
  Effect.gen(function*() {
    const forger = makeForger(strategy, options)
    const changes: Array<InternalDocument.AnyChange> = []
    const sent: Array<InternalDocument.AnyChange> = []
    let together: Array<InternalDocument.AnyChange> = []
    let late: Array<InternalDocument.AnyChange> = []
    const flush = Effect.fnUntraced(function*(batch: ReadonlyArray<InternalDocument.AnyChange>) {
      if (batch.length === 0) return
      const kept = yield* send(batch)
      sent.push(...batch)
      for (const change of sent) if (!kept(change)) forger.drop(change)
    })
    for (const command of session) {
      const change = forger.forge(command)
      changes.push(change)
      if (command.send === 3) late.push(change)
      else together.push(change)
      if (command.send < 2) {
        yield* flush(together)
        yield* flush(late)
        together = []
        late = []
      }
    }
    yield* flush(together)
    yield* flush(late)
    return changes
  })

// Sending --------------------------------------------------------------------

/** A delta as it arrives after being sent as JSON, as relays over RPC and stores on a `KeyValueStore` send it. */
export const throughJson = (delta: Delta.Delta): Delta.Delta =>
  Schema.decodeUnknownSync(Delta.Delta)(JSON.parse(JSON.stringify(Schema.encodeSync(Delta.Delta)(delta))))

/** A delta sent one hop: as it is, as between replicas in one process, or as JSON, picked at random. */
export const hop = (delta: Delta.Delta): Effect.Effect<Delta.Delta> =>
  Effect.map(Random.nextBoolean, (json) => json ? throughJson(delta) : delta)

// Honest edits ---------------------------------------------------------------

// Edits a replica makes to a project, among forged changes to the same parts.
// They're the same on an older version of the schema.

const EditCommand = Schema.Union([
  Schema.TaggedStruct("Status", { status: Project.fields.status }),
  Schema.TaggedStruct("Vote", { by: Schema.Literals([-1, 1, 2]) }),
  Schema.TaggedStruct("Type", { at: Fraction, text: Schema.Literals(["x", "yz"]) }),
  Schema.TaggedStruct("Note", { pinned: Schema.Boolean, remove: Schema.Boolean }),
  // Adds or moves an item, or removes it.
  Schema.TaggedStruct("Item", { key: Schema.Literals(["i1", "i2"]), at: Fraction, remove: Schema.Boolean }),
  Schema.TaggedStruct("Value", { at: Fraction, value: Schema.Literals(["x", "y"]), remove: Schema.Boolean }),
  // Waits until the replica has heard back about everything it sent.
  Schema.TaggedStruct("Wait", {})
])

export const EditSession = Schema.Array(EditCommand).check(Schema.isMaxLength(20))

/** Makes an edit, or for `Wait`, runs `wait`. */
export const editProject = Effect.fnUntraced(function*(
  doc: ReplicatedDocument.ReplicatedDocument<OldProject>,
  command: typeof EditCommand.Type,
  wait: Effect.Effect<void>
) {
  const update = (f: (project: OldProject) => OldProject) => Effect.orDie(ReplicatedDocument.update(doc, f))
  switch (command._tag) {
    case "Status":
      return yield* update((project) => ({ ...project, status: command.status }))
    case "Vote":
      // Forged increments can take the votes to the last safe integer.
      return yield* update((project) => ({
        ...project,
        votes: Math.max(Number.MIN_SAFE_INTEGER, Math.min(Number.MAX_SAFE_INTEGER, project.votes + command.by))
      }))
    case "Type": {
      const title = ReplicatedDocument.text(doc, ["title"])
      const before = yield* ReplicatedText.get(title)
      return yield* Effect.orDie(ReplicatedText.insert(title, gapAt(command.at, before.length), command.text))
    }
    case "Note":
      return yield* update(({ notes: { a, ...notes }, ...project }) => ({
        ...project,
        notes: command.remove ? notes : { ...notes, a: { body: a?.body ?? "", pinned: command.pinned } }
      }))
    case "Item":
      return yield* update((project) => {
        const found = project.items.find(({ id }) => id === command.key)
        const items = project.items.filter(({ id }) => id !== command.key)
        if (command.remove) return { ...project, items }
        const item = found ?? { id: command.key, title: "", done: false, tags: {}, subs: [] }
        const index = gapAt(command.at, items.length)
        return { ...project, items: [...items.slice(0, index), item, ...items.slice(index)] }
      })
    case "Value":
      return yield* update((project) => {
        const { vals } = project
        if (command.remove) return { ...project, vals: vals.filter((_, i) => i !== itemAt(command.at, vals.length)) }
        const index = gapAt(command.at, vals.length)
        return { ...project, vals: [...vals.slice(0, index), command.value, ...vals.slice(index)] }
      })
    case "Wait":
      return yield* wait
  }
})
