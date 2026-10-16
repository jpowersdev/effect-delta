import { assert } from "@effect/vitest"
import { Effect, Equal, Option, Random, Schema, Stream, Struct } from "effect"
import { Delta, Replica, ReplicatedDocument, ReplicatedText, Sync, VersionVector } from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import * as InternalDocument from "effect-delta/internal/document"
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
//
// About half are made honestly instead, by extra replicas that take some of
// the changes made so far and edit as an app does: they write, remove and
// move the same parts as each other and the forger, without seeing each
// other's changes, so well-formed changes meet in every way a merge reports.

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

// Every merge rule, with records and lists inside list items, and entries that
// are counters.
export const Project = Schema.Struct({
  status: Schema.Literals(["draft", "done"]),
  title: ReplicatedDocument.Text,
  votes: ReplicatedDocument.Counter,
  scores: Schema.Record(Schema.String, ReplicatedDocument.Counter),
  notes: Schema.Record(Schema.String, Schema.Struct({ body: ReplicatedDocument.Text, pinned: Schema.Boolean })),
  due: Schema.optionalKey(Schema.Literals(["mon", "fri"])),
  meta: Schema.optionalKey(Schema.Struct({ x: ReplicatedDocument.Text, y: Schema.Boolean })),
  items: ReplicatedDocument.List(Item, { key: "id" }),
  vals: ReplicatedDocument.List(Schema.String)
})
export type Project = typeof Project.Type

export const initialProject: Project = {
  status: "draft",
  title: "",
  votes: 0,
  scores: {},
  notes: {},
  items: [],
  vals: []
}

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
  ["scores", "k"],
  ["notes", "a"],
  ["due"],
  ["meta"],
  ["meta", "x"],
  ...each((i) => [["items", i], ["items", i, "tags", "t"], ["items", i, "extra"], ["items", i, "subs", "s"]])
]
const counters: ReadonlyArray<Path> = [["votes"], ["scores", "k"], ...each((i) => [["items", i, "tags", "t", "n"]])]
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
  { ops: [{ _tag: "Add", path: ["scores", "k"] }, { _tag: "Increment", path: ["scores", "k"], by: 1 }] },
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

const ItemKey = Schema.Literals(["i1", "i2"])

// An edit an app makes, whatever the replica shows.
const HonestEdit = Schema.Union([
  Schema.TaggedStruct("Status", { status: Project.fields.status }),
  Schema.TaggedStruct("Due", { due: Schema.optionalKey(Schema.Literals(["mon", "fri"])) }),
  Schema.TaggedStruct("Vote", { by: Schema.Literals([-1, 1, 2]) }),
  Schema.TaggedStruct("Score", { remove: Schema.Boolean, by: Schema.Literals([1, 2]) }),
  Schema.TaggedStruct("Note", { remove: Schema.Boolean, pinned: Schema.Boolean }),
  Schema.TaggedStruct("Meta", { remove: Schema.Boolean, y: Schema.Boolean }),
  // Adds or moves an item, or removes it.
  Schema.TaggedStruct("Item", { key: ItemKey, at: Fraction, remove: Schema.Boolean }),
  // The rest change an item, adding it if it doesn't show.
  Schema.TaggedStruct("Done", { key: ItemKey, done: Schema.Boolean }),
  Schema.TaggedStruct("Extra", { key: ItemKey, extra: Schema.optionalKey(Schema.Literals(["a", "b"])) }),
  Schema.TaggedStruct("Tag", { key: ItemKey, remove: Schema.Boolean, by: Schema.Literals([0, 1]) }),
  // Adds its sub, with or without its note, or removes it.
  Schema.TaggedStruct("Sub", { key: ItemKey, remove: Schema.Boolean, note: Schema.Boolean }),
  Schema.TaggedStruct("Value", { at: Fraction, value: Schema.Literals(["x", "y"]), remove: Schema.Boolean })
])

// Types `text` into one of `typed`, whether or not its part shows, or with "",
// removes up to two characters.
const Typing = Schema.Struct({ where: Fraction, at: Fraction, text: Schema.Literals(["x", "yz", ""]) })

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
  send: between(0, 3),
  // Made instead by the writer's honest replica, after it takes `others` and
  // what they were made on top of: typing if there's `typing`, or else the
  // edit. The rest is ignored.
  honest: Schema.optionalKey(Schema.Struct({ edit: HonestEdit, typing: Schema.optionalKey(Typing) }))
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

  const forgeDirectly = (command: typeof ChangeCommand.Type): InternalDocument.AnyChange => {
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

  // One replica per writer, made when it first edits.
  const honest = new Map<number, ReplicatedDocument.ReplicatedDocument<Project>>()
  const forgeHonestly = Effect.fnUntraced(
    function*(writer: number, others: ReadonlyArray<number>, edit: Edit) {
      const replica = `h${writer}`
      let doc = honest.get(writer)
      if (doc === undefined) {
        doc = yield* ReplicatedDocument.make(Project, {
          initial: initialProject,
          strategy,
          replica: Replica.ReplicaId.make(replica)
        })
        honest.set(writer, doc)
      }
      const taken = new Set<number>()
      for (const at of others) {
        const i = pick(made.flatMap((other, i) => other.dropped ? [] : [i]), at)
        if (i === undefined) continue
        taken.add(i)
        for (const before of made[i]!.past) taken.add(before)
      }
      // Bad ones among them are refused, and the rest taken.
      yield* Effect.exit(Replica.receive(doc, InternalDelta.fromChanges([...taken].map((i) => made[i]!.change))))
      const version = yield* Replica.version(doc)
      yield* editHonestly(doc, edit)
      const has = (yield* Replica.delta(doc, VersionVector.empty)) as InternalDelta.DeltaImpl
      const change = [...has.changes.values()].find((change) =>
        change.replica === replica && change.seq >= VersionVector.get(version, replica)
      )
      if (change === undefined) return undefined
      // Made on top of everything it had applied.
      const applied = new Set(appliedBy([...has.changes.values()], version))
      const past = new Set(made.flatMap((other, i) => applied.has(other.change) ? [i] : []))
      made.push({ change, past, ids: idsOf(change), dropped: false })
      return change
    }
  )

  const forge = (command: typeof ChangeCommand.Type): Effect.Effect<InternalDocument.AnyChange | undefined> =>
    command.honest === undefined
      ? Effect.succeed(forgeDirectly(command))
      : forgeHonestly(
        command.writer,
        command.others,
        command.honest.typing === undefined ? command.honest.edit : { _tag: "Type", ...command.honest.typing }
      )

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
      const change = yield* forger.forge(command)
      if (change === undefined) continue
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

// The texts honest edits type into, few so they often meet: the title, a
// note's body, an item's title, and the optional note of a sub in that item.
const typed: ReadonlyArray<Path> = [
  ["title"],
  ["notes", "a", "body"],
  ["items", "i1", "title"],
  ["items", "i1", "subs", "s", "note"]
]

// The characters a change inserted into each text, and the positions it placed
// in each list, by path.
const idsOf = (change: InternalDocument.AnyChange): Map<string, Array<Id>> => {
  const ids = new Map<string, Array<Id>>()
  const note = (path: Path, id: Id) => {
    const k = pathKey(path)
    ids.set(k, [...(ids.get(k) ?? []), id])
  }
  let seq = change.seq
  for (const op of change.ops) {
    if (op._tag === "Insert") {
      for (let i = 0; i < op.text.length; i++) {
        note(InternalDocument.pathOf(op), { replica: change.replica, seq: seq + i })
      }
    } else if (op._tag === "Place") {
      note(op.path.slice(0, -1), { replica: change.replica, seq })
    }
    seq += InternalDocument.opLength(op)
  }
  return ids
}

type ProjectItem = Project["items"][number]

// Forged increments can take a counter to the last safe integer.
const safely = (n: number) => Math.max(Number.MIN_SAFE_INTEGER, Math.min(Number.MAX_SAFE_INTEGER, n))

type Edit = typeof HonestEdit.Type | { readonly _tag: "Type" } & typeof Typing.Type

// Makes an honest edit to a project. Parts that come back keep the texts they
// had.
const editHonestly = Effect.fnUntraced(function*(
  doc: ReplicatedDocument.ReplicatedDocument<Project>,
  edit: Edit
) {
  const update = (f: (project: Project) => Project) => Effect.orDie(ReplicatedDocument.update(doc, f))
  const textAt = (path: Path) => ReplicatedDocument.text(doc, path as never)
  const kept = (path: Path) => ReplicatedText.get(textAt(path))
  // Changes an item, adding it at the end if it doesn't show.
  const inItem = (key: string, f: (item: ProjectItem) => ProjectItem) =>
    Effect.flatMap(kept(["items", key, "title"]), (title) =>
      update((project) => {
        const found = project.items.find(({ id }) => id === key)
        return {
          ...project,
          items: found === undefined
            ? [...project.items, f({ id: key, title, done: false, tags: {}, subs: [] })]
            : project.items.map((item) => item === found ? f(item) : item)
        }
      }))
  switch (edit._tag) {
    case "Status":
      return yield* update((project) => ({ ...project, status: edit.status }))
    case "Due":
      return yield* update(({ due: _, ...project }) => edit.due === undefined ? project : { ...project, due: edit.due })
    case "Vote":
      return yield* update((project) => ({ ...project, votes: safely(project.votes + edit.by) }))
    case "Type": {
      const text = textAt(pick(typed, edit.where)!)
      const before = yield* ReplicatedText.get(text)
      if (edit.text !== "") {
        return yield* Effect.orDie(ReplicatedText.insert(text, gapAt(edit.at, before.length), edit.text))
      }
      if (before.length === 0) return
      const index = itemAt(edit.at, before.length)
      return yield* Effect.orDie(ReplicatedText.remove(text, index, Math.min(2, before.length - index)))
    }
    case "Score":
      return yield* update(({ scores: { k, ...scores }, ...project }) => ({
        ...project,
        scores: edit.remove ? scores : { ...scores, k: safely((k ?? 0) + edit.by) }
      }))
    case "Note": {
      const body = yield* kept(["notes", "a", "body"])
      return yield* update(({ notes: { a: _, ...notes }, ...project }) => ({
        ...project,
        notes: edit.remove ? notes : { ...notes, a: { body, pinned: edit.pinned } }
      }))
    }
    case "Meta": {
      const x = yield* kept(["meta", "x"])
      return yield* update(({ meta: _, ...project }) => edit.remove ? project : { ...project, meta: { x, y: edit.y } })
    }
    case "Item": {
      const title = yield* kept(["items", edit.key, "title"])
      return yield* update((project) => {
        const found = project.items.find(({ id }) => id === edit.key)
        const items = project.items.filter(({ id }) => id !== edit.key)
        if (edit.remove) return { ...project, items }
        const item = found ?? { id: edit.key, title, done: false, tags: {}, subs: [] }
        const index = gapAt(edit.at, items.length)
        return { ...project, items: [...items.slice(0, index), item, ...items.slice(index)] }
      })
    }
    case "Done":
      return yield* inItem(edit.key, (item) => ({ ...item, done: edit.done }))
    case "Extra":
      return yield* inItem(
        edit.key,
        ({ extra: _, ...item }) => edit.extra === undefined ? item : { ...item, extra: edit.extra }
      )
    case "Tag": {
      const label = yield* kept(["items", edit.key, "tags", "t", "label"])
      return yield* inItem(edit.key, ({ tags: { t, ...tags }, ...item }) => ({
        ...item,
        tags: edit.remove ? tags : { ...tags, t: { label: t?.label ?? label, n: (t?.n ?? 0) + edit.by } }
      }))
    }
    case "Sub": {
      const note = yield* kept(["items", edit.key, "subs", "s", "note"])
      return yield* inItem(edit.key, (item) => ({
        ...item,
        subs: edit.remove ? [] : [edit.note ? { id: "s", note } : { id: "s" }]
      }))
    }
    case "Value":
      return yield* update((project) => {
        const { vals } = project
        if (edit.remove) return { ...project, vals: vals.filter((_, i) => i !== itemAt(edit.at, vals.length)) }
        const index = gapAt(edit.at, vals.length)
        return { ...project, vals: [...vals.slice(0, index), edit.value, ...vals.slice(index)] }
      })
  }
})

// Sending --------------------------------------------------------------------

// A value as it arrives after being sent as JSON.
const sent = <A, I>(schema: Schema.Codec<A, I>, value: A): A =>
  Schema.decodeUnknownSync(schema)(JSON.parse(JSON.stringify(Schema.encodeSync(schema)(value))))

/** A delta as it arrives after being sent as JSON, as relays over RPC and stores on a `KeyValueStore` send it. */
export const throughJson = (delta: Delta.Delta): Delta.Delta => sent(Delta.Delta, delta)

/** A relay or an authority reached as over RPC: every request, update, ack and error is sent as JSON. */
export const overJson = (server: Sync.Server): Sync.Server => ({
  subscribe: (request) =>
    server.subscribe(sent(Sync.Subscribe.payloadSchema, request)).pipe(
      Stream.map((update) => sent(Sync.Update, update)),
      Stream.mapError((error) => sent(Sync.SyncError, error))
    ),
  push: (request) =>
    server.push(sent(Sync.Push.payloadSchema, request)).pipe(
      Effect.map((ack) => sent(Sync.Ack, ack)),
      Effect.mapError((error) => sent(Sync.SyncError, error))
    )
})

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
  Schema.TaggedStruct("Wait", {}),
  // Closes the replica and opens it again from what it saved, whether or not
  // it has heard back.
  Schema.TaggedStruct("Reopen", {})
])

export const EditSession = Schema.Array(EditCommand).check(Schema.isMaxLength(20))

/** Makes an edit, or for `Wait` and `Reopen`, runs `wait` or `reopen`. */
export const editProject = Effect.fnUntraced(function*(
  doc: ReplicatedDocument.ReplicatedDocument<OldProject>,
  command: typeof EditCommand.Type,
  options: { readonly wait: Effect.Effect<void>; readonly reopen: Effect.Effect<void> }
) {
  const update = (f: (project: OldProject) => OldProject) => Effect.orDie(ReplicatedDocument.update(doc, f))
  switch (command._tag) {
    case "Status":
      return yield* update((project) => ({ ...project, status: command.status }))
    case "Vote":
      return yield* update((project) => ({ ...project, votes: safely(project.votes + command.by) }))
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
      return yield* options.wait
    case "Reopen":
      return yield* options.reopen
  }
})

// Overlaps -------------------------------------------------------------------

type Overlap = Exclude<InternalDocument.Overlap, { readonly _tag: "Text" }>

interface Written {
  readonly replica: string
  readonly seq: number
  readonly time: number
  /** What its change's author had seen. */
  readonly past: VersionVector.VersionVector
  readonly path: Path
  readonly key: string
  /** A value, whether the part exists, a list item's position, or an edit inside the part. */
  readonly kind: "value" | "presence" | "place" | "inside"
  readonly value?: Schema.Json
  readonly exists?: boolean
}

const saw = (by: Written, write: Written): boolean =>
  by.replica === write.replica ? write.seq < by.seq : (by.past[write.replica] ?? 0) > write.seq

const isLater = (a: Written, b: Written): boolean =>
  a.time !== b.time ? a.time > b.time : a.replica !== b.replica ? a.replica > b.replica : a.seq > b.seq

const latest = (writes: ReadonlyArray<Written>): Written | undefined =>
  writes.reduce<Written | undefined>(
    (found, write) => found === undefined || isLater(write, found) ? write : found,
    undefined
  )

// A change's writes, worked out once: its parents are among `changes`.
const written = new WeakMap<InternalDocument.AnyChange, ReadonlyArray<Written>>()
const pasts = new WeakMap<InternalDocument.AnyChange, VersionVector.VersionVector>()

const pastOf = (
  change: InternalDocument.AnyChange,
  changes: ReadonlyArray<InternalDocument.AnyChange>
): VersionVector.VersionVector => {
  let past = pasts.get(change)
  if (past === undefined) {
    const seen: Record<string, number> = {}
    for (const parent of change.parents) {
      const owner = changes.find((other) =>
        other.replica === parent.replica && other.seq <= parent.seq && parent.seq < other.seq + other.length
      )!
      for (const [replica, next] of Object.entries(pastOf(owner, changes))) {
        seen[replica] = Math.max(seen[replica] ?? 0, next)
      }
      seen[parent.replica] = Math.max(seen[parent.replica] ?? 0, parent.seq + 1)
    }
    past = seen
    pasts.set(change, past)
  }
  return past
}

// An op edits inside every part above it, and a text or counter op inside its
// own part too.
const writesOf = (
  change: InternalDocument.AnyChange,
  changes: ReadonlyArray<InternalDocument.AnyChange>
): ReadonlyArray<Written> => {
  let writes = written.get(change)
  if (writes === undefined) {
    const found: Array<Written> = []
    const past = pastOf(change, changes)
    let seq = change.seq
    for (const op of change.ops) {
      const path = InternalDocument.pathOf(op)
      const write = (kind: Written["kind"], at: Path, rest?: Pick<Written, "value" | "exists">) =>
        found.push({ replica: change.replica, seq, time: change.time, past, path: at, key: pathKey(at), kind, ...rest })
      for (let i = 0; i < path.length; i++) write("inside", path.slice(0, i))
      switch (op._tag) {
        case "Set":
          write("value", path, { value: op.value })
          write("presence", path, { exists: true })
          break
        case "Add":
          write("presence", path, { exists: true })
          break
        case "Unset":
          write("presence", path, { exists: false })
          break
        case "Place":
          write("place", path)
          break
        default:
          write("inside", path)
      }
      seq += InternalDocument.opLength(op)
    }
    writes = found
    written.set(change, writes)
  }
  return writes
}

/**
 * The changes a replica with `version` has applied, of `changes`: each
 * replica's from its first sequence number on, one after another. Changes that
 * wait can be anywhere, even among those.
 */
export const appliedBy = (
  changes: ReadonlyArray<InternalDocument.AnyChange>,
  version: VersionVector.VersionVector
): Array<InternalDocument.AnyChange> => {
  const byKey = new Map(changes.map((change) => [`${change.seq}@${change.replica}`, change]))
  return Object.entries(version).flatMap(([replica, end]) => {
    const chain: Array<InternalDocument.AnyChange> = []
    for (let seq = 0; seq < end;) {
      const change = byKey.get(`${seq}@${replica}`)!
      chain.push(change)
      seq += change.length
    }
    return chain
  })
}

const byPath = (writes: ReadonlyArray<Written>): Map<string, Array<Written>> => {
  const paths = new Map<string, Array<Written>>()
  for (const write of writes) {
    const at = paths.get(write.key)
    if (at === undefined) paths.set(write.key, [write])
    else at.push(write)
  }
  return paths
}

/**
 * The value and removal overlaps a merge reports, by their rule: a write the
 * replica had meets one it received that hadn't seen it. `Value` where they
 * wrote different values, said differently whether the part exists, or both
 * moved its item; `Removed` where one removed the part and the other edited
 * inside it or moved it, if it ends removed. `changes` are every change the
 * replica has, and `before` and `after` its versions around the merge.
 */
export const expectedOverlaps = (
  changes: ReadonlyArray<InternalDocument.AnyChange>,
  before: VersionVector.VersionVector,
  after: VersionVector.VersionVector
): Array<Overlap> => {
  const applied = appliedBy(changes, after)
  const earlier = new Set(appliedBy(changes, before))
  const writes = (changes: ReadonlyArray<InternalDocument.AnyChange>) =>
    changes.flatMap((change) => writesOf(change, applied))
  const had = byPath(writes([...earlier]))
  const received = writes(applied.filter((change) => !earlier.has(change)))
  const all = byPath([...[...had.values()].flat(), ...received])

  const found = new Map<
    string,
    { tag: Overlap["_tag"]; path: Path; replicas: Set<string>; over: Set<Written["kind"]> }
  >()
  const meet = (tag: Overlap["_tag"], write: Written, other: Written, over: Written["kind"]) => {
    const k = `${tag}${write.key}`
    const entry = found.get(k) ?? { tag, path: write.path, replicas: new Set(), over: new Set() }
    entry.replicas.add(write.replica).add(other.replica)
    entry.over.add(over)
    found.set(k, entry)
  }
  const removes = (write: Written) => write.kind === "presence" && write.exists === false
  const editsInside = (write: Written) => write.kind === "inside" || write.kind === "place"
  for (const write of received) {
    for (const other of had.get(write.key) ?? []) {
      if (saw(write, other)) continue
      if (write.kind === other.kind) {
        const differ = write.kind === "value"
          ? !Equal.equals(write.value, other.value)
          : write.kind === "presence"
          ? write.exists !== other.exists
          : write.kind === "place"
        if (differ) meet("Value", write, other, write.kind)
      }
      if ((removes(write) && editsInside(other)) || (editsInside(write) && removes(other))) {
        meet("Removed", write, other, "presence")
      }
    }
  }

  const at = (path: Path, kind: Written["kind"]) =>
    latest((all.get(pathKey(path)) ?? []).filter((write) => write.kind === kind))
  return [...found.values()].flatMap(({ over, path, replicas, tag }): Array<Overlap> => {
    const presence = at(path, "presence")
    const sorted = [...replicas].sort()
    if (tag === "Removed") return presence?.exists === false ? [{ _tag: "Removed", path, replicas: sorted }] : []
    // Whose write shows: a removal that won, or else the latest of what they
    // wrote.
    const kept = presence !== undefined && (presence.exists === false || (over.size === 1 && over.has("presence")))
      ? presence
      : at(path, over.has("value") ? "value" : "place")!
    return [{ _tag: "Value", path, replicas: sorted, kept: kept.replica }]
  })
}

// Text overlaps --------------------------------------------------------------

/** A text overlap a merge reports, with the characters its anchors name. */
interface TextOverlap {
  readonly path: Path
  readonly start: number
  readonly end: number
  readonly replicas: ReadonlyArray<string>
  /**
   * The first and last characters of the stretch that show, or for a stretch
   * that's all removed, its characters, any of which can be its anchors.
   */
  readonly anchors: { readonly start: Id; readonly end: Id } | { readonly among: ReadonlyArray<Id> }
}

// One change's ops on one text, which come together, as a merge sees them.
interface TextPart {
  readonly replica: string
  readonly seq: number
  length: number
  readonly past: VersionVector.VersionVector
  /** Whether it arrived in the merge. */
  readonly fresh: boolean
}

interface Character {
  readonly id: Id
  readonly insertedBy: TextPart
  readonly removedBy: Set<TextPart>
}

/**
 * The text overlaps a merge of tree texts reports, by their rule: a text is
 * split at characters that show and that no change touched (inserted or
 * removed) among those the merge received and those the replica had that a
 * received one hadn't seen, and a stretch between is an overlap where a
 * received change and one it hadn't seen both touched characters. The
 * characters' order comes from where `textAt(path)` locates them, for each
 * path the schema has a text at; their ids and who touched them come from
 * the changes' ops. `changes` are every change the replica has, and `before`
 * and `after` its versions around the merge.
 */
const expectedTextOverlaps = Effect.fnUntraced(function*(
  changes: ReadonlyArray<InternalDocument.AnyChange>,
  before: VersionVector.VersionVector,
  after: VersionVector.VersionVector,
  textAt: (path: Path) => ReplicatedText.Editable | undefined
) {
  const applied = appliedBy(changes, after)
  const earlier = new Set(appliedBy(changes, before))
  const texts = new Map<string, { readonly path: Path; readonly characters: Map<string, Character> }>()
  const textOf = (path: Path) => {
    const k = pathKey(path)
    let found = texts.get(k)
    if (found === undefined) {
      found = { path, characters: new Map() }
      texts.set(k, found)
    }
    return found
  }
  const idKey = (id: Id) => `${id.seq}@${id.replica}`
  const removals: Array<{ readonly path: Path; readonly id: Id; readonly by: TextPart }> = []
  for (const change of applied) {
    const past = pastOf(change, applied)
    let part: { readonly k: string; readonly part: TextPart } | undefined
    let seq = change.seq
    for (const op of change.ops) {
      if (op._tag === "Insert" || op._tag === "Remove") {
        const path = InternalDocument.pathOf(op)
        const k = pathKey(path)
        if (part?.k !== k) {
          part = { k, part: { replica: change.replica, seq, length: 0, past, fresh: !earlier.has(change) } }
        }
        const by = part.part
        by.length += InternalDocument.opLength(op)
        if (op._tag === "Insert") {
          for (let i = 0; i < op.text.length; i++) {
            const id = { replica: change.replica, seq: seq + i }
            textOf(path).characters.set(idKey(id), { id, insertedBy: by, removedBy: new Set() })
          }
        } else {
          for (const span of op.targets) {
            for (let i = 0; i < span.length; i++) removals.push({ path, id: { ...span, seq: span.seq + i }, by })
          }
        }
      } else {
        part = undefined
      }
      seq += InternalDocument.opLength(op)
    }
  }
  // Removals name characters of changes that may come later in `applied`.
  for (const { by, id, path } of removals) textOf(path).characters.get(idKey(id))!.removedBy.add(by)

  const unseenBy = (earlier: TextPart, later: TextPart) =>
    (later.past[earlier.replica] ?? 0) < earlier.seq + earlier.length
  const found: Array<TextOverlap> = []
  for (const { characters, path } of texts.values()) {
    const fresh = new Set([...characters.values()].flatMap(({ insertedBy, removedBy }) => [insertedBy, ...removedBy]))
    for (const part of fresh) if (!part.fresh) fresh.delete(part)
    const text = textAt(path)
    if (fresh.size === 0 || text === undefined) continue
    const concurrent = (part: TextPart) => !part.fresh && [...fresh].some((later) => unseenBy(part, later))
    const touchesOf = ({ insertedBy, removedBy }: Character) =>
      [insertedBy, ...removedBy].filter((part) => part.fresh || concurrent(part))
    // Where each touched character is: one that shows at its index, and a
    // removed one in the gap where it was. Removed characters in the same gap
    // can be in any order, which changes nothing but which of them anchor.
    const touched: Array<{ readonly character: Character; readonly at: number; readonly shows: boolean }> = []
    for (const character of characters.values()) {
      if (touchesOf(character).length === 0) continue
      const shows = character.removedBy.size === 0
      const locate = (side: "before" | "after") =>
        Effect.map(ReplicatedText.locate(text, { _tag: "Character", ...character.id, side }), Option.getOrThrow)
      const at = yield* locate("before")
      assert.strictEqual(yield* locate("after"), shows ? at + 1 : at, `${idKey(character.id)} at ${pathKey(path)}`)
      touched.push({ character, at, shows })
    }
    // In text order: a removed character comes before the one that shows at
    // its gap's index.
    const order = ({ at, shows }: (typeof touched)[number]) => 2 * at + (shows ? 1 : 0)
    touched.sort((a, b) => order(a) - order(b))
    let stretch: Array<(typeof touched)[number]> = []
    const close = () => {
      const parts = new Set(stretch.flatMap(({ character }) => touchesOf(character)))
      const received = [...parts].filter((part) => part.fresh)
      if ([...parts].some((part) => !part.fresh && received.some((later) => unseenBy(part, later)))) {
        const showing = stretch.filter(({ shows }) => shows)
        found.push({
          path,
          start: stretch[0]!.at,
          end: stretch[0]!.at + showing.length,
          replicas: [...new Set([...parts].map(({ replica }) => replica))].sort(),
          anchors: showing.length > 0
            ? { start: showing[0]!.character.id, end: showing.at(-1)!.character.id }
            : { among: stretch.map(({ character }) => character.id) }
        })
      }
      stretch = []
    }
    for (const next of touched) {
      const last = stretch.at(-1)
      // A character that shows between them, which nothing touched.
      if (last !== undefined && Math.ceil(order(last) / 2) < Math.floor(order(next) / 2)) close()
      stretch.push(next)
    }
    if (stretch.length > 0) close()
  }
  return found
})

/**
 * Checks the text overlaps a merge of tree texts reported against the ones
 * their rule gives (see `expectedTextOverlaps`), anchors included.
 */
export const checkTextOverlaps = Effect.fnUntraced(function*(
  overlaps: ReadonlyArray<Replica.Overlap>,
  merge: {
    readonly changes: ReadonlyArray<InternalDocument.AnyChange>
    readonly before: VersionVector.VersionVector
    readonly after: VersionVector.VersionVector
    readonly textAt: (path: Path) => ReplicatedText.Editable | undefined
  }
) {
  const { after, before, changes, textAt } = merge
  const where = ({ end, path, replicas, start }: ReplicatedText.Range & {
    readonly path: Path
    readonly replicas: ReadonlyArray<string>
  }) => JSON.stringify({ path, start, end, replicas })
  const expected = new Map(
    (yield* expectedTextOverlaps(changes, before, after, textAt)).map((overlap) => [where(overlap), overlap])
  )
  const texts = overlaps.flatMap((overlap) => overlap._tag === "Text" ? [overlap] : [])
  const earlier = new Set(appliedBy(changes, before))
  const received = appliedBy(changes, after).filter((change) => !earlier.has(change))
  assert.deepStrictEqual(
    texts.map((overlap) => where({ ...overlap, ...overlap.range })).sort(),
    [...expected.keys()].sort(),
    `receiving ${JSON.stringify(received)}`
  )
  const character = (anchor: ReplicatedText.Anchor, side: ReplicatedText.Side): Id | undefined =>
    anchor._tag === "Character" && anchor.side === side ? { replica: anchor.replica, seq: anchor.seq } : undefined
  for (const overlap of texts) {
    const { anchors } = expected.get(where({ ...overlap, ...overlap.range }))!
    const named = [character(overlap.anchors.start, "before"), character(overlap.anchors.end, "after")]
    if ("among" in anchors) {
      for (const id of named) assert.deepInclude(anchors.among, id, JSON.stringify(overlap))
    } else {
      assert.deepStrictEqual(named, [anchors.start, anchors.end], JSON.stringify(overlap))
    }
  }
})
