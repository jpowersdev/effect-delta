import { assert } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"
import type * as InternalDocument from "effect-delta/internal/document"
import { TestClock } from "effect/testing"

// Random sessions across three replicas, with partial syncs along the way.
// Positions are fractions of what's there by then, so every generated command
// is valid whatever happened before it. Properties generate them at size 100:
// the default size makes sessions of about five commands, at most ten, which
// rarely merge much.

export const Which = Schema.Literals([0, 1, 2])
export const Fraction = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))

export const letters = (s: string) => [...s].map((c) => "abc"[c.charCodeAt(0) % 3]!).join("")

export const syncAll = Effect.fnUntraced(function*(replicas: ReadonlyArray<Replica.Replica<unknown>>) {
  for (let round = 0; round < 2; round++) {
    for (let i = 0; i < replicas.length; i++) {
      for (let j = i + 1; j < replicas.length; j++) yield* Replica.exchange(replicas[i]!, replicas[j]!)
    }
  }
})

// Texts -------------------------------------------------------------------

// Edits, syncs and anchors, for running two kinds of text side by side.

const TextCommand = Schema.Union([
  Schema.TaggedStruct("Insert", {
    replica: Which,
    at: Fraction,
    text: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4))
  }),
  Schema.TaggedStruct("Remove", {
    replica: Which,
    at: Fraction,
    length: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3 }))
  }),
  Schema.TaggedStruct("Sync", { from: Which, to: Which }),
  Schema.TaggedStruct("Anchor", { replica: Which, at: Fraction, after: Schema.Boolean })
])

export const TextSession = Schema.Array(TextCommand).check(Schema.isMaxLength(50))

export const makeTexts = (strategy: Replica.Strategy) =>
  Effect.forEach(["r0", "r1", "r2"], (id) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(id) }))

// Documents ---------------------------------------------------------------

// A schema that uses every merge rule. Record keys and list item keys come
// from a small set, so replicas often add, remove, move and edit the same ones
// at once.

const Note = Schema.Struct({ body: ReplicatedDocument.Text, pinned: Schema.Boolean })
const Task = Schema.Struct({ id: Schema.String, title: ReplicatedDocument.Text, done: Schema.Boolean })
const Due = Schema.Literals(["mon", "fri"])

export const Board = Schema.Struct({
  status: Schema.Literals(["draft", "review", "done"]),
  size: Schema.Number,
  title: ReplicatedDocument.Text,
  votes: ReplicatedDocument.Counter,
  notes: Schema.Record(Schema.String, Note),
  due: Schema.optionalKey(Due),
  tasks: ReplicatedDocument.List(Task, { key: "id" }),
  tags: ReplicatedDocument.List(Schema.String)
})
type Board = typeof Board.Type

export const initialBoard: Board = { status: "draft", size: 0, title: "", votes: 0, notes: {}, tasks: [], tags: [] }

const Key = Schema.Literals(["a", "b", "c"])
const Short = Schema.String.check(Schema.isMaxLength(4))
const Small = Schema.Int.check(Schema.isBetween({ minimum: -2, maximum: 2 }))

const DocumentCommand = Schema.Union([
  // Changes some of the values, the title and the counter, as one update.
  Schema.TaggedStruct("Update", {
    replica: Which,
    status: Schema.optionalKey(Board.fields.status),
    size: Schema.optionalKey(Small),
    title: Schema.optionalKey(Short),
    votes: Schema.optionalKey(Small),
    due: Schema.optionalKey(Schema.NullOr(Due))
  }),
  // Edits the title, a note's body or a task's title through its handle,
  // whether or not the note or task exists.
  Schema.TaggedStruct("Insert", {
    replica: Which,
    field: Schema.Literals(["title", "note", "task"]),
    key: Key,
    at: Fraction,
    text: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4))
  }),
  Schema.TaggedStruct("Remove", {
    replica: Which,
    field: Schema.Literals(["title", "note", "task"]),
    key: Key,
    at: Fraction,
    length: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3 }))
  }),
  Schema.TaggedStruct("Note", { replica: Which, key: Key, body: Schema.optionalKey(Short), pinned: Schema.Boolean }),
  Schema.TaggedStruct("Unnote", { replica: Which, key: Key }),
  // Adds a task, or moves and edits it if it's there.
  Schema.TaggedStruct("Task", {
    replica: Which,
    key: Key,
    at: Fraction,
    title: Schema.optionalKey(Short),
    done: Schema.Boolean
  }),
  Schema.TaggedStruct("Untask", { replica: Which, at: Fraction }),
  Schema.TaggedStruct("Tag", { replica: Which, at: Fraction, tag: Schema.Literals(["x", "y", "z"]) }),
  Schema.TaggedStruct("Untag", { replica: Which, at: Fraction }),
  // Clocks can go back: write times must still move forward.
  Schema.TaggedStruct("Clock", { time: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 20 })) }),
  Schema.TaggedStruct("Sync", { from: Which, to: Which })
])

export const DocumentSession = Schema.Array(DocumentCommand).check(Schema.isMaxLength(60))
export const documentRuns = { runs: 100, size: 100 }

type BoardDocument = ReplicatedDocument.ReplicatedDocument<Board>

export const makeBoards = (strategy: Replica.Strategy, ids: ReadonlyArray<string> = ["r0", "r1", "r2"]) =>
  Effect.forEach(
    ids,
    (id) => ReplicatedDocument.make(Board, { initial: initialBoard, strategy, replica: Replica.ReplicaId.make(id) })
  )

// A gap in, or an item of, something of `length`.
export const gapAt = (at: number, length: number) => Math.floor(at * (length + 1) / 1001)
export const itemAt = (at: number, length: number) => Math.floor(at * length / 1001)

const textOf = (doc: BoardDocument, field: "title" | "note" | "task", key: string) =>
  ReplicatedDocument.text(
    doc,
    field === "title" ? ["title"] : field === "note" ? ["notes", key, "body"] : ["tasks", key, "title"]
  )

// The value, and the texts of every note and task, including ones that don't
// show: those are kept, and show again when the note or task is added back.
export const viewOf = Effect.fnUntraced(function*(doc: BoardDocument) {
  const texts: Array<string> = []
  for (const field of ["note", "task"] as const) {
    for (const key of Key.literals) texts.push(yield* ReplicatedText.get(textOf(doc, field, key)))
  }
  return { value: yield* ReplicatedDocument.get(doc), texts }
})

const updateBoard = Effect.fnUntraced(function*(doc: BoardDocument, f: (board: Board) => Board) {
  let next: Board | undefined
  yield* ReplicatedDocument.update(doc, (board) => (next = f(board)))
  // A replica shows its own edits immediately and exactly.
  assert.deepStrictEqual(yield* ReplicatedDocument.get(doc), next)
})

// Runs one command, returning the overlaps a sync found.
export const stepBoards = Effect.fnUntraced(function*(
  docs: ReadonlyArray<BoardDocument>,
  command: typeof DocumentCommand.Type
) {
  if (command._tag === "Sync") {
    return command.from === command.to ? [] : yield* Replica.exchange(docs[command.from]!, docs[command.to]!)
  }
  if (command._tag === "Clock") {
    yield* TestClock.setTime(command.time)
    return []
  }
  const doc = docs[command.replica]!
  switch (command._tag) {
    case "Update":
      yield* updateBoard(doc, ({ due, ...board }) => {
        const nextDue = command.due === undefined ? due : command.due ?? undefined
        return {
          ...board,
          status: command.status ?? board.status,
          size: command.size ?? board.size,
          title: command.title === undefined ? board.title : letters(command.title),
          votes: board.votes + (command.votes ?? 0),
          ...(nextDue === undefined ? {} : { due: nextDue })
        }
      })
      break
    case "Insert": {
      const text = textOf(doc, command.field, command.key)
      const before = yield* ReplicatedText.get(text)
      const index = gapAt(command.at, before.length)
      const inserted = letters(command.text)
      yield* ReplicatedText.insert(text, index, inserted)
      assert.strictEqual(yield* ReplicatedText.get(text), before.slice(0, index) + inserted + before.slice(index))
      break
    }
    case "Remove": {
      const text = textOf(doc, command.field, command.key)
      const before = yield* ReplicatedText.get(text)
      if (before.length === 0) break
      const index = itemAt(command.at, before.length)
      const length = Math.min(command.length, before.length - index)
      yield* ReplicatedText.remove(text, index, length)
      assert.strictEqual(yield* ReplicatedText.get(text), before.slice(0, index) + before.slice(index + length))
      break
    }
    case "Note": {
      // A removed note's body is kept, so adding it back shows it again.
      const kept = yield* ReplicatedText.get(textOf(doc, "note", command.key))
      const body = command.body === undefined ? kept : letters(command.body)
      yield* updateBoard(doc, (board) => ({
        ...board,
        notes: { ...board.notes, [command.key]: { body, pinned: command.pinned } }
      }))
      break
    }
    case "Unnote":
      yield* updateBoard(doc, (board) => {
        const { [command.key]: _, ...notes } = board.notes
        return { ...board, notes }
      })
      break
    case "Task": {
      const kept = yield* ReplicatedText.get(textOf(doc, "task", command.key))
      const task = {
        id: command.key,
        title: command.title === undefined ? kept : letters(command.title),
        done: command.done
      }
      yield* updateBoard(doc, (board) => {
        const tasks = board.tasks.filter(({ id }) => id !== command.key)
        const index = gapAt(command.at, tasks.length)
        return { ...board, tasks: [...tasks.slice(0, index), task, ...tasks.slice(index)] }
      })
      break
    }
    case "Untask":
      yield* updateBoard(doc, (board) => {
        const index = itemAt(command.at, board.tasks.length)
        return { ...board, tasks: board.tasks.filter((_, i) => i !== index) }
      })
      break
    case "Tag":
      yield* updateBoard(doc, (board) => {
        const index = gapAt(command.at, board.tags.length)
        return { ...board, tags: [...board.tags.slice(0, index), command.tag, ...board.tags.slice(index)] }
      })
      break
    case "Untag":
      yield* updateBoard(doc, (board) => {
        const index = itemAt(command.at, board.tags.length)
        return { ...board, tags: board.tags.filter((_, i) => i !== index) }
      })
      break
  }
  return []
})

// Two versions of a schema ---------------------------------------------------

// An older and a newer release of one app's schema, whose replicas sync with
// each other. The newer one takes more literals in every value, and has
// fields the older one lacks: optional, or filled in by a constructor
// default. List items are classes, and record entries tagged structs, whose
// `_tag` has a default; one record's keys follow a template; and records sit
// inside records and inside list items.

const withDefault = <const L extends ReadonlyArray<string>>(literals: L) =>
  Schema.Literals(literals).pipe(Schema.withConstructorDefault(Effect.succeed(literals[0]!)))

class OldCard extends Schema.Class<OldCard>("OldCard")({
  id: Schema.String,
  title: ReplicatedDocument.Text,
  level: withDefault(["low", "high"]),
  tags: Schema.Record(Schema.String, Schema.Literals(["a", "b"]))
}) {}

class Card extends Schema.Class<Card>("Card")({
  id: Schema.String,
  title: ReplicatedDocument.Text,
  level: withDefault(["low", "high", "top"]),
  color: withDefault(["gray", "red"]),
  tags: Schema.Record(Schema.String, Schema.Literals(["a", "b", "c"]))
}) {}

const NoteKey = Schema.TemplateLiteral(["n", Schema.String])

export const OldPlanner = Schema.Struct({
  status: Schema.Literals(["open", "shut"]),
  cards: ReplicatedDocument.List(OldCard, { key: "id" }),
  notes: Schema.Record(NoteKey, Schema.TaggedStruct("Note", { pinned: Schema.Boolean })),
  groups: Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Literals(["low", "high"])))
})

export const Planner = Schema.Struct({
  status: Schema.Literals(["open", "shut", "gone"]),
  cards: ReplicatedDocument.List(Card, { key: "id" }),
  notes: Schema.Record(
    NoteKey,
    Schema.TaggedStruct("Note", { pinned: Schema.Boolean, mood: Schema.optionalKey(Schema.Literals(["ok", "sad"])) })
  ),
  groups: Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Literals(["low", "high", "top"])))
})
export type Planner = typeof Planner.Type

// The same on both versions.
export const initialPlanner = { status: "open", cards: [], notes: {}, groups: {} } as const

// What each version can write.
const choices = {
  old: {
    status: ["open", "shut"],
    level: ["low", "high"],
    color: [],
    tag: ["a", "b"],
    mood: [],
    group: ["low", "high"]
  },
  new: {
    status: ["open", "shut", "gone"],
    level: ["low", "high", "top"],
    color: ["gray", "red"],
    tag: ["a", "b", "c"],
    mood: ["ok", "sad"],
    group: ["low", "high", "top"]
  }
} as const

const PlannerCommand = Schema.Union([
  Schema.TaggedStruct("Status", { replica: Which, pick: Fraction }),
  // Adds a card, or moves it, and changes what's given.
  Schema.TaggedStruct("Card", {
    replica: Which,
    key: Schema.Literals(["c1", "c2"]),
    at: Fraction,
    title: Schema.optionalKey(Short),
    level: Schema.optionalKey(Fraction),
    color: Schema.optionalKey(Fraction),
    // Sets one of its tags, or with null, removes it.
    tag: Schema.optionalKey(Schema.Struct({ key: Schema.Literals(["t1", "t2"]), pick: Schema.NullOr(Fraction) }))
  }),
  Schema.TaggedStruct("Uncard", { replica: Which, at: Fraction }),
  // Adds, changes or removes a note. "x1" is a key the record doesn't take.
  Schema.TaggedStruct("Note", {
    replica: Which,
    key: Schema.Literals(["n1", "n2", "x1"]),
    pinned: Schema.optionalKey(Schema.Boolean),
    mood: Schema.optionalKey(Schema.NullOr(Fraction)),
    remove: Schema.Boolean
  }),
  // Sets an entry of a group, or with null, removes it, or with `whole`, the
  // group.
  Schema.TaggedStruct("Group", {
    replica: Which,
    group: Schema.Literals(["g1", "g2"]),
    key: Schema.Literals(["k1", "k2"]),
    pick: Schema.NullOr(Fraction),
    whole: Schema.Boolean
  }),
  // Sets the value the replica shows.
  Schema.TaggedStruct("Same", { replica: Which }),
  // A change made directly, as a client of the newer version could send it,
  // which only an authority on that version lets through.
  Schema.TaggedStruct("Forge", {
    replica: Which,
    ops: Schema.Array(
      Schema.Struct({ op: Schema.Literals(["Set", "Add", "Unset"]), path: Fraction, value: Fraction })
    ).check(Schema.isMinLength(1), Schema.isMaxLength(3))
  }),
  Schema.TaggedStruct("Sync", { from: Which, to: Which })
])

export const PlannerSession = Schema.Array(PlannerCommand).check(Schema.isMaxLength(40))

const choose = <A>(among: ReadonlyArray<A>, at: number): A => among[itemAt(at, among.length)]!

/**
 * The value an edit sets, worked out from the value a replica on `version`
 * shows, with what that version can write.
 */
export const editPlanner = (
  version: keyof typeof choices,
  planner: Planner,
  command: Extract<typeof PlannerCommand.Type, { readonly _tag: "Status" | "Card" | "Uncard" | "Note" | "Group" }>
): Planner => {
  const can = choices[version]
  switch (command._tag) {
    case "Status":
      return { ...planner, status: choose(can.status, command.pick) }
    case "Card": {
      const existing = planner.cards.find(({ id }) => id === command.key)
      const others = planner.cards.filter(({ id }) => id !== command.key)
      const tags: Record<string, string> = { ...existing?.tags }
      if (command.tag?.pick === null) delete tags[command.tag.key]
      else if (command.tag !== undefined) tags[command.tag.key] = choose(can.tag, command.tag.pick)
      const fields = {
        ...existing,
        id: command.key,
        title: command.title === undefined ? existing?.title ?? "" : letters(command.title),
        tags,
        ...(command.level === undefined ? {} : { level: choose(can.level, command.level) }),
        ...(command.color === undefined || version === "old" ? {} : { color: choose(can.color, command.color) })
      }
      // Made with the version's class, which fills in its defaults.
      const card = (version === "old" ? new OldCard(fields as never) : new Card(fields as never)) as Card
      const index = gapAt(command.at, others.length)
      return { ...planner, cards: [...others.slice(0, index), card, ...others.slice(index)] }
    }
    case "Uncard": {
      const index = itemAt(command.at, planner.cards.length)
      return { ...planner, cards: planner.cards.filter((_, i) => i !== index) }
    }
    case "Note": {
      const { [command.key as `n${string}`]: existing, ...notes } = planner.notes
      if (command.remove) return { ...planner, notes }
      const mood = version === "old" || command.mood === null
        ? undefined
        : command.mood === undefined
        ? existing?.mood
        : choose(can.mood, command.mood)
      const note = { _tag: "Note", pinned: command.pinned ?? existing?.pinned ?? false, ...(mood && { mood }) } as const
      return { ...planner, notes: { ...notes, [command.key]: note } }
    }
    case "Group": {
      const { [command.group]: group, ...groups } = planner.groups
      if (command.whole) return { ...planner, groups }
      const { [command.key]: _, ...entries } = group ?? {}
      return {
        ...planner,
        groups: {
          ...groups,
          [command.group]: command.pick === null
            ? entries
            : { ...entries, [command.key]: choose(can.group, command.pick) }
        }
      }
    }
  }
}

// Where a forged op writes, and what it writes.
const plannerValues: ReadonlyArray<ReadonlyArray<string>> = [
  ["status"],
  ...["c1", "c2"].flatMap((c) => [["cards", c, "id"], ["cards", c, "level"], ["cards", c, "color"], [
    "cards",
    c,
    "tags",
    "t1"
  ]]),
  ...["n1", "x1"].flatMap((n) => [["notes", n, "_tag"], ["notes", n, "pinned"], ["notes", n, "mood"]]),
  ["groups", "g1", "k1"]
]
const plannerPresences: ReadonlyArray<ReadonlyArray<string>> = [
  ["cards", "c1"],
  ["cards", "c2"],
  ["cards", "c1", "tags", "t1"],
  ["notes", "n1"],
  ["notes", "n2"],
  ["notes", "n1", "mood"],
  ["groups", "g1"],
  ["groups", "g1", "k1"]
]
const plannerJson: ReadonlyArray<Schema.Json> = [
  "open",
  "gone",
  "low",
  "top",
  "red",
  "a",
  "c",
  "Note",
  "Card",
  "ok",
  true,
  false,
  "c1",
  "c2"
]

/** The ops of a forged change. A card is placed at the start as it's added. */
export const forgedPlannerOps = (
  ops: Extract<typeof PlannerCommand.Type, { readonly _tag: "Forge" }>["ops"]
): Array<InternalDocument.Op> =>
  ops.flatMap(({ op, path, value }): Array<InternalDocument.Op> => {
    if (op === "Set") return [{ _tag: "Set", path: choose(plannerValues, path), value: choose(plannerJson, value) }]
    const at = choose(plannerPresences, path)
    return op === "Add" && at[0] === "cards" && at.length === 2
      ? [{ _tag: "Add", path: at }, { _tag: "Place", path: at, parent: null, side: "R", rightOrigin: null }]
      : [{ _tag: op, path: at }]
  })
