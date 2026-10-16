import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, Random, Schema } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"
import type { Change, Id } from "effect-delta/internal/change"
import * as InternalDelta from "effect-delta/internal/delta"
import type * as ReplayText from "effect-delta/internal/replayText"
import { TestClock } from "effect/testing"
import cases from "./fixtures/eg-walker-conformance.json" with { type: "json" }

// The replay strategy must give exactly what the tree strategy gives.

interface Transaction {
  readonly span: ReadonlyArray<number>
  readonly parents: ReadonlyArray<number>
  readonly agent: string
  readonly seqStart: number
  readonly ops: ReadonlyArray<ReadonlyArray<number | string>>
}

interface Case {
  readonly txns: ReadonlyArray<Transaction>
  readonly endContent: string
}

// The conformance transactions are already replay changes: index edits on top
// of their parents.
const toChanges = (transactions: ReadonlyArray<Transaction>): Array<Change<ReplayText.Op>> => {
  const idOf = (version: number): Id => {
    const txn = transactions.find((txn) => txn.span[0]! <= version && version < txn.span[1]!)!
    return { replica: txn.agent, seq: txn.seqStart + version - txn.span[0]! }
  }
  return transactions.map((txn) => {
    const ops: Array<ReplayText.Op> = []
    for (const [index, removed, inserted] of txn.ops as ReadonlyArray<readonly [number, number, string]>) {
      if (removed > 0) ops.push({ _tag: "RemoveAt", index, length: removed })
      if (inserted.length > 0) ops.push({ _tag: "InsertAt", index, text: inserted })
    }
    return {
      replica: txn.agent,
      seq: txn.seqStart,
      length: txn.span[1]! - txn.span[0]!,
      parents: txn.parents.map(idOf),
      // A transaction's versions come after its parents', so they serve as write times.
      time: txn.span[0]!,
      ops
    }
  })
}

const makeReplay = (replica: string) =>
  ReplicatedText.make({ strategy: "replay", replica: Replica.ReplicaId.make(replica) })

describe("conformance with Eg-walker", () => {
  it.effect("replay text matches the reference for every case", () =>
    Effect.gen(function*() {
      for (const [index, { endContent, txns }] of (cases as ReadonlyArray<Case>).entries()) {
        const replica = yield* makeReplay("observer")
        for (const change of toChanges(txns)) yield* Replica.receive(replica, InternalDelta.fromChanges([change]))
        assert.strictEqual(yield* ReplicatedText.get(replica), endContent, `case ${index}`)
      }
    }))

  it.effect("in any order, or all at once", () =>
    Effect.gen(function*() {
      for (const [index, { endContent, txns }] of (cases as ReadonlyArray<Case>).slice(0, 100).entries()) {
        const shuffled = yield* makeReplay("shuffled")
        for (const change of yield* Random.shuffle(toChanges(txns))) {
          yield* Replica.receive(shuffled, InternalDelta.fromChanges([change]))
        }
        assert.strictEqual(yield* ReplicatedText.get(shuffled), endContent, `case ${index}, shuffled`)
        const atOnce = yield* makeReplay("at-once")
        yield* Replica.receive(atOnce, InternalDelta.fromChanges(toChanges(txns)))
        assert.strictEqual(yield* ReplicatedText.get(atOnce), endContent, `case ${index}, at once`)
      }
    }).pipe(Random.withSeed("replay")))
})

// Random sessions, run on tree replicas and replay replicas side by side. After
// every step each pair must show the same text, every merge must report the
// same overlaps, and anchors must be the same and land in the same places.

const Which = Schema.Literals([0, 1, 2])
const Fraction = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))

const Command = Schema.Union([
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

const Session = Schema.Array(Command).check(Schema.isMaxLength(50))

const letters = (s: string) => [...s].map((c) => "abc"[c.charCodeAt(0) % 3]!).join("")

const makeAll = (strategy: "tree" | "replay") =>
  Effect.forEach(["r0", "r1", "r2"], (id) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(id) }))

describe("replay against tree", () => {
  it.effect("an overlap where both removed the same words", () =>
    Effect.gen(function*() {
      const overlaps = yield* Effect.forEach(["tree", "replay"] as const, (strategy) =>
        Effect.gen(function*() {
          const make = (replica: string) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(replica) })
          const alice = yield* make("alice")
          const bob = yield* make("bob")
          yield* ReplicatedText.insert(alice, 0, "Sessions last 24 hours.")
          yield* Replica.exchange(alice, bob)
          yield* ReplicatedText.remove(alice, 14, 3)
          yield* ReplicatedText.remove(bob, 14, 3)
          const found = yield* Replica.exchange(alice, bob)
          assert.strictEqual(yield* ReplicatedText.get(alice), "Sessions last hours.")
          // Anchored on the removed characters, so they still find the spot.
          for (const overlap of found) {
            if (overlap._tag !== "Text") return assert.fail("expected a text overlap")
            assert.deepStrictEqual(yield* ReplicatedText.locate(alice, overlap.anchors.start), Option.some(14))
          }
          return found
        }))
      assert.strictEqual(overlaps[0]!.length, 1)
      assert.deepStrictEqual(overlaps[1], overlaps[0])
    }))

  it.effect.prop(
    "every step gives the same text, overlaps and anchors",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const tree = yield* makeAll("tree")
      const replay = yield* makeAll("replay")
      const anchors: Array<ReplicatedText.Anchor> = []

      for (const command of session) {
        const both = <A>(f: (replicas: ReadonlyArray<ReplicatedText.ReplicatedText>) => Effect.Effect<A, unknown>) =>
          Effect.all([f(tree), f(replay)])
        switch (command._tag) {
          case "Insert": {
            const text = yield* ReplicatedText.get(tree[command.replica]!)
            const index = Math.floor(command.at * (text.length + 1) / 1001)
            yield* both((replicas) => ReplicatedText.insert(replicas[command.replica]!, index, letters(command.text)))
            break
          }
          case "Remove": {
            const text = yield* ReplicatedText.get(tree[command.replica]!)
            if (text.length === 0) break
            const index = Math.floor(command.at * text.length / 1001)
            const length = Math.min(command.length, text.length - index)
            yield* both((replicas) => ReplicatedText.remove(replicas[command.replica]!, index, length))
            break
          }
          case "Sync": {
            if (command.from === command.to) break
            const [fromTree, fromReplay] = yield* both((replicas) =>
              Replica.exchange(replicas[command.from]!, replicas[command.to]!)
            )
            assert.deepStrictEqual(fromReplay, fromTree, "overlaps")
            break
          }
          case "Anchor": {
            const text = yield* ReplicatedText.get(tree[command.replica]!)
            const index = Math.floor(command.at * (text.length + 1) / 1001)
            const side = command.after ? "after" : "before"
            const [inTree, inReplay] = yield* both((replicas) =>
              ReplicatedText.anchor(replicas[command.replica]!, index, side)
            )
            assert.deepStrictEqual(inReplay, inTree, "anchor")
            anchors.push(inTree)
            break
          }
        }
        for (const i of [0, 1, 2]) {
          assert.strictEqual(yield* ReplicatedText.get(replay[i]!), yield* ReplicatedText.get(tree[i]!), `replica ${i}`)
        }
      }

      for (const anchor of anchors) {
        for (const i of [0, 1, 2]) {
          assert.deepStrictEqual(
            yield* ReplicatedText.locate(replay[i]!, anchor),
            yield* ReplicatedText.locate(tree[i]!, anchor),
            "located"
          )
        }
      }
    }),
    { arbitrary: { runs: 300, size: 100 } }
  )
})

// Documents ---------------------------------------------------------------

// Random document sessions, as in Convergence.test.ts (and kept in step with
// it), run on tree documents and replay documents side by side.

const Note = Schema.Struct({ body: ReplicatedDocument.Text, pinned: Schema.Boolean })
const Task = Schema.Struct({ id: Schema.String, title: ReplicatedDocument.Text, done: Schema.Boolean })
const Due = Schema.Literals(["mon", "fri"])

const Board = Schema.Struct({
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

const initialBoard: Board = { status: "draft", size: 0, title: "", votes: 0, notes: {}, tasks: [], tags: [] }

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

// Commands have several fields each, so sessions need a larger generation
// size than the default (10), which allows only two or three of them.
const DocumentSession = Schema.Array(DocumentCommand).check(Schema.isMaxLength(60))
const documentRuns = { runs: 100, size: 100 }

type BoardDocument = ReplicatedDocument.ReplicatedDocument<Board>

const makeBoards = (strategy: Replica.Strategy) =>
  Effect.forEach(
    ["r0", "r1", "r2"],
    (id) => ReplicatedDocument.make(Board, { initial: initialBoard, strategy, replica: Replica.ReplicaId.make(id) })
  )

// A gap in, or an item of, something of `length`.
const gapAt = (at: number, length: number) => Math.floor(at * (length + 1) / 1001)
const itemAt = (at: number, length: number) => Math.floor(at * length / 1001)

const textOf = (doc: BoardDocument, field: "title" | "note" | "task", key: string) =>
  ReplicatedDocument.text(
    doc,
    field === "title" ? ["title"] : field === "note" ? ["notes", key, "body"] : ["tasks", key, "title"]
  )

// The value, and the texts of every note and task, including ones that don't
// show: those are kept, and show again when the note or task is added back.
const viewOf = Effect.fnUntraced(function*(doc: BoardDocument) {
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
const stepBoards = Effect.fnUntraced(function*(
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

describe("replay against tree, for documents", () => {
  it.effect.prop(
    "every step gives the same value and overlaps",
    { session: DocumentSession },
    Effect.fnUntraced(function*({ session }) {
      const tree = yield* makeBoards("tree")
      const replay = yield* makeBoards("replay")
      for (const command of session) {
        const inTree = yield* stepBoards(tree, command)
        assert.deepStrictEqual(yield* stepBoards(replay, command), inTree, "overlaps")
        for (const i of [0, 1, 2]) {
          assert.deepStrictEqual(yield* viewOf(replay[i]!), yield* viewOf(tree[i]!), `replica ${i}`)
        }
      }
    }),
    { arbitrary: documentRuns }
  )
})

// A document's text field must be a replicated text in every way: the same
// edits and syncs give the same text, anchors and overlaps, step by step. Only
// the field is edited, so its characters get the same ids as the text's.

const Page = Schema.Struct({ title: Schema.String, body: ReplicatedDocument.Text })

describe.each(["tree", "replay"] as const)("a text field against a replicated text (%s)", (strategy) => {
  it.effect.prop(
    "every step gives the same text, overlaps and anchors",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const texts = yield* makeAll(strategy)
      const pages = yield* Effect.forEach(
        ["r0", "r1", "r2"],
        (id) =>
          ReplicatedDocument.make(Page, {
            initial: { title: "", body: "" },
            strategy,
            replica: Replica.ReplicaId.make(id)
          })
      )
      const fields = pages.map((page) => ReplicatedDocument.text(page, ["body"]))
      const anchors: Array<ReplicatedText.Anchor> = []
      // A text's overlaps are at [], a field's at its path.
      const withoutPath = (overlaps: ReadonlyArray<Replica.Overlap>, path: Replica.Path) =>
        overlaps.map(({ path: at, ...overlap }) => {
          assert.deepStrictEqual(at, path)
          return overlap
        })

      for (const command of session) {
        const both = <A>(f: (texts: ReadonlyArray<ReplicatedText.Editable>) => Effect.Effect<A, unknown>) =>
          Effect.all([f(texts), f(fields)])
        switch (command._tag) {
          case "Insert": {
            const text = yield* ReplicatedText.get(texts[command.replica]!)
            const index = Math.floor(command.at * (text.length + 1) / 1001)
            yield* both((editables) => ReplicatedText.insert(editables[command.replica]!, index, letters(command.text)))
            break
          }
          case "Remove": {
            const text = yield* ReplicatedText.get(texts[command.replica]!)
            if (text.length === 0) break
            const index = Math.floor(command.at * text.length / 1001)
            const length = Math.min(command.length, text.length - index)
            yield* both((editables) => ReplicatedText.remove(editables[command.replica]!, index, length))
            break
          }
          case "Sync": {
            if (command.from === command.to) break
            const fromText = yield* Replica.exchange(texts[command.from]!, texts[command.to]!)
            const fromField = yield* Replica.exchange(pages[command.from]!, pages[command.to]!)
            assert.deepStrictEqual(withoutPath(fromField, ["body"]), withoutPath(fromText, []), "overlaps")
            break
          }
          case "Anchor": {
            const text = yield* ReplicatedText.get(texts[command.replica]!)
            const index = Math.floor(command.at * (text.length + 1) / 1001)
            const side = command.after ? "after" : "before"
            const [inText, inField] = yield* both((editables) =>
              ReplicatedText.anchor(editables[command.replica]!, index, side)
            )
            assert.deepStrictEqual(inField, inText, "anchor")
            anchors.push(inText)
            break
          }
        }
        for (const i of [0, 1, 2]) {
          const text = yield* ReplicatedText.get(texts[i]!)
          assert.strictEqual(yield* ReplicatedText.get(fields[i]!), text, `replica ${i}`)
          assert.strictEqual((yield* ReplicatedDocument.get(pages[i]!)).body, text, `replica ${i}'s value`)
        }
      }

      for (const anchor of anchors) {
        for (const i of [0, 1, 2]) {
          assert.deepStrictEqual(
            yield* ReplicatedText.locate(fields[i]!, anchor),
            yield* ReplicatedText.locate(texts[i]!, anchor),
            "located"
          )
        }
      }
    }),
    // The default size allows sessions of only two or three commands.
    { arbitrary: { runs: 100, size: 100 } }
  )
})
