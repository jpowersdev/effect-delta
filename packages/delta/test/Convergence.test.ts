import { assert, describe, it } from "@effect/vitest"
import { Clock, Effect, Equal, Random, Schema } from "effect"
import { Delta, Replica, ReplicatedDocument, ReplicatedText, VersionVector } from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import { TestClock } from "effect/testing"

// Random editing sessions across three replicas, with partial syncs along the
// way. Positions are fractions of the current text, so every generated command
// is valid whatever the text is by then.

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
  Schema.TaggedStruct("Sync", { from: Which, to: Which })
])

const Session = Schema.Array(Command).check(Schema.isMaxLength(40))

const letters = (s: string) => [...s].map((c) => "abc"[c.charCodeAt(0) % 3]!).join("")

const run = Effect.fnUntraced(function*(strategy: Replica.Strategy, session: typeof Session.Type) {
  const make = (replica: string) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(replica) })
  const replicas = [yield* make("r0"), yield* make("r1"), yield* make("r2")] as const
  for (const command of session) {
    switch (command._tag) {
      case "Insert": {
        const replica = replicas[command.replica]
        const before = yield* ReplicatedText.get(replica)
        const index = Math.floor(command.at * (before.length + 1) / 1001)
        const text = letters(command.text)
        yield* ReplicatedText.insert(replica, index, text)
        // A replica shows its own edits immediately and exactly.
        assert.strictEqual(yield* ReplicatedText.get(replica), before.slice(0, index) + text + before.slice(index))
        break
      }
      case "Remove": {
        const replica = replicas[command.replica]
        const before = yield* ReplicatedText.get(replica)
        if (before.length === 0) break
        const index = Math.floor(command.at * before.length / 1001)
        const length = Math.min(command.length, before.length - index)
        yield* ReplicatedText.remove(replica, index, length)
        assert.strictEqual(yield* ReplicatedText.get(replica), before.slice(0, index) + before.slice(index + length))
        break
      }
      case "Sync":
        if (command.from !== command.to) yield* Replica.exchange(replicas[command.from], replicas[command.to])
        break
    }
  }
  return replicas
})

const syncAll = Effect.fnUntraced(function*(replicas: ReadonlyArray<Replica.Replica<unknown>>) {
  for (let round = 0; round < 2; round++) {
    for (let i = 0; i < replicas.length; i++) {
      for (let j = i + 1; j < replicas.length; j++) yield* Replica.exchange(replicas[i]!, replicas[j]!)
    }
  }
})

describe.each(["tree", "replay"] as const)("convergence (%s)", (strategy) => {
  it.effect.prop(
    "replicas that have exchanged everything show the same text",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const replicas = yield* run(strategy, session)
      yield* syncAll(replicas)
      const [first, ...rest] = yield* Effect.forEach(replicas, ReplicatedText.get)
      for (const text of rest) assert.strictEqual(text, first)
      const [version, ...versions] = yield* Effect.forEach(replicas, Replica.version)
      for (const other of versions) assert.deepStrictEqual(other, version)
    }),
    { arbitrary: { runs: 200, size: 100 } }
  )

  it.effect.prop(
    "receiving is order-independent and safe to repeat",
    { session: Session, seed: Schema.Int },
    Effect.fnUntraced(function*({ session, seed }) {
      const replicas = yield* run(strategy, session)
      const deltas = yield* Effect.forEach(replicas, (replica) => Replica.delta(replica, VersionVector.empty))
      const everything = deltas.reduce(Delta.combine, Delta.empty)

      const atOnce = yield* ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make("at-once") })
      yield* Replica.receive(atOnce, everything)

      const changes = [...(everything as InternalDelta.DeltaImpl).changes.values()]
      const shuffled = yield* Random.shuffle([...changes, ...changes]).pipe(Random.withSeed(seed))
      const oneByOne = yield* ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make("one-by-one") })
      for (const change of shuffled) yield* Replica.receive(oneByOne, InternalDelta.fromChanges([change]))

      yield* syncAll(replicas)
      const expected = yield* ReplicatedText.get(replicas[0])
      assert.strictEqual(yield* ReplicatedText.get(atOnce), expected)
      assert.strictEqual(yield* ReplicatedText.get(oneByOne), expected)
    }),
    { arbitrary: { runs: 100, size: 100 } }
  )

  it.effect.prop(
    "combining deltas is commutative, associative and idempotent",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const replicas = yield* run(strategy, session)
      const [a, b, c] = yield* Effect.forEach(replicas, (replica) => Replica.delta(replica, VersionVector.empty))
      assert.isTrue(Equal.equals(Delta.combine(a!, b!), Delta.combine(b!, a!)))
      assert.isTrue(Equal.equals(Delta.combine(Delta.combine(a!, b!), c!), Delta.combine(a!, Delta.combine(b!, c!))))
      assert.isTrue(Equal.equals(Delta.combine(a!, a!), a!))
      assert.isTrue(Equal.equals(Delta.combine(a!, Delta.empty), a!))
    }),
    { arbitrary: { runs: 100, size: 100 } }
  )
})

// The interleaving cases from "The Art of the Fugue": two people typing at the
// same spot without seeing each other must each get a contiguous passage.
describe.each(["tree", "replay"] as const)("non-interleaving (%s)", (strategy) => {
  const typeForward = Effect.fnUntraced(function*(replica: ReplicatedText.ReplicatedText, at: number, text: string) {
    for (let i = 0; i < text.length; i++) yield* ReplicatedText.insert(replica, at + i, text[i]!)
  })
  const typeBackward = Effect.fnUntraced(function*(replica: ReplicatedText.ReplicatedText, at: number, text: string) {
    for (let i = text.length - 1; i >= 0; i--) yield* ReplicatedText.insert(replica, at, text[i]!)
  })

  const make = (replica: string) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(replica) })
  const setup = Effect.gen(function*() {
    const alice = yield* make("alice")
    const bob = yield* make("bob")
    yield* ReplicatedText.insert(alice, 0, "[]")
    yield* Replica.exchange(alice, bob)
    return { alice, bob }
  })

  const assertOneOf = (text: string, options: ReadonlyArray<string>) =>
    assert.isTrue(options.includes(text), `${JSON.stringify(text)} is interleaved`)

  it.effect("typing forward", () =>
    Effect.gen(function*() {
      const { alice, bob } = yield* setup
      yield* typeForward(alice, 1, "milk")
      yield* typeForward(bob, 1, "eggs")
      yield* Replica.exchange(alice, bob)
      assertOneOf(yield* ReplicatedText.get(alice), ["[milkeggs]", "[eggsmilk]"])
    }))

  it.effect("typing backward", () =>
    Effect.gen(function*() {
      const { alice, bob } = yield* setup
      yield* typeBackward(alice, 1, "milk")
      yield* typeBackward(bob, 1, "eggs")
      yield* Replica.exchange(alice, bob)
      assertOneOf(yield* ReplicatedText.get(alice), ["[milkeggs]", "[eggsmilk]"])
    }))

  it.effect("typing backward across a reload, with a new replica id", () =>
    Effect.gen(function*() {
      const { alice, bob } = yield* setup
      yield* typeBackward(alice, 1, "lk")
      // Alice's tab reloads: a new replica with the same text.
      const reloaded = yield* make("alice-2")
      yield* Replica.exchange(alice, reloaded)
      yield* typeBackward(reloaded, 1, "mi")
      yield* typeBackward(bob, 1, "eggs")
      yield* Replica.exchange(reloaded, bob)
      assertOneOf(yield* ReplicatedText.get(bob), ["[milkeggs]", "[eggsmilk]"])
    }))
})

// Documents ---------------------------------------------------------------

// The same, for documents, over a schema that uses every merge rule. Record
// keys and list item keys come from a small set, so replicas often add,
// remove, move and edit the same ones at once.

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

describe.each(["tree", "replay"] as const)("document convergence (%s)", (strategy) => {
  it.effect.prop(
    "replicas that have exchanged everything show the same value",
    { session: DocumentSession },
    Effect.fnUntraced(function*({ session }) {
      const docs = yield* makeBoards(strategy)
      for (const command of session) yield* stepBoards(docs, command)
      yield* syncAll(docs)
      const [first, ...rest] = yield* Effect.forEach(docs, viewOf)
      for (const view of rest) assert.deepStrictEqual(view, first)
      const [version, ...versions] = yield* Effect.forEach(docs, Replica.version)
      for (const other of versions) assert.deepStrictEqual(other, version)
    }),
    { arbitrary: documentRuns }
  )

  it.effect.prop(
    "receiving is order-independent and safe to repeat",
    { session: DocumentSession, seed: Schema.Int },
    Effect.fnUntraced(function*({ session, seed }) {
      const docs = yield* makeBoards(strategy)
      for (const command of session) yield* stepBoards(docs, command)
      const deltas = yield* Effect.forEach(docs, (doc) => Replica.delta(doc, VersionVector.empty))
      const everything = deltas.reduce(Delta.combine, Delta.empty)
      const make = (replica: string) =>
        ReplicatedDocument.make(Board, { initial: initialBoard, strategy, replica: Replica.ReplicaId.make(replica) })

      const atOnce = yield* make("at-once")
      yield* Replica.receive(atOnce, everything)

      const changes = [...(everything as InternalDelta.DeltaImpl).changes.values()]
      const shuffled = yield* Random.shuffle([...changes, ...changes]).pipe(Random.withSeed(seed))
      const oneByOne = yield* make("one-by-one")
      for (const change of shuffled) yield* Replica.receive(oneByOne, InternalDelta.fromChanges([change]))

      yield* syncAll(docs)
      const expected = yield* viewOf(docs[0]!)
      assert.deepStrictEqual(yield* viewOf(atOnce), expected)
      assert.deepStrictEqual(yield* viewOf(oneByOne), expected)
    }),
    { arbitrary: documentRuns }
  )
})

// Latest write ------------------------------------------------------------

// Writes of one value across three replicas, against a model of what each
// replica has seen, with clocks moving forward and back between writes.

const Theme = Schema.Literals(["light", "dark", "dim"])
const Settings = Schema.Struct({ theme: Theme })

const WriteCommand = Schema.Union([
  Schema.TaggedStruct("Write", { replica: Which, theme: Theme }),
  Schema.TaggedStruct("Adjust", { millis: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 5 })) }),
  Schema.TaggedStruct("SetTime", { millis: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 20 })) }),
  Schema.TaggedStruct("Sync", { from: Which, to: Which })
])

const WriteSession = Schema.Array(WriteCommand).check(Schema.isMaxLength(40))

interface Write {
  readonly replica: string
  readonly seq: number
  readonly time: number
  readonly theme: typeof Theme.Type
  /** What its replica had seen. */
  readonly past: VersionVector.VersionVector
}

const writeKey = (write: Write) => `${write.seq}@${write.replica}`
const saw = (by: Write, write: Write) => (by.past[write.replica] ?? 0) > write.seq
const concurrent = (a: Write, b: Write) => !saw(a, b) && !saw(b, a)
const isLater = (a: Write, b: Write) =>
  a.time !== b.time ? a.time > b.time : a.replica !== b.replica ? a.replica > b.replica : a.seq > b.seq
const latestOf = (writes: Iterable<Write>): Write | undefined => {
  let found: Write | undefined
  for (const write of writes) if (found === undefined || isLater(write, found)) found = write
  return found
}
const versionOf = (writes: Iterable<Write>): VersionVector.VersionVector => {
  const version: Record<string, number> = {}
  for (const { replica, seq } of writes) version[replica] = Math.max(version[replica] ?? 0, seq + 1)
  return version
}

// A merge reports a Value overlap exactly when a write it receives is
// concurrent with another write, received or not, that has a different value.
// The overlap names every replica in such a pair, and whose write shows.
const assertValueOverlaps = (
  overlaps: ReadonlyArray<Replica.Overlap>,
  had: ReadonlyArray<Write>,
  received: ReadonlyArray<Write>
) => {
  const all = [...had, ...received]
  const met = received.flatMap((write) =>
    all.filter((other) => other.theme !== write.theme && concurrent(other, write)).map((other) => [write, other])
  )
  if (met.length === 0) return assert.deepStrictEqual(overlaps, [])
  assert.deepStrictEqual(overlaps, [{
    _tag: "Value",
    path: ["theme"],
    replicas: [...new Set(met.flat().map(({ replica }) => replica))].sort().map((id) => Replica.ReplicaId.make(id)),
    kept: Replica.ReplicaId.make(latestOf(all)!.replica)
  }])
}

describe("latest write", () => {
  it.effect.prop(
    "a value is its largest write, and merges report concurrent writes that differ",
    { session: WriteSession },
    Effect.fnUntraced(function*({ session }) {
      const ids = ["r0", "r1", "r2"]
      const docs = yield* Effect.forEach(
        ids,
        (id) => ReplicatedDocument.make(Settings, { initial: { theme: "light" }, replica: Replica.ReplicaId.make(id) })
      )
      // The writes each replica has seen, by key.
      const seen = ids.map(() => new Map<string, Write>())
      const shows = (writes: Map<string, Write>) => latestOf(writes.values())?.theme ?? "light"

      for (const command of session) {
        switch (command._tag) {
          case "Write": {
            const known = seen[command.replica]!
            yield* ReplicatedDocument.update(docs[command.replica]!, () => ({ theme: command.theme }))
            // Writing what it already shows writes nothing.
            if (shows(known) === command.theme) break
            const replica = ids[command.replica]!
            const writes = [...known.values()]
            const write: Write = {
              replica,
              seq: writes.filter((write) => write.replica === replica).length,
              // Later than everything the replica had seen, whatever its clock says.
              time: Math.max(yield* Clock.currentTimeMillis, ...writes.map(({ time }) => time + 1)),
              theme: command.theme,
              past: versionOf(writes)
            }
            known.set(writeKey(write), write)
            break
          }
          case "Adjust":
            yield* TestClock.adjust(command.millis)
            break
          case "SetTime":
            yield* TestClock.setTime(command.millis)
            break
          case "Sync": {
            if (command.from === command.to) break
            const [a, b] = [docs[command.from]!, docs[command.to]!]
            const [knownA, knownB] = [seen[command.from]!, seen[command.to]!]
            const [toA, toB] = yield* Effect.all([
              Replica.delta(b, yield* Replica.version(a)),
              Replica.delta(a, yield* Replica.version(b))
            ])
            const [hadA, hadB] = [[...knownA.values()], [...knownB.values()]]
            const forA = hadB.filter((write) => !knownA.has(writeKey(write)))
            const forB = hadA.filter((write) => !knownB.has(writeKey(write)))
            assertValueOverlaps(yield* Replica.receive(a, toA), hadA, forA)
            assertValueOverlaps(yield* Replica.receive(b, toB), hadB, forB)
            for (const write of [...forA, ...forB]) {
              knownA.set(writeKey(write), write)
              knownB.set(writeKey(write), write)
            }
            break
          }
        }
        for (const [i, doc] of docs.entries()) {
          assert.strictEqual((yield* ReplicatedDocument.get(doc)).theme, shows(seen[i]!), `r${i}'s value`)
          assert.deepStrictEqual(yield* Replica.version(doc), versionOf(seen[i]!.values()), `r${i}'s version`)
        }
      }

      yield* syncAll(docs)
      const everything = latestOf(seen.flatMap((writes) => [...writes.values()]))
      for (const doc of docs) {
        assert.strictEqual((yield* ReplicatedDocument.get(doc)).theme, everything?.theme ?? "light")
      }
    }),
    { arbitrary: { runs: 200, size: 100 } }
  )
})
