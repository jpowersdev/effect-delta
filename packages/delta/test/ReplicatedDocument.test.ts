import { assert, describe, it } from "@effect/vitest"
import { Clock, Effect, Exit, Fiber, Option, Scheduler, Schema, Stream } from "effect"
import { Delta, HistoryStore, Replica, ReplicatedDocument, ReplicatedText, VersionVector } from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import type * as InternalDocument from "effect-delta/internal/document"
import { TestClock } from "effect/testing"
import { letters, makeTexts, syncAll, TextSession, Which } from "./utils/sessions.ts"

const Comment = Schema.Struct({ author: Schema.String, text: ReplicatedDocument.Text })

const Doc = Schema.Struct({
  title: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  votes: ReplicatedDocument.Counter,
  comments: Schema.Record(Schema.String, Comment),
  labels: Schema.Record(Schema.String, Schema.Int),
  due: Schema.optionalKey(Schema.String)
})
type Doc = typeof Doc.Type

const initial: Doc = { title: "", status: "draft", votes: 0, comments: {}, labels: {} }

const make = (replica: string, strategy: Replica.Strategy = "tree") =>
  ReplicatedDocument.make(Doc, { initial, strategy, replica: Replica.ReplicaId.make(replica) })

const pair = Effect.gen(function*() {
  const ada = yield* make("ada")
  const ben = yield* make("ben")
  return { ada, ben }
})

// Both replicas show the same value, which is returned.
const converged = Effect.fnUntraced(function*(a: ReplicatedDocument.ReplicatedDocument<Doc>, b: typeof a) {
  const value = yield* ReplicatedDocument.get(a)
  assert.deepStrictEqual(yield* ReplicatedDocument.get(b), value)
  return value
})

describe("ReplicatedDocument", () => {
  describe("make", () => {
    const refuses = (schema: Schema.Top, start: unknown, message: string) =>
      Effect.gen(function*() {
        const exit = yield* Effect.exit(
          ReplicatedDocument.make(schema as Schema.Schema<unknown>, {
            initial: start,
            replica: Replica.ReplicaId.make("r")
          })
        )
        assert.isTrue(Exit.isFailure(exit) && Exit.hasDies(exit), "dies")
        assert.include(String(Exit.isFailure(exit) ? exit.cause : ""), message)
      })

    it.effect("starts with the initial value", () =>
      Effect.gen(function*() {
        assert.deepStrictEqual(yield* ReplicatedDocument.get(yield* make("ada")), initial)
      }))

    it.effect("refuses a schema whose merge rules can't hold", () =>
      Effect.gen(function*() {
        yield* refuses(ReplicatedDocument.Text, "", "must be a struct, a class or a record")
        yield* refuses(
          Schema.Struct({ title: ReplicatedDocument.Text.check(Schema.isMaxLength(10)) }),
          { title: "" },
          "a text can't have checks"
        )
        yield* refuses(
          Schema.Struct({ a: Schema.Int, b: Schema.Int }).check(Schema.makeFilter(({ a, b }) => a <= b)),
          { a: 0, b: 0 },
          "a struct can't have checks"
        )
        yield* refuses(
          Schema.Struct({ votes: ReplicatedDocument.Counter.check(Schema.isBetween({ minimum: 0, maximum: 5 })) }),
          { votes: 0 },
          "a counter can't have checks"
        )
        yield* refuses(
          Schema.Struct({ body: Schema.NullOr(ReplicatedDocument.Text) }),
          { body: null },
          "can't be inside a value that's written whole"
        )
        yield* refuses(
          Schema.Struct({
            tasks: ReplicatedDocument.List(Schema.Struct({ title: Schema.String }), { key: "id" as never })
          }),
          { tasks: [] },
          `required "id" field`
        )
      }))

    it.effect("refuses an initial value that doesn't fit, or writes texts, entries or optional fields", () =>
      Effect.gen(function*() {
        yield* refuses(Doc, { ...initial, title: "Draft" }, "a text starts empty")
        yield* refuses(Doc, { ...initial, labels: { urgent: 1 } }, "a record starts empty")
        yield* refuses(Doc, { ...initial, due: "friday" }, "an optional field starts absent")
        yield* refuses(Doc, { ...initial, status: "done" }, "must fit its schema")
      }))
  })

  describe("values", () => {
    it.effect("concurrent edits to different fields both show", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, due: "friday" }))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        const doc = yield* converged(ada, ben)
        assert.deepStrictEqual([doc.status, doc.due], ["review", "friday"])
      }))

    it.effect("the later of two concurrent writes shows, and the merge says so", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "approved" }))
        yield* TestClock.adjust("1 second")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))
        const overlaps = yield* Replica.exchange(ben, ada)
        assert.deepStrictEqual(overlaps, [{
          _tag: "Value",
          path: ["status"],
          replicas: [Replica.ReplicaId.make("ada"), Replica.ReplicaId.make("ben")],
          kept: Replica.ReplicaId.make("ada")
        }])
        assert.strictEqual((yield* converged(ada, ben)).status, "review")
      }))

    it.effect("an update that changes nothing writes nothing", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc }))
        assert.deepStrictEqual(yield* Replica.version(ada), VersionVector.empty)
      }))

    it.effect("an update is one change", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review", due: "friday", votes: 2 }))
        const delta = yield* Replica.delta(ada, VersionVector.empty)
        assert.strictEqual((delta as InternalDelta.DeltaImpl).changes.size, 1)
      }))

    it.effect("an update fails without writing if the value doesn't fit", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        const failed = yield* Effect.flip(
          ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "done" as never, votes: 3 }))
        )
        assert.strictEqual(failed._tag, "SchemaError")
        const counted = yield* Effect.flip(ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 2.5 })))
        assert.strictEqual(counted._tag, "SchemaError")
        assert.deepStrictEqual(yield* Replica.version(ada), VersionVector.empty)
      }))

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

    it.effect.prop(
      "a value is its largest write, and merges report concurrent writes that differ",
      { session: WriteSession },
      Effect.fnUntraced(function*({ session }) {
        const ids = ["r0", "r1", "r2"]
        const docs = yield* Effect.forEach(
          ids,
          (id) =>
            ReplicatedDocument.make(Settings, { initial: { theme: "light" }, replica: Replica.ReplicaId.make(id) })
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

  describe("counters", () => {
    it.effect("concurrent increments add up, and aren't overlaps", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 5 }))
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: doc.votes + 2 }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, votes: doc.votes + 3 }))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.strictEqual((yield* converged(ada, ben)).votes, 10)
      }))

    it.effect("start from the initial value", () =>
      Effect.gen(function*() {
        const Poll = Schema.Struct({ yes: ReplicatedDocument.Counter })
        const poll = yield* ReplicatedDocument.make(Poll, {
          initial: { yes: 10 },
          replica: Replica.ReplicaId.make("r")
        })
        yield* ReplicatedDocument.update(poll, ({ yes }) => ({ yes: yes - 1 }))
        assert.deepStrictEqual(yield* ReplicatedDocument.get(poll), { yes: 9 })
      }))

    it.effect("add up to the same total in any order, however large", () =>
      Effect.gen(function*() {
        const Poll = Schema.Struct({ yes: ReplicatedDocument.Counter })
        const poll = (replica: string) =>
          ReplicatedDocument.make(Poll, { initial: { yes: 0 }, replica: Replica.ReplicaId.make(replica) })
        // Three replicas' increments. Added up in the first order, the total
        // passes 2^53 on the way, where a float can't hold every whole number.
        const deltas = yield* Effect.forEach([[Number.MAX_SAFE_INTEGER], [1, 1], [-2]], (increments, i) =>
          Effect.gen(function*() {
            const voter = yield* poll(`r${i}`)
            for (const by of increments) {
              yield* ReplicatedDocument.update(voter, ({ yes }) => ({ yes: yes + by }))
            }
            return yield* Replica.delta(voter, VersionVector.empty)
          }))
        const totals = yield* Effect.forEach([[0, 1, 2], [2, 0, 1]], (order) =>
          Effect.gen(function*() {
            const counted = yield* poll("counted")
            for (const i of order) {
              yield* Replica.receive(counted, deltas[i]!)
            }
            return (yield* ReplicatedDocument.get(counted)).yes
          }))
        assert.deepStrictEqual(totals, [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER])
      }))

    it.effect("read as the nearest whole number the schema allows, so the document stays readable", () =>
      Effect.gen(function*() {
        class Poll extends Schema.Class<Poll>("Poll")({ yes: ReplicatedDocument.Counter, title: Schema.String }) {}
        const poll = (replica: string) =>
          ReplicatedDocument.make(Poll, {
            initial: new Poll({ yes: 0, title: "" }),
            replica: Replica.ReplicaId.make(replica)
          })
        const ada = yield* poll("ada")
        const ben = yield* poll("ben")
        // Together they pass the largest safe integer, which isn't an Int.
        yield* ReplicatedDocument.update(ada, (doc) => new Poll({ ...doc, yes: Number.MAX_SAFE_INTEGER }))
        yield* ReplicatedDocument.update(ben, (doc) => new Poll({ ...doc, yes: 1 }))
        yield* Replica.exchange(ada, ben)
        assert.strictEqual((yield* ReplicatedDocument.get(ada)).yes, Number.MAX_SAFE_INTEGER)
        yield* ReplicatedDocument.update(ada, (doc) => new Poll({ ...doc, title: "Lunch?" }))
        assert.strictEqual((yield* ReplicatedDocument.get(ada)).title, "Lunch?")
      }))
  })

  describe("entries", () => {
    const comment = (author: string, text: string) => ({ author, text })

    it.effect("entries added concurrently both show", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: { a1: comment("ada", "Nice") } }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, comments: { b1: comment("ben", "Why?") } }))
        yield* Replica.exchange(ada, ben)
        assert.deepStrictEqual(Object.keys((yield* converged(ada, ben)).comments), ["a1", "b1"])
      }))

    it.effect("a removal wins over edits inside the entry it hadn't seen, and the merge says so", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: { c1: comment("ada", "Draft") } }))
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: {} }))
        yield* ReplicatedText.insert(ReplicatedDocument.text(ben, ["comments", "c1", "text"]), 5, " two")
        const overlaps = yield* Replica.exchange(ben, ada)
        assert.deepStrictEqual(overlaps, [{
          _tag: "Removed",
          path: ["comments", "c1"],
          replicas: [Replica.ReplicaId.make("ada"), Replica.ReplicaId.make("ben")]
        }])
        assert.deepStrictEqual((yield* converged(ada, ben)).comments, {})
      }))

    it.effect("adding a removed entry back brings back everyone's edits", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: { c1: comment("ada", "Draft") } }))
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: {} }))
        yield* ReplicatedText.insert(ReplicatedDocument.text(ben, ["comments", "c1", "text"]), 5, " two")
        yield* Replica.exchange(ada, ben)
        // The text is kept, so the handle still shows it.
        assert.strictEqual(
          yield* ReplicatedText.get(ReplicatedDocument.text(ada, ["comments", "c1", "text"])),
          "Draft two"
        )

        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: { c1: comment("ada", "Draft two!") } }))
        yield* Replica.exchange(ada, ben)
        assert.deepStrictEqual((yield* converged(ada, ben)).comments, { c1: comment("ada", "Draft two!") })
      }))

    it.effect("one key added concurrently is one entry, merged", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments: { c1: comment("ada", "Yes") } }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, comments: { c1: comment("ben", "No") } }))
        yield* Replica.exchange(ada, ben)
        const { comments } = yield* converged(ada, ben)
        assert.deepStrictEqual(Object.keys(comments), ["c1"])
        assert.include(["YesNo", "NoYes"], comments.c1!.text)
        assert.strictEqual(comments.c1!.author, "ben")
      }))

    it.effect("a record of values merges entry by entry", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, labels: { urgent: 1, later: 2 } }))
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, labels: { urgent: 3 } }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, labels: { ...doc.labels, soon: 4 } }))
        yield* Replica.exchange(ada, ben)
        assert.deepStrictEqual((yield* converged(ada, ben)).labels, { urgent: 3, soon: 4 })
      }))

    it.effect("an entry's key can be any string", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        // Parsed, so `__proto__` is a key rather than the object's prototype.
        const comments = JSON.parse(`{ "__proto__": { "author": "ada", "text": "Hi" } }`)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, comments, labels: JSON.parse(`{ "__proto__": 1 }`) }))
        yield* Replica.exchange(ada, ben)
        const doc = yield* converged(ada, ben)
        assert.deepStrictEqual(Object.entries(doc.comments), [["__proto__", comment("ada", "Hi")]])
        assert.deepStrictEqual(Object.entries(doc.labels), [["__proto__", 1]])
      }))

    it.effect("an optional field comes and goes", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, due: "friday" }))
        assert.strictEqual((yield* ReplicatedDocument.get(ada)).due, "friday")
        yield* ReplicatedDocument.update(ada, ({ due: _, ...doc }) => doc)
        assert.isFalse(Object.hasOwn(yield* ReplicatedDocument.get(ada), "due"))
      }))
  })

  describe("text fields", () => {
    it.effect("are edited like a replicated text, and merge character by character", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        const title = (doc: typeof ada) => ReplicatedDocument.text(doc, ["title"])
        yield* ReplicatedText.insert(title(ada), 0, "Sessions last 24 hours")
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedText.update(title(ada), (text) => text.replace("24", "8"))
        yield* ReplicatedText.update(title(ben), (text) => text.replace("24", "12"))
        const [overlap, ...rest] = yield* Replica.exchange(ada, ben)
        assert.deepStrictEqual(rest, [])
        if (overlap?._tag !== "Text") return assert.fail("expected a text overlap")
        assert.deepStrictEqual(overlap.path, ["title"])
        const { title: merged } = yield* converged(ada, ben)
        assert.include(["Sessions last 812 hours", "Sessions last 128 hours"], merged)
        assert.deepStrictEqual(
          yield* ReplicatedText.locateRange(title(ada), overlap.anchors),
          Option.some(overlap.range)
        )
      }))

    it.effect("watching a text field sees only its own edits", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        const title = ReplicatedDocument.text(ada, ["title"])
        const { edits, text } = yield* ReplicatedText.subscribe(title)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))
        yield* ReplicatedText.insert(title, 0, "Hi")
        assert.strictEqual(text, "")
        assert.deepStrictEqual(
          yield* Stream.runHead(edits),
          Option.some({ _tag: "Local" as const, patch: [{ _tag: "Insert" as const, text: "Hi" }] })
        )
      }))

    it.effect("a path that isn't a text is a defect", () =>
      Effect.gen(function*() {
        const doc = yield* make("ada")
        assert.throws(() => ReplicatedDocument.text(doc, ["status"]), "isn't a text field")
      }))

    // A document's text field must be a replicated text in every way: the same
    // edits and syncs give the same text, anchors and overlaps, step by step. Only
    // the field is edited, so its characters get the same ids as the text's.

    const Page = Schema.Struct({ title: Schema.String, body: ReplicatedDocument.Text })

    describe.each(["tree", "replay"] as const)("against a replicated text (%s)", (strategy) => {
      it.effect.prop(
        "every step gives the same text, overlaps and anchors",
        { session: TextSession },
        Effect.fnUntraced(function*({ session }) {
          const texts = yield* makeTexts(strategy)
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
                yield* both((editables) =>
                  ReplicatedText.insert(editables[command.replica]!, index, letters(command.text))
                )
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
        { arbitrary: { runs: 100, size: 100 } }
      )
    })
  })

  describe("reading", () => {
    it.effect("unchanged parts keep their identity", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({
          ...doc,
          comments: { c1: { author: "ada", text: "One" }, c2: { author: "ada", text: "Two" } }
        }))
        const before = yield* ReplicatedDocument.get(ada)
        yield* ReplicatedText.insert(ReplicatedDocument.text(ada, ["comments", "c2", "text"]), 3, "!")
        const after = yield* ReplicatedDocument.get(ada)
        assert.strictEqual(after.comments.c1, before.comments.c1)
        assert.strictEqual(after.labels, before.labels)
        assert.notStrictEqual(after.comments, before.comments)
      }))

    it.effect("builds classes", () =>
      Effect.gen(function*() {
        class Person extends Schema.Class<Person>("Person")({ name: Schema.String, bio: ReplicatedDocument.Text }) {}
        const Team = Schema.Struct({ lead: Person, members: Schema.Record(Schema.String, Person) })
        const team = yield* ReplicatedDocument.make(Team, {
          initial: { lead: new Person({ name: "Ada", bio: "" }), members: {} },
          replica: Replica.ReplicaId.make("r")
        })
        yield* ReplicatedDocument.update(team, (value) => ({
          ...value,
          members: { ben: new Person({ name: "Ben", bio: "Reviewer" }) }
        }))
        const { lead, members } = yield* ReplicatedDocument.get(team)
        assert.instanceOf(lead, Person)
        assert.instanceOf(members.ben, Person)
        assert.strictEqual(members.ben!.bio, "Reviewer")
      }))

    it.effect("subscribing sees each edit with the value after it", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        const { edits, value } = yield* ReplicatedDocument.subscribe(ada)
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "review" }))
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 1 }))
        yield* Replica.exchange(ada, ben)
        assert.deepStrictEqual(value, initial)
        const [local, remote] = yield* Stream.runCollect(Stream.take(edits, 2))
        assert.deepStrictEqual(local, { _tag: "Local", value: { ...initial, votes: 1 } })
        assert.deepStrictEqual(remote, {
          _tag: "Remote",
          value: { ...initial, votes: 1, status: "review" },
          overlaps: []
        })
      }))

    it.effect("an update or a merge interrupted partway shows what the replica has", () =>
      Effect.gen(function*() {
        // Yields every few steps, so an interruption can land between any two.
        const stepwise = Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)
        for (let steps = 0; steps < 40; steps++) {
          const { ada, ben } = yield* pair
          const interrupted: ReadonlyArray<Effect.Effect<unknown, unknown>> = [
            ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" })),
            Effect.flatMap(Replica.delta(ada, VersionVector.empty), (delta) => Replica.receive(ben, delta))
          ]
          for (const effect of interrupted) {
            const fiber = yield* Effect.forkChild(stepwise(effect))
            for (let i = 0; i < steps; i++) yield* Effect.yieldNow
            yield* Fiber.interrupt(fiber)
          }
          // Each shows what a replica it sends everything to shows.
          for (const doc of [ada, ben]) {
            const copy = yield* make("copy")
            yield* Replica.receive(copy, yield* Replica.delta(doc, VersionVector.empty))
            assert.deepStrictEqual(yield* ReplicatedDocument.get(doc), yield* ReplicatedDocument.get(copy))
          }
        }
      }))
  })

  describe("schema versions", () => {
    const V2 = Schema.Struct({
      ...Doc.fields,
      status: Schema.Literals(["draft", "review", "approved", "archived"]),
      owner: Schema.String
    })

    it.effect("an older schema keeps what it can't read, and shows the latest write it can", () =>
      Effect.gen(function*() {
        const old = yield* make("old")
        const updated = yield* ReplicatedDocument.make(V2, {
          initial: { ...initial, owner: "nobody" },
          replica: Replica.ReplicaId.make("new")
        })
        yield* ReplicatedDocument.update(old, (doc) => ({ ...doc, status: "review" }))
        yield* Replica.exchange(old, updated)
        yield* ReplicatedDocument.update(updated, (doc) => ({ ...doc, status: "archived", owner: "ada" }))
        yield* Replica.exchange(old, updated)

        const seen = yield* ReplicatedDocument.get(old)
        assert.strictEqual(seen.status, "review")
        assert.isFalse(Object.hasOwn(seen, "owner"))
        assert.deepStrictEqual(yield* ReplicatedDocument.get(updated), {
          ...initial,
          status: "archived",
          owner: "ada"
        })
        // The old one keeps and sends on what it can't read.
        const relayed = yield* ReplicatedDocument.make(V2, {
          initial: { ...initial, owner: "nobody" },
          replica: Replica.ReplicaId.make("later")
        })
        yield* Replica.exchange(old, relayed)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(relayed), yield* ReplicatedDocument.get(updated))

        // Its own edits leave alone what it can't read.
        yield* ReplicatedDocument.update(old, (doc) => ({ ...doc, votes: 1 }))
        yield* Replica.exchange(old, updated)
        assert.strictEqual((yield* ReplicatedDocument.get(updated)).status, "archived")
      }))

    it.effect("a newer schema fills in an entry's missing field from its constructor default", () =>
      Effect.gen(function*() {
        const Label = Schema.Struct({ name: Schema.String })
        const LabelV2 = Schema.Struct({
          ...Label.fields,
          color: Schema.String.pipe(Schema.withConstructorDefault(Effect.succeed("gray")))
        })
        const old = yield* ReplicatedDocument.make(Schema.Struct({ labels: Schema.Record(Schema.String, Label) }), {
          initial: { labels: {} },
          replica: Replica.ReplicaId.make("old")
        })
        const updated = yield* ReplicatedDocument.make(
          Schema.Struct({ labels: Schema.Record(Schema.String, LabelV2) }),
          {
            initial: { labels: {} },
            replica: Replica.ReplicaId.make("new")
          }
        )
        yield* ReplicatedDocument.update(old, () => ({ labels: { bug: { name: "Bug" } } }))
        yield* Replica.exchange(old, updated)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(updated), {
          labels: { bug: { name: "Bug", color: "gray" } }
        })
      }))

    it.effect("an optional field an older schema can't read is left out, and kept", () =>
      Effect.gen(function*() {
        const Ticket = Schema.Struct({
          votes: ReplicatedDocument.Counter,
          priority: Schema.optionalKey(Schema.Literals(["low", "high"]))
        })
        const TicketV2 = Schema.Struct({
          ...Ticket.fields,
          priority: Schema.optionalKey(Schema.Literals(["low", "high", "urgent"]))
        })
        const old = yield* ReplicatedDocument.make(Ticket, {
          initial: { votes: 0 },
          replica: Replica.ReplicaId.make("old")
        })
        const updated = yield* ReplicatedDocument.make(TicketV2, {
          initial: { votes: 0 },
          replica: Replica.ReplicaId.make("new")
        })
        yield* ReplicatedDocument.update(updated, (ticket) => ({ ...ticket, priority: "urgent" }))
        yield* Replica.exchange(old, updated)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(old), { votes: 0 })

        // Its own edits leave it alone.
        yield* ReplicatedDocument.update(old, (ticket) => ({ ...ticket, votes: 1 }))
        yield* Replica.exchange(old, updated)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(updated), { votes: 1, priority: "urgent" })
      }))

    it.effect("an older schema isn't told of overlaps in parts it doesn't describe", () =>
      Effect.gen(function*() {
        const V3 = Schema.Struct({ ...V2.fields, notes: ReplicatedDocument.Text })
        const newer = (replica: string) =>
          ReplicatedDocument.make(V3, {
            initial: { ...initial, owner: "nobody", notes: "" },
            replica: Replica.ReplicaId.make(replica)
          })
        const ada = yield* newer("ada")
        const ben = yield* newer("ben")
        // Both write the new parts at once.
        yield* ReplicatedText.insert(ReplicatedDocument.text(ada, ["notes"]), 0, "Ada's")
        yield* ReplicatedText.insert(ReplicatedDocument.text(ben, ["notes"]), 0, "Ben's")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, owner: "ada" }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, owner: "ben" }))

        const old = yield* make("old")
        yield* Replica.receive(old, yield* Replica.delta(ada, VersionVector.empty))
        assert.deepStrictEqual(yield* Replica.receive(old, yield* Replica.delta(ben, VersionVector.empty)), [])
      }))
  })

  describe("changes that don't fit", () => {
    const malformed = (error: Replica.DeltaError, message: string) => {
      if (error.reason._tag !== "Malformed") return assert.fail(`expected Malformed, got ${error.reason._tag}`)
      assert.include(error.reason.message, message)
    }

    it.effect("a change can only name what its author had seen, whatever this replica has", () =>
      Effect.gen(function*() {
        const Tagged = Schema.Struct({ title: ReplicatedDocument.Text, tags: ReplicatedDocument.List(Schema.String) })
        const makeTagged = (replica: string) =>
          ReplicatedDocument.make(Tagged, {
            initial: { title: "", tags: [] },
            replica: Replica.ReplicaId.make(replica)
          })
        const ada = yield* makeTagged("ada")
        yield* ReplicatedDocument.update(ada, () => ({ title: "a", tags: ["x"] }))
        const fromAda = yield* Replica.delta(ada, VersionVector.empty)
        // Changes made without seeing ada's, that name her character and her
        // item's position.
        const forged = (ops: InternalDocument.AnyChange["ops"]) =>
          InternalDelta.fromChanges([{ replica: "mallory", seq: 0, length: 1, parents: [], time: 0, ops }])
        const text = forged([{
          _tag: "Insert",
          path: ["title"],
          text: "b",
          parent: { replica: "ada", seq: 0 },
          side: "R",
          rightOrigin: null
        }])
        const position = forged([{
          _tag: "Place",
          path: ["tags", "x"],
          parent: { replica: "ada", seq: 2 },
          side: "R",
          rightOrigin: null
        }])
        for (
          const [delta, message] of [
            [text, "inserts next to a character it hasn't seen"],
            [position, "places an item next to a position it hasn't seen"]
          ] as const
        ) {
          const ben = yield* makeTagged("ben")
          yield* Replica.receive(ben, fromAda)
          malformed(yield* Effect.flip(Replica.receive(ben, delta)), message)
        }
      }))

    it.effect("changes the schema has another rule for are kept and passed on, but don't show", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        const text = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("text") })
        yield* ReplicatedText.insert(text, 0, "Hi")
        // A text and a document under one document id: each keeps the other's
        // changes, and shows only its own.
        yield* Replica.exchange(ada, text)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(ada), initial)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 1 }))
        yield* Replica.exchange(ada, text)
        assert.strictEqual(yield* ReplicatedText.get(text), "Hi")

        // A schema where `status` is a text, which this one has as a value.
        const Other = Schema.Struct({ status: ReplicatedDocument.Text })
        const other = yield* ReplicatedDocument.make(Other, {
          initial: { status: "" },
          replica: Replica.ReplicaId.make("other")
        })
        yield* ReplicatedText.insert(ReplicatedDocument.text(other, ["status"]), 0, "Hi")
        yield* Replica.exchange(ada, other)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))
        yield* Replica.exchange(ada, other)
        assert.strictEqual((yield* ReplicatedDocument.get(ada)).status, "review")
        assert.deepStrictEqual(yield* ReplicatedDocument.get(other), { status: "Hi" })
        const later = yield* ReplicatedDocument.make(Other, {
          initial: { status: "" },
          replica: Replica.ReplicaId.make("later")
        })
        yield* Replica.exchange(later, ada)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(later), { status: "Hi" })
      }))

    it.effect("a change must be later than what it was made on top of", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* TestClock.setTime(1000)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 1 }))
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 2 }))
        const changes = [...(yield* Replica.delta(ada, VersionVector.empty) as Effect.Effect<InternalDelta.DeltaImpl>)
          .changes.values()]
        const early = { ...changes[1]!, time: changes[0]!.time }
        const ben = yield* make("ben")
        malformed(
          yield* Effect.flip(Replica.receive(ben, InternalDelta.fromChanges([changes[0]!, early]))),
          "no later than a change it was made on top of"
        )
        assert.strictEqual((yield* ReplicatedDocument.get(ben)).votes, 1)
      }))

    it.effect("a bad change is dropped, and the changes ready behind it still apply", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedText.insert(ReplicatedDocument.text(ada, ["title"]), 0, "A")
        yield* ReplicatedText.insert(ReplicatedDocument.text(ben, ["title"]), 0, "B")
        // Dee types in front of both, having seen them.
        const dee = yield* make("dee")
        yield* Replica.exchange(dee, ada)
        yield* Replica.exchange(dee, ben)
        yield* ReplicatedText.insert(ReplicatedDocument.text(dee, ["title"]), 0, "D")

        const carol = yield* make("carol")
        yield* Replica.exchange(carol, ada)
        const title = ReplicatedDocument.text(carol, ["title"])
        const { edits } = yield* ReplicatedText.subscribe(title)
        // Ready in this order: Ben's, which meets Ada's, the bad one, and Dee's.
        const bad: InternalDocument.AnyChange = {
          replica: "cy",
          seq: 0,
          length: 2,
          parents: [],
          time: 0,
          ops: [{ _tag: "Set", path: ["status"], value: "review" }]
        }
        const fromDee = yield* Replica.delta(dee, yield* Replica.version(carol))
        malformed(
          yield* Effect.flip(Replica.receive(carol, Delta.combine(fromDee, InternalDelta.fromChanges([bad])))),
          "says it has 2 sequence numbers"
        )
        assert.deepStrictEqual(yield* ReplicatedDocument.get(carol), yield* ReplicatedDocument.get(dee))
        // Where Ben's met Ada's is where its anchors are, after Dee's.
        const edit = Option.getOrThrow(yield* Stream.runHead(edits))
        if (edit._tag !== "Remote") return assert.fail(`expected Remote, got ${edit._tag}`)
        assert.strictEqual(edit.overlaps.length, 1)
        for (const overlap of edit.overlaps) {
          if (overlap._tag !== "Text") return assert.fail(`expected Text, got ${overlap._tag}`)
          assert.deepStrictEqual(yield* ReplicatedText.locateRange(title, overlap.anchors), Option.some(overlap.range))
        }
      }).pipe(Effect.scoped))

    it.effect("every op takes at least one sequence number", () =>
      Effect.gen(function*() {
        for (const strategy of ["tree", "replay"] as const) {
          const ada = yield* make("ada", strategy)
          yield* ReplicatedText.insert(ReplicatedDocument.text(ada, ["title"]), 0, "hello")
          const fromAda = yield* Replica.delta(ada, VersionVector.empty)
          const status: InternalDocument.Op = { _tag: "Set", path: ["status"], value: "review" }
          // Each takes one sequence number in all, so the removal that takes
          // fewer than none gives the ops after it ones that came before.
          const forged: ReadonlyArray<InternalDocument.AnyChange["ops"]> = strategy === "tree"
            ? [
              [status, { _tag: "Insert", path: ["title"], text: "", parent: null, side: "L" }],
              [status, { _tag: "Remove", path: ["title"], targets: [] }],
              [{ _tag: "Remove", path: ["title"], targets: [{ replica: "ada", seq: 0, length: -1 }] }, status, status]
            ]
            : [
              [status, { _tag: "InsertAt", path: ["title"], index: 0, text: "" }],
              [status, { _tag: "RemoveAt", path: ["title"], index: 0, length: 0 }],
              [{ _tag: "RemoveAt", path: ["title"], index: 2, length: -1 }, status, status]
            ]
          for (const ops of forged) {
            const ben = yield* make("ben", strategy)
            yield* Replica.receive(ben, fromAda)
            const delta = InternalDelta.fromChanges([{
              replica: "mallory",
              seq: 0,
              length: 1,
              parents: [{ replica: "ada", seq: 4 }],
              time: 1000,
              ops
            }])
            malformed(yield* Effect.flip(Replica.receive(ben, delta)), "has an op that takes no sequence numbers")
            assert.deepStrictEqual(yield* ReplicatedDocument.get(ben), yield* ReplicatedDocument.get(ada))
          }
        }
      }))

    it.effect("a change that takes no sequence numbers is refused whenever it arrives", () => {
      const first: InternalDocument.AnyChange = {
        replica: "mallory",
        seq: 0,
        length: 1,
        parents: [],
        time: 0,
        ops: [{ _tag: "Set", path: ["status"], value: "review" }]
      }
      // After `first`, and taking none of the sequence numbers after it.
      const empty: InternalDocument.AnyChange = {
        replica: "mallory",
        seq: 1,
        length: 0,
        parents: [{ replica: "mallory", seq: 0 }],
        time: 1,
        ops: [
          { _tag: "Place", path: ["comments", "c1"], parent: { replica: "nobody", seq: 0 }, side: "L" },
          { _tag: "RemoveAt", path: ["title"], index: 0, length: -1 }
        ]
      }
      const other: InternalDocument.AnyChange = {
        replica: "ben",
        seq: 0,
        length: 1,
        parents: [],
        time: 0,
        ops: [{ _tag: "Increment", path: ["votes"], by: 1 }]
      }
      const saved = Replica.DocumentId.make("saved")
      // Receives the changes in order, saving as it goes, then opens what it
      // saved.
      const receiving = (changes: ReadonlyArray<InternalDocument.AnyChange>) =>
        Effect.gen(function*() {
          yield* Effect.scoped(Effect.gen(function*() {
            const ada = yield* make("ada", "replay")
            yield* HistoryStore.persist(ada, saved)
            for (const change of changes) {
              const received = yield* Effect.exit(Replica.receive(ada, InternalDelta.fromChanges([change])))
              assert.strictEqual(Exit.isFailure(received), change === empty, `receiving ${key(change)}`)
              // Saved before the next arrives.
              yield* Effect.yieldNow
            }
          }))
          const reopened = yield* Effect.scoped(Effect.gen(function*() {
            const ada = yield* make("ada-2", "replay")
            yield* HistoryStore.persist(ada, saved)
            return yield* ReplicatedDocument.get(ada)
          }))
          assert.strictEqual(reopened.status, "review")
        }).pipe(Effect.provide(HistoryStore.layerMemory))
      const key = (change: InternalDocument.AnyChange) => `${change.seq}@${change.replica}`
      return Effect.gen(function*() {
        yield* receiving([first, empty])
        // Waiting for `first` while `other` is saved, then reopened from
        // the texts saved with what it had.
        yield* receiving([empty, other, first])
      })
    })

    it.effect("a change that names a parent before any change is refused, and the rest still apply", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 1 }))
        const forged = InternalDelta.fromChanges([{
          replica: "mallory",
          seq: 0,
          length: 1,
          parents: [{ replica: "ada", seq: -1 }],
          time: 1,
          ops: [{ _tag: "Set", path: ["status"], value: "review" }]
        }])
        const ben = yield* make("ben")
        malformed(
          yield* Effect.flip(
            Replica.receive(ben, Delta.combine(yield* Replica.delta(ada, VersionVector.empty), forged))
          ),
          "has a parent before any change"
        )
        assert.deepStrictEqual(yield* ReplicatedDocument.get(ben), yield* ReplicatedDocument.get(ada))
      }))

    it.effect("says which change a bad text op is in", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        // The text op comes second, at sequence number 1 of change 0@other.
        const change: InternalDocument.AnyChange = {
          replica: "other",
          seq: 0,
          length: 2,
          parents: [],
          time: 0,
          ops: [
            { _tag: "Set", path: ["status"], value: "review" },
            { _tag: "Insert", path: ["title"], text: "a", parent: { replica: "nobody", seq: 0 }, side: "L" }
          ]
        }
        malformed(
          yield* Effect.flip(Replica.receive(ada, InternalDelta.fromChanges([change]))),
          "Change 0@other inserts next to a character it hasn't seen"
        )
      }))
  })

  describe("lists", () => {
    const Task = Schema.Struct({
      id: Schema.String,
      title: ReplicatedDocument.Text,
      status: Schema.Literals(["todo", "done"])
    })
    type Task = typeof Task.Type

    const Board = Schema.Struct({
      name: Schema.String,
      tasks: ReplicatedDocument.List(Task, { key: "id" }),
      tags: ReplicatedDocument.List(Schema.String)
    })
    type Board = typeof Board.Type
    type BoardDocument = ReplicatedDocument.ReplicatedDocument<Board>

    const empty: Board = { name: "", tasks: [], tags: [] }

    const task = (id: string, title = "", status: Task["status"] = "todo"): Task => ({ id, title, status })
    const ids = (board: Board) => board.tasks.map(({ id }) => id)
    const withTasks = (...tasks: Array<Task>) => (board: Board): Board => ({ ...board, tasks })
    const withTags = (...tags: Array<string>) => (board: Board): Board => ({ ...board, tags })

    // Moves task `id` to `index`, as dragging it would.
    const move = (id: string, index: number) => (board: Board): Board => {
      const moved = board.tasks.find((task) => task.id === id)!
      const rest = board.tasks.filter((task) => task !== moved)
      return { ...board, tasks: [...rest.slice(0, index), moved, ...rest.slice(index)] }
    }

    const makeBoard = (replica: string) =>
      ReplicatedDocument.make(Board, { initial: empty, replica: Replica.ReplicaId.make(replica) })

    // Two boards that start in sync, with `tasks`.
    const boards = (...tasks: Array<Task>) =>
      Effect.gen(function*() {
        const ada = yield* makeBoard("ada")
        const ben = yield* makeBoard("ben")
        yield* ReplicatedDocument.update(ada, withTasks(...tasks))
        yield* Replica.exchange(ada, ben)
        return { ada, ben }
      })

    // Both boards show the same value, which is returned.
    const same = Effect.fnUntraced(function*(a: BoardDocument, b: BoardDocument) {
      const value = yield* ReplicatedDocument.get(a)
      assert.deepStrictEqual(yield* ReplicatedDocument.get(b), value)
      return value
    })

    // Brings both up to date, returning the overlaps each one found.
    const merge = Effect.fnUntraced(function*(a: BoardDocument, b: BoardDocument) {
      const toA = yield* Replica.delta(b, yield* Replica.version(a))
      const toB = yield* Replica.delta(a, yield* Replica.version(b))
      return [yield* Replica.receive(a, toA), yield* Replica.receive(b, toB)] as const
    })

    const ada = Replica.ReplicaId.make("ada")
    const ben = Replica.ReplicaId.make("ben")

    it.effect("update inserts, removes, moves and edits items by key", () =>
      Effect.gen(function*() {
        const board = yield* makeBoard("ada")
        yield* ReplicatedDocument.update(board, withTasks(task("a"), task("b"), task("c")))
        assert.deepStrictEqual(ids(yield* ReplicatedDocument.get(board)), ["a", "b", "c"])

        yield* ReplicatedDocument.update(board, withTasks(task("a"), task("d"), task("c")))
        assert.deepStrictEqual(ids(yield* ReplicatedDocument.get(board)), ["a", "d", "c"])

        yield* ReplicatedDocument.update(board, withTasks(task("c"), task("a", "Ship it", "done"), task("d")))
        assert.deepStrictEqual(
          (yield* ReplicatedDocument.get(board)).tasks,
          [task("c"), task("a", "Ship it", "done"), task("d")]
        )

        // A replica that receives the changes shows the same.
        const other = yield* makeBoard("ben")
        yield* Replica.exchange(board, other)
        yield* same(board, other)
      }))

    it.effect("an item's text fields are edited through text, and merge character by character", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* boards(task("a", "Write docs"))
        const title = (board: BoardDocument) => ReplicatedDocument.text(board, ["tasks", "a", "title"])
        yield* ReplicatedText.insert(title(ada), 10, " today")
        yield* ReplicatedText.insert(title(ben), 0, "Please ")
        assert.deepStrictEqual((yield* ReplicatedDocument.get(ada)).tasks, [task("a", "Write docs today")])
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.deepStrictEqual((yield* same(ada, ben)).tasks, [task("a", "Please Write docs today")])
      }))

    it.effect("items inserted concurrently at one spot all show, without interleaving", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* boards(task("first"), task("last"))
        yield* ReplicatedDocument.update(ada, withTasks(task("first"), task("a1"), task("a2"), task("last")))
        yield* ReplicatedDocument.update(ben, withTasks(task("first"), task("b1"), task("b2"), task("last")))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.deepInclude(
          [["first", "a1", "a2", "b1", "b2", "last"], ["first", "b1", "b2", "a1", "a2", "last"]],
          ids(yield* same(ada, ben))
        )
      }))

    it.effect("an item moved by both ends up in one place, where it was moved last, and the merge says so", () =>
      Effect.gen(function*() {
        const replicas = yield* boards(task("a"), task("b"), task("c"), task("d"))
        yield* ReplicatedDocument.update(replicas.ben, move("c", 3))
        // Ada's move is later, though Ben's replica id is larger.
        yield* TestClock.adjust("1 second")
        yield* ReplicatedDocument.update(replicas.ada, move("c", 0))
        const [onAda, onBen] = yield* merge(replicas.ada, replicas.ben)
        const overlap: Replica.Overlap = { _tag: "Value", path: ["tasks", "c"], replicas: [ada, ben], kept: ada }
        assert.deepStrictEqual(onAda, [overlap])
        assert.deepStrictEqual(onBen, [overlap])
        assert.deepStrictEqual(ids(yield* same(replicas.ada, replicas.ben)), ["c", "a", "b", "d"])
      }))

    it.effect("a move and a concurrent edit inside the item both apply", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* boards(task("a"), task("b"), task("c", "Fix login"))
        yield* ReplicatedDocument.update(ada, move("c", 0))
        yield* ReplicatedText.insert(ReplicatedDocument.text(ben, ["tasks", "c", "title"]), 9, " on Safari")
        yield* ReplicatedDocument.update(ben, (board) => ({
          ...board,
          tasks: board.tasks.map((task) => task.id === "c" ? { ...task, status: "done" } : task)
        }))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.deepStrictEqual(
          (yield* same(ada, ben)).tasks,
          [task("c", "Fix login on Safari", "done"), task("a"), task("b")]
        )
      }))

    it.effect("a removal wins over a concurrent edit inside the item, and adding it back brings that edit", () =>
      Effect.gen(function*() {
        const replicas = yield* boards(task("a", "Draft"), task("b"))
        yield* ReplicatedDocument.update(replicas.ada, withTasks(task("b")))
        yield* ReplicatedText.insert(ReplicatedDocument.text(replicas.ben, ["tasks", "a", "title"]), 5, " two")
        const [onAda, onBen] = yield* merge(replicas.ada, replicas.ben)
        const overlap: Replica.Overlap = { _tag: "Removed", path: ["tasks", "a"], replicas: [ada, ben] }
        assert.deepStrictEqual(onAda, [overlap])
        assert.deepStrictEqual(onBen, [overlap])
        assert.deepStrictEqual(ids(yield* same(replicas.ada, replicas.ben)), ["b"])

        // Its content is kept, Ben's edit included.
        const title = ReplicatedDocument.text(replicas.ada, ["tasks", "a", "title"])
        assert.strictEqual(yield* ReplicatedText.get(title), "Draft two")
        yield* ReplicatedDocument.update(replicas.ada, withTasks(task("a", "Draft two"), task("b")))
        yield* Replica.exchange(replicas.ada, replicas.ben)
        assert.deepStrictEqual(
          (yield* same(replicas.ada, replicas.ben)).tasks,
          [task("a", "Draft two"), task("b")]
        )
      }))

    it.effect("a removal undone before the merge isn't reported", () =>
      Effect.gen(function*() {
        const replicas = yield* boards(task("a", "Draft"))
        yield* ReplicatedDocument.update(replicas.ada, withTasks())
        yield* ReplicatedText.insert(ReplicatedDocument.text(replicas.ben, ["tasks", "a", "title"]), 5, " two")
        yield* ReplicatedDocument.update(replicas.ada, withTasks(task("a", "Draft")))
        const [onAda, onBen] = yield* merge(replicas.ada, replicas.ben)
        assert.deepStrictEqual((yield* same(replicas.ada, replicas.ben)).tasks, [task("a", "Draft two")])
        assert.deepStrictEqual(onAda, [])
        assert.deepStrictEqual(onBen, [])
      }))

    it.effect("a removal wins over a concurrent move, and the merge says so", () =>
      Effect.gen(function*() {
        const replicas = yield* boards(task("a"), task("b"), task("c"))
        yield* ReplicatedDocument.update(replicas.ada, withTasks(task("b"), task("c")))
        yield* ReplicatedDocument.update(replicas.ben, move("a", 2))
        const removed = [{ _tag: "Removed" as const, path: ["tasks", "a"], replicas: [ada, ben] }]
        assert.deepStrictEqual(yield* merge(replicas.ada, replicas.ben), [removed, removed])
        assert.deepStrictEqual(ids(yield* same(replicas.ada, replicas.ben)), ["b", "c"])
      }))

    it.effect("a list of values is compared by value, so changing an item removes it and inserts another", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* boards()
        yield* ReplicatedDocument.update(ada, withTags("bug", "ui", "bug"))
        yield* ReplicatedDocument.update(ada, withTags("bug", "ui"))
        yield* ReplicatedDocument.update(ada, withTags("urgent", "bug", "ui"))
        assert.deepStrictEqual((yield* ReplicatedDocument.get(ada)).tags, ["urgent", "bug", "ui"])
        yield* Replica.exchange(ada, ben)

        // Ada's change is a new item, so Ben removing the old one leaves it.
        yield* ReplicatedDocument.update(ada, withTags("urgent", "bug", "UI"))
        yield* ReplicatedDocument.update(ben, withTags("urgent", "bug"))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.deepStrictEqual((yield* same(ada, ben)).tags, ["urgent", "bug", "UI"])
      }))

    it.effect("values inserted concurrently both show", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* boards()
        yield* ReplicatedDocument.update(ada, withTags("bug"))
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedDocument.update(ada, withTags("bug", "ui"))
        yield* ReplicatedDocument.update(ben, withTags("bug", "urgent"))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.deepInclude([["bug", "ui", "urgent"], ["bug", "urgent", "ui"]], (yield* same(ada, ben)).tags)
      }))

    it.effect("an update with two items of one key fails, writing nothing", () =>
      Effect.gen(function*() {
        const board = yield* makeBoard("ada")
        yield* ReplicatedDocument.update(board, withTasks(task("a")))
        const version = yield* Replica.version(board)
        const failed = yield* Effect.flip(ReplicatedDocument.update(board, (value) => ({
          ...value,
          name: "Launch",
          tasks: [task("a"), task("b"), task("b", "Again")]
        })))
        assert.strictEqual(failed._tag, "SchemaError")
        assert.deepStrictEqual(yield* Replica.version(board), version)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(board), { ...empty, tasks: [task("a")] })
      }))

    it.effect("an item whose id isn't its key is left out, so updates still work", () =>
      Effect.gen(function*() {
        const board = yield* makeBoard("ada")
        // Items a and b, which both say they're "same".
        const item = (key: string): InternalDocument.AnyChange["ops"] => [
          { _tag: "Add", path: ["tasks", key] },
          { _tag: "Set", path: ["tasks", key, "id"], value: "same" },
          { _tag: "Set", path: ["tasks", key, "status"], value: "todo" },
          { _tag: "Place", path: ["tasks", key], parent: null, side: "L" }
        ]
        const change = { replica: "other", seq: 0, length: 8, parents: [], time: 0, ops: [...item("a"), ...item("b")] }
        yield* Replica.receive(board, InternalDelta.fromChanges([change]))
        assert.deepStrictEqual(yield* ReplicatedDocument.get(board), empty)

        yield* ReplicatedDocument.update(board, withTasks(task("c")))
        assert.deepStrictEqual(ids(yield* ReplicatedDocument.get(board)), ["c"])
      }))

    it.effect("a change that places an item next to a position in another list is malformed", () =>
      Effect.gen(function*() {
        const board = yield* makeBoard("ada")
        // A tag, then a task placed after the tag's position.
        const change: InternalDocument.AnyChange = {
          replica: "other",
          seq: 0,
          length: 4,
          parents: [],
          time: 0,
          ops: [
            { _tag: "Set", path: ["tags", "x"], value: "bug" },
            { _tag: "Place", path: ["tags", "x"], parent: null, side: "L" },
            { _tag: "Add", path: ["tasks", "a"] },
            { _tag: "Place", path: ["tasks", "a"], parent: { replica: "other", seq: 1 }, side: "L" }
          ]
        }
        const error = yield* Effect.flip(Replica.receive(board, InternalDelta.fromChanges([change])))
        assert.strictEqual(error.reason._tag, "Malformed")
        assert.deepStrictEqual(yield* ReplicatedDocument.get(board), empty)
        assert.deepStrictEqual(yield* Replica.version(board), VersionVector.empty)
      }))

    it.effect("an older schema keeps items it can't read, and its updates leave them alone", () =>
      Effect.gen(function*() {
        const TaskV2 = Schema.Struct({
          ...Task.fields,
          status: Schema.Literals(["todo", "doing", "done"]),
          owner: Schema.optionalKey(Schema.String)
        })
        const BoardV2 = Schema.Struct({ ...Board.fields, tasks: ReplicatedDocument.List(TaskV2, { key: "id" }) })
        const old = yield* makeBoard("old")
        const updated = yield* ReplicatedDocument.make(BoardV2, {
          initial: empty,
          replica: Replica.ReplicaId.make("new")
        })
        yield* ReplicatedDocument.update(updated, (board) => ({
          ...board,
          tasks: [
            { ...task("a"), owner: "ada" }, // a field the older schema doesn't have
            { id: "b", title: "", status: "doing" }, // a status the older schema can't decode
            task("c"),
            task("e")
          ]
        }))
        yield* Replica.exchange(old, updated)
        assert.deepStrictEqual((yield* ReplicatedDocument.get(old)).tasks, [task("a"), task("c"), task("e")])

        // Removes c, moves e to the front, finishes a, and adds d.
        yield* ReplicatedDocument.update(old, withTasks(task("e"), task("a", "", "done"), task("d")))
        yield* Replica.exchange(old, updated)
        assert.deepStrictEqual(ids(yield* ReplicatedDocument.get(old)), ["e", "a", "d"])
        const { tasks } = yield* ReplicatedDocument.get(updated)
        assert.deepStrictEqual(tasks.map(({ id }) => id).filter((id) => id !== "b"), ["e", "a", "d"])
        assert.deepStrictEqual(tasks.find(({ id }) => id === "b"), { id: "b", title: "", status: "doing" })
        assert.deepStrictEqual(tasks.find(({ id }) => id === "a"), { ...task("a", "", "done"), owner: "ada" })
      }))
  })
})
