import { assert, describe, it } from "@effect/vitest"
import { Effect, Exit, Option, Schema, Stream } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText, VersionVector } from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import { TestClock } from "effect/testing"

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

    it.effect("refuses an initial value that writes texts, entries or optional fields", () =>
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

    it.effect("writes in the same millisecond go to the larger replica id", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "approved" }))
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))
        yield* Replica.exchange(ada, ben)
        assert.strictEqual((yield* converged(ada, ben)).status, "approved")
      }))

    it.effect("a write beats the writes it was made on top of, even with a clock behind", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* TestClock.setTime(60_000)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "approved" }))
        yield* Replica.exchange(ada, ben)
        yield* TestClock.setTime(0)
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "review" }))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
        assert.strictEqual((yield* converged(ada, ben)).status, "review")
      }))

    it.effect("concurrent writes of the same value aren't an overlap", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "review" }))
        assert.deepStrictEqual(yield* Replica.exchange(ada, ben), [])
      }))

    it.effect("an update that changes nothing writes nothing", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc }))
        assert.deepStrictEqual(yield* Replica.version(ada), VersionVector.empty)
      }))

    it.effect("an update is one change, and fails without writing if the value doesn't fit", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review", due: "friday", votes: 2 }))
        const delta = yield* Replica.delta(ada, VersionVector.empty)
        assert.strictEqual((delta as InternalDelta.DeltaImpl).changes.size, 1)

        const failed = yield* Effect.flip(
          ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "done" as never, votes: 3 }))
        )
        assert.strictEqual(failed._tag, "SchemaError")
        const counted = yield* Effect.flip(ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 2.5 })))
        assert.strictEqual(counted._tag, "SchemaError")
        assert.deepStrictEqual(yield* Replica.version(ada), { ada: 3 })
      }))
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

    it.effect("entries whose content is a value are written whole", () =>
      Effect.gen(function*() {
        const { ada, ben } = yield* pair
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, labels: { urgent: 1, later: 2 } }))
        yield* Replica.exchange(ada, ben)
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, labels: { urgent: 3 } }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, labels: { ...doc.labels, soon: 4 } }))
        yield* Replica.exchange(ada, ben)
        assert.deepStrictEqual((yield* converged(ada, ben)).labels, { urgent: 3, soon: 4 })
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

    it("a path that isn't a text is a defect", () => {
      const doc = Effect.runSync(make("ada"))
      assert.throws(() => ReplicatedDocument.text(doc, ["status"]), "isn't a text field")
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
  })

  describe("changes that don't fit", () => {
    const malformed = (error: Replica.DeltaError, message: string) => {
      if (error.reason._tag !== "Malformed") return assert.fail(`expected Malformed, got ${error.reason._tag}`)
      assert.include(error.reason.message, message)
    }

    it.effect("a document refuses changes its schema has another rule for, and a text's", () =>
      Effect.gen(function*() {
        const ada = yield* make("ada")
        const text = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("text") })
        yield* ReplicatedText.insert(text, 0, "Hi")
        malformed(yield* Effect.flip(Replica.exchange(ada, text)), "where the schema has a struct")
        // The document is no different.
        assert.deepStrictEqual(yield* ReplicatedDocument.get(ada), initial)

        const other = yield* ReplicatedDocument.make(Schema.Struct({ status: ReplicatedDocument.Text }), {
          initial: { status: "" },
          replica: Replica.ReplicaId.make("other")
        })
        yield* ReplicatedText.insert(ReplicatedDocument.text(other, ["status"]), 0, "Hi")
        malformed(
          yield* Effect.flip(Replica.receive(ada, yield* Replica.delta(other, VersionVector.empty))),
          "where the schema has a value"
        )
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
  })
})
