import { assert, describe, it } from "@effect/vitest"
import { Delta, Replica, ReplicatedText, TextPatch, VersionVector } from "@jpowersdev/effect-delta"
import { Effect, Option, Schema, Stream } from "effect"

const make = (replica: string) => ReplicatedText.make({ replica: Replica.makeId(replica) })

describe("ReplicatedText", () => {
  describe("editing", () => {
    it.effect("inserts and removes text", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "Hello world")
        yield* ReplicatedText.insert(notes, 5, ",")
        yield* ReplicatedText.remove(notes, 6, 1)
        assert.strictEqual(yield* ReplicatedText.get(notes), "Hello,world")
      }))

    it.effect("rejects indexes outside the text", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "Hi")
        const error = yield* Effect.flip(ReplicatedText.insert(notes, 3, "!"))
        assert.strictEqual(error.reason._tag, "OutOfBounds")
        const removal = yield* Effect.flip(ReplicatedText.remove(notes, 1, 2))
        assert.strictEqual(removal.reason._tag, "OutOfBounds")
        assert.strictEqual(yield* ReplicatedText.get(notes), "Hi")
      }))

    it.effect("rejects edits that would split an emoji", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "a😀b")
        const insert = yield* Effect.flip(ReplicatedText.insert(notes, 2, "x"))
        assert.strictEqual(insert.reason._tag, "SplitsSurrogatePair")
        const remove = yield* Effect.flip(ReplicatedText.remove(notes, 1, 1))
        assert.strictEqual(remove.reason._tag, "SplitsSurrogatePair")
        yield* ReplicatedText.remove(notes, 1, 2)
        assert.strictEqual(yield* ReplicatedText.get(notes), "ab")
      }))

    it.effect("set makes one replacement", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        const { edits } = yield* ReplicatedText.subscribe(notes)
        yield* ReplicatedText.set(notes, "Sessions last 24 hours.")
        yield* ReplicatedText.update(notes, (text) => text.replace("24", "8"))
        const both = yield* Stream.runCollect(Stream.take(edits, 2))
        assert.deepStrictEqual(both[1], {
          _tag: "Local",
          patch: [{ _tag: "Retain", length: 14 }, { _tag: "Insert", text: "8" }, { _tag: "Remove", length: 2 }]
        })
        assert.strictEqual(yield* ReplicatedText.get(notes), "Sessions last 8 hours.")
      }))

    it.effect("modify returns its result, and leaves the text alone for an empty patch", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.set(notes, "Sessions last 24 hours.")
        const { edits } = yield* ReplicatedText.subscribe(notes)
        const found = yield* ReplicatedText.modify(notes, (text) => [
          text.indexOf("24"),
          TextPatch.get(text, text.replace("24", "8"))
        ])
        const missing = yield* ReplicatedText.modify(notes, (text) => [text.indexOf("24"), TextPatch.empty])
        assert.deepStrictEqual([found, missing], [14, -1])
        assert.strictEqual(yield* ReplicatedText.get(notes), "Sessions last 8 hours.")
        yield* ReplicatedText.insert(notes, 0, "> ")
        const after = yield* Stream.runCollect(Stream.take(edits, 2))
        assert.deepStrictEqual(after.map((edit) => edit.patch), [
          [{ _tag: "Retain", length: 14 }, { _tag: "Insert", text: "8" }, { _tag: "Remove", length: 2 }],
          [{ _tag: "Insert", text: "> " }]
        ])
      }))
  })

  describe("observing", () => {
    it.effect("subscribe misses nothing between the text and its edits", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "Hi")
        const { text, edits } = yield* ReplicatedText.subscribe(notes)
        yield* ReplicatedText.insert(notes, 2, "!")
        assert.strictEqual(text, "Hi")
        const [edit] = yield* Stream.runCollect(Stream.take(edits, 1))
        assert.deepStrictEqual(edit, {
          _tag: "Local",
          patch: [{ _tag: "Retain", length: 2 }, { _tag: "Insert", text: "!" }]
        })
      }))

    it.effect("remote edits arrive as patches that rebuild the text", () =>
      Effect.gen(function*() {
        const laptop = yield* make("laptop")
        const phone = yield* make("phone")
        yield* ReplicatedText.insert(laptop, 0, "Meeting notes")
        yield* Replica.exchange(laptop, phone)
        const { text, edits } = yield* ReplicatedText.subscribe(phone)
        yield* ReplicatedText.insert(laptop, 7, " (draft)")
        yield* ReplicatedText.remove(laptop, 0, 1)
        yield* Replica.exchange(laptop, phone)
        const [edit] = yield* Stream.runCollect(Stream.take(edits, 1))
        assert.strictEqual(edit!._tag, "Remote")
        const { TextPatch } = yield* Effect.promise(() => import("@jpowersdev/effect-delta"))
        assert.strictEqual(TextPatch.apply(edit!.patch, text), yield* ReplicatedText.get(laptop))
      }))

    it.effect("changes streams each new value", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "a")
        const values = yield* ReplicatedText.changes(notes).pipe(Stream.take(1), Stream.runCollect)
        assert.deepStrictEqual(values, ["a"])
      }))
  })

  describe("anchors", () => {
    it.effect("follow their character through edits on either side", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "world")
        const before = yield* ReplicatedText.anchor(notes, 0, "before")
        const after = yield* ReplicatedText.anchor(notes, 0, "after")
        yield* ReplicatedText.insert(notes, 0, "Hello, ")
        assert.deepStrictEqual(yield* ReplicatedText.locate(notes, before), Option.some(7))
        assert.deepStrictEqual(yield* ReplicatedText.locate(notes, after), Option.some(0))
      }))

    it.effect("stay where a removed character was", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "abcdef")
        const anchor = yield* ReplicatedText.anchor(notes, 3)
        yield* ReplicatedText.remove(notes, 2, 3)
        assert.deepStrictEqual(yield* ReplicatedText.locate(notes, anchor), Option.some(2))
      }))

    it.effect("follow text through merges, and are None where the text is unknown", () =>
      Effect.gen(function*() {
        const laptop = yield* make("laptop")
        const phone = yield* make("phone")
        yield* ReplicatedText.insert(laptop, 0, "comment here")
        const range = yield* ReplicatedText.anchorRange(laptop, { start: 8, end: 12 })
        assert.deepStrictEqual(yield* ReplicatedText.locateRange(phone, range), Option.none())
        yield* Replica.exchange(laptop, phone)
        yield* ReplicatedText.insert(phone, 0, "A ")
        yield* ReplicatedText.insert(phone, 10, "right ")
        yield* Replica.exchange(laptop, phone)
        assert.strictEqual(yield* ReplicatedText.get(laptop), "A comment right here")
        assert.deepStrictEqual(yield* ReplicatedText.locateRange(laptop, range), Option.some({ start: 16, end: 20 }))
      }))

    it.effect("a range is None once all of it is removed", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        yield* ReplicatedText.insert(notes, 0, "keep drop keep")
        const range = yield* ReplicatedText.anchorRange(notes, { start: 5, end: 9 })
        yield* ReplicatedText.remove(notes, 5, 4)
        assert.deepStrictEqual(yield* ReplicatedText.locateRange(notes, range), Option.none())
      }))
  })

  describe("authorship", () => {
    const owner = Replica.makeId("owner")
    const agent = Replica.makeId("agent")

    for (const strategy of ["tree", "replay"] as const) {
      it.effect(`says who typed each stretch, and drops removed text (${strategy})`, () =>
        Effect.gen(function*() {
          const mine = yield* ReplicatedText.make({ replica: owner, strategy })
          const theirs = yield* ReplicatedText.make({ replica: agent, strategy })
          yield* ReplicatedText.insert(mine, 0, "Ship it Friday.")
          yield* Replica.exchange(mine, theirs)
          yield* ReplicatedText.insert(theirs, 14, " after the review")
          yield* ReplicatedText.remove(theirs, 0, 5)
          yield* Replica.exchange(mine, theirs)
          assert.strictEqual(yield* ReplicatedText.get(mine), "it Friday after the review.")
          const expected = [
            { range: { start: 0, end: 9 }, replica: owner },
            { range: { start: 9, end: 26 }, replica: agent },
            { range: { start: 26, end: 27 }, replica: owner }
          ]
          assert.deepStrictEqual(yield* ReplicatedText.authorship(mine), expected)
          assert.deepStrictEqual(yield* ReplicatedText.authorship(theirs), expected)
        }))
    }

    it.effect("is empty for an empty text", () =>
      Effect.gen(function*() {
        const notes = yield* make("laptop")
        assert.deepStrictEqual(yield* ReplicatedText.authorship(notes), [])
      }))
  })

  describe("merging", () => {
    it.effect("reports where concurrent edits met", () =>
      Effect.gen(function*() {
        const alice = yield* make("alice")
        const bob = yield* make("bob")
        yield* ReplicatedText.insert(alice, 0, "Launch is Friday. Sessions last 24 hours.")
        yield* Replica.exchange(alice, bob)

        yield* ReplicatedText.insert(alice, 0, "# Decisions\n")
        yield* ReplicatedText.update(alice, (text) => text.replace("24 hours", "8 hours"))
        yield* ReplicatedText.update(bob, (text) => text.replace("24 hours", "12 hours"))

        const overlaps = yield* Replica.exchange(alice, bob)
        const text = yield* ReplicatedText.get(alice)
        assert.strictEqual(text, yield* ReplicatedText.get(bob))
        assert.strictEqual(text, "# Decisions\nLaunch is Friday. Sessions last 812 hours.")
        assert.strictEqual(overlaps.length, 1)
        const [overlap] = overlaps
        assert.strictEqual(text.slice(overlap!.range.start, overlap!.range.end), "812")
        assert.deepStrictEqual(overlap!.replicas.map(String), ["alice", "bob"])
        assert.deepStrictEqual(
          yield* ReplicatedText.locateRange(alice, overlap!.anchors),
          Option.some(overlap!.range)
        )
      }))

    it.effect("doesn't report edits that saw each other, or far-apart edits", () =>
      Effect.gen(function*() {
        const alice = yield* make("alice")
        const bob = yield* make("bob")
        yield* ReplicatedText.insert(alice, 0, "one two three")
        yield* Replica.exchange(alice, bob)
        yield* ReplicatedText.insert(alice, 0, "zero ")
        yield* ReplicatedText.insert(bob, 13, " four")
        assert.deepStrictEqual(yield* Replica.exchange(alice, bob), [])
        yield* ReplicatedText.insert(bob, 0, "minus one ")
        assert.deepStrictEqual(yield* Replica.exchange(alice, bob), [])
      }))

    it.effect("exchanging again changes nothing", () =>
      Effect.gen(function*() {
        const alice = yield* make("alice")
        const bob = yield* make("bob")
        yield* ReplicatedText.insert(alice, 0, "hello")
        yield* Replica.exchange(alice, bob)
        const version = yield* Replica.version(bob)
        assert.deepStrictEqual(yield* Replica.exchange(alice, bob), [])
        assert.deepStrictEqual(yield* Replica.version(bob), version)
        assert.isTrue(Delta.isEmpty(yield* Replica.delta(alice, version)))
      }))

    it.effect("catches a reused replica id once one copy has written more", () =>
      Effect.gen(function*() {
        const tab1 = yield* make("restored")
        const tab2 = yield* make("restored")
        yield* ReplicatedText.insert(tab1, 0, "one!")
        yield* ReplicatedText.insert(tab2, 0, "two")
        const error = yield* Effect.flip(Replica.exchange(tab1, tab2))
        assert.strictEqual(error.reason._tag, "ReplicaIdReused")
      }))

    it.effect("deltas survive a round trip through JSON", () =>
      Effect.gen(function*() {
        const laptop = yield* make("laptop")
        const phone = yield* make("phone")
        yield* ReplicatedText.insert(laptop, 0, "Hello 😀")
        yield* ReplicatedText.remove(laptop, 0, 1)
        const json = JSON.stringify(Schema.encodeSync(Delta.Delta)(yield* Replica.delta(laptop, VersionVector.empty)))
        yield* Replica.receive(phone, Schema.decodeUnknownSync(Delta.Delta)(JSON.parse(json)))
        assert.strictEqual(yield* ReplicatedText.get(phone), "ello 😀")
      }))
  })
})
