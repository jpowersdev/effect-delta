import { assert, describe, it } from "@effect/vitest"
import { Delta, Replica, ReplicatedText, TextPatch } from "@jpowersdev/effect-delta"
import type { Change } from "@jpowersdev/effect-delta/internal/change"
import type * as InternalDelta from "@jpowersdev/effect-delta/internal/delta"
import * as History from "@jpowersdev/effect-delta/internal/history"
import * as ReplayText from "@jpowersdev/effect-delta/internal/replayText"
import * as InternalText from "@jpowersdev/effect-delta/internal/text"
import type * as TreeText from "@jpowersdev/effect-delta/internal/treeText"
import { Effect, Schema } from "effect"

// A large batch applies in one pass; a small one, character by character. They
// must not be told apart by anything but how long they take.

const Which = Schema.Literals([0, 1, 2])
const Fraction = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))

const Command = Schema.Union([
  Schema.TaggedStruct("Insert", { replica: Which, at: Fraction, text: Schema.Literals(["a", "bc", "def"]) }),
  Schema.TaggedStruct("Remove", {
    replica: Which,
    at: Fraction,
    length: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 40 }))
  }),
  Schema.TaggedStruct("Sync", { from: Which, to: Which })
])

const Session = Schema.Array(Command).check(Schema.isMaxLength(40))

const inOrder = (changes: Iterable<InternalText.AnyChange>): ReadonlyArray<InternalText.AnyChange> => {
  const history = History.make<InternalText.Op>()
  for (const change of changes) history.pending.set(`${change.replica}:${change.seq}`, change)
  return History.takeReady(history)
}

const concurrentHistory = Effect.fnUntraced(function*(session: typeof Session.Type) {
  const make = (replica: string) => ReplicatedText.make({ replica: Replica.ReplicaId.make(replica) })
  const replicas = [yield* make("r0"), yield* make("r1"), yield* make("r2")] as const
  yield* ReplicatedText.insert(replicas[0], 0, "The opening text every replica starts from.\n".repeat(10))
  yield* Replica.exchange(replicas[0], replicas[1])
  yield* Replica.exchange(replicas[0], replicas[2])
  for (const command of session) {
    if (command._tag === "Sync") {
      if (command.from !== command.to) yield* Replica.exchange(replicas[command.from], replicas[command.to])
      continue
    }
    const replica = replicas[command.replica]
    const { length } = yield* ReplicatedText.get(replica)
    if (command._tag === "Insert") {
      yield* ReplicatedText.insert(replica, Math.floor(command.at * (length + 1) / 1001), command.text)
    } else if (length > 0) {
      const index = Math.floor(command.at * length / 1001)
      yield* ReplicatedText.remove(replica, index, Math.min(command.length, length - index))
    }
  }
  yield* Replica.exchange(replicas[0], replicas[1])
  yield* Replica.exchange(replicas[0], replicas[2])
  yield* Replica.exchange(replicas[0], replicas[1])
  const delta = yield* Replica.delta(replicas[0], {})
  return {
    changes: inOrder((delta as InternalDelta.DeltaImpl).changes.values()),
    text: yield* ReplicatedText.get(replicas[0])
  }
})

describe("a large batch", () => {
  it.effect.prop(
    "gives the text, patch and anchors of the same changes one at a time",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const { changes, text } = yield* concurrentHistory(session)
      const opening = changes.slice(0, 1)
      const rest = changes.slice(1)

      const batched = InternalText.make("tree")
      batched.integrate(opening, { overlaps: false })
      const before = batched.value
      // Long enough to be applied in one pass, whatever the session did.
      const filler: InternalText.AnyChange = {
        replica: "filler",
        seq: 0,
        length: 300,
        parents: [],
        ops: [{ _tag: "Insert", text: "f".repeat(300), parent: null, side: "L" }]
      }
      const all = [...rest, filler]
      const inOnePass = batched.integrate(all, { overlaps: false })

      const single = InternalText.make("tree")
      single.integrate(opening, { overlaps: false })
      // Small batches walk the tree; only the filler is applied in one pass.
      let patch = TextPatch.empty
      for (const change of rest) {
        patch = TextPatch.compose(patch, single.integrate([change], { overlaps: false }).patch)
      }
      patch = TextPatch.compose(patch, single.integrate([filler], { overlaps: false }).patch)

      assert.isUndefined(inOnePass.problem)
      assert.strictEqual(batched.value, single.value)
      assert.strictEqual(TextPatch.apply(inOnePass.patch, before), batched.value)
      assert.deepStrictEqual(inOnePass.patch, patch)
      assert.strictEqual(batched.value, "f".repeat(300) + text)

      // Sizes are right too: every position finds the same character and anchor.
      for (let index = 0; index < batched.value.length; index += 17) {
        assert.deepStrictEqual(batched.anchor(index, "before"), single.anchor(index, "before"))
      }
      assert.deepStrictEqual(batched.locate(batched.anchor(5, "after")), single.locate(single.anchor(5, "after")))
    })
  )

  it("stops at a bad change and keeps the ones before it", () => {
    const good: InternalText.AnyChange = {
      replica: "a",
      seq: 0,
      length: 300,
      parents: [],
      ops: [{ _tag: "Insert", text: "g".repeat(300), parent: null, side: "L" }]
    }
    const bad: InternalText.AnyChange = {
      replica: "a",
      seq: 300,
      length: 1,
      parents: [{ replica: "a", seq: 299 }],
      ops: [{ _tag: "Remove", targets: [{ replica: "nobody", seq: 0, length: 1 }] }]
    }
    const text = InternalText.make("tree")
    const { patch, problem } = text.integrate([good, bad], { overlaps: false })
    assert.match(problem!, /removes a character it hasn't seen/)
    assert.strictEqual(text.value, "g".repeat(300))
    assert.deepStrictEqual(patch, TextPatch.insert(0, "g".repeat(300)))
    assert.strictEqual(text.anchor(299, "before")._tag, "Character")
  })

  // Walking per character takes several seconds here; one pass takes far less
  // than the test's timeout.
  it("costs one pass however many characters it removes", { timeout: 2000 }, () => {
    const body: Change<TreeText.Op> = {
      replica: "a",
      seq: 0,
      length: 47_000,
      parents: [],
      ops: [{ _tag: "Insert", text: "x".repeat(47_000), parent: null, side: "L" }]
    }
    const changes: Array<Change<TreeText.Op>> = [body]
    for (let i = 0; i < 1000; i++) {
      changes.push({
        replica: "a",
        seq: 47_000 + i * 40,
        length: 40,
        parents: [{ replica: "a", seq: 47_000 + i * 40 - 1 }],
        ops: [{ _tag: "Remove", targets: [{ replica: "a", seq: i * 45, length: 40 }] }]
      })
    }
    const text = InternalText.make("tree")
    const { problem } = text.integrate(changes, { overlaps: false })
    assert.isUndefined(problem)
    assert.strictEqual(text.value.length, 47_000 - 40_000)
  })
})

describe("replaying a run", () => {
  it("tells the ancestors once, not once per character", () => {
    const walker = ReplayText.makeWalker(0)
    const seen = walker.seen.get(walker.tree.root)!
    let writes = 0
    let size = seen.size
    Object.defineProperty(seen, "size", {
      get: () => size,
      set: (value: number) => {
        writes++
        size = value
      }
    })
    const change: Change<ReplayText.Op> = {
      replica: "a",
      seq: 0,
      length: 13,
      parents: [],
      ops: [{ _tag: "InsertAt", index: 0, text: "a run of text" }]
    }
    ReplayText.replay(walker, change, { replica: "a", seq: 0, length: 13, past: {} }, false)
    assert.strictEqual(writes, 1)
    assert.strictEqual(ReplayText.viewLength(walker), 13)
  })
})

describe("Delta.version", () => {
  it.effect("is how far a delta's changes reach, for each replica", () =>
    Effect.gen(function*() {
      const a = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("a") })
      const b = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("b") })
      assert.deepStrictEqual(Delta.version(Delta.empty), {})
      yield* ReplicatedText.insert(a, 0, "abc")
      const first = yield* Replica.version(a)
      yield* Replica.exchange(a, b)
      yield* ReplicatedText.insert(a, 3, "de")
      yield* ReplicatedText.remove(b, 0, 1)
      yield* Replica.exchange(a, b)
      assert.deepStrictEqual(Delta.version(yield* Replica.delta(a, {})), yield* Replica.version(a))
      assert.deepStrictEqual(Delta.version(yield* Replica.delta(a, first)), { a: 5, b: 1 })
    }))
})
