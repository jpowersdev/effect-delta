import { assert, describe, it } from "@effect/vitest"
import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import type { Change, Id } from "@jpowersdev/effect-delta/internal/change"
import * as InternalDelta from "@jpowersdev/effect-delta/internal/delta"
import type * as ReplayText from "@jpowersdev/effect-delta/internal/replayText"
import { Effect, Option, Random, Schema } from "effect"
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
      ops
    }
  })
}

const makeReplay = (replica: string) => ReplicatedText.make({ strategy: "replay", replica: Replica.makeId(replica) })

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
  Effect.forEach(["r0", "r1", "r2"], (id) => ReplicatedText.make({ strategy, replica: Replica.makeId(id) }))

describe("replay against tree", () => {
  it.effect("an overlap where both removed the same words", () =>
    Effect.gen(function*() {
      const overlaps = yield* Effect.forEach(["tree", "replay"] as const, (strategy) =>
        Effect.gen(function*() {
          const make = (replica: string) => ReplicatedText.make({ strategy, replica: Replica.makeId(replica) })
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
    { arbitrary: { runs: 300 } }
  )
})
