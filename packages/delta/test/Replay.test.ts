import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, Random } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText, VersionVector } from "effect-delta"
import type { Change, Id } from "effect-delta/internal/change"
import * as InternalDelta from "effect-delta/internal/delta"
import type * as ReplayText from "effect-delta/internal/replayText"
import cases from "./fixtures/eg-walker-conformance.json" with { type: "json" }
import { checkTextOverlaps } from "./utils/forged.ts"
import {
  documentRuns,
  DocumentSession,
  letters,
  makeBoards,
  makeTexts,
  stepBoards,
  TextSession,
  viewOf
} from "./utils/sessions.ts"

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

// The tests in this file are heavy, so they run one at a time: side by side on
// one thread, each would take about as long as all of them together.

describe("conformance with Eg-walker", { concurrent: false }, () => {
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

describe("replay against tree", { concurrent: false }, () => {
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
    { session: TextSession },
    Effect.fnUntraced(function*({ session }) {
      const tree = yield* makeTexts("tree")
      const replay = yield* makeTexts("replay")
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
            const merged = tree[command.from]!
            const before = yield* Replica.version(merged)
            const [fromTree, fromReplay] = yield* both((replicas) =>
              Replica.exchange(replicas[command.from]!, replicas[command.to]!)
            )
            assert.deepStrictEqual(fromReplay, fromTree, "overlaps")
            // And they're the ones their rule gives.
            const delta = (yield* Replica.delta(merged, VersionVector.empty)) as InternalDelta.DeltaImpl
            yield* checkTextOverlaps(fromTree, {
              changes: [...delta.changes.values()],
              before,
              after: yield* Replica.version(merged),
              textAt: () => merged
            })
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

// Random document sessions, run on tree documents and replay documents side by
// side.

describe("replay against tree, for documents", { concurrent: false }, () => {
  it.effect.prop(
    "every step gives the same value and overlaps",
    { session: DocumentSession },
    Effect.fnUntraced(function*({ session }) {
      const tree = yield* makeBoards("tree")
      const replay = yield* makeBoards("replay")
      for (const command of session) {
        const merged = command._tag === "Sync" ? tree[command.from]! : undefined
        const before = merged === undefined ? undefined : yield* Replica.version(merged)
        const inTree = yield* stepBoards(tree, command)
        assert.deepStrictEqual(yield* stepBoards(replay, command), inTree, "overlaps")
        // And the text overlaps are the ones their rule gives.
        if (merged !== undefined) {
          const delta = (yield* Replica.delta(merged, VersionVector.empty)) as InternalDelta.DeltaImpl
          yield* checkTextOverlaps(inTree, {
            changes: [...delta.changes.values()],
            before: before!,
            after: yield* Replica.version(merged),
            textAt: (path) => ReplicatedDocument.text(merged, path as never)
          })
        }
        for (const i of [0, 1, 2]) {
          assert.deepStrictEqual(yield* viewOf(replay[i]!), yield* viewOf(tree[i]!), `replica ${i}`)
        }
      }
    }),
    { arbitrary: documentRuns }
  )
})
