import { assert, describe, it } from "@effect/vitest"
import { Effect, Random } from "effect"
import { Replica, ReplicatedText } from "effect-delta"
import type { Change, Id } from "effect-delta/internal/change"
import * as InternalDelta from "effect-delta/internal/delta"
import * as History from "effect-delta/internal/history"
import * as TreeText from "effect-delta/internal/treeText"
import cases from "./fixtures/eg-walker-conformance.json" with { type: "json" }

// Concurrent editing histories with known results, from the Eg-walker
// reference implementation (see fixtures/README.md). Each transaction's edits
// are indexes into the text at its parents, so to turn them into tree ops we
// rebuild the tree at those parents and make the edits there, exactly as the
// agent's replica would have.

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

const toChanges = (transactions: ReadonlyArray<Transaction>): Array<Change<TreeText.Op>> => {
  const owner = (version: number) => transactions.findIndex((txn) => txn.span[0]! <= version && version < txn.span[1]!)
  const idOf = (version: number): Id => {
    const txn = transactions[owner(version)]!
    return { replica: txn.agent, seq: txn.seqStart + version - txn.span[0]! }
  }
  const pastOf = (index: number): Set<number> => {
    const past = new Set<number>()
    const visit = (version: number) => {
      const i = owner(version)
      if (past.has(i)) return
      past.add(i)
      for (const parent of transactions[i]!.parents) visit(parent)
    }
    for (const parent of transactions[index]!.parents) visit(parent)
    return past
  }

  const changes: Array<Change<TreeText.Op>> = []
  transactions.forEach((txn, index) => {
    const state = TreeText.make()
    const history = History.make<TreeText.Op>()
    for (const earlier of [...pastOf(index)].sort((a, b) => a - b)) apply(state, history, changes[earlier]!)
    const length = txn.span[1]! - txn.span[0]!
    const info: TreeText.Info = { replica: txn.agent, seq: txn.seqStart, length, past: { ...history.version } }
    const ops: Array<TreeText.Op> = []
    let seq = txn.seqStart
    for (const [position, removed, inserted] of txn.ops as ReadonlyArray<readonly [number, number, string]>) {
      if (removed > 0) {
        ops.push(TreeText.remove(state, position, removed, info)[0])
        seq += removed
      }
      if (inserted.length > 0) {
        ops.push(TreeText.insert(state, position, inserted, { replica: txn.agent, seq }, info)[0])
        seq += inserted.length
      }
    }
    // A transaction's versions come after its parents', so they serve as write times.
    changes.push({
      replica: txn.agent,
      seq: txn.seqStart,
      length,
      parents: txn.parents.map(idOf),
      time: txn.span[0]!,
      ops
    })
  })
  return changes
}

const apply = (state: TreeText.State, history: History.History<TreeText.Op>, change: Change<TreeText.Op>) => {
  const past = History.pastOf(history, change.parents)
  const info: TreeText.Info = { replica: change.replica, seq: change.seq, length: change.length, past }
  let seq = change.seq
  for (const op of change.ops) {
    TreeText.apply(state, op, { replica: change.replica, seq }, info)
    seq += TreeText.opLength(op)
  }
  History.record(history, change, past)
}

describe("conformance with Eg-walker", () => {
  it("tree text matches the reference for every case", () => {
    for (const [index, { endContent, txns }] of (cases as ReadonlyArray<Case>).entries()) {
      const state = TreeText.make()
      const history = History.make<TreeText.Op>()
      for (const change of toChanges(txns)) apply(state, history, change)
      assert.strictEqual(state.text, endContent, `case ${index}`)
    }
  })

  it.effect("a replica receiving the changes in any order gets the same text", () =>
    Effect.gen(function*() {
      for (const [index, { endContent, txns }] of (cases as ReadonlyArray<Case>).slice(0, 100).entries()) {
        const changes = yield* Random.shuffle(toChanges(txns))
        const replica = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("observer") })
        for (const change of changes) yield* Replica.receive(replica, InternalDelta.fromChanges([change]))
        assert.strictEqual(yield* ReplicatedText.get(replica), endContent, `case ${index}`)
      }
    }).pipe(Random.withSeed("conformance")))
})
