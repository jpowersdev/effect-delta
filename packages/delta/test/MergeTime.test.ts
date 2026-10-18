import { assert, describe, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { type Delta, Replica, ReplicatedDocument, TextPatch } from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import * as InternalDocument from "effect-delta/internal/document"

// Each test times two merges that take about as long when merging is linear,
// and fails if one takes far longer. They run one at a time, in a file of their
// own: each is heavy, and would hold up the tests running beside it, whose time
// would in turn be counted in its own.

describe("merge time", { concurrent: false }, () => {
  it.effect(
    "a removal over many edits in one change merges about as fast wherever in it they are",
    () =>
      Effect.gen(function*() {
        const Shelf = Schema.Struct({
          first: Schema.Record(Schema.String, Schema.Int),
          boxes: Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Int)),
          last: Schema.Record(Schema.String, Schema.Int)
        })
        const values = Object.fromEntries(Array.from({ length: 8000 }, (_, i) => [`k${i}`, i]))
        // Ben removes a box while Ada fills it, in one change that writes as
        // much before or after it.
        const merging = Effect.fnUntraced(function*(other: "first" | "last") {
          const open = (replica: string) =>
            ReplicatedDocument.make(Shelf, {
              initial: { first: {}, boxes: {}, last: {} },
              replica: Replica.ReplicaId.make(replica)
            })
          const ada = yield* open("ada")
          const ben = yield* open("ben")
          yield* ReplicatedDocument.update(ada, (shelf) => ({ ...shelf, boxes: { b: {} } }))
          yield* Replica.exchange(ada, ben)
          yield* ReplicatedDocument.update(ben, (shelf) => ({ ...shelf, boxes: {} }))
          yield* ReplicatedDocument.update(ada, (shelf) => ({ ...shelf, boxes: { b: values }, [other]: values }))
          const delta = yield* Replica.delta(ada, yield* Replica.version(ben))
          // The test clock stands still, so this is the time on the wall.
          const start = performance.now()
          const overlaps = yield* Replica.receive(ben, delta)
          const took = performance.now() - start
          assert.deepStrictEqual(overlaps.map(({ _tag }) => _tag), ["Removed"])
          return took
        })
        const editsLast = yield* merging("first")
        const editsFirst = yield* merging("last")
        assert.isBelow(editsLast / editsFirst, 8)
      }),
    60_000
  )

  it.effect(
    "a removal of every entry one change edits merges about as fast as a removal of one of them",
    () =>
      Effect.gen(function*() {
        const Shelf = Schema.Struct({
          boxes: Schema.Record(Schema.String, Schema.Struct({ count: ReplicatedDocument.Counter }))
        })
        const boxes = Array.from({ length: 8000 }, (_, i) => `b${i}`)
        const added: InternalDocument.AnyChange = {
          replica: "added",
          seq: 0,
          length: boxes.length,
          parents: [],
          time: 1,
          ops: boxes.map((box) => ({ _tag: "Add", path: ["boxes", box] }))
        }
        const last = { replica: "added", seq: boxes.length - 1 }
        // Ada's one change counts every box again, while Ben removes one
        // box, or every box.
        const counted: InternalDocument.AnyChange = {
          replica: "ada",
          seq: 0,
          length: boxes.length,
          parents: [last],
          time: 2,
          ops: boxes.map((box) => ({ _tag: "Increment", path: ["boxes", box, "count"], by: 1 }))
        }
        const removed = (removes: ReadonlyArray<string>): InternalDocument.AnyChange => ({
          replica: "ben",
          seq: 0,
          length: removes.length,
          parents: [last],
          time: 2,
          ops: removes.map((box) => ({ _tag: "Unset", path: ["boxes", box] }))
        })
        // How long a replica that had Ada's counts takes to take in Ben's
        // removal.
        const merging = Effect.fnUntraced(function*(removes: ReadonlyArray<string>) {
          const carol = yield* ReplicatedDocument.make(Shelf, {
            initial: { boxes: {} },
            replica: Replica.ReplicaId.make("carol")
          })
          yield* Replica.receive(carol, InternalDelta.fromChanges([added, counted]))
          // The test clock stands still, so this is the time on the wall.
          const start = performance.now()
          const overlaps = yield* Replica.receive(carol, InternalDelta.fromChanges([removed(removes)]))
          const took = performance.now() - start
          assert.strictEqual(overlaps.length, removes.length)
          return took
        })
        // The least of a few, so a pause elsewhere doesn't count.
        const least = (removes: ReadonlyArray<string>) =>
          Effect.map(Effect.all([merging(removes), merging(removes), merging(removes)]), (took) => Math.min(...took))
        const one = yield* least(["b0"])
        const every = yield* least(boxes)
        assert.isBelow(every / one, 8)
      }),
    60_000
  )

  it.effect(
    "a removal of entries another replica edited merges about as fast whether it edited them in one change or one each",
    () =>
      Effect.gen(function*() {
        const Shelf = Schema.Struct({
          boxes: Schema.Record(Schema.String, Schema.Struct({ count: ReplicatedDocument.Counter }))
        })
        const boxes = Array.from({ length: 2000 }, (_, i) => `b${i}`)
        const added: InternalDocument.AnyChange = {
          replica: "added",
          seq: 0,
          length: boxes.length,
          parents: [],
          time: 1,
          ops: boxes.map((box) => ({ _tag: "Add", path: ["boxes", box] }))
        }
        const last = { replica: "added", seq: boxes.length - 1 }
        const count = (box: string): InternalDocument.Op => ({
          _tag: "Increment",
          path: ["boxes", box, "count"],
          by: 1
        })
        // Ada counts every box again, in one change or one change each,
        // while Ben removes every box.
        const counted = {
          once: [{ replica: "ada", seq: 0, length: boxes.length, parents: [last], time: 2, ops: boxes.map(count) }],
          each: boxes.map((box, i): InternalDocument.AnyChange => ({
            replica: "ada",
            seq: i,
            length: 1,
            parents: [i === 0 ? last : { replica: "ada", seq: i - 1 }],
            time: 2 + i,
            ops: [count(box)]
          }))
        }
        const removed: InternalDocument.AnyChange = {
          replica: "ben",
          seq: 0,
          length: boxes.length,
          parents: [last],
          time: 2,
          ops: boxes.map((box) => ({ _tag: "Unset", path: ["boxes", box] }))
        }
        // How long a replica that had Ada's counts takes to take in Ben's
        // removal.
        const merging = Effect.fnUntraced(function*(split: "once" | "each") {
          const carol = yield* ReplicatedDocument.make(Shelf, {
            initial: { boxes: {} },
            replica: Replica.ReplicaId.make("carol")
          })
          yield* Replica.receive(carol, InternalDelta.fromChanges([added, ...counted[split]]))
          // The test clock stands still, so this is the time on the wall.
          const start = performance.now()
          const overlaps = yield* Replica.receive(carol, InternalDelta.fromChanges([removed]))
          const took = performance.now() - start
          assert.strictEqual(overlaps.length, boxes.length)
          return took
        })
        // The least of a few, so a pause elsewhere doesn't count.
        const least = (split: "once" | "each") =>
          Effect.map(Effect.all([merging(split), merging(split), merging(split)]), (took) => Math.min(...took))
        const once = yield* least("once")
        const each = yield* least("each")
        assert.isBelow(each / once, 8)
      }),
    60_000
  )

  it.effect(
    "an older removal of two entries merges about as fast as of one, after many edits that saw the removal that wins",
    () =>
      Effect.gen(function*() {
        const Shelf = Schema.Struct({
          boxes: Schema.Record(Schema.String, Schema.Struct({ count: ReplicatedDocument.Counter }))
        })
        const added: InternalDocument.AnyChange = {
          replica: "added",
          seq: 0,
          length: 2,
          parents: [],
          time: 1,
          ops: [{ _tag: "Add", path: ["boxes", "p"] }, { _tag: "Add", path: ["boxes", "q"] }]
        }
        const removes = (
          replica: string,
          time: number,
          boxes: ReadonlyArray<string>
        ): InternalDocument.AnyChange => ({
          replica,
          seq: 0,
          length: boxes.length,
          parents: [{ replica: "added", seq: 1 }],
          time,
          ops: boxes.map((box) => ({ _tag: "Unset", path: ["boxes", box] }))
        })
        // Ada saw a later removal of both boxes, then went on counting deep
        // inside them.
        const later = removes("later", 1_000_000, ["p", "q"])
        const deep = Array.from({ length: 20 }, (_, i) => `d${i}`)
        const counted = Array.from({ length: 1000 }, (_, i): InternalDocument.AnyChange => ({
          replica: "ada",
          seq: i * 8,
          length: 8,
          parents: [i === 0 ? { replica: "later", seq: 1 } : { replica: "ada", seq: i * 8 - 1 }],
          time: 1_000_001 + i,
          ops: ["p", "q"].flatMap((box) =>
            Array.from({ length: 4 }, (_, j): InternalDocument.Op => ({
              _tag: "Increment",
              path: ["boxes", box, ...deep, `c${j}`],
              by: 1
            }))
          )
        }))
        // How long a replica with all that takes in an older removal.
        const merging = Effect.fnUntraced(function*(boxes: ReadonlyArray<string>) {
          const carol = yield* ReplicatedDocument.make(Shelf, {
            initial: { boxes: {} },
            replica: Replica.ReplicaId.make("carol")
          })
          yield* Replica.receive(carol, InternalDelta.fromChanges([added, later, ...counted]))
          // The test clock stands still, so this is the time on the wall.
          const start = performance.now()
          const overlaps = yield* Replica.receive(carol, InternalDelta.fromChanges([removes("ben", 2, boxes)]))
          const took = performance.now() - start
          assert.deepStrictEqual(overlaps, [])
          return took
        })
        // The least of a few, so a pause elsewhere doesn't count.
        const least = (boxes: ReadonlyArray<string>) =>
          Effect.map(Effect.all([merging(boxes), merging(boxes), merging(boxes)]), (took) => Math.min(...took))
        const one = yield* least(["p"])
        const two = yield* least(["p", "q"])
        assert.isBelow(two / one, 8)
      }),
    60_000
  )

  it.effect(
    "an older removal made many times over merges about as fast as made a few times, after many edits that saw a later one",
    () =>
      Effect.gen(function*() {
        const Shelf = Schema.Struct({
          boxes: Schema.Record(Schema.String, Schema.Struct({ count: ReplicatedDocument.Counter }))
        })
        const added: InternalDocument.AnyChange = {
          replica: "added",
          seq: 0,
          length: 1,
          parents: [],
          time: 1,
          ops: [{ _tag: "Add", path: ["boxes", "p"] }]
        }
        // Ada saw a later removal of the box, then went on counting in it.
        const later: InternalDocument.AnyChange = {
          replica: "later",
          seq: 0,
          length: 1,
          parents: [{ replica: "added", seq: 0 }],
          time: 1_000_000,
          ops: [{ _tag: "Unset", path: ["boxes", "p"] }]
        }
        const counted = Array.from({ length: 2000 }, (_, i): InternalDocument.AnyChange => ({
          replica: "ada",
          seq: i,
          length: 1,
          parents: [i === 0 ? { replica: "later", seq: 0 } : { replica: "ada", seq: i - 1 }],
          time: 1_000_001 + i,
          ops: [{ _tag: "Increment", path: ["boxes", "p", "count"], by: 1 }]
        }))
        // Ben, offline, removes the box and adds it back, again and again.
        const toggles = (times: number) =>
          Array.from({ length: 2 * times }, (_, i): InternalDocument.AnyChange => ({
            replica: "ben",
            seq: i,
            length: 1,
            parents: [i === 0 ? { replica: "added", seq: 0 } : { replica: "ben", seq: i - 1 }],
            time: 2 + i,
            ops: [{ _tag: i % 2 === 0 ? "Unset" : "Add", path: ["boxes", "p"] }]
          }))
        // How long a replica with Ada's counts takes in Ben's toggles.
        const merging = Effect.fnUntraced(function*(times: number) {
          const carol = yield* ReplicatedDocument.make(Shelf, {
            initial: { boxes: {} },
            replica: Replica.ReplicaId.make("carol")
          })
          yield* Replica.receive(carol, InternalDelta.fromChanges([added, later, ...counted]))
          // The test clock stands still, so this is the time on the wall.
          const start = performance.now()
          const overlaps = yield* Replica.receive(carol, InternalDelta.fromChanges(toggles(times)))
          const took = performance.now() - start
          // They meet the later removal, but no count was lost to them.
          assert.deepStrictEqual(overlaps.map(({ _tag }) => _tag), ["Value"])
          return took
        })
        // The least of a few, so a pause elsewhere doesn't count.
        const least = (times: number) =>
          Effect.map(Effect.all([merging(times), merging(times), merging(times)]), (took) => Math.min(...took))
        const few = yield* least(25)
        const many = yield* least(400)
        assert.isBelow(many / few, 8)
      }),
    60_000
  )

  it.effect(
    "a change that starts a text merges about as fast as one that edits a text, however long the history",
    () =>
      Effect.gen(function*() {
        const Notes = Schema.Struct({ notes: Schema.Record(Schema.String, ReplicatedDocument.Text) })
        // Changes that each type a character, into a text of their own or all
        // into one.
        const typed = (strategy: Replica.Strategy, textOf: (i: number) => string) => {
          const document = InternalDocument.make(strategy)
          return InternalDelta.fromChanges(Array.from({ length: 6000 }, (_, i) => {
            const path = ["notes", textOf(i)]
            const patch = TextPatch.insert(InternalDocument.textAt(document, path).length, "x")
            return InternalDocument.edit(document, { replica: "ada", seq: i, time: i }, [{
              _tag: "Text",
              path,
              patch
            }])!
              .change
          }))
        }
        const merging = Effect.fnUntraced(function*(strategy: Replica.Strategy, delta: Delta.Delta) {
          const doc = yield* ReplicatedDocument.make(Notes, {
            initial: { notes: {} },
            strategy,
            replica: Replica.ReplicaId.make("ben")
          })
          // The test clock stands still, so this is the time on the wall.
          const start = performance.now()
          yield* Replica.receive(doc, delta)
          return performance.now() - start
        })
        for (const strategy of ["tree", "replay"] as const) {
          const own = yield* merging(strategy, typed(strategy, (i) => `n${i}`))
          const shared = yield* merging(strategy, typed(strategy, () => "n"))
          assert.isBelow(own / shared, 8, strategy)
        }
      }),
    60_000
  )
})
