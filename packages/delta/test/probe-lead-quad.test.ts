import { it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Replica, ReplicatedDocument } from "effect-delta"

const Shelf = Schema.Struct({ boxes: Schema.Record(Schema.String, Schema.Struct({ v: Schema.Int })) })

const run = Effect.fnUntraced(function*(n: number, removeAll: boolean) {
  const open = (r: string) =>
    ReplicatedDocument.make(Shelf, { initial: { boxes: {} }, replica: Replica.ReplicaId.make(r) })
  const ada = yield* open("ada")
  const ben = yield* open("ben")
  const boxes = (v: number) => Object.fromEntries(Array.from({ length: n }, (_, i) => [`k${i}`, { v }]))
  yield* ReplicatedDocument.update(ada, () => ({ boxes: boxes(0) }))
  yield* Replica.exchange(ada, ben)
  yield* ReplicatedDocument.update(ben, (shelf) => {
    if (removeAll) return { boxes: {} }
    const { k0: _, ...rest } = shelf.boxes
    return { boxes: rest }
  })
  yield* ReplicatedDocument.update(ada, () => ({ boxes: boxes(1) }))
  const delta = yield* Replica.delta(ada, yield* Replica.version(ben))
  const start = performance.now()
  const overlaps = yield* Replica.receive(ben, delta)
  return [performance.now() - start, overlaps.length] as const
})

it.effect("quad", () =>
  Effect.gen(function*() {
    for (const n of [2000, 4000, 8000]) {
      const one = yield* run(n, false)
      const all = yield* run(n, true)
      console.log(
        n,
        "one removed",
        one.map(Math.round),
        "all removed",
        all.map(Math.round),
        "ratio",
        (all[0] / one[0]).toFixed(1)
      )
    }
  }), 120_000)
