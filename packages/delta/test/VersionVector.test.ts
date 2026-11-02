import { assert, describe, it } from "@effect/vitest"
import { Schema } from "effect"
import { VersionVector } from "effect-delta"

const Vector = Schema.Record(
  Schema.Literals(["a", "b", "c"]),
  Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 5 }))
)

describe("VersionVector", () => {
  it.prop("combine is commutative, associative and idempotent", { x: Vector, y: Vector, z: Vector }, ({ x, y, z }) => {
    const { combine } = VersionVector
    assert.deepStrictEqual(combine(x, y), combine(y, x))
    assert.deepStrictEqual(combine(combine(x, y), z), combine(x, combine(y, z)))
    assert.deepStrictEqual(combine(x, x), x)
  })

  it.prop("compare agrees with combine", { x: Vector, y: Vector }, ({ x, y }) => {
    const combined = VersionVector.combine(x, y)
    const comparison = VersionVector.compare(x, y)
    const same = (a: VersionVector.VersionVector, b: VersionVector.VersionVector) =>
      VersionVector.compare(a, b) === "equal"
    assert.strictEqual(comparison === "before" || comparison === "equal", same(combined, y))
    assert.strictEqual(comparison === "after" || comparison === "equal", same(combined, x))
  })

  it("treats a missing replica as nothing seen", () => {
    assert.strictEqual(VersionVector.compare({ a: 0 }, {}), "equal")
    assert.strictEqual(VersionVector.compare({ a: 1 }, {}), "after")
  })
})
