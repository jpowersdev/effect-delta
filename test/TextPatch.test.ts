import { assert, describe, it } from "@effect/vitest"
import { TextPatch } from "@jpowersdev/effect-delta"
import { Schema } from "effect"

// Maps any string onto a few letters, a space and an emoji, so patches collide
// often and surrogate pairs come up.
const pick = (s: string) => [...s].map((c) => "ab😀 "[c.charCodeAt(0) % 4]!).join("")

describe("TextPatch", () => {
  it("encodes to the Quill delta shape", () => {
    const patch = TextPatch.compose(TextPatch.insert(2, "xy"), TextPatch.remove(4, 1))
    const encoded = Schema.encodeSync(TextPatch.TextPatch)(patch)
    assert.deepStrictEqual(encoded, [{ retain: 2 }, { insert: "xy" }, { delete: 1 }])
    assert.deepStrictEqual(Schema.decodeUnknownSync(TextPatch.TextPatch)(encoded), patch)
  })

  it("ignores formatting attributes in Quill deltas", () => {
    const patch = Schema.decodeUnknownSync(TextPatch.TextPatch)([{ insert: "Hi", attributes: { bold: true } }])
    assert.strictEqual(TextPatch.apply(patch, "!"), "Hi!")
  })

  it("throws when a patch reaches past the end of the text", () => {
    assert.throws(() => TextPatch.apply(TextPatch.remove(2, 5), "abc"))
  })

  it("puts the replacement ahead of the text it replaces", () => {
    assert.deepStrictEqual(TextPatch.get("Sessions last 24 hours", "Sessions last 12 hours"), [
      { _tag: "Retain", length: 14 },
      { _tag: "Insert", text: "12" },
      { _tag: "Remove", length: 2 }
    ])
  })

  it("never splits a surrogate pair", () => {
    // 😀 and 😃 share their first code unit.
    assert.deepStrictEqual(TextPatch.get("a😀", "a😃"), [
      { _tag: "Retain", length: 1 },
      { _tag: "Insert", text: "😃" },
      { _tag: "Remove", length: 2 }
    ])
  })

  it.prop(
    "get gives a patch that turns before into after",
    { before: Schema.String, after: Schema.String },
    (input) => {
      const before = pick(input.before)
      const after = pick(input.after)
      assert.strictEqual(TextPatch.apply(TextPatch.get(before, after), before), after)
    }
  )

  it.prop(
    "compose applies one patch after the other",
    { a: Schema.String, b: Schema.String, c: Schema.String },
    (input) => {
      const [a, b, c] = [pick(input.a), pick(input.b), pick(input.c)]
      const composed = TextPatch.compose(TextPatch.get(a, b), TextPatch.get(b, c))
      assert.strictEqual(TextPatch.apply(composed, a), c)
    }
  )

  it.prop(
    "transforming concurrent patches past each other converges",
    { base: Schema.String, left: Schema.String, right: Schema.String },
    (input) => {
      const [base, left, right] = [pick(input.base), pick(input.left), pick(input.right)]
      const a = TextPatch.get(base, left)
      const b = TextPatch.get(base, right)
      const viaA = TextPatch.apply(TextPatch.transform(b, a, "after"), TextPatch.apply(a, base))
      const viaB = TextPatch.apply(TextPatch.transform(a, b, "before"), TextPatch.apply(b, base))
      assert.strictEqual(viaA, viaB)
    }
  )

  it.prop(
    "mapIndex moves an index past an insert according to its side",
    {
      at: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10 })),
      index: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 10 }))
    },
    ({ at, index }) => {
      const patch = TextPatch.insert(at, "xyz")
      assert.strictEqual(TextPatch.mapIndex(patch, index, "before"), at <= index ? index + 3 : index)
      assert.strictEqual(TextPatch.mapIndex(patch, index, "after"), at < index ? index + 3 : index)
    }
  )

  it("mapIndex moves an index inside removed text to where it was", () => {
    assert.strictEqual(TextPatch.mapIndex(TextPatch.remove(2, 4), 4, "before"), 2)
    assert.strictEqual(TextPatch.mapIndex(TextPatch.remove(2, 4), 8, "before"), 4)
  })

  it("is a Differ", () => {
    const patch = TextPatch.Differ.diff("cat", "cart")
    assert.strictEqual(TextPatch.Differ.patch("cat", patch), "cart")
    assert.strictEqual(TextPatch.Differ.patch("cat", TextPatch.Differ.empty), "cat")
  })
})
