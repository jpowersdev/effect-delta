import { assert, describe, it } from "@effect/vitest"
import * as Fugue from "@jpowersdev/effect-delta/internal/fugue"

const make = () => Fugue.make<null>(null)

describe("Fugue", () => {
  it("counts a run in every subtree it joins", () => {
    const tree = make()
    const first = Fugue.insert(tree, Fugue.place(tree, 0), { replica: "a", seq: 0 }, "abcd", null)
    const inner = Fugue.insert(tree, Fugue.place(tree, 2), { replica: "b", seq: 0 }, "xy", null)
    assert.strictEqual(tree.root.size, 6)
    assert.strictEqual(first.size, 6)
    assert.strictEqual(inner.size, 2)
    assert.strictEqual(Array.from({ length: 6 }, (_, i) => Fugue.nodeAt(tree, i).char).join(""), "abxycd")
    assert.strictEqual(Fugue.indexOf(inner), 2)
  })

  it("updates the ancestors of a run once, not once per character", () => {
    const tree = make()
    let writes = 0
    let size = tree.root.size
    Object.defineProperty(tree.root, "size", {
      get: () => size,
      set: (value: number) => {
        writes++
        size = value
      }
    })
    Fugue.insert(tree, Fugue.place(tree, 0), { replica: "a", seq: 0 }, "a run of text", null)
    assert.strictEqual(size, 13)
    assert.strictEqual(writes, 1)
  })

  it("leaves sizes alone until told to recount", () => {
    const tree = make()
    let writes = 0
    let size = tree.root.size
    Object.defineProperty(tree.root, "size", {
      get: () => size,
      set: (value: number) => {
        writes++
        size = value
      }
    })
    const uncounted = { counted: false }
    const first = Fugue.insert(
      tree,
      { parent: undefined, side: "R", rightOrigin: null },
      { replica: "a", seq: 0 },
      "abcd",
      null,
      uncounted
    )
    const next = Fugue.find(tree, { replica: "a", seq: 1 })!
    Fugue.remove(next, null, uncounted)
    Fugue.insert(
      tree,
      { parent: { replica: "a", seq: 3 }, side: "R", rightOrigin: null },
      { replica: "b", seq: 0 },
      "xy",
      null,
      uncounted
    )
    assert.strictEqual(writes, 0)

    Fugue.recount(tree)
    assert.strictEqual(size, 5)
    assert.strictEqual(first.size, 5)
    assert.strictEqual(next.size, 4)
    assert.strictEqual(Array.from({ length: 5 }, (_, i) => Fugue.nodeAt(tree, i).char).join(""), "acdxy")
  })
})
