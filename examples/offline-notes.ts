/**
 * Two laptops edit the same meeting notes offline, then reconnect. Every edit
 * survives, and the merge points at the sentence they both rewrote.
 *
 * Strategy: tree.
 *
 *   node examples/offline-notes.ts
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Console, Effect, Option } from "effect"
import { SecureRandom } from "effect/crypto"

const program = Effect.gen(function*() {
  const alice = yield* ReplicatedText.make()
  yield* ReplicatedText.insert(alice, 0, "Launch is Friday. Sessions last 24 hours.")

  // Bob's laptop gets a copy before they both go offline.
  const bob = yield* ReplicatedText.make()
  yield* Replica.exchange(alice, bob)

  // Offline, Alice adds a heading. Both change the session length.
  yield* ReplicatedText.insert(alice, 0, "# Decisions\n")
  yield* ReplicatedText.update(alice, (text) => text.replace("24 hours", "8 hours"))
  yield* ReplicatedText.update(bob, (text) => text.replace("24 hours", "12 hours"))

  // Back online.
  const overlaps = yield* Replica.exchange(alice, bob)

  yield* Console.log(yield* ReplicatedText.get(alice))
  // # Decisions
  // Launch is Friday. Sessions last 812 hours.    (or 128; the same on both laptops)

  for (const overlap of overlaps) {
    const range = yield* ReplicatedText.locateRange(alice, overlap.anchors)
    const text = yield* ReplicatedText.get(alice)
    yield* Console.log(
      "Both changed:",
      Option.match(range, {
        onNone: () => "(removed)",
        onSome: ({ start, end }) => JSON.stringify(text.slice(start, end))
      })
    )
  }
})

program.pipe(Effect.provide([NodeServices.layer, SecureRandom.layerWebCrypto]), NodeRuntime.runMain)
