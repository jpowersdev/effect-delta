/**
 * A writer keeps a long manuscript on their laptop, and a co-author edits a
 * copy on theirs. Reopening the manuscript reads the saved text instead of
 * rebuilding its history, and a week of the co-author's edits merges in
 * quickly.
 *
 * Strategy: replay.
 *
 *   pnpm add @jpowersdev/effect-delta @effect/platform-node effect
 *   node manuscript.ts
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { HistoryStore, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Console, Duration, Effect, FileSystem, Layer } from "effect"
import { SecureRandom } from "effect/crypto"
import { KeyValueStore } from "effect/persistence"

const manuscript = Replica.DocumentId.make("manuscript")

// Opens the manuscript on the writer's laptop: it loads what was saved, then
// saves every change until the scope closes.
const open = Effect.gen(function*() {
  const laptop = yield* ReplicatedText.make({ strategy: "replay" })
  yield* HistoryStore.persist(laptop, manuscript)
  return laptop
})

const milliseconds = (duration: Duration.Duration) => `${Math.round(Duration.toMillis(duration))} ms`

const append = (self: ReplicatedText.ReplicatedText, text: string) =>
  Effect.flatMap(ReplicatedText.get(self), (current) => ReplicatedText.insert(self, current.length, text))

const program = Effect.gen(function*() {
  const coauthor = yield* ReplicatedText.make({ strategy: "replay" })

  // Month one: the writer drafts forty chapters, a sentence at a time, and the
  // co-author takes a copy.
  yield* Effect.scoped(Effect.gen(function*() {
    const laptop = yield* open
    for (let chapter = 1; chapter <= 40; chapter++) {
      yield* append(laptop, `Chapter ${chapter}\n\n`)
      for (let sentence = 0; sentence < 50; sentence++) {
        yield* append(laptop, "The lighthouse keeper counted the ships that never came. ")
      }
      yield* append(laptop, "\n\n")
    }
    yield* Replica.exchange(laptop, coauthor)
  }))

  // A week apart. The co-author renames the keeper in every chapter, while the
  // writer adds an epilogue.
  for (let chapter = 1; chapter <= 40; chapter++) {
    yield* ReplicatedText.update(coauthor, (text) => {
      const at = text.indexOf(`Chapter ${chapter}\n`)
      return text.slice(0, at) + text.slice(at).replace("The lighthouse keeper", "Old Maren")
    })
  }
  yield* Effect.scoped(Effect.gen(function*() {
    const laptop = yield* open
    yield* append(laptop, "Epilogue\n\nThe light still turns.")
  }))

  // Back at the laptop.
  yield* Effect.scoped(Effect.gen(function*() {
    const [loading, laptop] = yield* Effect.timed(open)
    const text = yield* ReplicatedText.get(laptop)
    yield* Console.log(`Reopened ${text.length} characters in ${milliseconds(loading)}`)

    const [merging] = yield* Effect.timed(Replica.exchange(laptop, coauthor))
    yield* Console.log(`Merged the co-author's week in ${milliseconds(merging)}`)

    const merged = yield* ReplicatedText.get(laptop)
    yield* Console.log(merged.slice(merged.indexOf("Chapter 40"), merged.indexOf("Chapter 40") + 50))
    yield* Console.log(merged.slice(merged.indexOf("Epilogue")))
    yield* Console.log("Same on both laptops:", merged === (yield* ReplicatedText.get(coauthor)))
  }))
})

// The writer's laptop keeps manuscripts in files, here in a temporary folder.
const StoreLive = Layer.unwrap(Effect.gen(function*() {
  const directory = yield* FileSystem.FileSystem.use((fs) => fs.makeTempDirectoryScoped())
  return HistoryStore.layerKeyValueStore({ prefix: "manuscripts" }).pipe(
    Layer.provide(KeyValueStore.layerFileSystem(directory))
  )
}))

program.pipe(
  Effect.provide(StoreLive),
  Effect.provide([NodeServices.layer, SecureRandom.layerWebCrypto]),
  NodeRuntime.runMain
)
