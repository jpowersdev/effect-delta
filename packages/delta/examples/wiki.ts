/**
 * A long wiki page, kept on a laptop and edited by a colleague on theirs.
 * Reopening the page reads its saved texts instead of rebuilding their
 * history, and a week of the colleague's edits to the body, the tags and the
 * view count merges in quickly.
 *
 * Strategy: replay.
 *
 *   node packages/delta/examples/wiki.ts
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Duration, Effect, FileSystem, Layer, Schema } from "effect"
import { HistoryStore, Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"
import { KeyValueStore } from "effect/persistence"

const Page = Schema.Struct({
  title: ReplicatedDocument.Text,
  body: ReplicatedDocument.Text,
  tags: ReplicatedDocument.List(Schema.String),
  views: ReplicatedDocument.Counter
})
type Page = typeof Page.Type

// What a page shows before anyone writes to it. The same on every device.
const initial: Page = { title: "", body: "", tags: [], views: 0 }

const onboarding = Replica.DocumentId.make("onboarding")

// Opens the page on the laptop: it loads what was saved, then saves every
// change until the scope closes.
const open = Effect.gen(function*() {
  const laptop = yield* ReplicatedDocument.make(Page, { initial, strategy: "replay" })
  yield* HistoryStore.persist(laptop, onboarding)
  return laptop
})

// Nothing else edits the text meanwhile, so the end it reads is still the end
// when it inserts there.
const append = (text: ReplicatedText.Editable, more: string) =>
  Effect.flatMap(ReplicatedText.get(text), (current) => ReplicatedText.insert(text, current.length, more))

const view = (page: ReplicatedDocument.ReplicatedDocument<Page>) =>
  ReplicatedDocument.update(page, (current) => ({ ...current, views: current.views + 1 }))

const milliseconds = (duration: Duration.Duration) => `${Math.round(Duration.toMillis(duration))} ms`

const program = Effect.gen(function*() {
  const colleague = yield* ReplicatedDocument.make(Page, { initial, strategy: "replay" })

  // Month one: the page is written a sentence at a time, and the colleague
  // takes a copy.
  yield* Effect.scoped(Effect.gen(function*() {
    const laptop = yield* open
    yield* ReplicatedText.insert(ReplicatedDocument.text(laptop, ["title"]), 0, "Onboarding")
    const body = ReplicatedDocument.text(laptop, ["body"])
    for (let section = 1; section <= 40; section++) {
      yield* append(body, `## Week ${section}\n\n`)
      for (let sentence = 0; sentence < 40; sentence++) {
        yield* append(body, "Pair with your buddy on the staging cluster. ")
      }
      yield* append(body, "\n\n")
    }
    yield* ReplicatedDocument.update(laptop, (page) => ({ ...page, tags: ["handbook"] }))
    yield* view(laptop)
    yield* Replica.exchange(laptop, colleague)
  }))

  // A week apart. The colleague renames the cluster in every section, tags
  // the page and reads it twice; on the laptop, someone adds a section and
  // reads it.
  const body = ReplicatedDocument.text(colleague, ["body"])
  for (let section = 1; section <= 40; section++) {
    yield* ReplicatedText.update(body, (text) => {
      const at = text.indexOf(`## Week ${section}\n`)
      return text.slice(0, at) + text.slice(at).replace("staging cluster", "sandbox")
    })
  }
  yield* ReplicatedDocument.update(colleague, (page) => ({ ...page, tags: [...page.tags, "engineering"] }))
  yield* view(colleague)
  yield* view(colleague)
  yield* Effect.scoped(Effect.gen(function*() {
    const laptop = yield* open
    yield* append(ReplicatedDocument.text(laptop, ["body"]), "## FAQ\n\nAsk in #help.")
    yield* view(laptop)
  }))

  // Back at the laptop.
  yield* Effect.scoped(Effect.gen(function*() {
    const [loading, laptop] = yield* Effect.timed(open)
    const { body } = yield* ReplicatedDocument.get(laptop)
    yield* Console.log(`Reopened ${body.length} characters in ${milliseconds(loading)}`)

    const [merging] = yield* Effect.timed(Replica.exchange(laptop, colleague))
    yield* Console.log(`Merged the colleague's week in ${milliseconds(merging)}`)

    const page = yield* ReplicatedDocument.get(laptop)
    yield* Console.log(page.body.slice(page.body.indexOf("## Week 40"), page.body.indexOf("## Week 40") + 48))
    yield* Console.log(page.body.slice(page.body.indexOf("## FAQ")))
    yield* Console.log(page.tags, page.views)
    // [ 'handbook', 'engineering' ] 4
    yield* Console.log(
      "Same on both laptops:",
      Schema.toEquivalence(Page)(page, yield* ReplicatedDocument.get(colleague))
    )
  }))
})

// The laptop keeps pages in files, here in a temporary folder.
const StoreLive = Layer.unwrap(Effect.gen(function*() {
  const directory = yield* FileSystem.FileSystem.use((fs) => fs.makeTempDirectoryScoped())
  return HistoryStore.layerKeyValueStore({ prefix: "wiki" }).pipe(
    Layer.provide(KeyValueStore.layerFileSystem(directory))
  )
}))

program.pipe(
  Effect.provide(StoreLive),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain
)
