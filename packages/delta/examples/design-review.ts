/**
 * A design doc with a title, a body, a status, and comments pinned to the
 * body. Two people work on it offline. Their edits merge, and the merge says
 * where they both changed the same thing.
 *
 * Strategy: tree.
 *
 *   node packages/delta/examples/design-review.ts
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Effect, Option, Schema } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"

const Comment = Schema.Struct({
  author: Schema.String,
  range: ReplicatedText.AnchorRange,
  text: ReplicatedDocument.Text
})

const DesignDoc = Schema.Struct({
  title: ReplicatedDocument.Text,
  body: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  comments: Schema.Record(Schema.String, Comment)
})

// What a design doc shows before anyone writes to it. The same on every device.
const initial: typeof DesignDoc.Type = { title: "", body: "", status: "draft", comments: {} }

const program = Effect.gen(function*() {
  const ada = yield* ReplicatedDocument.make(DesignDoc, { initial })
  yield* ReplicatedDocument.update(ada, (doc) => ({
    ...doc,
    title: "Sessions",
    body: "Sessions last 24 hours. Tokens refresh hourly."
  }))

  // Ben's laptop gets a copy before they both go offline.
  const ben = yield* ReplicatedDocument.make(DesignDoc, { initial })
  yield* Replica.exchange(ada, ben)

  // Ben comments on the second sentence and approves.
  const range = yield* ReplicatedText.anchorRange(ReplicatedDocument.text(ben, ["body"]), { start: 24, end: 46 })
  yield* ReplicatedDocument.update(ben, (doc) => ({
    ...doc,
    status: "approved",
    comments: { ...doc.comments, c1: { author: "ben", range, text: "Hourly seems often." } }
  }))

  // Ada changes that sentence, and sends the doc back for review.
  const body = ReplicatedDocument.text(ada, ["body"])
  yield* ReplicatedText.update(body, (text) => text.replace("hourly", "daily"))
  yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))

  // Back online.
  const overlaps = yield* Replica.exchange(ada, ben)

  const doc = yield* ReplicatedDocument.get(ada)
  const pinned = yield* ReplicatedText.locateRange(body, doc.comments.c1!.range)
  yield* Console.log(Option.getOrUndefined(Option.map(pinned, ({ end, start }) => doc.body.slice(start, end))))
  // Tokens refresh daily.    the comment still points at its sentence

  yield* Console.log(doc.status, overlaps)
  // review [{ _tag: "Value", path: ["status"], replicas: [...], kept: <Ada's id> }]
  //   (whichever status was written later; the same on both laptops)
})

program.pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain)
