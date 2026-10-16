/**
 * A design doc that anyone on the team can edit, but only its owner can
 * approve. Ben's approval is rejected and rolled back on his screen, while
 * his comment goes through; then Ada, the owner, approves it.
 *
 * Strategy: tree, with an authority. Here the authority runs in the same
 * process; spec-review.ts shows one on a server.
 *
 *   node packages/delta/examples/approvals.ts
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Effect, Layer, Schema, Stream } from "effect"
import { Authority, HistoryStore, Replica, ReplicatedDocument, Sync } from "effect-delta"

const Comment = Schema.Struct({ author: Schema.String, text: ReplicatedDocument.Text })

const DesignDoc = Schema.Struct({
  title: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  comments: Schema.Record(Schema.String, Comment)
})

const initial: typeof DesignDoc.Type = { title: "", status: "draft", comments: {} }

// The app's own records: who owns each doc.
const owners = new Map([["sessions", "ada"]])

// The rule: only a doc's owner approves it. Reading the change with the
// schema also rejects one that doesn't fit it.
const OwnerApproves = Authority.layer({
  check: (proposal) =>
    Effect.gen(function*() {
      if (proposal._tag === "Text") return yield* new Authority.Rejected({ message: "Design docs aren't texts" })
      const { after, before } = yield* ReplicatedDocument.readProposal(proposal, DesignDoc, { initial })
      const approving = after.status === "approved" && before.status !== "approved"
      if (approving && proposal.user !== owners.get(proposal.document)) {
        return yield* new Authority.Rejected({ message: "Only the owner can approve" })
      }
    })
}).pipe(Layer.provide(HistoryStore.layerMemory))

const sessions = Replica.DocumentId.make("sessions")

// Opens the doc as `user`, and waits until it has caught up.
const open = (user: string) =>
  Effect.gen(function*() {
    const doc = yield* ReplicatedDocument.makeConfirmed(DesignDoc, { initial, document: sessions })
    yield* doc.connection.status.pipe(Stream.filter((status) => status._tag === "Synced"), Stream.runHead)
    return doc
  }).pipe(Effect.provideService(Sync.CurrentUser, user))

// Waits until the authority has every change made so far.
const saved = (doc: ReplicatedDocument.Confirmed<typeof DesignDoc.Type>) =>
  doc.connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)

const program = Effect.gen(function*() {
  const ada = yield* open("ada")
  yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, title: "Sessions", status: "review" }))
  yield* saved(ada)

  // Ben comments, then tries to approve.
  const ben = yield* open("ben")
  const { edits } = yield* ReplicatedDocument.subscribe(ben)
  yield* ReplicatedDocument.update(ben, (doc) => ({
    ...doc,
    comments: { ...doc.comments, c1: { author: "ben", text: "Looks good to me" } }
  }))
  yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "approved" }))
  yield* Console.log("Ben typed: ", (yield* ReplicatedDocument.get(ben)).status)

  yield* edits.pipe(
    Stream.filter((edit) => edit._tag === "Rejected"),
    Stream.take(1),
    Stream.runForEach((edit) => Console.log("Rejected:  ", edit.message))
  )
  const seen = yield* ReplicatedDocument.get(ben)
  yield* Console.log("Ben sees:  ", seen.status, Object.keys(seen.comments))
  // Ben sees:   review [ 'c1' ]    rolled back, but his comment stayed

  // Ada, the owner, approves.
  yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "approved" }))
  yield* saved(ada)
  yield* Console.log("Ada sees:  ", (yield* ReplicatedDocument.get(ada)).status)
})

program.pipe(Effect.scoped, Effect.provide([OwnerApproves, NodeServices.layer]), NodeRuntime.runMain)
