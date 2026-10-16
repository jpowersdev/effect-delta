import type { Crypto, Effect } from "effect"
import { Schema } from "effect"
import type { Authority } from "effect-delta"
import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"
import { describe, expect, it } from "tstyche"

const Comment = Schema.Struct({
  author: Schema.String,
  range: ReplicatedText.AnchorRange,
  text: ReplicatedDocument.Text
})

const Task = Schema.Struct({
  id: Schema.String,
  title: ReplicatedDocument.Text,
  done: Schema.Boolean,
  due: Schema.optionalKey(Schema.String)
})

const DesignDoc = Schema.Struct({
  title: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  votes: ReplicatedDocument.Counter,
  comments: Schema.Record(Schema.String, Comment),
  tasks: ReplicatedDocument.List(Task, { key: "id" })
})
type DesignDoc = typeof DesignDoc.Type

declare const initial: DesignDoc
declare const doc: ReplicatedDocument.ReplicatedDocument<DesignDoc>
declare const id: string
declare const proposal: Authority.DocumentProposal

describe("ReplicatedDocument", () => {
  it("make needs Crypto only without a replica id", () => {
    expect(ReplicatedDocument.make(DesignDoc, { initial })).type.toBe<
      Effect.Effect<ReplicatedDocument.ReplicatedDocument<DesignDoc>, never, Crypto.Crypto>
    >()
    expect(ReplicatedDocument.make(DesignDoc, { initial, replica: Replica.ReplicaId.make("laptop") })).type.toBe<
      Effect.Effect<ReplicatedDocument.ReplicatedDocument<DesignDoc>>
    >()
  })

  it("a document is a replica of its schema's type", () => {
    expect(doc).type.toBeAssignableTo<Replica.Replica<DesignDoc>>()
    expect(doc).type.not.toBeAssignableTo<Replica.Replica<string>>()
    expect(ReplicatedDocument.get(doc)).type.toBe<Effect.Effect<DesignDoc>>()
  })

  it("update's callback keeps literal types", () => {
    expect(ReplicatedDocument.update(doc, (value) => ({ ...value, status: "review" }))).type.toBe<
      Effect.Effect<void, Schema.SchemaError>
    >()
    expect(ReplicatedDocument.update).type.toBeCallableWith(
      doc,
      (value: DesignDoc) => ({ ...value, status: "review" })
    )
    expect(ReplicatedDocument.update).type.not.toBeCallableWith(
      doc,
      (value: DesignDoc) => ({ ...value, status: "done" })
    )
  })

  it("set and update are pipeable", () => {
    expect(doc.pipe(ReplicatedDocument.set(initial))).type.toBe<Effect.Effect<void, Schema.SchemaError>>()
    expect(doc.pipe(ReplicatedDocument.update((value) => ({ ...value, status: "review" })))).type.toBe<
      Effect.Effect<void, Schema.SchemaError>
    >()
  })

  it("text takes paths that end at a string", () => {
    expect(ReplicatedDocument.text(doc, ["title"])).type.toBe<ReplicatedText.Editable>()
    expect(ReplicatedDocument.text(doc, ["comments", id, "text"])).type.toBe<ReplicatedText.Editable>()
    expect(ReplicatedDocument.text(doc, ["tasks", id, "title"])).type.toBe<ReplicatedText.Editable>()
    expect(doc.pipe(ReplicatedDocument.text(["title"]))).type.toBe<ReplicatedText.Editable>()
    expect(ReplicatedDocument.text).type.toBeCallableWith(doc, ["title"])
  })

  it("text rejects paths that don't end at a string", () => {
    expect(ReplicatedDocument.text).type.not.toBeCallableWith(doc, ["votes"])
    expect(ReplicatedDocument.text).type.not.toBeCallableWith(doc, ["missing"])
    expect(ReplicatedDocument.text).type.not.toBeCallableWith(doc, ["comments", id])
    expect(ReplicatedDocument.text).type.not.toBeCallableWith(doc, ["tasks", id, "done"])
  })

  it("a list's key is a string field of its items", () => {
    expect(ReplicatedDocument.List(Task, { key: "id" })).type.toBe<Schema.$Array<typeof Task>>()
    expect(ReplicatedDocument.List(Schema.String)).type.toBe<Schema.$Array<Schema.String>>()
    expect(ReplicatedDocument.List).type.toBeCallableWith(Task, { key: "id" })
    expect(ReplicatedDocument.List).type.not.toBeCallableWith(Task, { key: "done" })
    expect(ReplicatedDocument.List).type.not.toBeCallableWith(Task, { key: "missing" })
  })

  it("readProposal reads typed values", () => {
    const proposed = ReplicatedDocument.readProposal(proposal, DesignDoc, { initial })
    expect(proposed).type.toBe<ReplicatedDocument.Proposed<DesignDoc>>()
    expect(proposed.before).type.toBe<DesignDoc>()
    expect(proposed.after.status).type.toBe<"draft" | "review" | "approved">()
  })
})
