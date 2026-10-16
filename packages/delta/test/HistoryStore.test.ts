import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Cause, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect"
import {
  Authority,
  Delta,
  HistoryStore,
  Replica,
  ReplicatedDocument,
  ReplicatedText,
  VersionVector
} from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import type * as InternalDocument from "effect-delta/internal/document"
import { KeyValueStore } from "effect/persistence"
import { TestCrypto } from "effect/testing"

const make = (replica: string) => ReplicatedText.make({ replica: Replica.ReplicaId.make(replica) })
const makeReplay = (replica: string) =>
  ReplicatedText.make({ strategy: "replay", replica: Replica.ReplicaId.make(replica) })
const notes = Replica.DocumentId.make("notes")

// Opens `notes` as a replica that saves to the store.
const open = (replica: string) =>
  Effect.gen(function*() {
    const text = yield* make(replica)
    yield* HistoryStore.persist(text, notes)
    return text
  })

const Comment = Schema.Struct({ author: Schema.String, text: ReplicatedDocument.Text })
const Task = Schema.Struct({ id: Schema.String, title: ReplicatedDocument.Text, done: Schema.Boolean })

// Every merge rule, so every kind of op is stored.
const DesignDoc = Schema.Struct({
  title: ReplicatedDocument.Text,
  body: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  votes: ReplicatedDocument.Counter,
  comments: Schema.Record(Schema.String, Comment),
  tasks: ReplicatedDocument.List(Task, { key: "id" })
})
type DesignDoc = typeof DesignDoc.Type
const initial: DesignDoc = { title: "", body: "", status: "draft", votes: 0, comments: {}, tasks: [] }

const makeDoc = (replica: string, strategy: Replica.Strategy = "tree") =>
  ReplicatedDocument.make(DesignDoc, { initial, strategy, replica: Replica.ReplicaId.make(replica) })

// Opens `notes` as a document that saves to the store.
const openDoc = (replica: string, strategy?: Replica.Strategy) =>
  Effect.gen(function*() {
    const doc = yield* makeDoc(replica, strategy)
    yield* HistoryStore.persist(doc, notes)
    return doc
  })

// A key-value store that remembers every key written to it.
const recording = (keys: Set<string>) =>
  Layer.effect(KeyValueStore.KeyValueStore)(Effect.gen(function*() {
    const store = yield* KeyValueStore.KeyValueStore
    return { ...store, set: (key, value) => Effect.andThen(Effect.sync(() => keys.add(key)), store.set(key, value)) }
  })).pipe(Layer.provide(KeyValueStore.layerMemory))

// A key-value store whose writes empty a key before they fill it, as a file
// system's do.
const truncating = Layer.effect(KeyValueStore.KeyValueStore)(Effect.gen(function*() {
  const store = yield* KeyValueStore.KeyValueStore
  return {
    ...store,
    set: (key, value) => store.set(key, "").pipe(Effect.andThen(Effect.yieldNow), Effect.andThen(store.set(key, value)))
  }
})).pipe(Layer.provide(KeyValueStore.layerMemory))

describe("HistoryStore", () => {
  it.effect("persist loads what was saved, and saves what's new", () =>
    Effect.gen(function*() {
      yield* Effect.scoped(Effect.gen(function*() {
        const first = yield* open("session-1")
        yield* ReplicatedText.insert(first, 0, "Hello")
      }))
      yield* Effect.scoped(Effect.gen(function*() {
        const second = yield* open("session-2")
        assert.strictEqual(yield* ReplicatedText.get(second), "Hello")
        yield* ReplicatedText.insert(second, 5, ", world")
      }))
      const third = yield* Effect.scoped(Effect.flatMap(open("session-3"), ReplicatedText.get))
      assert.strictEqual(third, "Hello, world")
    }).pipe(Effect.provide(HistoryStore.layerMemory)))

  it.effect("persist keeps a document across restarts, in either store", () =>
    Effect.forEach([
      HistoryStore.layerMemory,
      HistoryStore.layerKeyValueStore({ prefix: "docs" }).pipe(Layer.provide(KeyValueStore.layerMemory))
    ], (store) =>
      Effect.gen(function*() {
        const saved: DesignDoc = {
          ...initial,
          body: "Tokens refresh hourly.",
          status: "review",
          votes: 2,
          comments: { c1: { author: "ada", text: "Hourly?" }, c2: { author: "ben", text: "Fine" } },
          tasks: [{ id: "t1", title: "Write it", done: false }]
        }
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openDoc("session-1")
          yield* ReplicatedDocument.set(first, { ...saved, body: "" })
          yield* ReplicatedText.insert(ReplicatedDocument.text(first, ["body"]), 0, saved.body)
        }))

        const edited: DesignDoc = {
          ...saved,
          votes: 1,
          comments: { c2: { author: "ben", text: "Fine by me" } },
          tasks: [{ id: "t0", title: "Ask", done: true }, ...saved.tasks]
        }
        yield* Effect.scoped(Effect.gen(function*() {
          const second = yield* openDoc("session-2")
          assert.deepStrictEqual(yield* ReplicatedDocument.get(second), saved)
          yield* ReplicatedDocument.set(second, { ...edited, comments: { c2: saved.comments.c2! } })
          yield* ReplicatedText.insert(ReplicatedDocument.text(second, ["comments", "c2", "text"]), 4, " by me")
        }))
        const third = yield* Effect.scoped(Effect.flatMap(openDoc("session-3"), ReplicatedDocument.get))
        assert.deepStrictEqual(third, edited)
      }).pipe(Effect.provide(store)), { discard: true }))

  it.effect("persist opens a document that saved a change which turned out bad", () =>
    Effect.gen(function*() {
      const review: InternalDocument.AnyChange = {
        replica: "ada",
        seq: 0,
        length: 1,
        parents: [],
        time: 10,
        ops: [{ _tag: "Set", path: ["status"], value: "review" }]
      }
      // Made on top of `review`, but stamped earlier: it waits for `review`,
      // and is saved while it waits, then it's refused.
      const early: InternalDocument.AnyChange = {
        replica: "mallory",
        seq: 0,
        length: 1,
        parents: [{ replica: "ada", seq: 0 }],
        time: 5,
        ops: [{ _tag: "Set", path: ["status"], value: "approved" }]
      }
      const receive = (doc: Replica.Replica<unknown>, change: InternalDocument.AnyChange) =>
        Effect.exit(Replica.receive(doc, InternalDelta.fromChanges([change])))
      yield* Effect.scoped(Effect.gen(function*() {
        const first = yield* openDoc("session-1")
        yield* receive(first, early)
        yield* ReplicatedDocument.update(first, (doc) => ({ ...doc, votes: 1 }))
      }))
      yield* Effect.scoped(Effect.flatMap(openDoc("session-2"), (second) => receive(second, review)))
      const third = yield* Effect.scoped(Effect.flatMap(openDoc("session-3"), ReplicatedDocument.get))
      assert.deepStrictEqual(third, { ...initial, status: "review", votes: 1 })
    }).pipe(Effect.provide(HistoryStore.layerMemory)))

  it.effect("persist keeps what a replica shows when a change reuses the key of one that turned out bad", () =>
    Effect.gen(function*() {
      const review: InternalDocument.AnyChange = {
        replica: "ada",
        seq: 0,
        length: 1,
        parents: [],
        time: 10,
        ops: [{ _tag: "Set", path: ["status"], value: "review" }]
      }
      // Made on top of `review`, but stamped earlier: saved while it waits,
      // then refused.
      const early: InternalDocument.AnyChange = {
        replica: "mallory",
        seq: 0,
        length: 1,
        parents: [{ replica: "ada", seq: 0 }],
        time: 5,
        ops: [{ _tag: "Set", path: ["status"], value: "approved" }]
      }
      // Another change under its key, which the store can't keep beside it.
      const reused: InternalDocument.AnyChange = {
        ...early,
        time: 60,
        ops: [{ _tag: "Increment", path: ["votes"], by: 1 }]
      }
      const receive = (doc: Replica.Replica<unknown>, change: InternalDocument.AnyChange) =>
        Effect.exit(Replica.receive(doc, InternalDelta.fromChanges([change])))
      const [shown, received] = yield* Effect.scoped(Effect.gen(function*() {
        const first = yield* openDoc("session-1")
        yield* receive(first, early)
        yield* ReplicatedDocument.update(first, (doc) => ({ ...doc, title: "Plan" }))
        // Saved before the next arrives.
        yield* Effect.yieldNow
        yield* receive(first, review)
        const received = yield* receive(first, reused)
        return [yield* ReplicatedDocument.get(first), received] as const
      }))
      const reopened = yield* Effect.scoped(Effect.flatMap(openDoc("session-2"), ReplicatedDocument.get))
      assert.deepStrictEqual(reopened, shown)
      if (Exit.isSuccess(received)) return assert.fail("expected the reused key to be refused")
      assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(received.cause)).reason._tag, "ReplicaIdReused")
    }).pipe(Effect.provide(HistoryStore.layerMemory)))

  it.effect("layerKeyValueStore keeps every key under the prefix, and folds", () => {
    const keys = new Set<string>()
    return Effect.gen(function*() {
      const store = yield* HistoryStore.HistoryStore
      const laptop = yield* make("laptop")
      for (const word of ["one", " two", " three", " four", " five"]) {
        const before = yield* Replica.version(laptop)
        yield* ReplicatedText.insert(laptop, (yield* ReplicatedText.get(laptop)).length, word)
        yield* store.append(notes, yield* Replica.delta(laptop, before))
      }
      yield* store.addWriter(notes, Replica.ReplicaId.make("laptop"), "alice")

      const stored = Option.getOrThrow(yield* store.load(notes))
      assert.deepStrictEqual(stored.writers, { laptop: "alice" })
      const restored = yield* make("phone")
      yield* Replica.receive(restored, stored.history)
      assert.strictEqual(yield* ReplicatedText.get(restored), "one two three four five")

      for (const key of keys) assert.isTrue(key.startsWith("manuscripts/notes/"), key)
      // The fourth append folded three records into one; the fifth added one.
      assert.isTrue(keys.has("manuscripts/notes/history/2"))
      const kv = yield* KeyValueStore.KeyValueStore
      assert.isUndefined(yield* kv.get("manuscripts/notes/history/2"))
      assert.isDefined(yield* kv.get("manuscripts/notes/history/1"))
      assert.isTrue(Delta.isEmpty(stored.unconfirmed))
    }).pipe(
      Effect.provide(
        HistoryStore.layerKeyValueStore({ prefix: "manuscripts", foldAfter: 3 }).pipe(
          Layer.provideMerge(recording(keys))
        )
      )
    )
  })

  it.effect("layerKeyValueStore keeps each record whole when a write is interrupted", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      const delta = yield* Replica.delta(laptop, VersionVector.empty)
      for (let steps = 0; steps < 10; steps++) {
        yield* Effect.gen(function*() {
          const store = yield* HistoryStore.HistoryStore
          const appending = yield* Effect.forkChild(store.append(notes, delta))
          for (let i = 0; i < steps; i++) yield* Effect.yieldNow
          yield* Fiber.interrupt(appending)
          // It loads, with the delta or without it.
          yield* store.load(notes)
        }).pipe(Effect.provide(HistoryStore.layerKeyValueStore({ prefix: "docs" }).pipe(Layer.provide(truncating))))
      }
    }))

  describe("snapshots, for the replay strategy", () => {
    const openReplay = (replica: string) =>
      Effect.gen(function*() {
        const text = yield* ReplicatedText.make({ strategy: "replay", replica: Replica.ReplicaId.make(replica) })
        yield* HistoryStore.persist(text, notes)
        return text
      })

    it.effect("reopening starts from the saved text, then merges what came after", () =>
      Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openReplay("session-1")
          yield* ReplicatedText.insert(first, 0, "Chapter one")
        }))
        const snapshot = Option.getOrThrow(Option.getOrThrow(yield* store.load(notes)).snapshot)
        assert.deepStrictEqual(snapshot, { version: { "session-1": 11 }, texts: [{ path: [], text: "Chapter one" }] })

        // A snapshot whose text the history wouldn't give shows it wasn't replayed.
        yield* store.setSnapshot(notes, { ...snapshot, texts: [{ path: [], text: "Chapter ONE" }] })
        // A change saved after the snapshot, as when an app quits before its last save.
        const other = yield* makeReplay("other")
        yield* Replica.receive(other, Option.getOrThrow(yield* store.load(notes)).history)
        yield* ReplicatedText.insert(other, 11, ".")
        yield* store.append(notes, yield* Replica.delta(other, snapshot.version))

        const reopened = yield* Effect.scoped(Effect.flatMap(openReplay("session-2"), ReplicatedText.get))
        assert.strictEqual(reopened, "Chapter ONE.")
      }).pipe(Effect.provide(HistoryStore.layerMemory)))

    it.effect("a document's snapshot has every text by path, and reopening starts from it", () =>
      Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openDoc("session-1", "replay")
          yield* ReplicatedText.insert(ReplicatedDocument.text(first, ["title"]), 0, "Sessions")
          yield* ReplicatedDocument.update(first, (doc) => ({
            ...doc,
            status: "review",
            comments: { c1: { author: "ada", text: "Hourly?" } }
          }))
          yield* ReplicatedText.insert(ReplicatedDocument.text(first, ["body"]), 0, "Tokens refresh hourly.")
        }))
        const snapshot = Option.getOrThrow(Option.getOrThrow(yield* store.load(notes)).snapshot)
        assert.deepStrictEqual(snapshot.texts, [
          { path: ["title"], text: "Sessions" },
          { path: ["comments", "c1", "text"], text: "Hourly?" },
          { path: ["body"], text: "Tokens refresh hourly." }
        ])

        // A text the history wouldn't give shows it wasn't replayed.
        yield* store.setSnapshot(notes, {
          ...snapshot,
          texts: snapshot.texts.map((saved) =>
            saved.path[0] === "body" ? { ...saved, text: "Tokens refresh HOURLY." } : saved
          )
        })
        // Changes saved after the snapshot.
        const other = yield* makeDoc("other", "replay")
        yield* Replica.receive(other, Option.getOrThrow(yield* store.load(notes)).history)
        yield* ReplicatedText.insert(ReplicatedDocument.text(other, ["body"]), 22, " Or daily?")
        yield* ReplicatedDocument.update(other, (doc) => ({ ...doc, votes: 1 }))
        yield* store.append(notes, yield* Replica.delta(other, snapshot.version))

        const reopened = yield* Effect.scoped(Effect.flatMap(openDoc("session-2", "replay"), ReplicatedDocument.get))
        assert.deepStrictEqual(reopened, {
          ...initial,
          title: "Sessions",
          body: "Tokens refresh HOURLY. Or daily?",
          status: "review",
          votes: 1,
          comments: { c1: { author: "ada", text: "Hourly?" } }
        })
      }).pipe(Effect.provide(HistoryStore.layerMemory)))

    it.effect("an edit that changes nothing leaves no text in the snapshot", () =>
      Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openDoc("session-1", "replay")
          yield* ReplicatedText.set(ReplicatedDocument.text(first, ["body"]), "")
          yield* ReplicatedText.insert(ReplicatedDocument.text(first, ["title"]), 0, "Sessions")
        }))
        const snapshot = Option.getOrThrow(Option.getOrThrow(yield* store.load(notes)).snapshot)
        assert.deepStrictEqual(snapshot.texts, [{ path: ["title"], text: "Sessions" }])
      }).pipe(Effect.provide(HistoryStore.layerMemory)))

    it.effect("a snapshot that doesn't match the history is ignored", () =>
      Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openReplay("session-1")
          yield* ReplicatedText.insert(first, 0, "Chapter one")
        }))
        yield* store.setSnapshot(notes, { version: { "session-1": 5 }, texts: [{ path: [], text: "Wrong" }] })
        const reopened = yield* Effect.scoped(Effect.flatMap(openReplay("session-2"), ReplicatedText.get))
        assert.strictEqual(reopened, "Chapter one")
      }).pipe(Effect.provide(HistoryStore.layerMemory)))

    it.effect("a reopened text still merges concurrent edits like a tree", () =>
      Effect.gen(function*() {
        const results = yield* Effect.forEach(["tree", "replay"] as const, (strategy) =>
          Effect.gen(function*() {
            const make = (replica: string) =>
              ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(replica) })
            const coauthor = yield* make("coauthor")
            yield* Effect.scoped(Effect.gen(function*() {
              const first = yield* make("session-1")
              yield* HistoryStore.persist(first, notes)
              yield* ReplicatedText.insert(first, 0, "The ship left at dawn.")
              yield* Replica.exchange(first, coauthor)
              yield* ReplicatedText.insert(first, 13, " quietly")
            }))
            yield* ReplicatedText.update(coauthor, (text) => text.replace("dawn", "midnight"))
            return yield* Effect.scoped(Effect.gen(function*() {
              const second = yield* make("session-2")
              yield* HistoryStore.persist(second, notes)
              yield* Replica.exchange(second, coauthor)
              return yield* ReplicatedText.get(second)
            }))
          }).pipe(Effect.provide(Layer.fresh(HistoryStore.layerMemory))))
        assert.strictEqual(results[0], "The ship left quietly at midnight.")
        assert.strictEqual(results[1], results[0])
      }))
  })

  it.effect("a rejected change doesn't come back on reload", () =>
    Effect.gen(function*() {
      const spec = Replica.DocumentId.make("spec")
      yield* Effect.scoped(Effect.gen(function*() {
        const text = yield* ReplicatedText.makeConfirmed({ document: spec })
        yield* HistoryStore.persist(text, spec)
        const { edits } = yield* ReplicatedText.subscribe(text)
        yield* ReplicatedText.insert(text, 0, "ok")
        yield* ReplicatedText.insert(text, 2, " secret")
        yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
      }))

      // Read the saved history back with a plain replica.
      const reader = yield* make("reader")
      const store = yield* HistoryStore.HistoryStore
      const stored = Option.getOrThrow(yield* store.load(spec))
      yield* Replica.receive(reader, Delta.combine(stored.history, stored.unconfirmed))
      assert.strictEqual(yield* ReplicatedText.get(reader), "ok")
    }).pipe(
      Effect.provide([
        Authority.layer({
          check: (proposal) =>
            proposal._tag === "Text" && proposal.after.includes("secret")
              ? Effect.fail(new Authority.Rejected({ message: "No secrets" }))
              : Effect.void
        }).pipe(Layer.provide(HistoryStore.layerMemory)),
        HistoryStore.layerKeyValueStore({ prefix: "client" }).pipe(Layer.provide(KeyValueStore.layerMemory)),
        TestCrypto.layer("store").pipe(Layer.provide(NodeCrypto.layer))
      ])
    ))
})
