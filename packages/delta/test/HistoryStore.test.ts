import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Cause, Context, Effect, Exit, Fiber, Layer, Option, Schedule, Schema, Stream } from "effect"
import {
  Authority,
  Delta,
  HistoryStore,
  Replica,
  ReplicatedDocument,
  ReplicatedText,
  Sync,
  VersionVector
} from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import type * as InternalDocument from "effect-delta/internal/document"
import { KeyValueStore } from "effect/persistence"
import { TestClock, TestCrypto } from "effect/testing"

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

  it.effect("persist saves only what the store doesn't have", () => {
    // Counts the changes each append saves.
    const appended: Array<number> = []
    const Counting = Layer.effect(HistoryStore.HistoryStore)(Effect.gen(function*() {
      const store = yield* HistoryStore.HistoryStore
      return {
        ...store,
        append: (document, delta) => {
          appended.push((delta as InternalDelta.DeltaImpl).changes.size)
          return store.append(document, delta)
        }
      }
    })).pipe(Layer.provide(HistoryStore.layerMemory))
    return Effect.gen(function*() {
      yield* Effect.scoped(Effect.flatMap(open("session-1"), (first) => ReplicatedText.insert(first, 0, "Hello")))
      appended.length = 0
      // Opened again, with no edits: nothing new to save.
      yield* Effect.scoped(open("session-2"))
      assert.deepStrictEqual(appended, [])
    }).pipe(Effect.provide(Counting))
  })

  it.effect("persist saves a change that waits once", () => {
    const appended: Array<string> = []
    const Recording = Layer.effect(HistoryStore.HistoryStore)(Effect.gen(function*() {
      const store = yield* HistoryStore.HistoryStore
      return {
        ...store,
        append: (document, delta) => {
          appended.push(...(delta as InternalDelta.DeltaImpl).changes.keys())
          return store.append(document, delta)
        }
      }
    })).pipe(Layer.provide(HistoryStore.layerMemory))
    return Effect.gen(function*() {
      const peer = yield* make("peer")
      const other = yield* make("other")
      yield* ReplicatedText.insert(peer, 0, "a")
      const before = yield* Replica.version(peer)
      yield* ReplicatedText.insert(peer, 1, "b")
      yield* ReplicatedText.insert(other, 0, "x")
      yield* Replica.receive(other, yield* Replica.delta(peer, VersionVector.empty))
      yield* ReplicatedText.insert(other, 0, "y")
      // One waits for the peer's first change, and one, after the other's
      // first change, for that change: neither ever comes.
      const waiting = Delta.combine(
        yield* Replica.delta(peer, before),
        yield* Replica.delta(other, yield* Replica.version(peer))
      )
      for (const session of ["session-1", "session-2"]) {
        yield* Effect.scoped(Effect.gen(function*() {
          const text = yield* open(session)
          if (session === "session-1") yield* Replica.receive(text, waiting)
          for (const word of ["one", "two", "three"]) {
            yield* ReplicatedText.insert(text, 0, word)
            for (let i = 0; i < 5; i++) yield* Effect.yieldNow
          }
        }))
      }
      assert.strictEqual(appended.length, new Set(appended).size, `appended ${appended}`)
    }).pipe(Effect.provide(Recording))
  })

  it.effect("persist saves only what the store doesn't have, for a confirmed replica too", () => {
    const appended: Array<number> = []
    const Counting = Layer.effect(HistoryStore.HistoryStore)(Effect.gen(function*() {
      const store = yield* HistoryStore.HistoryStore
      return {
        ...store,
        append: (document, delta) => {
          appended.push((delta as InternalDelta.DeltaImpl).changes.size)
          return store.append(document, delta)
        }
      }
    })).pipe(
      Layer.provide(HistoryStore.layerKeyValueStore({ prefix: "client" })),
      Layer.provide(KeyValueStore.layerMemory)
    )
    const spec = Replica.DocumentId.make("spec")
    const session = (text: string) =>
      Effect.scoped(Effect.gen(function*() {
        const confirmed = yield* ReplicatedText.makeConfirmed({ document: spec })
        yield* HistoryStore.persist(confirmed, spec)
        yield* ReplicatedText.insert(confirmed, (yield* ReplicatedText.get(confirmed)).length, text)
        yield* Stream.runHead(Stream.filter(confirmed.connection.unconfirmed, (count) => count === 0))
      }))
    return Effect.gen(function*() {
      yield* session("Hello")
      appended.length = 0
      // Reopened, it saves its new change, and none of what it saved before.
      yield* session(", world")
      assert.deepStrictEqual(appended, [1])
    }).pipe(
      Effect.provide([
        Counting,
        Authority.layer({ check: () => Effect.void }).pipe(Layer.provide(HistoryStore.layerMemory)),
        TestCrypto.layer("store").pipe(Layer.provide(NodeCrypto.layer))
      ])
    )
  })

  it.effect("persist saves a confirmed change as confirmed, though the authority's answer was lost", () =>
    Effect.gen(function*() {
      const spec = Replica.DocumentId.make("spec")
      const authority = Context.get(
        yield* Layer.build(
          Layer.fresh(Authority.layer({ check: () => Effect.void }).pipe(Layer.provide(HistoryStore.layerMemory)))
        ),
        Authority.Authority
      )
      // Takes the first change it's pushed, but the answer never arrives.
      let answered = false
      const losing = Authority.Authority.of({
        subscribe: authority.subscribe,
        push: (request) =>
          Effect.andThen(authority.push(request), (ack) => {
            if (answered) return Effect.succeed(ack)
            answered = true
            return Effect.fail(new Sync.SyncError({ reason: new Sync.Disconnected({ cause: "lost" }) }))
          })
      })
      yield* Effect.scoped(Effect.gen(function*() {
        const text = yield* ReplicatedText.makeConfirmed({ document: spec, retry: Schedule.forever })
        yield* HistoryStore.persist(text, spec)
        yield* ReplicatedText.insert(text, 0, "Hello")
        // Reconnected, it learns the authority has it.
        yield* Stream.runHead(Stream.filter(text.connection.unconfirmed, (count) => count === 0))
      })).pipe(Effect.provideService(Authority.Authority, losing))
      const store = yield* HistoryStore.HistoryStore
      const stored = Option.getOrThrow(yield* store.load(spec))
      assert.deepStrictEqual(
        [...(stored.history as InternalDelta.DeltaImpl).changes.values()].map(({ ops }) => ops.length),
        [1]
      )
      assert.isTrue(Delta.isEmpty(stored.unconfirmed))
    }).pipe(
      Effect.provide([HistoryStore.layerMemory, TestCrypto.layer("store").pipe(Layer.provide(NodeCrypto.layer))])
    ))

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

  it.effect("persist opens a document whose merged fields a class's constructor won't build", () =>
    Effect.gen(function*() {
      // A class whose constructor takes only some values of its fields.
      class Range extends Schema.Class<Range>("Range")({ start: Schema.Int, end: Schema.Int }) {
        // Schema advises against overriding it, for reasons such as this.
        // @effect-diagnostics-next-line overriddenSchemaConstructor:off
        constructor(fields: { readonly start: number; readonly end: number }) {
          if (fields.start > fields.end) throw new Error("A range can't end before it starts")
          super(fields)
        }
      }
      const Plan = Schema.Struct({ title: ReplicatedDocument.Text, range: Range })
      const open = (replica: string) =>
        Effect.gen(function*() {
          const plan = yield* ReplicatedDocument.make(Plan, {
            initial: { title: "", range: new Range({ start: 0, end: 10 }) },
            replica: Replica.ReplicaId.make(replica)
          })
          yield* HistoryStore.persist(plan, notes)
          return plan
        })
      // Ada moves the start up, then takes in Ben's later end, before it.
      const ben = yield* ReplicatedDocument.make(Plan, {
        initial: { title: "", range: new Range({ start: 0, end: 10 }) },
        replica: Replica.ReplicaId.make("ben")
      })
      yield* Effect.scoped(Effect.gen(function*() {
        const ada = yield* open("ada")
        yield* ReplicatedDocument.update(ada, (value) => ({ ...value, range: new Range({ start: 5, end: 10 }) }))
        yield* TestClock.adjust("1 second")
        yield* ReplicatedDocument.update(ben, (value) => ({ ...value, range: new Range({ start: 0, end: 3 }) }))
        yield* Replica.exchange(ada, ben)
      }))
      // Opened again with both, it shows the range as Ada did.
      yield* Effect.scoped(Effect.gen(function*() {
        const again = yield* open("ada-again")
        assert.deepStrictEqual(yield* Replica.version(again), yield* Replica.version(ben))
        const { range } = yield* ReplicatedDocument.get(again)
        assert.deepStrictEqual([range.start, range.end], [5, 10])
      }))
    }).pipe(Effect.provide(HistoryStore.layerMemory)))

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

    it.effect("reopening under the replica id that wrote it is refused, from a snapshot too", () =>
      Effect.gen(function*() {
        for (const strategy of ["tree", "replay"] as const) {
          const reopened = yield* Effect.gen(function*() {
            yield* Effect.scoped(Effect.gen(function*() {
              const first = yield* openDoc("laptop", strategy)
              yield* ReplicatedText.insert(ReplicatedDocument.text(first, ["title"]), 0, "Hello")
            }))
            return yield* Effect.scoped(Effect.flip(openDoc("laptop", strategy)))
          }).pipe(Effect.provide(Layer.fresh(HistoryStore.layerMemory)))
          assert.strictEqual(reopened._tag === "DeltaError" ? reopened.reason._tag : reopened._tag, "ReplicaIdReused")
        }
      }))

    it.effect("reopening reports no overlaps with what came after the snapshot, as loading never does", () =>
      Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        // Concurrent edits: one saved with a snapshot, the other saved after it.
        const ada = yield* makeDoc("ada", "replay")
        const ben = yield* makeDoc("ben", "replay")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review", body: "hello" }))
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "approved", body: "world" }))
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openDoc("session-1", "replay")
          yield* Replica.receive(first, yield* Replica.delta(ada, VersionVector.empty))
        }))
        yield* store.append(notes, yield* Replica.delta(ben, VersionVector.empty))

        yield* Effect.scoped(Effect.gen(function*() {
          const reopened = yield* makeDoc("session-2", "replay")
          const { edits } = yield* ReplicatedDocument.subscribe(reopened)
          yield* HistoryStore.persist(reopened, notes)
          // The snapshot, then what came after it.
          const loaded = yield* edits.pipe(Stream.take(2), Stream.runCollect)
          assert.deepStrictEqual(loaded.flatMap((edit) => edit._tag === "Remote" ? edit.overlaps : []), [])
        }))
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

    it.effect("a change that can never be applied doesn't keep reopening from replaying everything", () =>
      Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        // Forged inside the sequence numbers of a change that arrives after
        // it: it waits, then can never be applied, and it's saved.
        const forged: InternalDocument.AnyChange = {
          replica: "other",
          seq: 1,
          length: 1,
          parents: [],
          time: 1,
          ops: [{ _tag: "InsertAt", index: 0, text: "x" }]
        }
        const other = yield* makeReplay("other")
        yield* ReplicatedText.insert(other, 0, "Chapter")
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* openReplay("session-1")
          yield* Replica.receive(first, InternalDelta.fromChanges([forged]))
          yield* Replica.receive(first, yield* Replica.delta(other, VersionVector.empty))
          yield* ReplicatedText.insert(first, 7, " one")
        }))
        const snapshot = Option.getOrThrow(Option.getOrThrow(yield* store.load(notes)).snapshot)
        assert.deepStrictEqual(snapshot.texts, [{ path: [], text: "Chapter one" }])

        // A snapshot whose text the history wouldn't give shows it wasn't replayed.
        yield* store.setSnapshot(notes, { ...snapshot, texts: [{ path: [], text: "Chapter ONE" }] })
        const reopened = yield* Effect.scoped(Effect.flatMap(openReplay("session-2"), ReplicatedText.get))
        assert.strictEqual(reopened, "Chapter ONE")
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

  it.effect("a reopen refused for a change under the key of another leaves the replica as it was", () =>
    Effect.gen(function*() {
      const spec = Replica.DocumentId.make("spec")
      const replica = Replica.ReplicaId.make("laptop")
      yield* Effect.scoped(Effect.gen(function*() {
        const text = yield* ReplicatedText.makeConfirmed({ document: spec, replica })
        yield* HistoryStore.persist(text, spec)
        yield* ReplicatedText.insert(text, 0, "saved")
        yield* Stream.runHead(Stream.filter(text.connection.unconfirmed, (count) => count === 0))
      }))
      // Another copy under the same id, offline, writes its own first change.
      const offline = Authority.Authority.of({ subscribe: () => Stream.never, push: () => Effect.never })
      yield* Effect.scoped(Effect.gen(function*() {
        const copy = yield* ReplicatedText.makeConfirmed({ document: spec, replica })
        yield* ReplicatedText.insert(copy, 0, "other")
        const refused = yield* Effect.flip(HistoryStore.persist(copy, spec))
        assert.strictEqual(refused._tag === "DeltaError" && refused.reason._tag, "ReplicaIdReused")
        // Its change is still unconfirmed, so it's in no delta.
        assert.isTrue(Delta.isEmpty(yield* Replica.delta(copy, VersionVector.empty)))
      })).pipe(Effect.provideService(Authority.Authority, offline))
    }).pipe(
      Effect.provide([
        Authority.layer({ check: () => Effect.void }).pipe(Layer.provide(Layer.fresh(HistoryStore.layerMemory))),
        HistoryStore.layerMemory,
        TestCrypto.layer("store").pipe(Layer.provide(NodeCrypto.layer))
      ])
    ))
})
