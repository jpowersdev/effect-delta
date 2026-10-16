import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Option, Stream } from "effect"
import { Authority, Delta, HistoryStore, Replica, ReplicatedText } from "effect-delta"
import { KeyValueStore } from "effect/persistence"
import { TestCrypto } from "effect/testing"

const make = (replica: string) => ReplicatedText.make({ replica: Replica.ReplicaId.make(replica) })
const notes = Replica.DocumentId.make("notes")

// Opens `notes` as a replica that saves to the store.
const open = (replica: string) =>
  Effect.gen(function*() {
    const text = yield* make(replica)
    yield* HistoryStore.persist(text, notes)
    return text
  })

// A key-value store that remembers every key written to it.
const recording = (keys: Set<string>) =>
  Layer.effect(KeyValueStore.KeyValueStore)(Effect.gen(function*() {
    const store = yield* KeyValueStore.KeyValueStore
    return { ...store, set: (key, value) => Effect.andThen(Effect.sync(() => keys.add(key)), store.set(key, value)) }
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
          check: ({ after }) =>
            after.includes("secret") ? Effect.fail(new Authority.Rejected({ message: "No secrets" })) : Effect.void
        }).pipe(Layer.provide(HistoryStore.layerMemory)),
        HistoryStore.layerKeyValueStore({ prefix: "client" }).pipe(Layer.provide(KeyValueStore.layerMemory)),
        TestCrypto.layer("store").pipe(Layer.provide(NodeCrypto.layer))
      ])
    ))
})
