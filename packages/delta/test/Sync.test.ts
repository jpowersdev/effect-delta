import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import {
  Cause,
  Context,
  type Crypto,
  Deferred,
  Effect,
  Equal,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Schedule,
  Schema,
  Scope,
  Semaphore,
  Stream
} from "effect"
import {
  Authority,
  Delta,
  HistoryStore,
  Relay,
  Replica,
  ReplicatedDocument,
  ReplicatedText,
  Sync,
  VersionVector
} from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"
import * as InternalDocument from "effect-delta/internal/document"
import type * as InternalEngine from "effect-delta/internal/engine"
import * as InternalShape from "effect-delta/internal/shape"
import { KeyValueStore } from "effect/persistence"
import { TestCrypto } from "effect/testing"
import {
  editProject,
  EditSession,
  forgedRuns,
  ForgedSession,
  forgeSession,
  initialProject,
  overJson,
  Project
} from "./utils/forged.ts"
import {
  Board,
  documentRuns,
  DocumentSession,
  initialBoard,
  makeBoards,
  stepBoards,
  syncAll
} from "./utils/sessions.ts"

const make = (replica: string) => ReplicatedText.make({ replica: Replica.ReplicaId.make(replica) })
const notes = Replica.DocumentId.make("notes")
const spec = Replica.DocumentId.make("spec")

// Waits until the replica shows `text`.
const showing = (self: ReplicatedText.ReplicatedText, text: string) =>
  ReplicatedText.changes(self).pipe(Stream.filter((current) => current === text), Stream.runHead, Effect.asVoid)

const Comment = Schema.Struct({ author: Schema.String, text: ReplicatedDocument.Text })
const Task = Schema.Struct({ id: Schema.String, title: ReplicatedDocument.Text, done: Schema.Boolean })

// Every merge rule, so a relay sees every kind of op.
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

// Waits until the document shows a value `done` accepts.
const until = <A>(self: ReplicatedDocument.ReplicatedDocument<A>, done: (value: A) => boolean) =>
  ReplicatedDocument.changes(self).pipe(Stream.filter(done), Stream.runHead, Effect.asVoid)

const synced = (connection: Sync.Connection) =>
  connection.status.pipe(Stream.filter((status) => status._tag === "Synced"), Stream.runHead, Effect.asVoid)

const unconfirmed = (connection: Sync.Connection) =>
  Effect.map(Stream.runHead(connection.unconfirmed), Option.getOrThrow)

const asUser = (user: string) => Effect.provideService(Sync.CurrentUser, user)

const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))

// Rejects any change that leaves "secret" in the text.
const NoSecrets = Authority.layer({
  check: (proposal) =>
    proposal._tag === "Text" && proposal.after.includes("secret")
      ? Effect.fail(new Authority.Rejected({ message: "No secrets" }))
      : Effect.void
}).pipe(Layer.provide(HistoryStore.layerMemory))

// A store whose first save fails.
const failingOnce = Layer.effect(HistoryStore.HistoryStore)(Effect.gen(function*() {
  const store = yield* HistoryStore.HistoryStore
  let failed = false
  return {
    ...store,
    append: (document, delta) => {
      if (failed) return store.append(document, delta)
      failed = true
      return Effect.fail(new HistoryStore.HistoryStoreError({ message: "Disk full", cause: undefined }))
    }
  }
})).pipe(Layer.provide(HistoryStore.layerMemory))

const day = 24 * 60 * 60 * 1000

// A store that holds the next save until interrupted, once `next` is set, and
// tells `next` when it starts.
const holdingSaves = (hold: { next: Deferred.Deferred<void> | undefined }) =>
  Layer.effect(HistoryStore.HistoryStore)(Effect.gen(function*() {
    const store = yield* HistoryStore.HistoryStore
    return {
      ...store,
      append: (document, delta) => {
        const saving = hold.next
        if (saving === undefined) return store.append(document, delta)
        hold.next = undefined
        return Effect.andThen(Deferred.succeed(saving, undefined), Effect.never)
      }
    }
  })).pipe(Layer.provide(HistoryStore.layerMemory))

const CryptoTest = TestCrypto.layer("sync").pipe(Layer.provide(NodeCrypto.layer))

describe("Relay", () => {
  it.effect("syncs replicas both ways", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      const phone = yield* make("phone")
      yield* Relay.connect(laptop, { document: notes })
      yield* Relay.connect(phone, { document: notes })

      yield* ReplicatedText.insert(laptop, 0, "Hello")
      yield* showing(phone, "Hello")
      yield* ReplicatedText.insert(phone, 5, ", world")
      yield* showing(laptop, "Hello, world")
    }).pipe(Effect.scoped, Effect.provide(RelayLive)))

  it.effect("uploads what was made offline when it reconnects", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      const phone = yield* make("phone")
      yield* Relay.connect(phone, { document: notes })

      yield* Effect.scoped(Effect.gen(function*() {
        yield* Relay.connect(laptop, { document: notes })
        yield* ReplicatedText.insert(laptop, 0, "Sessions last 24 hours.")
        yield* showing(phone, "Sessions last 24 hours.")
      }))

      // The laptop is offline now. Both change the same words.
      yield* ReplicatedText.update(laptop, (text) => text.replace("24", "8"))
      yield* ReplicatedText.update(phone, (text) => text.replace("24", "12"))

      const connection = yield* Relay.connect(laptop, { document: notes })
      yield* synced(connection)
      const merged = yield* ReplicatedText.get(laptop)
      assert.include(["Sessions last 812 hours.", "Sessions last 128 hours."], merged)
      yield* showing(phone, merged)
      assert.strictEqual(yield* unconfirmed(connection), 0)
    }).pipe(Effect.scoped, Effect.provide(RelayLive)))

  it.effect("counts changes the relay hasn't acknowledged", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      const connection = yield* Relay.connect(laptop, { document: notes })
      yield* synced(connection)
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      // Counted at once, before the push goes out.
      assert.strictEqual(yield* unconfirmed(connection), 1)
      yield* connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)
      const phone = yield* make("phone")
      yield* synced(yield* Relay.connect(phone, { document: notes }))
      assert.strictEqual(yield* ReplicatedText.get(phone), "Hello")
    }).pipe(Effect.scoped, Effect.provide(RelayLive)))

  it.effect("holds a change until what it builds on arrives", () =>
    Effect.gen(function*() {
      const alice = yield* make("alice")
      const bob = yield* make("bob")
      const carol = yield* make("carol")

      // Bob gets Alice's text directly and adds to it. Only Bob uploads.
      yield* ReplicatedText.insert(alice, 0, "Hello")
      yield* Replica.exchange(alice, bob)
      yield* ReplicatedText.insert(bob, 5, ", world")
      const fromBob = yield* Relay.connect(bob, { document: notes })
      yield* synced(fromBob)
      // The relay holds Bob's change, so Bob doesn't push it again.
      assert.strictEqual(yield* unconfirmed(fromBob), 0)

      yield* Relay.connect(carol, { document: notes })
      yield* Relay.connect(alice, { document: notes })
      yield* showing(carol, "Hello, world")
    }).pipe(Effect.scoped, Effect.provide(RelayLive)))

  it.effect("keeps documents in the history store", () =>
    Effect.gen(function*() {
      yield* Effect.scoped(
        Effect.gen(function*() {
          const laptop = yield* make("laptop")
          yield* ReplicatedText.insert(laptop, 0, "Saved")
          yield* synced(yield* Relay.connect(laptop, { document: notes }))
        }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
      )

      // A new relay, as after a restart, loads it from the store.
      yield* Effect.scoped(
        Effect.gen(function*() {
          const phone = yield* make("phone")
          yield* synced(yield* Relay.connect(phone, { document: notes }))
          assert.strictEqual(yield* ReplicatedText.get(phone), "Saved")
        }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
      )
    }).pipe(Effect.provide(HistoryStore.layerMemory)))

  it.effect("syncs replay replicas, and keeps their document", () =>
    Effect.gen(function*() {
      const makeReplay = (replica: string) =>
        ReplicatedText.make({ strategy: "replay", replica: Replica.ReplicaId.make(replica) })
      yield* Effect.scoped(
        Effect.gen(function*() {
          const laptop = yield* makeReplay("laptop")
          const phone = yield* makeReplay("phone")
          yield* ReplicatedText.insert(laptop, 0, "Sessions last 24 hours.")
          yield* synced(yield* Relay.connect(laptop, { document: notes }))
          yield* synced(yield* Relay.connect(phone, { document: notes }))
          // Both edit at once, while connected.
          yield* ReplicatedText.update(laptop, (text) => text.replace("24", "8"))
          yield* ReplicatedText.update(phone, (text) => text.replace("hours", "minutes"))
          yield* showing(laptop, "Sessions last 8 minutes.")
          yield* showing(phone, "Sessions last 8 minutes.")
        }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
      )

      // A new relay loads the document as a replay document.
      yield* Effect.scoped(
        Effect.gen(function*() {
          const tablet = yield* makeReplay("tablet")
          yield* synced(yield* Relay.connect(tablet, { document: notes }))
          assert.strictEqual(yield* ReplicatedText.get(tablet), "Sessions last 8 minutes.")
          const tree = yield* make("tree")
          const error = yield* Effect.flip((yield* Relay.connect(tree, { document: notes })).await)
          assert.strictEqual(error.reason._tag, "ProtocolViolation")
        }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
      )
    }).pipe(Effect.provide(HistoryStore.layerMemory)))

  it.effect("starts over from the store when saving fails", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      const phone = yield* make("phone")
      const retry = Schedule.forever
      yield* synced(yield* Relay.connect(phone, { document: notes, retry }))
      yield* Relay.connect(laptop, { document: notes, retry })
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      yield* showing(phone, "Hello")
    }).pipe(
      Effect.scoped,
      Effect.provide(Relay.layer.pipe(Layer.provide(failingOnce)))
    ))

  it.effect("never sends a change it failed to save to a subscriber that waited for the push", () =>
    Effect.gen(function*() {
      const saving = yield* Deferred.make<void>()
      const fail = yield* Deferred.make<void>()
      const Failing = Layer.effect(HistoryStore.HistoryStore)(Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        return {
          ...store,
          append: () =>
            Deferred.succeed(saving, undefined).pipe(
              Effect.andThen(Deferred.await(fail)),
              Effect.andThen(
                Effect.fail(new HistoryStore.HistoryStoreError({ message: "Disk full", cause: undefined }))
              )
            )
        }
      })).pipe(Layer.provide(HistoryStore.layerMemory))

      yield* Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const laptop = yield* make("laptop")
        yield* ReplicatedText.insert(laptop, 0, "Hello")
        const delta = yield* Replica.delta(laptop, VersionVector.empty)
        const pushed = yield* Effect.forkChild(
          Effect.exit(relay.push({ format: Sync.FORMAT, document: notes, delta }).pipe(asUser("laptop")))
        )
        yield* Deferred.await(saving)
        // Opens the document, then waits for the push to finish with it.
        const sent: Array<Sync.Update> = []
        const subscribed = yield* Effect.forkChild(Stream.runForEach(
          relay.subscribe({ format: Sync.FORMAT, document: notes, strategy: "tree", version: {}, authority: false }),
          (update) => Effect.sync(() => sent.push(update))
        ))
        for (let i = 0; i < 50; i++) yield* Effect.yieldNow
        yield* Deferred.succeed(fail, undefined)
        assert.isTrue(Exit.isFailure(yield* Fiber.join(pushed)))
        // Told to subscribe again, once the document is loaded from the store.
        const error = yield* Effect.flip(Fiber.join(subscribed))
        assert.strictEqual(error.reason._tag, "Unavailable")
        assert.deepStrictEqual(sent, [])
      }).pipe(Effect.provide(Relay.layer.pipe(Layer.provide(Failing))))
    }))

  it.effect("uploads everything again to a relay that lost it", () =>
    Effect.gen(function*() {
      // A relay that keeps documents only in memory, restarted.
      const before = Context.get(yield* Layer.build(Layer.fresh(RelayLive)), Relay.Relay)
      const after = Context.get(yield* Layer.build(Layer.fresh(RelayLive)), Relay.Relay)
      let current = before
      let lost = yield* Deferred.make<never, Sync.SyncError>()
      const restarting = Relay.Relay.of({
        subscribe: (request) =>
          Stream.unwrap(Effect.sync(() => current.subscribe(request).pipe(Stream.interruptWhen(Deferred.await(lost))))),
        push: (request) => Effect.suspend(() => current.push(request))
      })

      const laptop = yield* make("laptop")
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      const connection = yield* Relay.connect(laptop, { document: notes, retry: Schedule.forever }).pipe(
        Effect.provideService(Relay.Relay, restarting)
      )
      yield* synced(connection)

      const disconnected = lost
      lost = yield* Deferred.make<never, Sync.SyncError>()
      current = after
      yield* Deferred.fail(disconnected, new Sync.SyncError({ reason: new Sync.Disconnected({ cause: "restart" }) }))

      const phone = yield* make("phone")
      yield* Relay.connect(phone, { document: notes }).pipe(Effect.provideService(Relay.Relay, after))
      yield* showing(phone, "Hello")
    }).pipe(Effect.scoped))

  it.effect("forbids pushing under another user's replica id", () =>
    Effect.gen(function*() {
      const relay = yield* Relay.Relay
      const laptop = yield* make("laptop")
      const push = Effect.gen(function*() {
        const delta = yield* Replica.delta(laptop, VersionVector.empty)
        return yield* relay.push({ format: Sync.FORMAT, document: notes, delta })
      })

      yield* ReplicatedText.insert(laptop, 0, "Mine")
      yield* push.pipe(asUser("alice"))
      yield* ReplicatedText.insert(laptop, 4, "!")
      const error = yield* Effect.flip(push.pipe(asUser("mallory")))
      assert.strictEqual(error.reason._tag, "Forbidden")
      const ack = yield* push.pipe(asUser("alice"))
      assert.strictEqual(ack._tag, "Accepted")
    }).pipe(Effect.scoped, Effect.provide(RelayLive)))

  it.effect("refuses replicas that expect an authority", () =>
    Effect.gen(function*() {
      const relay = yield* Relay.Relay
      const error = yield* relay.subscribe({
        format: Sync.FORMAT,
        document: notes,
        strategy: "tree",
        version: VersionVector.empty,
        authority: true
      }).pipe(Stream.runHead, Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolViolation")
    }).pipe(Effect.provide(RelayLive)))

  describe("documents", () => {
    it.effect("sends subscribers only changes it has checked", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const laptop = yield* makeDoc("laptop")
        const connection = yield* Relay.connect(laptop, { document: notes })
        yield* synced(connection)
        // Waits until the laptop shows a value `done` accepts, while it's connected.
        const shows = (done: (doc: DesignDoc) => boolean) => Effect.raceFirst(until(laptop, done), connection.await)
        const push = (change: InternalDocument.AnyChange) =>
          relay.push({ format: Sync.FORMAT, document: notes, delta: InternalDelta.fromChanges([change]) })
        const first: InternalDocument.AnyChange = {
          replica: "ada",
          seq: 0,
          length: 1,
          parents: [],
          time: 10,
          ops: [{ _tag: "Set", path: ["status"], value: "review" }]
        }
        // Made on top of `first`, but stamped earlier: it waits for `first`,
        // then it's refused.
        const early: InternalDocument.AnyChange = {
          replica: "mallory",
          seq: 0,
          length: 1,
          parents: [{ replica: "ada", seq: 0 }],
          time: 5,
          ops: [{ _tag: "Set", path: ["status"], value: "approved" }]
        }
        yield* push(early)
        yield* push(first)
        yield* shows((doc) => doc.status === "review")
        yield* push({
          ...first,
          seq: 1,
          parents: [{ replica: "ada", seq: 0 }],
          time: 11,
          ops: [{ _tag: "Increment", path: ["votes"], by: 1 }]
        })
        yield* shows((doc) => doc.votes === 1)
      }).pipe(Effect.scoped, Effect.provide(RelayLive)))

    it.effect("keeps a change that waits though its push brought a bad one, as replicas do", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const push = (delta: Delta.Delta) => relay.push({ format: Sync.FORMAT, document: notes, delta })
        const ada = yield* makeDoc("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 1 }))
        const fromAda = yield* Replica.delta(ada, VersionVector.empty)
        // Ben's change waits for Ada's, which Ada hasn't pushed yet.
        const ben = yield* makeDoc("ben")
        yield* Replica.receive(ben, fromAda)
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, status: "review" }))
        const fromBen = yield* Replica.delta(ben, yield* Replica.version(ada))
        const bad = InternalDelta.fromChanges([{
          replica: "mallory",
          seq: 0,
          length: 2,
          parents: [],
          time: 0,
          ops: [{ _tag: "Set", path: ["title"], value: "x" }]
        }])
        const error = yield* Effect.flip(push(Delta.combine(fromBen, bad)))
        assert.strictEqual(error.reason._tag, "DeltaError")
        yield* push(fromAda)
        // Whoever subscribes gets Ben's change.
        const carol = yield* makeDoc("carol")
        const first = yield* Stream.runHead(
          relay.subscribe({ format: Sync.FORMAT, document: notes, strategy: "tree", version: {}, authority: false })
        )
        yield* Replica.receive(carol, Option.getOrThrow(first).delta)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(carol), yield* ReplicatedDocument.get(ben))
      }).pipe(Effect.provide(RelayLive)))

    it.effect("refuses a bad change that waited when it's pushed again with what it waited for, as replicas do", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const ada = yield* makeDoc("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, votes: 1 }))
        const fromAda = (yield* Replica.delta(ada, VersionVector.empty)) as InternalDelta.DeltaImpl
        // Made on top of Ada's change, and bad: it has fewer ops than it says.
        const bad: InternalDocument.AnyChange = {
          replica: "mallory",
          seq: 0,
          length: 2,
          parents: [{ replica: "ada", seq: 0 }],
          time: 1000,
          ops: [{ _tag: "Set", path: ["status"], value: "review" }]
        }
        const both = InternalDelta.fromChanges([...fromAda.changes.values(), bad])
        const ben = yield* makeDoc("ben")
        const received = yield* Effect.forEach(
          [InternalDelta.fromChanges([bad]), both],
          (delta) => Effect.map(Effect.exit(Replica.receive(ben, delta)), Exit.isSuccess)
        )
        const pushed = yield* Effect.forEach(
          [InternalDelta.fromChanges([bad]), both],
          (delta) =>
            Effect.map(Effect.exit(relay.push({ format: Sync.FORMAT, document: notes, delta })), Exit.isSuccess)
        )
        assert.deepStrictEqual(received, [true, false])
        assert.deepStrictEqual(pushed, received)
      }).pipe(Effect.provide(RelayLive)))

    it.effect("refuses a whole push with a change under another's key, though it's malformed too, as replicas do", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const first: InternalDocument.AnyChange = {
          replica: "mallory",
          seq: 0,
          length: 1,
          parents: [],
          time: 1,
          ops: [{ _tag: "Set", path: ["status"], value: "review" }]
        }
        // Under the key of `first`, and refused as it arrives: it takes no
        // sequence numbers.
        const reused: InternalDocument.AnyChange = { ...first, length: 0 }
        const vote: InternalDocument.AnyChange = {
          replica: "ben",
          seq: 0,
          length: 1,
          parents: [],
          time: 1,
          ops: [{ _tag: "Increment", path: ["votes"], by: 1 }]
        }
        const reason = (exit: Exit.Exit<unknown, Replica.DeltaError | Sync.SyncError>) => {
          const error = Exit.isFailure(exit) ? Option.getOrUndefined(Cause.findErrorOption(exit.cause)) : undefined
          const delta = error?._tag === "SyncError" ? error.reason : error
          return delta?._tag === "DeltaError" ? delta.reason._tag : undefined
        }
        const ada = yield* makeDoc("ada")
        yield* Replica.receive(ada, InternalDelta.fromChanges([first]))
        const received = yield* Effect.exit(Replica.receive(ada, InternalDelta.fromChanges([reused, vote])))
        const push = (changes: ReadonlyArray<InternalDocument.AnyChange>) =>
          Effect.exit(relay.push({ format: Sync.FORMAT, document: notes, delta: InternalDelta.fromChanges(changes) }))
        yield* push([first])
        const pushed = yield* push([reused, vote])
        assert.strictEqual(reason(received), "ReplicaIdReused")
        assert.strictEqual(reason(pushed), reason(received))
        // Neither took Ben's vote.
        const subscriber = yield* makeDoc("subscriber")
        const sent = yield* Stream.runHead(
          relay.subscribe({ format: Sync.FORMAT, document: notes, strategy: "tree", version: {}, authority: false })
        )
        yield* Replica.receive(subscriber, Option.getOrThrow(sent).delta)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(subscriber), yield* ReplicatedDocument.get(ada))
      }).pipe(Effect.provide(RelayLive)))

    it.effect("keeps what it sent when a change reuses the key of one that turned out bad", () =>
      Effect.gen(function*() {
        const review: InternalDocument.AnyChange = {
          replica: "ada",
          seq: 0,
          length: 1,
          parents: [],
          time: 10,
          ops: [{ _tag: "Set", path: ["status"], value: "review" }]
        }
        // Made on top of `review`, but stamped earlier: stored while it
        // waits, then refused.
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
        // What a relay sends a replica that subscribes.
        const sent = Effect.gen(function*() {
          const relay = yield* Relay.Relay
          const first = yield* Stream.runHead(
            relay.subscribe({ format: Sync.FORMAT, document: notes, strategy: "tree", version: {}, authority: false })
          )
          const doc = yield* makeDoc("subscriber")
          yield* Replica.receive(doc, Option.getOrThrow(first).delta)
          return yield* ReplicatedDocument.get(doc)
        })
        const [before, pushed] = yield* Effect.gen(function*() {
          const relay = yield* Relay.Relay
          const push = (change: InternalDocument.AnyChange) =>
            Effect.exit(
              relay.push({ format: Sync.FORMAT, document: notes, delta: InternalDelta.fromChanges([change]) })
            )
          yield* push(early)
          yield* push(review)
          const pushed = yield* push(reused)
          return [yield* sent, pushed] as const
        }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
        // Loaded again from the store.
        assert.deepStrictEqual(yield* sent.pipe(Effect.provide(Layer.fresh(Relay.layer))), before)
        if (Exit.isSuccess(pushed)) return assert.fail("expected the reused key to be refused")
        const error = Option.getOrThrow(Cause.findErrorOption(pushed.cause))
        assert.isTrue(error.reason._tag === "DeltaError" && error.reason.reason._tag === "ReplicaIdReused")
      }).pipe(Effect.provide(HistoryStore.layerMemory)))

    it.effect("stores documents it can't read, and replicas reload them intact", () =>
      Effect.gen(function*() {
        const laptop = yield* makeDoc("laptop")
        yield* Effect.scoped(
          Effect.gen(function*() {
            const connection = yield* Relay.connect(laptop, { document: notes })
            yield* ReplicatedDocument.set(laptop, {
              ...initial,
              status: "review",
              votes: 3,
              comments: { c1: { author: "ada", text: "Hourly?" }, c2: { author: "ben", text: "Fine" } },
              tasks: [{ id: "t1", title: "Write it", done: false }, { id: "t2", title: "Ship it", done: false }]
            })
            yield* ReplicatedDocument.update(laptop, (doc) => ({
              ...doc,
              comments: { c2: doc.comments.c2! },
              tasks: [doc.tasks[1]!, { ...doc.tasks[0]!, done: true }]
            }))
            yield* ReplicatedText.insert(ReplicatedDocument.text(laptop, ["tasks", "t2", "title"]), 7, " today")
            yield* ReplicatedText.insert(ReplicatedDocument.text(laptop, ["body"]), 0, "Tokens refresh hourly.")
            yield* connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)
          }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
        )

        // A new relay, as after a restart, loads it from the store.
        yield* Effect.scoped(
          Effect.gen(function*() {
            const phone = yield* makeDoc("phone")
            yield* synced(yield* Relay.connect(phone, { document: notes }))
            assert.deepStrictEqual(yield* ReplicatedDocument.get(phone), {
              ...initial,
              body: "Tokens refresh hourly.",
              status: "review",
              votes: 3,
              comments: { c2: { author: "ben", text: "Fine" } },
              tasks: [{ id: "t2", title: "Ship it today", done: false }, { id: "t1", title: "Write it", done: true }]
            })
            assert.deepStrictEqual(yield* Replica.version(phone), yield* Replica.version(laptop))
          }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
        )
      }).pipe(
        Effect.provide(
          HistoryStore.layerKeyValueStore({ prefix: "relay" }).pipe(Layer.provide(KeyValueStore.layerMemory))
        )
      ))

    it.effect("syncs replicas on two versions of a schema, which it doesn't need", () =>
      Effect.gen(function*() {
        // The newer version adds a field and a status.
        const DesignDocV2 = Schema.Struct({
          ...DesignDoc.fields,
          status: Schema.Literals(["draft", "review", "approved", "archived"]),
          owner: Schema.String
        })
        const makeV2 = (replica: string) =>
          ReplicatedDocument.make(DesignDocV2, {
            initial: { ...initial, owner: "nobody" },
            replica: Replica.ReplicaId.make(replica)
          })
        const old = yield* makeDoc("old")
        const updated = yield* makeV2("new")
        yield* Relay.connect(old, { document: notes })
        yield* Relay.connect(updated, { document: notes })

        yield* ReplicatedDocument.update(old, (doc) => ({ ...doc, status: "review", body: "Draft" }))
        yield* until(updated, (doc) => doc.status === "review")
        yield* ReplicatedDocument.update(updated, (doc) => ({
          ...doc,
          status: "archived",
          owner: "ada",
          body: "Draft, done"
        }))
        // The text arrives with the parts the old version can't read.
        yield* until(old, (doc) => doc.body === "Draft, done")

        const newer = { ...initial, body: "Draft, done", status: "archived", owner: "ada" } as const
        assert.deepStrictEqual(yield* ReplicatedDocument.get(updated), newer)
        // The latest status it can read.
        assert.deepStrictEqual(yield* ReplicatedDocument.get(old), {
          ...initial,
          body: "Draft, done",
          status: "review"
        })
        assert.deepStrictEqual(yield* Replica.version(old), yield* Replica.version(updated))
        // The old one kept what it can't read.
        const upgraded = yield* makeV2("upgraded")
        yield* Replica.exchange(old, upgraded)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(upgraded), newer)
      }).pipe(Effect.scoped, Effect.provide(RelayLive)))

    it.effect("learns a document's strategy from its first text op", () =>
      Effect.gen(function*() {
        const subscribeTree = Effect.gen(function*() {
          const tree = yield* makeDoc("tree")
          return yield* Effect.flip((yield* Relay.connect(tree, { document: notes })).await)
        })
        yield* Effect.scoped(
          Effect.gen(function*() {
            const relay = yield* Relay.Relay
            // Pushed straight to the relay: a subscriber would say which strategy it is.
            const laptop = yield* makeDoc("laptop", "replay")
            const push = Effect.flatMap(
              Replica.delta(laptop, VersionVector.empty),
              (delta) => relay.push({ format: Sync.FORMAT, document: notes, delta })
            )
            // A change without text ops fits either strategy.
            yield* ReplicatedDocument.update(laptop, (doc) => ({ ...doc, status: "review" }))
            yield* push
            yield* ReplicatedText.insert(ReplicatedDocument.text(laptop, ["body"]), 0, "Hello")
            yield* push

            assert.strictEqual((yield* subscribeTree).reason._tag, "ProtocolViolation")
            const phone = yield* makeDoc("phone", "replay")
            yield* synced(yield* Relay.connect(phone, { document: notes }))
            assert.deepStrictEqual(yield* ReplicatedDocument.get(phone), {
              ...initial,
              body: "Hello",
              status: "review"
            })
          }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
        )

        // A new relay learns it from the store, whose first change has no text ops.
        yield* Effect.scoped(
          Effect.gen(function*() {
            assert.strictEqual((yield* subscribeTree).reason._tag, "ProtocolViolation")
          }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
        )
      }).pipe(Effect.provide(HistoryStore.layerMemory)))

    it.effect("refuses what it can tell is malformed without the schema", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        // Pushes `changes` one at a time to a new document; the last is refused.
        const refuses = Effect.fnUntraced(
          function*(changes: ReadonlyArray<InternalDocument.AnyChange>, message: string) {
            const document = Replica.DocumentId.make(message)
            const push = (change: InternalDocument.AnyChange) =>
              relay.push({ format: Sync.FORMAT, document, delta: InternalDelta.fromChanges([change]) })
            for (const change of changes.slice(0, -1)) yield* push(change)
            const error = yield* Effect.flip(push(changes.at(-1)!))
            if (error.reason._tag !== "DeltaError" || error.reason.reason._tag !== "Malformed") {
              return assert.fail(`expected Malformed, got ${error.reason._tag}`)
            }
            assert.include(error.reason.reason.message, message)
          }
        )
        const change = (
          seq: number,
          ops: InternalDocument.AnyChange["ops"],
          more?: Partial<InternalDocument.AnyChange>
        ): InternalDocument.AnyChange => ({ replica: "r", seq, length: ops.length, parents: [], time: 5, ops, ...more })
        const set = (name: string): InternalDocument.Op => ({ _tag: "Set", path: [name], value: 1 })
        const insert = (text: string, parent: { replica: string; seq: number } | null): InternalDocument.Op => ({
          _tag: "Insert",
          path: ["body"],
          text,
          parent,
          side: "R",
          rightOrigin: null
        })

        // A change's ops on one text come together.
        yield* refuses(
          [change(0, [insert("a", null), set("status"), insert("b", { replica: "r", seq: 0 })])],
          "edits one text in more than one run of ops"
        )
        // Parents are the last ids of changes, and a change is later than its parents.
        const first = change(0, [set("status"), set("title")])
        yield* refuses(
          [first, change(2, [set("status")], { parents: [{ replica: "r", seq: 0 }], time: 6 })],
          "has a parent in the middle of a change"
        )
        yield* refuses(
          [first, change(2, [set("status")], { parents: [{ replica: "r", seq: 1 }] })],
          "has a write time no later than a change it was made on top of"
        )
        // Write times follow its clock, give or take a day.
        yield* refuses([change(0, [set("status")], { time: 2 * day })], "is stamped too far in the future")
      }).pipe(Effect.provide(RelayLive)))

    it.effect("refuses a change whose sequence numbers run past the safe integers, though it would wait", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const push = (changes: ReadonlyArray<InternalDocument.AnyChange>, user: string) =>
          relay.push({ format: Sync.FORMAT, document: spec, delta: InternalDelta.fromChanges(changes) }).pipe(
            asUser(user)
          )
        const status: InternalDocument.Op = { _tag: "Set", path: ["status"], value: "review" }
        // The second waits for a change nobody has, so nothing checks its ops
        // against its length; a relay moves the version it reports past it.
        const forged: ReadonlyArray<InternalDocument.AnyChange> = [
          { replica: "f", seq: 0, length: 1, parents: [], time: 1, ops: [status] },
          {
            replica: "f",
            seq: 1,
            length: Number.MAX_SAFE_INTEGER,
            parents: [{ replica: "ghost", seq: 0 }],
            time: 2,
            ops: [status]
          }
        ]
        const refused = yield* Effect.flip(
          Replica.receive(yield* makeDoc("replica"), InternalDelta.fromChanges(forged))
        )
        assert.strictEqual(refused.reason._tag, "Malformed")
        const error = yield* Effect.flip(push(forged, "forger"))
        assert.deepStrictEqual(error.reason, refused)
        // The document still takes everyone else's changes.
        const ada = yield* makeDoc("ada")
        yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "approved" }))
        const ack = yield* push([
          ...((yield* Replica.delta(ada, VersionVector.empty)) as InternalDelta.DeltaImpl).changes.values()
        ], "ada")
        assert.strictEqual(ack._tag, "Accepted")
      }).pipe(Effect.provide(RelayLive)))

    it.effect("passes on changes a replica's schema has another rule for, without cutting it off", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const push = (delta: Delta.Delta) => relay.push({ format: Sync.FORMAT, document: notes, delta })
        // A schema where `status` is a text, which DesignDoc has as a value.
        const Other = Schema.Struct({ status: ReplicatedDocument.Text })
        const makeOther = (replica: string) =>
          ReplicatedDocument.make(Other, { initial: { status: "" }, replica: Replica.ReplicaId.make(replica) })
        const phone = yield* makeDoc("phone")
        const other = yield* makeOther("other")
        yield* ReplicatedDocument.update(phone, (doc) => ({ ...doc, status: "review" }))
        yield* ReplicatedText.insert(ReplicatedDocument.text(other, ["status"]), 0, "Hi")
        assert.strictEqual((yield* push(yield* Replica.delta(phone, VersionVector.empty)))._tag, "Accepted")
        assert.strictEqual((yield* push(yield* Replica.delta(other, VersionVector.empty)))._tag, "Accepted")

        // Replicas of either schema take everything, and show their own.
        const everything = (yield* Stream.runHead(
          relay.subscribe({ format: Sync.FORMAT, document: notes, strategy: "tree", version: {}, authority: false })
        )).pipe(Option.getOrThrow)
        const laptop = yield* makeDoc("laptop")
        yield* Replica.receive(laptop, everything.delta)
        assert.strictEqual((yield* ReplicatedDocument.get(laptop)).status, "review")
        const later = yield* makeOther("later")
        yield* Replica.receive(later, everything.delta)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(later), { status: "Hi" })
      }).pipe(Effect.provide(RelayLive)))

    it.effect("saves a push it was interrupted saving, when it's pushed again", () => {
      const hold: { next: Deferred.Deferred<void> | undefined } = { next: undefined }
      return Effect.gen(function*() {
        const store = yield* HistoryStore.HistoryStore
        const relay = yield* Relay.Relay
        const saving = yield* Deferred.make<void>()
        hold.next = saving
        const laptop = yield* makeDoc("laptop")
        yield* ReplicatedDocument.update(laptop, (doc) => ({ ...doc, status: "review" }))
        const delta = yield* Replica.delta(laptop, VersionVector.empty)
        const push = relay.push({ format: Sync.FORMAT, document: notes, delta })

        const pushing = yield* Effect.forkChild(push)
        yield* Deferred.await(saving)
        yield* Fiber.interrupt(pushing)
        assert.strictEqual((yield* push)._tag, "Accepted")
        const stored = Option.getOrThrow(yield* store.load(notes))
        assert.isTrue(Equal.equals(stored.history, delta))
      }).pipe(Effect.provide(Relay.layer.pipe(Layer.provideMerge(holdingSaves(hold)))))
    })

    it.live("keeps serving a reloaded document's subscribers when a push waited on the old one", () => {
      const hold: { next: Deferred.Deferred<void> | undefined } = { next: undefined }
      return Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const push = (delta: Delta.Delta) => relay.push({ format: Sync.FORMAT, document: notes, delta })
        const laptop = yield* makeDoc("laptop")
        yield* ReplicatedDocument.update(laptop, (doc) => ({ ...doc, status: "review" }))
        const fromLaptop = yield* Replica.delta(laptop, VersionVector.empty)
        const phone = yield* makeDoc("phone")
        yield* ReplicatedDocument.update(phone, (doc) => ({ ...doc, votes: 1 }))
        const fromPhone = yield* Replica.delta(phone, VersionVector.empty)

        // The laptop's push is interrupted while saving, with the phone's
        // waiting behind it, so the document is reloaded.
        const laptopSaving = yield* Deferred.make<void>()
        hold.next = laptopSaving
        const pushing = yield* Effect.forkChild(push(fromLaptop))
        yield* Deferred.await(laptopSaving)
        const phoneSaving = yield* Deferred.make<void>()
        hold.next = phoneSaving
        const waiting = yield* Effect.forkChild(push(fromPhone), { startImmediately: true })
        yield* Fiber.interrupt(pushing)

        // A tablet subscribes to the reloaded document. The phone's push then
        // ends, possibly interrupted too.
        const updates = yield* Stream.toQueue(
          relay.subscribe({ format: Sync.FORMAT, document: notes, strategy: "tree", version: {}, authority: false }),
          { capacity: "unbounded" }
        )
        yield* Queue.take(updates)
        yield* Effect.raceFirst(Deferred.await(phoneSaving), Fiber.await(waiting))
        yield* Fiber.interrupt(waiting)
        hold.next = undefined

        // Pushed again, the laptop's change reaches the tablet.
        assert.strictEqual((yield* push(fromLaptop))._tag, "Accepted")
        const update = yield* Queue.take(updates).pipe(Effect.timeout("1 second"))
        assert.isTrue(Equal.equals(update.delta, fromLaptop))
      }).pipe(Effect.scoped, Effect.provide(Relay.layer.pipe(Layer.provideMerge(holdingSaves(hold)))))
    })

    it.effect("keeps nothing of a change it rejects", () =>
      Effect.gen(function*() {
        const relay = yield* Relay.Relay
        // A replay text inside `status`, edited where it has no text.
        const bad = InternalDelta.fromChanges([{
          replica: "bad",
          seq: 0,
          length: 1,
          parents: [],
          time: 0,
          ops: [{ _tag: "InsertAt", path: ["status", "note"], index: 5, text: "a" }]
        }])
        const error = yield* Effect.flip(relay.push({ format: Sync.FORMAT, document: notes, delta: bad }))
        assert.strictEqual(error.reason._tag === "DeltaError" && error.reason.reason._tag, "Malformed")

        // Neither its text nor its strategy is left behind, so a value can go
        // where the text would have been, and a tree replica can subscribe.
        const laptop = yield* makeDoc("laptop")
        yield* ReplicatedDocument.update(laptop, (doc) => ({ ...doc, status: "review" }))
        const ack = yield* relay.push({
          format: Sync.FORMAT,
          document: notes,
          delta: yield* Replica.delta(laptop, VersionVector.empty)
        })
        assert.strictEqual(ack._tag, "Accepted")
        yield* synced(yield* Relay.connect(yield* makeDoc("phone"), { document: notes }))
      }).pipe(Effect.scoped, Effect.provide(RelayLive)))
  })
})

// Only the owner approves. Every proposal the check reads goes into `read`.
const ownerApproves = (read: Array<ReplicatedDocument.Proposed<DesignDoc>> = []) =>
  Authority.layer({
    check: (proposal) =>
      Effect.gen(function*() {
        if (proposal._tag === "Text") return yield* new Authority.Rejected({ message: "Design docs aren't texts" })
        const proposed = yield* ReplicatedDocument.readProposal(proposal, DesignDoc, { initial })
        read.push(proposed)
        if (
          proposed.after.status === "approved" && proposed.before.status !== "approved" && proposal.user !== "owner"
        ) {
          return yield* new Authority.Rejected({ message: "Only the owner can approve" })
        }
      })
  }).pipe(Layer.provide(HistoryStore.layerMemory))

// A change an authority checked, during the push it came with.
interface Checked {
  readonly proposal: Authority.Proposal
  // What `readProposal` read, if the change fits the schema.
  proposed: ReplicatedDocument.Proposed<Project> | undefined
  accepted: boolean
}

// A push, as an authority took it.
interface Push {
  readonly changes: ReadonlyArray<InternalDocument.AnyChange>
  readonly exit: Exit.Exit<Sync.Ack, Sync.SyncError>
  readonly checked: ReadonlyArray<Checked>
}

const key = (change: InternalDocument.AnyChange) => `${change.seq}@${change.replica}`

const changeOf = (proposal: Authority.DocumentProposal): InternalDocument.AnyChange =>
  [...(proposal.delta as InternalDelta.DeltaImpl).changes.values()][0]!

// The changes an authority accepted from a push, in the order it checked them.
const accepted = (push: Push): Array<InternalDocument.AnyChange> =>
  push.checked.flatMap(({ accepted, proposal }) => accepted && proposal._tag === "Document" ? [changeOf(proposal)] : [])

// A replica's document as an authority's proposal has it: its JSON, and which
// parts exist.
const proposalOf = <A>(doc: ReplicatedDocument.ReplicatedDocument<A>) => {
  const state = (doc as unknown as { readonly engine: InternalEngine.Engine }).engine.state.document
  const exists: Array<Replica.Path> = []
  return { json: InternalDocument.toJson(state, exists), exists, text: InternalDocument.textAt(state, []) }
}

// The paths a change writes to, each once, in order.
const pathsOf = (change: InternalDocument.AnyChange): Array<Replica.Path> => {
  const paths = new Map<string, Replica.Path>()
  for (const op of change.ops) paths.set(JSON.stringify(op.path ?? []), op.path ?? [])
  return [...paths.values()]
}

// Whether a change removes a part that nothing in `changes`, or before it in
// the change, wrote as existing or not.
const removesUnadded = (change: InternalDocument.AnyChange, changes: ReadonlyArray<InternalDocument.AnyChange>) =>
  change.ops.some((op, i) => {
    const writes = (other: InternalDocument.Op) =>
      (other._tag === "Set" || other._tag === "Add" || other._tag === "Unset") &&
      JSON.stringify(other.path) === JSON.stringify(op.path)
    return op._tag === "Unset" && !changes.some(({ ops }) => ops.some(writes)) &&
      !change.ops.slice(0, i).some((other) => other._tag !== "Unset" && writes(other))
  })

// The change a message from an authority is about.
const named = (changes: ReadonlyArray<InternalDocument.AnyChange>, message: string): InternalDocument.AnyChange => {
  const found = changes.find((change) => message.startsWith(`Change ${key(change)} `))
  assert.isDefined(found, message)
  return found!
}

const projectShape = InternalShape.compile(Project)

// Checks what an authority did with a push against what replicas do with the
// same changes. `replica` has the changes it accepted before the push, and
// takes in each it accepts from it, one at a time in the order it checked
// them; every proposal must show the document as `replica` has it around that
// change. `relayed` takes in every push whole, as a relay does: whether a
// change is malformed doesn't depend on what else a replica has, so it judges
// each change as the authority must. `make` makes a replica.
const checkPush = Effect.fnUntraced(function*(push: Push, { make, relayed, replica }: {
  readonly replica: ReplicatedDocument.ReplicatedDocument<Project>
  readonly relayed: ReplicatedDocument.ReplicatedDocument<Project>
  readonly make: (replica: string) => Effect.Effect<ReplicatedDocument.ReplicatedDocument<Project>>
}) {
  const { changes, checked, exit } = push
  const version = yield* Replica.version(replica)
  // What's new to the authority. It ignores the rest, as replicas do.
  const fresh = changes.filter((change) => change.seq + change.length > (version[change.replica] ?? 0))
  const relayedExit = yield* Effect.exit(Replica.receive(relayed, InternalDelta.fromChanges(changes)))
  const kept = ((yield* Replica.delta(relayed, VersionVector.empty)) as InternalDelta.DeltaImpl).changes
  for (const { accepted, proposal, proposed } of checked) {
    const before = proposalOf(replica)
    if (proposal._tag === "Text") {
      // A change all at the root, where a document has no text.
      assert.strictEqual(proposal.before, before.text)
      continue
    }
    const change = changeOf(proposal)
    const shown = JSON.stringify(change)
    // Checked, so it's well-formed: a replica takes it.
    assert.isTrue(kept.has(key(change)), shown)
    assert.deepStrictEqual(proposal.paths, pathsOf(change), shown)
    assert.deepStrictEqual([proposal.before, proposal.exists.before], [before.json, before.exists], shown)
    const value = yield* ReplicatedDocument.get(replica)
    // Turned down, it's shown as a replica would show it all the same.
    const doc = accepted ? replica : yield* make("turned-down")
    if (!accepted) yield* Replica.receive(doc, yield* Replica.delta(replica, VersionVector.empty))
    yield* Replica.receive(doc, InternalDelta.fromChanges([change]))
    assert.strictEqual(
      VersionVector.get(yield* Replica.version(doc), change.replica),
      change.seq + change.length,
      shown
    )
    const after = proposalOf(doc)
    assert.deepStrictEqual([proposal.after, proposal.exists.after], [after.json, after.exists], shown)
    if (proposed !== undefined) {
      assert.deepStrictEqual([proposed.before, proposed.after], [value, yield* ReplicatedDocument.get(doc)], shown)
    }
  }

  const took = accepted(push)
  if (Exit.isSuccess(exit)) {
    if (exit.value._tag === "Accepted") {
      // It took everything new to it, as a replica does.
      assert.deepStrictEqual(fresh.map(key).sort(), took.map(key).sort())
      assert.isTrue(Exit.isSuccess(relayedExit), String(relayedExit))
      return
    }
    const last = checked.at(-1)
    if (last !== undefined && !last.accepted) return
    // Turned down before its check, so a proposal never has two kinds of
    // thing at one path: a change a replica takes, but that doesn't fit the
    // schema, or removes what nobody wrote, which replicas never do.
    const change = named(fresh, exit.value.message)
    assert.isTrue(kept.has(key(change)), exit.value.message)
    assert.isTrue(
      InternalShape.misfit(projectShape, change) !== undefined ||
        removesUnadded(change, [
          ...((yield* Replica.delta(replica, VersionVector.empty)) as InternalDelta.DeltaImpl).changes.values()
        ]),
      exit.value.message
    )
    return
  }
  const error = Option.getOrThrow(Cause.findErrorOption(exit.cause))
  if (error.reason._tag !== "DeltaError" || error.reason.reason._tag !== "Malformed") {
    // Refused whole.
    assert.deepStrictEqual(took, [], String(error))
    return
  }
  const { message } = error.reason.reason
  const change = named(changes, message)
  if (message.endsWith("builds on changes the authority doesn't have")) {
    // Which a replica keeps waiting.
    const has = yield* Replica.version(replica)
    assert.isFalse(
      (has[change.replica] ?? 0) === change.seq &&
        change.parents.every(({ replica, seq }) => seq < (has[replica] ?? 0)),
      message
    )
  } else {
    // A replica refuses it too.
    assert.isFalse(kept.has(key(change)), message)
    assert.isTrue(Exit.isFailure(relayedExit), message)
  }
})

describe("Authority", () => {
  it.effect("shares what it accepts and rolls back what it rejects", () =>
    Effect.gen(function*() {
      const editor = yield* ReplicatedText.makeConfirmed({ document: spec })
      const reviewer = yield* ReplicatedText.makeConfirmed({ document: spec })
      yield* synced(reviewer.connection)
      const before = yield* Replica.id(reviewer)
      const { edits } = yield* ReplicatedText.subscribe(reviewer)

      yield* ReplicatedText.insert(editor, 0, "Spec v1")
      yield* showing(reviewer, "Spec v1")

      yield* ReplicatedText.insert(reviewer, 0, "secret ")
      const rejected = yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
      assert.deepStrictEqual(Option.getOrThrow(rejected), {
        _tag: "Rejected",
        patch: [{ _tag: "Remove", length: 7 }],
        message: "No secrets"
      })
      assert.strictEqual(yield* ReplicatedText.get(reviewer), "Spec v1")
      assert.notStrictEqual(yield* Replica.id(reviewer), before)

      // The reviewer carries on under the new id, and the editor never saw it.
      yield* ReplicatedText.insert(reviewer, 7, ", reviewed")
      yield* showing(editor, "Spec v1, reviewed")
    }).pipe(Effect.scoped, Effect.provide([NoSecrets, CryptoTest])))

  it.effect("drops unconfirmed changes made on top of a rejected one", () =>
    Effect.gen(function*() {
      const gate = yield* Deferred.make<void>()
      const Gated = Layer.effect(Authority.Authority)(Effect.gen(function*() {
        const authority = yield* Authority.Authority
        return {
          subscribe: authority.subscribe,
          push: (request: Sync.PushRequest) => Effect.andThen(Deferred.await(gate), authority.push(request))
        }
      })).pipe(Layer.provide(NoSecrets))

      yield* Effect.gen(function*() {
        const notes = yield* ReplicatedText.makeConfirmed({ document: spec })
        yield* synced(notes.connection)
        // Held at the gate, then rejected.
        yield* ReplicatedText.insert(notes, 0, "a secret")
        // Made on top of it before the answer comes back.
        yield* ReplicatedText.insert(notes, 8, " plan")
        yield* Deferred.succeed(gate, undefined)
        yield* showing(notes, "")
        yield* ReplicatedText.insert(notes, 0, "a plan")
        yield* showing(notes, "a plan")
        const other = yield* ReplicatedText.makeConfirmed({ document: spec })
        yield* showing(other, "a plan")
      }).pipe(Effect.provide(Gated))
    }).pipe(Effect.scoped, Effect.provide(CryptoTest)))

  it.effect("tells a text of a rejection that leaves it as it was", () =>
    Effect.gen(function*() {
      const gate = yield* Deferred.make<void>()
      const Gated = Layer.effect(Authority.Authority)(Effect.gen(function*() {
        const authority = yield* Authority.Authority
        return {
          subscribe: authority.subscribe,
          push: (request: Sync.PushRequest) => Effect.andThen(Deferred.await(gate), authority.push(request))
        }
      })).pipe(Layer.provide(NoSecrets))

      yield* Effect.gen(function*() {
        const notes = yield* ReplicatedText.makeConfirmed({ document: spec })
        yield* synced(notes.connection)
        const { edits } = yield* ReplicatedText.subscribe(notes)
        // Held at the gate, then rejected, but removed before the answer
        // comes back: rolling both back leaves the text as it was.
        yield* ReplicatedText.insert(notes, 0, "secret")
        yield* ReplicatedText.remove(notes, 0, 6)
        yield* Deferred.succeed(gate, undefined)
        yield* Stream.runHead(Stream.filter(notes.connection.unconfirmed, (count) => count === 0))
        yield* ReplicatedText.insert(notes, 0, "a plan")
        let local = 0
        const told = yield* edits.pipe(
          Stream.takeUntil((edit) => edit._tag === "Local" && ++local === 3),
          Stream.runCollect
        )
        assert.deepStrictEqual(told.map(({ _tag }) => _tag), ["Local", "Local", "Rejected", "Local"])
      }).pipe(Effect.provide(Gated))
    }).pipe(Effect.scoped, Effect.provide(CryptoTest)))

  it.effect("rolls back what others made on top of a rejected change, when an authority that lost it rejects it", () =>
    Effect.gen(function*() {
      // An authority that keeps documents only in memory, restarted with a
      // stricter check.
      const Lenient = Authority.layer({ check: () => Effect.void }).pipe(Layer.provide(HistoryStore.layerMemory))
      const before = Context.get(yield* Layer.build(Layer.fresh(Lenient)), Authority.Authority)
      const after = Context.get(yield* Layer.build(Layer.fresh(NoSecrets)), Authority.Authority)
      let current = before
      // The laptop's answers from before the restart never arrive.
      const restarted = yield* Deferred.make<void>()
      const restarting = Authority.Authority.of({
        subscribe: (request) => Stream.unwrap(Effect.sync(() => current.subscribe(request))),
        push: (request) =>
          Effect.suspend(() =>
            current === before
              ? before.push(request).pipe(
                Effect.andThen(Deferred.await(restarted)),
                Effect.andThen(Effect.fail(new Sync.SyncError({ reason: new Sync.Disconnected({ cause: "restart" }) })))
              )
              : current.push(request)
          )
      })

      const laptop = yield* ReplicatedText.makeConfirmed({ document: spec, retry: Schedule.forever }).pipe(
        Effect.provideService(Authority.Authority, restarting)
      )
      const tablet = yield* ReplicatedText.makeConfirmed({ document: spec }).pipe(
        Effect.provideService(Authority.Authority, before)
      )
      yield* synced(laptop.connection)
      yield* synced(tablet.connection)
      yield* ReplicatedText.insert(laptop, 0, "a secret")
      // Built on the laptop's change, which the laptop hasn't heard is accepted.
      yield* showing(tablet, "a secret")
      yield* ReplicatedText.insert(tablet, 8, " plan")
      yield* showing(laptop, "a secret plan")

      const { edits } = yield* ReplicatedText.subscribe(laptop)
      current = after
      yield* Deferred.succeed(restarted, undefined)
      // The restarted authority has neither change, and rejects the laptop's.
      const rejected = yield* Effect.raceFirst(
        edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead),
        laptop.connection.await
      )
      assert.deepStrictEqual(Option.getOrThrow(rejected), {
        _tag: "Rejected",
        patch: [{ _tag: "Remove", length: 13 }],
        message: "No secrets"
      })
      assert.strictEqual(yield* ReplicatedText.get(laptop), "")

      // The laptop carries on with the restarted authority.
      yield* ReplicatedText.insert(laptop, 0, "a plan")
      const phone = yield* ReplicatedText.makeConfirmed({ document: spec }).pipe(
        Effect.provideService(Authority.Authority, after)
      )
      yield* showing(phone, "a plan")
    }).pipe(Effect.scoped, Effect.provide(CryptoTest)))

  it.effect("rolls back a change it had confirmed, when an authority that lost it rejects it", () =>
    Effect.gen(function*() {
      // An authority that keeps documents only in memory, restarted with a
      // stricter check.
      const Lenient = Authority.layer({ check: () => Effect.void }).pipe(Layer.provide(HistoryStore.layerMemory))
      const before = Context.get(yield* Layer.build(Layer.fresh(Lenient)), Authority.Authority)
      const after = Context.get(yield* Layer.build(Layer.fresh(NoSecrets)), Authority.Authority)
      let current = before
      let lost = yield* Deferred.make<never, Sync.SyncError>()
      let pushes = 0
      // Told once the laptop has pushed to the restarted authority many times.
      const again = yield* Deferred.make<void>()
      const restarting = Authority.Authority.of({
        subscribe: (request) =>
          Stream.unwrap(Effect.sync(() => current.subscribe(request).pipe(Stream.interruptWhen(Deferred.await(lost))))),
        push: (request) =>
          Effect.suspend(() => {
            pushes++
            return Effect.andThen(pushes > 10 ? Deferred.succeed(again, undefined) : Effect.void, current.push(request))
          })
      })

      const laptop = yield* ReplicatedText.makeConfirmed({ document: spec, retry: Schedule.forever }).pipe(
        Effect.provideService(Authority.Authority, restarting)
      )
      yield* synced(laptop.connection)
      yield* ReplicatedText.insert(laptop, 0, "a secret")
      yield* Stream.runHead(Stream.filter(laptop.connection.unconfirmed, (count) => count === 0))

      const { edits } = yield* ReplicatedText.subscribe(laptop)
      const disconnected = lost
      lost = yield* Deferred.make<never, Sync.SyncError>()
      current = after
      yield* Deferred.fail(disconnected, new Sync.SyncError({ reason: new Sync.Disconnected({ cause: "restart" }) }))
      // The restarted authority doesn't have the change, and rejects it when
      // it's pushed again.
      const rejected = yield* Effect.raceFirst(
        edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead),
        Effect.andThen(
          Deferred.await(again),
          Effect.die(new Error("The laptop pushes the rejected change again and again"))
        )
      )
      assert.deepStrictEqual(Option.getOrThrow(rejected), {
        _tag: "Rejected",
        patch: [{ _tag: "Remove", length: 8 }],
        message: "No secrets"
      })
      assert.strictEqual(yield* ReplicatedText.get(laptop), "")

      // The laptop stops pushing it, and carries on with the restarted authority.
      const pushed = pushes
      yield* ReplicatedText.insert(laptop, 0, "a plan")
      const phone = yield* ReplicatedText.makeConfirmed({ document: spec }).pipe(
        Effect.provideService(Authority.Authority, after)
      )
      yield* showing(phone, "a plan")
      assert.strictEqual(pushes, pushed + 1)
    }).pipe(Effect.scoped, Effect.provide(CryptoTest)))

  it.effect("accepts a push up to the first change it rejects", () =>
    Effect.gen(function*() {
      const authority = yield* Authority.Authority
      const laptop = yield* make("laptop")
      yield* ReplicatedText.insert(laptop, 0, "a")
      yield* ReplicatedText.insert(laptop, 1, " secret")
      yield* ReplicatedText.insert(laptop, 0, "b")
      const delta = yield* Replica.delta(laptop, VersionVector.empty)
      const ack = yield* authority.push({ format: Sync.FORMAT, document: spec, delta })
      assert.deepStrictEqual(ack, new Sync.PushRejected({ confirmed: { laptop: 1 }, message: "No secrets" }))
    }).pipe(Effect.provide(NoSecrets)))

  it.effect("rejects a push that builds on changes it doesn't have", () =>
    Effect.gen(function*() {
      const authority = yield* Authority.Authority
      const alice = yield* make("alice")
      const bob = yield* make("bob")
      yield* ReplicatedText.insert(alice, 0, "Hello")
      yield* Replica.exchange(alice, bob)
      yield* ReplicatedText.insert(bob, 5, "!")
      const delta = yield* Replica.delta(bob, yield* Replica.version(alice))
      const error = yield* Effect.flip(authority.push({ format: Sync.FORMAT, document: spec, delta }))
      assert.strictEqual(error.reason._tag, "DeltaError")
    }).pipe(Effect.provide(NoSecrets)))

  it.effect("keeps its subscribers through a push interrupted while it's checked", () => {
    // Holds the check while `checking` is set, and tells it the check started.
    const hold: { checking: Deferred.Deferred<void> | undefined } = { checking: undefined }
    const Holding = Authority.layer({
      check: () =>
        hold.checking === undefined
          ? Effect.void
          : Effect.andThen(Deferred.succeed(hold.checking, undefined), Effect.never)
    }).pipe(Layer.provide(HistoryStore.layerMemory))
    return Effect.gen(function*() {
      const authority = yield* Authority.Authority
      const updates = yield* Stream.toQueue(
        authority.subscribe({ format: Sync.FORMAT, document: spec, strategy: "tree", version: {}, authority: true }),
        { capacity: "unbounded" }
      )
      yield* Queue.take(updates)
      const laptop = yield* make("laptop")
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      const delta = yield* Replica.delta(laptop, VersionVector.empty)
      const push = authority.push({ format: Sync.FORMAT, document: spec, delta })

      const checking = yield* Deferred.make<void>()
      hold.checking = checking
      const pushing = yield* Effect.forkChild(push)
      yield* Deferred.await(checking)
      hold.checking = undefined
      yield* Fiber.interrupt(pushing)

      // Nothing was saved, so the subscriber is still there, and the change
      // is checked again when it's pushed again.
      assert.strictEqual((yield* push)._tag, "Accepted")
      const update = yield* Queue.take(updates)
      assert.isTrue(Equal.equals(update.delta, delta))
    }).pipe(Effect.scoped, Effect.provide(Holding))
  })

  it.effect("drops what a push applied when its check dies", () => {
    const check = { dies: true }
    const Dying = Authority.layer({
      check: () => check.dies ? Effect.die(new Error("A bug in the check")) : Effect.void
    }).pipe(Layer.provideMerge(HistoryStore.layerMemory))
    return Effect.gen(function*() {
      const store = yield* HistoryStore.HistoryStore
      const authority = yield* Authority.Authority
      const laptop = yield* make("laptop")
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      const delta = yield* Replica.delta(laptop, VersionVector.empty)
      const push = authority.push({ format: Sync.FORMAT, document: spec, delta })

      assert.isTrue(Exit.hasDies(yield* Effect.exit(push)))
      // Pushed again once the check is fixed, it's checked and saved.
      check.dies = false
      assert.strictEqual((yield* push)._tag, "Accepted")
      const stored = Option.getOrThrow(yield* store.load(spec))
      assert.isTrue(Equal.equals(stored.history, delta))
    }).pipe(Effect.provide(Dying))
  })

  it.effect("a confirmed replica syncs with nothing else", () =>
    Effect.gen(function*() {
      const confirmed = yield* ReplicatedText.makeConfirmed({ document: spec })
      const peer = yield* make("peer")
      const exchanged = yield* Effect.flip(Replica.exchange(peer, confirmed))
      assert.strictEqual(exchanged.reason._tag, "AuthorityOnly")
      const received = yield* Effect.flip(Replica.receive(confirmed, yield* Replica.delta(peer, VersionVector.empty)))
      assert.strictEqual(received.reason._tag, "AuthorityOnly")
    }).pipe(Effect.scoped, Effect.provide([NoSecrets, CryptoTest])))

  it.effect("refuses replicas that don't expect an authority", () =>
    Effect.gen(function*() {
      const connection = yield* Relay.connect(yield* make("laptop"), { document: spec })
      const error = yield* Effect.flip(connection.await)
      assert.strictEqual(error.reason._tag, "ProtocolViolation")
    }).pipe(
      Effect.scoped,
      // An authority served as if it were a relay.
      Effect.provide(Layer.effect(Relay.Relay)(Effect.service(Authority.Authority)).pipe(Layer.provide(NoSecrets)))
    ))

  describe("documents", () => {
    it.effect("a text field hears of a rejection only if it rolls the text back", () =>
      Effect.gen(function*() {
        const editor = yield* ReplicatedDocument.makeConfirmed(DesignDoc, { initial, document: spec }).pipe(
          asUser("editor")
        )
        yield* synced(editor.connection)
        const body = ReplicatedDocument.text(editor, ["body"])
        const fromBody = (yield* ReplicatedText.subscribe(body)).edits
        const fromDoc = (yield* ReplicatedDocument.subscribe(editor)).edits
        // Rejected, and the body didn't change.
        yield* ReplicatedDocument.update(editor, (doc) => ({ ...doc, status: "approved" }))
        yield* fromDoc.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
        yield* ReplicatedText.insert(body, 0, "Hi")
        const first = Option.getOrThrow(yield* Stream.runHead(fromBody))
        assert.strictEqual(first._tag, "Local")
      }).pipe(Effect.scoped, Effect.provide([ownerApproves(), CryptoTest])))

    it.effect("checks documents with their schema, and rolls back what it rejects", () => {
      const proposals: Array<ReplicatedDocument.Proposed<DesignDoc>> = []
      const open = (user: string) =>
        ReplicatedDocument.makeConfirmed(DesignDoc, { initial, document: spec }).pipe(asUser(user))

      return Effect.gen(function*() {
        const owner = yield* open("owner")
        const editor = yield* open("editor")
        yield* synced(editor.connection)
        const before = yield* Replica.id(editor)
        const { edits } = yield* ReplicatedDocument.subscribe(editor)

        yield* ReplicatedDocument.update(
          editor,
          (doc) => ({ ...doc, body: "Sessions last 24 hours.", status: "review" })
        )
        const inReview = { ...initial, body: "Sessions last 24 hours.", status: "review" } as const
        yield* until(owner, (doc) => doc.status === "review")
        assert.deepStrictEqual(proposals, [{ before: initial, after: inReview, paths: [["body"], ["status"]] }])

        yield* ReplicatedDocument.update(editor, (doc) => ({ ...doc, status: "approved" }))
        const rejected = yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
        assert.deepStrictEqual(Option.getOrThrow(rejected), {
          _tag: "Rejected",
          value: inReview,
          message: "Only the owner can approve"
        })
        assert.deepStrictEqual(yield* ReplicatedDocument.get(editor), inReview)
        assert.notStrictEqual(yield* Replica.id(editor), before)

        // The editor carries on under the new id, and the owner never saw the approval.
        yield* ReplicatedText.update(ReplicatedDocument.text(editor, ["body"]), (text) => text.replace("24", "8"))
        yield* until(owner, (doc) => doc.body === "Sessions last 8 hours.")
        assert.strictEqual((yield* ReplicatedDocument.get(owner)).status, "review")

        // The owner's approval is accepted, and reaches everyone.
        yield* ReplicatedDocument.update(owner, (doc) => ({ ...doc, status: "approved" }))
        yield* until(editor, (doc) => doc.status === "approved")
      }).pipe(Effect.scoped, Effect.provide([ownerApproves(proposals), CryptoTest]))
    })

    it.effect("rejects a change that would make a path hold two kinds of thing", () =>
      Effect.gen(function*() {
        const authority = yield* Authority.Authority
        // Pushes `changes` one at a time to a new document; the last is
        // rejected, as a check rejects, so a confirmed replica rolls it back.
        const refuses = Effect.fnUntraced(
          function*(changes: ReadonlyArray<InternalDocument.AnyChange>, message: string) {
            const document = Replica.DocumentId.make(message)
            const push = (change: InternalDocument.AnyChange) =>
              authority.push({ format: Sync.FORMAT, document, delta: InternalDelta.fromChanges([change]) })
            for (const change of changes.slice(0, -1)) yield* push(change)
            const ack = yield* push(changes.at(-1)!)
            if (ack._tag !== "Rejected") return assert.fail(`expected Rejected for ${message}`)
            assert.include(ack.message, message)
          }
        )
        // Each change is made on top of the one before.
        const change = (seq: number, ops: InternalDocument.AnyChange["ops"]): InternalDocument.AnyChange => ({
          replica: "r",
          seq,
          length: ops.length,
          parents: seq === 0 ? [] : [{ replica: "r", seq: seq - 1 }],
          time: seq,
          ops
        })
        const place = (path: ReadonlyArray<string>): InternalDocument.Op => ({
          _tag: "Place",
          path,
          parent: null,
          side: "R",
          rightOrigin: null
        })

        yield* refuses(
          [change(0, [{ _tag: "Set", path: ["votes"], value: 1 }, { _tag: "Increment", path: ["votes"], by: 1 }])],
          "writes a counter where there's a value"
        )
        // The root is a struct or a record, never a list.
        yield* refuses(
          [change(0, [{ _tag: "Set", path: ["status"], value: "approved" }, place(["status"])])],
          "places a list item in the root"
        )
        // Nor is a record or struct that has parts.
        yield* refuses(
          [change(0, [{ _tag: "Add", path: ["comments", "c1"] }]), change(1, [place(["comments", "c2"])])],
          "places a list item among parts that aren't items"
        )
        // A list item is never a text or a counter.
        yield* refuses(
          [change(0, [place(["tasks", "t1"]), { _tag: "Increment", path: ["tasks", "t1"], by: 1 }])],
          "writes a counter where there's a list item"
        )
      }).pipe(Effect.provide(NoSecrets)))

    it.effect("refuses a malformed change as replicas do, though it would make a path hold two kinds of thing", () =>
      Effect.gen(function*() {
        const authority = yield* Authority.Authority
        // Stamped no later than the start of the document, and writing a
        // counter where it writes a value.
        const delta = InternalDelta.fromChanges([{
          replica: "r",
          seq: 0,
          length: 2,
          parents: [],
          time: -1,
          ops: [{ _tag: "Set", path: ["votes"], value: 1 }, { _tag: "Increment", path: ["votes"], by: 1 }]
        }])
        const refused = yield* Effect.flip(Replica.receive(yield* makeDoc("replica"), delta))
        const pushed = yield* Effect.exit(authority.push({ format: Sync.FORMAT, document: spec, delta }))
        assert.deepStrictEqual(
          Exit.map(pushed, (ack) => ack._tag),
          Exit.fail(new Sync.SyncError({ reason: refused }))
        )
      }).pipe(Effect.provide(NoSecrets)))

    it.effect("rejects a change that doesn't fit the schema, so a check reads what replicas show", () =>
      Effect.gen(function*() {
        const authority = yield* Authority.Authority
        const owner = yield* makeDoc("owner")
        yield* ReplicatedDocument.update(owner, (doc) => ({ ...doc, status: "review" }))
        const fromOwner = yield* Replica.delta(owner, VersionVector.empty)
        yield* authority.push({ format: Sync.FORMAT, document: spec, delta: fromOwner }).pipe(asUser("owner"))
        const parent = { replica: "owner", seq: VersionVector.get(yield* Replica.version(owner), "owner") - 1 }
        const rejects = Effect.fnUntraced(function*(ops: InternalDocument.AnyChange["ops"], message: string) {
          const replica = Replica.ReplicaId.make(`intruder-${message.length}`)
          const delta = InternalDelta.fromChanges([{
            replica,
            seq: 0,
            length: ops.length,
            parents: [parent],
            time: 1000,
            ops
          }])
          const ack = yield* authority.push({ format: Sync.FORMAT, document: spec, delta }).pipe(asUser("intruder"))
          if (ack._tag !== "Rejected") return assert.fail(`expected Rejected for ${message}`)
          assert.include(ack.message, message)
        })
        // Each would approve where replicas look, and hide it from the JSON the
        // check reads: written over with a value replicas can't read, or with
        // the required field removed, which replicas ignore.
        yield* rejects(
          [{ _tag: "Set", path: ["status"], value: "approved" }, { _tag: "Set", path: ["status"], value: 42 }],
          "a value the schema can't read"
        )
        yield* rejects(
          [{ _tag: "Set", path: ["status"], value: "approved" }, { _tag: "Unset", path: ["status"] }],
          "with a Unset op, where the schema has a value"
        )
        yield* rejects(
          [{ _tag: "Set", path: ["tasks", "x"], value: 1 }],
          "with a Set op, where the schema has a struct"
        )
        yield* rejects([{ _tag: "Set", path: ["owner"], value: "intruder" }], "which the schema doesn't have")
        // A task that says it's another one, which replicas leave out.
        yield* rejects(
          [
            { _tag: "Add", path: ["tasks", "t1"] },
            { _tag: "Set", path: ["tasks", "t1", "id"], value: "t2" },
            { _tag: "Set", path: ["tasks", "t1", "done"], value: false },
            { _tag: "Place", path: ["tasks", "t1"], parent: null, side: "R", rightOrigin: null }
          ],
          "a value other than its item's key"
        )
      }).pipe(Effect.provide(ownerApproves())))

    it.effect.prop(
      "accepts every honest change, and its check reads each one as replicas show it",
      { session: DocumentSession },
      Effect.fnUntraced(function*({ session }) {
        const proposals: Array<
          { readonly proposed: ReplicatedDocument.Proposed<typeof Board.Type>; readonly delta: Delta.Delta }
        > = []
        const Reading = Authority.layer({
          check: (proposal) =>
            proposal._tag === "Text"
              ? Effect.fail(new Authority.Rejected({ message: "Boards aren't texts" }))
              : Effect.map(ReplicatedDocument.readProposal(proposal, Board, { initial: initialBoard }), (proposed) => {
                proposals.push({ proposed, delta: proposal.delta })
              })
        }).pipe(Layer.provide(HistoryStore.layerMemory))
        yield* Effect.gen(function*() {
          const authority = yield* Authority.Authority
          const boards = yield* makeBoards("tree")
          for (const command of session) yield* stepBoards(boards, command)
          yield* syncAll(boards)
          const delta = yield* Replica.delta(boards[0]!, VersionVector.empty)
          assert.strictEqual((yield* authority.push({ format: Sync.FORMAT, document: spec, delta }))._tag, "Accepted")

          // A replica that receives the changes in the order they were checked.
          const replica = yield* ReplicatedDocument.make(Board, {
            initial: initialBoard,
            replica: Replica.ReplicaId.make("replica")
          })
          for (const { delta, proposed } of proposals) {
            assert.deepStrictEqual(proposed.before, yield* ReplicatedDocument.get(replica))
            yield* Replica.receive(replica, delta)
            assert.deepStrictEqual(proposed.after, yield* ReplicatedDocument.get(replica))
          }
        }).pipe(Effect.provide(Reading))
      }),
      { arbitrary: documentRuns }
    )

    it.effect.prop(
      "takes or turns down each change as replicas do, checks each as replicas show it, and confirmed replicas end where it does",
      { session: ForgedSession, edits: EditSession },
      Effect.fnUntraced(function*({ edits, session }) {
        // Every change checked during the push it came with, in the order it
        // was checked, with what the check read. It rejects approvals, and
        // more than two votes.
        let checked: Array<Checked> = []
        const Reading = Authority.layer({
          check: (proposal) => {
            const entry: Checked = { proposal, proposed: undefined, accepted: false }
            checked.push(entry)
            return proposal._tag === "Text"
              ? Effect.fail(new Authority.Rejected({ message: "Projects aren't texts" }))
              : Effect.flatMap(
                ReplicatedDocument.readProposal(proposal, Project, { initial: initialProject }),
                (proposed) => {
                  entry.proposed = proposed
                  entry.accepted = !(proposed.after.status === "done" && proposed.before.status !== "done") &&
                    proposed.after.votes <= 2
                  return entry.accepted ? Effect.void : Effect.fail(new Authority.Rejected({ message: "No" }))
                }
              )
          }
        }).pipe(Layer.provide(HistoryStore.layerMemory))
        yield* Effect.gen(function*() {
          const authority = yield* Authority.Authority
          // Every push, in the order the authority took them: one at a time,
          // and each finishes, as on a server, even when its pusher stops
          // waiting for it and never hears back. Reached as over RPC.
          const pushes: Array<Push> = []
          const one = yield* Semaphore.make(1)
          const remote: Sync.Server = {
            ...overJson(authority),
            push: (request) =>
              Effect.gen(function*() {
                checked = []
                const exit = yield* Effect.exit(overJson(authority).push(request))
                pushes.push({
                  changes: [...(request.delta as InternalDelta.DeltaImpl).changes.values()],
                  exit,
                  checked
                })
                return yield* exit
              }).pipe(Effect.uninterruptible, one.withPermit)
          }
          // An editor making its own edits among the forged changes, some
          // rejected, and some made on top of ones that will be. It saves what
          // it has, and now and then it's closed and opened again from that.
          const outer = yield* Effect.scope
          const open = Effect.gen(function*() {
            const scope = yield* Scope.fork(outer)
            const editor = yield* Effect.gen(function*() {
              const editor = yield* ReplicatedDocument.makeConfirmed(Project, {
                initial: initialProject,
                document: spec
              })
              yield* Effect.orDie(HistoryStore.persist(editor, spec))
              return editor
            }).pipe(Effect.provideService(Authority.Authority, remote), asUser("editor"), Scope.provide(scope))
            return { editor, scope }
          })
          let current = yield* open
          yield* synced(current.editor.connection)
          const reopen = Effect.gen(function*() {
            yield* Scope.close(current.scope, Exit.void)
            current = yield* open
          }).pipe(Effect.provideContext(yield* Effect.context<Crypto.Crypto | HistoryStore.HistoryStore>()))
          // Until the editor has heard back about everything it sent, and has
          // everything the authority accepted.
          const settled = Effect.gen(function*() {
            for (let i = 0; i < 1000; i++) {
              const has = Effect.orDie(Stream.runHead(authority.subscribe({
                format: Sync.FORMAT,
                document: spec,
                strategy: "tree",
                version: yield* Replica.version(current.editor),
                authority: true
              })))
              if (
                (yield* unconfirmed(current.editor.connection)) === 0 &&
                Delta.isEmpty(Option.getOrThrow(yield* has).delta)
              ) {
                return
              }
              yield* Effect.yieldNow
            }
            assert.fail("The editor didn't settle")
          })
          const edit = (command: (typeof edits)[number]) =>
            editProject(current.editor, command, { wait: settled, reopen })
          let next = 0
          yield* forgeSession("tree", session, (changes) =>
            Effect.gen(function*() {
              const command = edits[next++]
              if (command !== undefined) yield* edit(command)
              const delta = InternalDelta.fromChanges(changes)
              yield* Effect.exit(remote.push({ format: Sync.FORMAT, document: spec, delta }).pipe(asUser("forger")))
              const kept = new Set(pushes.flatMap((push) => accepted(push).map(key)))
              return (change: InternalDocument.AnyChange) => kept.has(key(change))
            }))
          for (const command of edits.slice(next)) yield* edit(command)
          yield* settled

          // A replica that receives the changes the authority accepted, in
          // the order it checked them, and one that receives every push, as
          // a relay would take them.
          const make = (replica: string) =>
            ReplicatedDocument.make(Project, { initial: initialProject, replica: Replica.ReplicaId.make(replica) })
          const replica = yield* make("replica")
          const relayed = yield* make("relayed")
          for (const push of pushes) yield* checkPush(push, { replica, relayed, make })
          assert.deepStrictEqual(yield* ReplicatedDocument.get(current.editor), yield* ReplicatedDocument.get(replica))
          // Opened again from what it saved, it ends there too.
          yield* reopen
          yield* settled
          assert.deepStrictEqual(yield* ReplicatedDocument.get(current.editor), yield* ReplicatedDocument.get(replica))
        }).pipe(
          Effect.scoped,
          Effect.provide([
            Reading,
            CryptoTest,
            HistoryStore.layerKeyValueStore({ prefix: "editor" }).pipe(Layer.provide(KeyValueStore.layerMemory))
          ])
        )
      }),
      { arbitrary: forgedRuns }
    )

    it.effect("a change that fits the schema can't stop a list from starting", () =>
      Effect.gen(function*() {
        const authority = yield* Authority.Authority
        // A task written at, but never placed in the list: replicas don't
        // show it, and the authority can't tell it from a record's entry.
        const unplaced: ReadonlyArray<InternalDocument.Op> = [
          { _tag: "Unset", path: ["tasks", "t9"] },
          { _tag: "Add", path: ["tasks", "t9"] }
        ]
        for (const op of unplaced) {
          const document = Replica.DocumentId.make(op._tag)
          const push = (delta: Delta.Delta) => authority.push({ format: Sync.FORMAT, document, delta })
          const intruder = InternalDelta.fromChanges([{
            replica: "intruder",
            seq: 0,
            length: 1,
            parents: [],
            time: 0,
            ops: [op]
          }])
          yield* Effect.exit(push(intruder).pipe(asUser("intruder")))
          // The list's first task is still accepted.
          const laptop = yield* makeDoc(`laptop-${op._tag}`)
          yield* ReplicatedDocument.update(laptop, (doc) => ({ ...doc, tasks: [{ id: "t1", title: "", done: false }] }))
          const ack = yield* push(yield* Replica.delta(laptop, VersionVector.empty))
          assert.strictEqual(ack._tag, "Accepted")
        }
      }).pipe(Effect.provide(ownerApproves())))

    it.effect("accepts a list's first item after text was typed into an item nobody added", () =>
      Effect.gen(function*() {
        const authority = yield* Authority.Authority
        // Ada types into a task before it's added; Ben, who hasn't seen that,
        // adds the board's first task.
        const ada = yield* makeDoc("ada")
        yield* ReplicatedText.insert(ReplicatedDocument.text(ada, ["tasks", "t1", "title"]), 0, "Draft")
        const ben = yield* makeDoc("ben")
        yield* ReplicatedDocument.update(ben, (doc) => ({ ...doc, tasks: [{ id: "t2", title: "Ship", done: false }] }))
        for (const doc of [ada, ben]) {
          const delta = yield* Replica.delta(doc, VersionVector.empty)
          const ack = yield* authority.push({ format: Sync.FORMAT, document: spec, delta })
          assert.strictEqual(ack._tag, "Accepted")
        }
      }).pipe(Effect.provide(ownerApproves())))

    it.effect("a check doesn't see an entry nobody added, as replicas don't", () => {
      const read: Array<ReplicatedDocument.Proposed<DesignDoc>> = []
      return Effect.gen(function*() {
        const authority = yield* Authority.Authority
        // Written inside a comment that was never added.
        const delta = InternalDelta.fromChanges([{
          replica: "intruder",
          seq: 0,
          length: 1,
          parents: [],
          time: 0,
          ops: [{ _tag: "Set", path: ["comments", "c9", "author"], value: "ada" }]
        }])
        yield* authority.push({ format: Sync.FORMAT, document: spec, delta }).pipe(asUser("intruder"))
        const replica = yield* makeDoc("replica")
        yield* Replica.receive(replica, delta)
        assert.deepStrictEqual((yield* ReplicatedDocument.get(replica)).comments, {})
        assert.deepStrictEqual(read.at(-1)?.after.comments, {})
      }).pipe(Effect.provide(ownerApproves(read)))
    })

    it.effect("a check doesn't see an optional field nobody added inside a list item, as replicas don't", () => {
      const Item = Schema.Struct({ id: Schema.String, note: Schema.optionalKey(ReplicatedDocument.Text) })
      const List = Schema.Struct({ items: ReplicatedDocument.List(Item, { key: "id" }) })
      const empty: typeof List.Type = { items: [] }
      const read: Array<typeof List.Type> = []
      const Recording = Authority.layer({
        check: (proposal) =>
          proposal._tag === "Text"
            ? Effect.void
            : Effect.map(ReplicatedDocument.readProposal(proposal, List, { initial: empty }), ({ after }) => {
              read.push(after)
            })
      }).pipe(Layer.provide(HistoryStore.layerMemory))
      return Effect.gen(function*() {
        const authority = yield* Authority.Authority
        const laptop = yield* ReplicatedDocument.make(List, {
          initial: empty,
          replica: Replica.ReplicaId.make("laptop")
        })
        yield* ReplicatedDocument.update(laptop, () => ({ items: [{ id: "i1" }] }))
        // Typed into the item's note before the note is added.
        yield* ReplicatedText.insert(ReplicatedDocument.text(laptop, ["items", "i1", "note"]), 0, "Draft")
        const delta = yield* Replica.delta(laptop, VersionVector.empty)
        assert.strictEqual((yield* authority.push({ format: Sync.FORMAT, document: spec, delta }))._tag, "Accepted")
        assert.deepStrictEqual(read.at(-1), yield* ReplicatedDocument.get(laptop))
      }).pipe(Effect.provide(Recording))
    })

    it.effect("reading a proposal with an initial value that doesn't fit the schema is a defect, as making a document is", () =>
      Effect.gen(function*() {
        const proposal: Authority.DocumentProposal = {
          _tag: "Document",
          document: spec,
          user: "ada",
          before: {},
          after: {},
          exists: { before: [], after: [] },
          paths: [],
          delta: Delta.empty
        }
        const exit = yield* Effect.exit(
          ReplicatedDocument.readProposal(proposal, DesignDoc, { initial: { ...initial, status: "done" as never } })
        )
        assert.isTrue(Exit.hasDies(exit))
      }))

    it.effect("a proposal is plain data: a copy reads the same", () => {
      const reads: Array<readonly [DesignDoc, DesignDoc]> = []
      const Copying = Authority.layer({
        check: (proposal) =>
          proposal._tag === "Text" ? Effect.void : Effect.gen(function*() {
            const original = yield* ReplicatedDocument.readProposal(proposal, DesignDoc, { initial })
            const copy = yield* ReplicatedDocument.readProposal({ ...proposal }, DesignDoc, { initial })
            reads.push([original.after, copy.after])
          })
      }).pipe(Layer.provide(HistoryStore.layerMemory))
      return Effect.gen(function*() {
        const authority = yield* Authority.Authority
        // Written inside a comment that was never added, so the copy has to
        // know what exists to leave it out.
        const delta = InternalDelta.fromChanges([{
          replica: "intruder",
          seq: 0,
          length: 1,
          parents: [],
          time: 0,
          ops: [{ _tag: "Set", path: ["comments", "c9", "author"], value: "ada" }]
        }])
        yield* authority.push({ format: Sync.FORMAT, document: spec, delta })
        const [original, copy] = reads.at(-1)!
        assert.deepStrictEqual(copy, original)
      }).pipe(Effect.provide(Copying))
    })

    it.effect("a proposal names what exists by the same paths as what the change wrote", () => {
      const proposals: Array<Authority.DocumentProposal> = []
      const Recording = Authority.layer({
        check: (proposal) => Effect.sync(() => proposal._tag === "Document" && proposals.push(proposal))
      }).pipe(Layer.provide(HistoryStore.layerMemory))
      return Effect.gen(function*() {
        const authority = yield* Authority.Authority
        const doc = yield* makeDoc("doc")
        yield* ReplicatedDocument.update(doc, (value) => ({
          ...value,
          tasks: [{ id: "t1", title: "", done: false }],
          comments: { c1: { author: "ada", text: "" } }
        }))
        yield* authority.push({ format: Sync.FORMAT, document: spec, delta: yield* Replica.delta(doc, {}) })
        const [proposal] = proposals
        assert.deepStrictEqual(proposal!.paths.filter((path) => path.length === 2), [["comments", "c1"], [
          "tasks",
          "t1"
        ]])
        const exists = proposal!.exists.after.map((path) => JSON.stringify(path))
        assert.include(exists, JSON.stringify(["tasks", "t1"]))
        assert.notInclude(exists, JSON.stringify(["tasks", "0"]))
      }).pipe(Effect.provide(Recording))
    })

    it.effect("a proposal has every entry, whatever its key", () => {
      const proposed: Array<DesignDoc> = []
      const Recording = Authority.layer({
        check: (proposal) =>
          proposal._tag === "Text"
            ? Effect.void
            : Effect.map(ReplicatedDocument.readProposal(proposal, DesignDoc, { initial }), ({ after }) => {
              proposed.push(after)
            })
      }).pipe(Layer.provide(HistoryStore.layerMemory))
      return Effect.gen(function*() {
        const doc = yield* ReplicatedDocument.makeConfirmed(DesignDoc, { initial, document: spec })
        // Parsed, so `__proto__` is a key rather than the object's prototype.
        const comments = JSON.parse(`{ "__proto__": { "author": "ada", "text": "Hi" } }`)
        yield* ReplicatedDocument.update(doc, (value) => ({ ...value, comments }))
        yield* doc.connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)
        assert.deepStrictEqual(Object.entries(proposed.at(-1)!.comments), [["__proto__", {
          author: "ada",
          text: "Hi"
        }]])
      }).pipe(Effect.scoped, Effect.provide([Recording, CryptoTest]))
    })

    it.effect("reads an item without its key as replicas do, though the key has a default", () => {
      const Item = Schema.Struct({
        id: Schema.String.pipe(Schema.withConstructorDefault(Effect.succeed("new"))),
        done: Schema.Boolean
      })
      const List = Schema.Struct({ items: ReplicatedDocument.List(Item, { key: "id" }) })
      const empty: typeof List.Type = { items: [] }
      const read: Array<typeof List.Type> = []
      const Recording = Authority.layer({
        check: (proposal) =>
          proposal._tag === "Text"
            ? Effect.void
            : Effect.map(ReplicatedDocument.readProposal(proposal, List, { initial: empty }), ({ after }) => {
              read.push(after)
            })
      }).pipe(Layer.provide(HistoryStore.layerMemory))
      return Effect.gen(function*() {
        const authority = yield* Authority.Authority
        const delta = InternalDelta.fromChanges([{
          replica: "other",
          seq: 0,
          length: 3,
          parents: [],
          time: 0,
          ops: [
            { _tag: "Add", path: ["items", "i1"] },
            { _tag: "Set", path: ["items", "i1", "done"], value: false },
            { _tag: "Place", path: ["items", "i1"], parent: null, side: "R", rightOrigin: null }
          ]
        }])
        yield* authority.push({ format: Sync.FORMAT, document: spec, delta })
        const laptop = yield* ReplicatedDocument.make(List, {
          initial: empty,
          replica: Replica.ReplicaId.make("laptop")
        })
        yield* Replica.receive(laptop, delta)
        assert.deepStrictEqual(read, [yield* ReplicatedDocument.get(laptop)])
      }).pipe(Effect.provide(Recording))
    })
  })
})
