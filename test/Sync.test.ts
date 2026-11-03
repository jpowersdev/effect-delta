import { assert, describe, it } from "@effect/vitest"
import {
  Authority,
  Delta,
  HistoryStore,
  Relay,
  Replica,
  ReplicatedText,
  Sync,
  VersionVector
} from "@jpowersdev/effect-delta"
import { Context, Deferred, Effect, Fiber, Layer, Option, Schedule, Stream } from "effect"
import { layerSeeded } from "./utils/secureRandom.ts"

const make = (replica: string) => ReplicatedText.make({ replica: Replica.ReplicaId.make(replica) })
const notes = Replica.DocumentId.make("notes")
const spec = Replica.DocumentId.make("spec")

// Waits until the replica shows `text`.
const showing = (self: ReplicatedText.ReplicatedText, text: string) =>
  ReplicatedText.changes(self).pipe(Stream.filter((current) => current === text), Stream.runHead, Effect.asVoid)

const synced = (connection: Sync.Connection) =>
  connection.status.pipe(Stream.filter((status) => status._tag === "Synced"), Stream.runHead, Effect.asVoid)

const unconfirmed = (connection: Sync.Connection) =>
  Effect.map(Stream.runHead(connection.unconfirmed), Option.getOrThrow)

const asUser = (user: string) => Effect.provideService(Sync.CurrentUser, user)

const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))

// Rejects any change that leaves "secret" in the text.
const NoSecrets = Authority.layer({
  check: ({ after }) =>
    after.includes("secret") ? Effect.fail(new Authority.Rejected({ message: "No secrets" })) : Effect.void
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

const CryptoTest = layerSeeded("sync")

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

  it.effect("sync waits for what the server already has, even if it changes no text", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      const phone = yield* make("phone")
      const a = yield* Relay.connect(laptop, { document: notes })
      const b = yield* Relay.connect(phone, { document: notes })
      yield* ReplicatedText.insert(laptop, 0, "abc")
      yield* showing(phone, "abc")
      yield* b.sync

      // Both remove the same character, so one of the removals changes nothing.
      const tablet = yield* make("tablet")
      yield* Replica.receive(tablet, yield* Replica.delta(laptop, VersionVector.empty))
      yield* ReplicatedText.remove(tablet, 0, 1)
      yield* ReplicatedText.remove(laptop, 0, 1)
      yield* Stream.runHead(a.unconfirmed.pipe(Stream.filter((count) => count === 0)))
      yield* Relay.connect(tablet, { document: notes }).pipe(Effect.flatMap((c) => c.sync))
      yield* a.sync
      yield* b.sync
      assert.deepStrictEqual(yield* Replica.version(phone), yield* Replica.version(laptop))
      assert.strictEqual(yield* ReplicatedText.get(phone), "bc")
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

  it.effect("recovers when the replica whose push failed is also subscribed", () =>
    Effect.gen(function*() {
      const laptop = yield* make("laptop")
      const retry = Schedule.forever
      yield* synced(yield* Relay.connect(laptop, { document: notes, retry }))
      // Failing the save ends this replica's subscription, which interrupts the push
      // that's reporting the failure.
      yield* ReplicatedText.insert(laptop, 0, "Hello")
      const phone = yield* make("phone")
      yield* Relay.connect(phone, { document: notes, retry })
      yield* showing(phone, "Hello")
    }).pipe(
      Effect.scoped,
      Effect.provide(Relay.layer.pipe(Layer.provide(failingOnce)))
    ))

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
})

describe("Authority", () => {
  for (const step of ["check", "append", "addWriter"] as const) {
    it.effect(`a push interrupted during ${step} is still saved and shared`, () =>
      Effect.gen(function*() {
        const reached = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        const holding = (at: typeof step) =>
          at === step
            ? Effect.andThen(Deferred.succeed(reached, undefined), Deferred.await(resume))
            : Effect.void
        const Held = Layer.effect(HistoryStore.HistoryStore)(Effect.map(HistoryStore.HistoryStore, (store) => ({
          ...store,
          append: (document, delta) => Effect.andThen(holding("append"), store.append(document, delta)),
          addWriter: (document, replica, owner) =>
            Effect.andThen(holding("addWriter"), store.addWriter(document, replica, owner))
        }))).pipe(Layer.provideMerge(HistoryStore.layerMemory))

        yield* Effect.gen(function*() {
          const reviewer = yield* ReplicatedText.makeConfirmed({ document: spec })
          yield* synced(reviewer.connection)
          const authority = yield* Authority.Authority
          const writer = yield* make("writer")
          yield* ReplicatedText.insert(writer, 0, "Spec v1")
          const pushing = yield* authority.push({
            format: Sync.FORMAT,
            document: spec,
            delta: yield* Replica.delta(writer, {})
          }).pipe(asUser("editor"), Effect.forkChild)

          yield* Deferred.await(reached)
          pushing.interruptUnsafe()
          yield* Deferred.succeed(resume, undefined)
          yield* Fiber.await(pushing)

          yield* reviewer.connection.sync
          assert.strictEqual(yield* ReplicatedText.get(reviewer), "Spec v1")
          const store = yield* HistoryStore.HistoryStore
          const stored = Option.getOrThrow(yield* store.load(spec))
          assert.deepStrictEqual(Delta.version(stored.history), yield* Replica.version(writer))
          assert.strictEqual(stored.writers["writer"], "editor")
        }).pipe(
          Effect.scoped,
          Effect.provide(
            Authority.layer({ check: () => holding("check") }).pipe(Layer.provideMerge(Held))
          ),
          Effect.provide(CryptoTest)
        )
      }))
  }

  it.effect("a change that comes back in an update is confirmed, before its push is answered", () =>
    Effect.gen(function*() {
      const answer = yield* Deferred.make<void>()
      const authority = yield* Authority.Authority
      const replica = yield* ReplicatedText.makeConfirmed({ document: spec }).pipe(
        Effect.provideService(Authority.Authority, {
          ...authority,
          // The server has saved and published it, and the answer is still on its way.
          push: (request) => Effect.tap(authority.push(request), () => Deferred.await(answer))
        })
      )
      yield* synced(replica.connection)
      yield* ReplicatedText.insert(replica, 0, "Saved")
      yield* replica.connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)
      assert.deepStrictEqual(Delta.version(yield* Replica.delta(replica, {})), yield* Replica.version(replica))
      yield* Deferred.succeed(answer, undefined)
    }).pipe(Effect.scoped, Effect.provide([NoSecrets, CryptoTest])))

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
})
