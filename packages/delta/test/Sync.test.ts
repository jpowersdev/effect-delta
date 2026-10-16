import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Context, Deferred, Effect, Layer, Option, Schedule, Stream } from "effect"
import { Authority, HistoryStore, Relay, Replica, ReplicatedText, Sync, VersionVector } from "effect-delta"
import { TestCrypto } from "effect/testing"

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
