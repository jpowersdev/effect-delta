import { assert, describe, it } from "@effect/vitest"
import { Effect, Equal, Exit, Layer, Queue, Random, Schema, Stream } from "effect"
import {
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
import type * as InternalDocument from "effect-delta/internal/document"
import { forgedRuns, ForgedSession, initialProject, makeForger, Project } from "./utils/forged.ts"
import {
  Board,
  documentRuns,
  DocumentSession,
  Fraction,
  initialBoard,
  letters,
  makeBoards,
  stepBoards,
  syncAll,
  viewOf,
  Which
} from "./utils/sessions.ts"

// Random editing sessions across three replicas, with partial syncs along the
// way. Positions are fractions of the current text, so every generated command
// is valid whatever the text is by then.

const Command = Schema.Union([
  Schema.TaggedStruct("Insert", {
    replica: Which,
    at: Fraction,
    text: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4))
  }),
  Schema.TaggedStruct("Remove", {
    replica: Which,
    at: Fraction,
    length: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3 }))
  }),
  Schema.TaggedStruct("Sync", { from: Which, to: Which })
])

const Session = Schema.Array(Command).check(Schema.isMaxLength(40))

const run = Effect.fnUntraced(function*(strategy: Replica.Strategy, session: typeof Session.Type) {
  const make = (replica: string) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(replica) })
  const replicas = [yield* make("r0"), yield* make("r1"), yield* make("r2")] as const
  for (const command of session) {
    switch (command._tag) {
      case "Insert": {
        const replica = replicas[command.replica]
        const before = yield* ReplicatedText.get(replica)
        const index = Math.floor(command.at * (before.length + 1) / 1001)
        const text = letters(command.text)
        yield* ReplicatedText.insert(replica, index, text)
        // A replica shows its own edits immediately and exactly.
        assert.strictEqual(yield* ReplicatedText.get(replica), before.slice(0, index) + text + before.slice(index))
        break
      }
      case "Remove": {
        const replica = replicas[command.replica]
        const before = yield* ReplicatedText.get(replica)
        if (before.length === 0) break
        const index = Math.floor(command.at * before.length / 1001)
        const length = Math.min(command.length, before.length - index)
        yield* ReplicatedText.remove(replica, index, length)
        assert.strictEqual(yield* ReplicatedText.get(replica), before.slice(0, index) + before.slice(index + length))
        break
      }
      case "Sync":
        if (command.from !== command.to) yield* Replica.exchange(replicas[command.from], replicas[command.to])
        break
    }
  }
  return replicas
})

describe.each(["tree", "replay"] as const)("convergence (%s)", (strategy) => {
  it.effect.prop(
    "replicas that have exchanged everything show the same text",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const replicas = yield* run(strategy, session)
      yield* syncAll(replicas)
      const [first, ...rest] = yield* Effect.forEach(replicas, ReplicatedText.get)
      for (const text of rest) assert.strictEqual(text, first)
      const [version, ...versions] = yield* Effect.forEach(replicas, Replica.version)
      for (const other of versions) assert.deepStrictEqual(other, version)
    }),
    { arbitrary: { runs: 200, size: 100 } }
  )

  it.effect.prop(
    "receiving is order-independent and safe to repeat",
    { session: Session, seed: Schema.Int },
    Effect.fnUntraced(function*({ session, seed }) {
      const replicas = yield* run(strategy, session)
      const deltas = yield* Effect.forEach(replicas, (replica) => Replica.delta(replica, VersionVector.empty))
      const everything = deltas.reduce(Delta.combine, Delta.empty)

      const atOnce = yield* ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make("at-once") })
      yield* Replica.receive(atOnce, everything)

      const changes = [...(everything as InternalDelta.DeltaImpl).changes.values()]
      const shuffled = yield* Random.shuffle([...changes, ...changes]).pipe(Random.withSeed(seed))
      const oneByOne = yield* ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make("one-by-one") })
      for (const change of shuffled) yield* Replica.receive(oneByOne, InternalDelta.fromChanges([change]))

      yield* syncAll(replicas)
      const expected = yield* ReplicatedText.get(replicas[0])
      assert.strictEqual(yield* ReplicatedText.get(atOnce), expected)
      assert.strictEqual(yield* ReplicatedText.get(oneByOne), expected)
    }),
    { arbitrary: { runs: 100, size: 100 } }
  )

  it.effect.prop(
    "combining deltas is commutative, associative and idempotent",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const replicas = yield* run(strategy, session)
      const [a, b, c] = yield* Effect.forEach(replicas, (replica) => Replica.delta(replica, VersionVector.empty))
      assert.isTrue(Equal.equals(Delta.combine(a!, b!), Delta.combine(b!, a!)))
      assert.isTrue(Equal.equals(Delta.combine(Delta.combine(a!, b!), c!), Delta.combine(a!, Delta.combine(b!, c!))))
      assert.isTrue(Equal.equals(Delta.combine(a!, a!), a!))
      assert.isTrue(Equal.equals(Delta.combine(a!, Delta.empty), a!))
    }),
    { arbitrary: { runs: 100, size: 100 } }
  )
})

// The interleaving cases from "The Art of the Fugue": two people typing at the
// same spot without seeing each other must each get a contiguous passage.
describe.each(["tree", "replay"] as const)("non-interleaving (%s)", (strategy) => {
  const typeForward = Effect.fnUntraced(function*(replica: ReplicatedText.ReplicatedText, at: number, text: string) {
    for (let i = 0; i < text.length; i++) yield* ReplicatedText.insert(replica, at + i, text[i]!)
  })
  const typeBackward = Effect.fnUntraced(function*(replica: ReplicatedText.ReplicatedText, at: number, text: string) {
    for (let i = text.length - 1; i >= 0; i--) yield* ReplicatedText.insert(replica, at, text[i]!)
  })

  const make = (replica: string) => ReplicatedText.make({ strategy, replica: Replica.ReplicaId.make(replica) })
  const setup = Effect.gen(function*() {
    const alice = yield* make("alice")
    const bob = yield* make("bob")
    yield* ReplicatedText.insert(alice, 0, "[]")
    yield* Replica.exchange(alice, bob)
    return { alice, bob }
  })

  const assertOneOf = (text: string, options: ReadonlyArray<string>) =>
    assert.isTrue(options.includes(text), `${JSON.stringify(text)} is interleaved`)

  it.effect("typing forward", () =>
    Effect.gen(function*() {
      const { alice, bob } = yield* setup
      yield* typeForward(alice, 1, "milk")
      yield* typeForward(bob, 1, "eggs")
      yield* Replica.exchange(alice, bob)
      assertOneOf(yield* ReplicatedText.get(alice), ["[milkeggs]", "[eggsmilk]"])
    }))

  it.effect("typing backward", () =>
    Effect.gen(function*() {
      const { alice, bob } = yield* setup
      yield* typeBackward(alice, 1, "milk")
      yield* typeBackward(bob, 1, "eggs")
      yield* Replica.exchange(alice, bob)
      assertOneOf(yield* ReplicatedText.get(alice), ["[milkeggs]", "[eggsmilk]"])
    }))

  it.effect("typing backward across a reload, with a new replica id", () =>
    Effect.gen(function*() {
      const { alice, bob } = yield* setup
      yield* typeBackward(alice, 1, "lk")
      // Alice's tab reloads: a new replica with the same text.
      const reloaded = yield* make("alice-2")
      yield* Replica.exchange(alice, reloaded)
      yield* typeBackward(reloaded, 1, "mi")
      yield* typeBackward(bob, 1, "eggs")
      yield* Replica.exchange(reloaded, bob)
      assertOneOf(yield* ReplicatedText.get(bob), ["[milkeggs]", "[eggsmilk]"])
    }))
})

// Documents ---------------------------------------------------------------

// The same, for documents, over a schema that uses every merge rule.

describe.each(["tree", "replay"] as const)("document convergence (%s)", (strategy) => {
  it.effect.prop(
    "replicas that have exchanged everything show the same value",
    { session: DocumentSession },
    Effect.fnUntraced(function*({ session }) {
      const docs = yield* makeBoards(strategy)
      for (const command of session) yield* stepBoards(docs, command)
      yield* syncAll(docs)
      const [first, ...rest] = yield* Effect.forEach(docs, viewOf)
      for (const view of rest) assert.deepStrictEqual(view, first)
      const [version, ...versions] = yield* Effect.forEach(docs, Replica.version)
      for (const other of versions) assert.deepStrictEqual(other, version)
    }),
    { arbitrary: documentRuns }
  )

  it.effect.prop(
    "receiving is order-independent and safe to repeat",
    { session: DocumentSession, seed: Schema.Int },
    Effect.fnUntraced(function*({ session, seed }) {
      const docs = yield* makeBoards(strategy)
      for (const command of session) yield* stepBoards(docs, command)
      const deltas = yield* Effect.forEach(docs, (doc) => Replica.delta(doc, VersionVector.empty))
      const everything = deltas.reduce(Delta.combine, Delta.empty)
      const make = (replica: string) =>
        ReplicatedDocument.make(Board, { initial: initialBoard, strategy, replica: Replica.ReplicaId.make(replica) })

      const atOnce = yield* make("at-once")
      yield* Replica.receive(atOnce, everything)

      const changes = [...(everything as InternalDelta.DeltaImpl).changes.values()]
      const shuffled = yield* Random.shuffle([...changes, ...changes]).pipe(Random.withSeed(seed))
      const oneByOne = yield* make("one-by-one")
      for (const change of shuffled) yield* Replica.receive(oneByOne, InternalDelta.fromChanges([change]))

      yield* syncAll(docs)
      const expected = yield* viewOf(docs[0]!)
      assert.deepStrictEqual(yield* viewOf(atOnce), expected)
      assert.deepStrictEqual(yield* viewOf(oneByOne), expected)
    }),
    { arbitrary: documentRuns }
  )
})

// Forged changes ----------------------------------------------------------

// Changes made directly, many of them malformed or not fitting the schema, as
// a buggy or malicious client could send them. Replicas and relays must still
// accept and refuse the same ones, and show the same value however they
// arrive.

describe.each(["tree", "replay"] as const)("forged changes (%s)", (strategy) => {
  const make = (replica: string) =>
    ReplicatedDocument.make(Project, { initial: initialProject, strategy, replica: Replica.ReplicaId.make(replica) })
  const view = Effect.fnUntraced(function*(doc: ReplicatedDocument.ReplicatedDocument<Project>) {
    return { value: yield* ReplicatedDocument.get(doc), version: yield* Replica.version(doc) }
  })
  const one = (change: InternalDocument.AnyChange) => InternalDelta.fromChanges([change])
  const push = (relay: Sync.Server, document: Replica.DocumentId, change: InternalDocument.AnyChange) =>
    relay.push({ format: Sync.FORMAT, document, delta: one(change) })
  const subscribe = (relay: Sync.Server, document: Replica.DocumentId) =>
    Stream.toQueue(
      relay.subscribe({ format: Sync.FORMAT, document, strategy, version: VersionVector.empty, authority: false }),
      { capacity: "unbounded" }
    )
  // Receives a relay's updates until the replica has `version`, accepting
  // every one, as a connection must.
  const catchUp = Effect.fnUntraced(function*(
    doc: Replica.Replica<unknown>,
    updates: Queue.Dequeue<Sync.Update, unknown>,
    version: VersionVector.VersionVector
  ) {
    while (VersionVector.compare(yield* Replica.version(doc), version) !== "equal") {
      yield* Replica.receive(doc, (yield* Queue.take(updates)).delta)
    }
  })
  const inOrder = Replica.DocumentId.make("in-order")
  const shuffled = Replica.DocumentId.make("shuffled")
  const saved = Replica.DocumentId.make("saved")

  it.effect.prop(
    "replicas, relays and reloads accept the same changes, and show the same value whatever the order",
    { session: ForgedSession, seed: Schema.Int },
    Effect.fnUntraced(function*({ seed, session }) {
      const expected = yield* Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const updates = yield* subscribe(relay, shuffled)
        // In the order they were made, a replica and a relay accept the same
        // ones.
        const doc = yield* make("in-order")
        const forger = makeForger(strategy)
        const changes: Array<InternalDocument.AnyChange> = []
        for (const command of session) {
          const change = forger.forge(command)
          changes.push(change)
          const received = yield* Effect.exit(Replica.receive(doc, one(change)))
          const pushed = yield* Effect.exit(push(relay, inOrder, change))
          assert.strictEqual(Exit.isSuccess(pushed), Exit.isSuccess(received), JSON.stringify(change))
          forger.settle(Exit.isSuccess(received))
        }
        const expected = yield* view(doc)

        const atOnce = yield* make("at-once")
        yield* Effect.exit(Replica.receive(atOnce, InternalDelta.fromChanges(changes)))
        assert.deepStrictEqual(yield* view(atOnce), expected)

        // One at a time, shuffled and repeated, to a replica that saves them,
        // and to a relay whose subscriber takes what it sends.
        const order = yield* Random.shuffle([...changes, ...changes]).pipe(Random.withSeed(seed))
        yield* Effect.scoped(Effect.gen(function*() {
          const oneByOne = yield* make("one-by-one")
          yield* HistoryStore.persist(oneByOne, saved)
          for (const change of order) {
            yield* Effect.exit(Replica.receive(oneByOne, one(change)))
            yield* Effect.exit(push(relay, shuffled, change))
            // Saved before the next arrives.
            yield* Effect.yieldNow
          }
          assert.deepStrictEqual(yield* view(oneByOne), expected)
        }))
        const subscriber = yield* make("subscriber")
        yield* catchUp(subscriber, updates, expected.version)
        assert.deepStrictEqual(yield* view(subscriber), expected)
        return expected
      }).pipe(Effect.provide(Layer.fresh(Relay.layer)))

      // Started again from what they stored.
      const reopened = yield* Effect.scoped(Effect.gen(function*() {
        const doc = yield* make("reopened")
        yield* HistoryStore.persist(doc, saved)
        return yield* view(doc)
      }))
      assert.deepStrictEqual(reopened, expected)
      yield* Effect.gen(function*() {
        const relay = yield* Relay.Relay
        for (const document of [inOrder, shuffled]) {
          const doc = yield* make(`from-${document}`)
          yield* catchUp(doc, yield* subscribe(relay, document), expected.version)
          assert.deepStrictEqual(yield* view(doc), expected)
        }
      }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
    }, (effect) =>
      effect.pipe(
        Effect.scoped,
        Effect.provide(HistoryStore.layerMemory),
        Effect.provideService(Sync.CurrentUser, "forger")
      )),
    { arbitrary: forgedRuns }
  )
})
