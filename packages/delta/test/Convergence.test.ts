import { assert, describe, it } from "@effect/vitest"
import { Effect, Equal, Exit, Layer, Option, Predicate, Queue, Random, Schema, Scope, Stream } from "effect"
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
import { KeyValueStore } from "effect/persistence"
import { forgedRuns, ForgedSession, forgeSession, hop, initialProject, Project } from "./utils/forged.ts"
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
// a buggy or malicious client could send them, several at a time and some
// late. Replicas and relays must still accept and refuse the same ones, and
// show the same value however they arrive: in memory, or as JSON over RPC and
// in a store.

// The part of a document's value at `path`, if it shows. List items are found
// by their `id`.
const partAt = (value: unknown, path: ReadonlyArray<string>): unknown => {
  let current = value
  for (const segment of path) {
    if (Array.isArray(current)) {
      current = current.find((item) => Predicate.hasProperty(item, "id") && item.id === segment)
    } else if (Predicate.isObject(current) && Object.hasOwn(current, segment)) {
      current = (current as Record<string, unknown>)[segment]
    } else return undefined
  }
  return current
}

// Checks the overlaps edits reported against the value they came with: a text
// overlap's range is in its text, and a removal's entry or item doesn't show.
// Which overlaps a merge reports depends on what the replica had before it,
// so they aren't the same however the changes arrive.
const checkOverlaps = (edits: Iterable<ReplicatedDocument.Edit<Project>>): void => {
  for (const edit of edits) {
    if (edit._tag !== "Remote") continue
    for (const overlap of edit.overlaps) {
      const part = partAt(edit.value, overlap.path)
      switch (overlap._tag) {
        case "Text":
          if (Predicate.isString(part)) {
            assert.isTrue(
              0 <= overlap.range.start && overlap.range.start <= overlap.range.end && overlap.range.end <= part.length,
              `${JSON.stringify(overlap)} in ${JSON.stringify(part)}`
            )
          }
          break
        case "Removed":
          assert.isUndefined(part, JSON.stringify(overlap))
          break
      }
    }
  }
}

describe.each(["tree", "replay"] as const)("forged changes (%s)", (strategy) => {
  const make = (replica: string) =>
    ReplicatedDocument.make(Project, { initial: initialProject, strategy, replica: Replica.ReplicaId.make(replica) })
  const view = Effect.fnUntraced(function*(doc: ReplicatedDocument.ReplicatedDocument<Project>) {
    return { value: yield* ReplicatedDocument.get(doc), version: yield* Replica.version(doc) }
  })
  const key = (change: InternalDocument.AnyChange) => `${change.seq}@${change.replica}`
  const push = Effect.fnUntraced(
    function*(relay: Sync.Server, document: Replica.DocumentId, changes: ReadonlyArray<InternalDocument.AnyChange>) {
      return yield* relay.push({ format: Sync.FORMAT, document, delta: yield* hop(InternalDelta.fromChanges(changes)) })
    }
  )
  const subscribe = (relay: Sync.Server, document: Replica.DocumentId, version: VersionVector.VersionVector) =>
    relay.subscribe({ format: Sync.FORMAT, document, strategy, version, authority: false })
  // Takes in what a relay has applied since the replica's version, as a
  // connection does when it subscribes.
  const pull = Effect.fnUntraced(
    function*(doc: Replica.Replica<unknown>, relay: Sync.Server, document: Replica.DocumentId) {
      const first = yield* Stream.runHead(subscribe(relay, document, yield* Replica.version(doc)))
      yield* Replica.receive(doc, yield* hop(Option.getOrThrow(first).delta))
    }
  )
  // What a relay has applied, as a new replica that takes it in shows it.
  const fromRelay = Effect.fnUntraced(function*(relay: Sync.Server, document: Replica.DocumentId) {
    const doc = yield* make(`from-${document}`)
    yield* pull(doc, relay, document)
    return yield* view(doc)
  })
  // Receives a relay's updates until the replica has `version`, accepting
  // every one, as a connection must.
  const catchUp = Effect.fnUntraced(function*(
    doc: Replica.Replica<unknown>,
    updates: Queue.Dequeue<Sync.Update, unknown>,
    version: VersionVector.VersionVector
  ) {
    while (VersionVector.compare(yield* Replica.version(doc), version) !== "equal") {
      yield* Replica.receive(doc, yield* hop((yield* Queue.take(updates)).delta))
    }
  })
  // A replica that receives deltas, failing or not, and afterwards says what
  // its subscribers were told.
  const watched = Effect.fnUntraced(function*(replica: string) {
    const doc = yield* make(replica)
    const { edits } = yield* ReplicatedDocument.subscribe(doc)
    let merges = 0
    return {
      doc,
      receive: Effect.fnUntraced(function*(changes: ReadonlyArray<InternalDocument.AnyChange>) {
        const before = yield* Replica.version(doc)
        const received = yield* Effect.exit(Replica.receive(doc, yield* hop(InternalDelta.fromChanges(changes))))
        // Each merge that applies something is one edit.
        if (VersionVector.compare(yield* Replica.version(doc), before) !== "equal") merges++
        return received
      }),
      edits: Effect.suspend(() => Stream.runCollect(Stream.take(edits, merges)))
    }
  })
  const inOrder = Replica.DocumentId.make("in-order")
  const shuffled = Replica.DocumentId.make("shuffled")
  const saved = Replica.DocumentId.make("saved")
  const behind = Replica.DocumentId.make("behind")

  it.effect.prop(
    "replicas, relays and reloads accept the same changes, and show the same value whatever the order",
    // Stored in memory, or as JSON.
    { session: ForgedSession, seed: Schema.Int, json: Schema.Boolean },
    Effect.fnUntraced(function*({ seed, session }) {
      const expected = yield* Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const updates = yield* Stream.toQueue(subscribe(relay, shuffled, VersionVector.empty), {
          capacity: "unbounded"
        })
        // As the session sends them, a replica and a relay accept the same
        // ones.
        const doc = yield* watched("in-order")
        const changes = yield* forgeSession(strategy, session, (batch) =>
          Effect.gen(function*() {
            const received = yield* doc.receive(batch)
            const pushed = yield* Effect.exit(push(relay, inOrder, batch))
            assert.strictEqual(Exit.isSuccess(pushed), Exit.isSuccess(received), JSON.stringify(batch))
            const kept = (yield* Replica.delta(doc.doc, VersionVector.empty)) as InternalDelta.DeltaImpl
            return (change: InternalDocument.AnyChange) => kept.changes.has(key(change))
          }))
        const expected = yield* view(doc.doc)
        checkOverlaps(yield* doc.edits)
        assert.deepStrictEqual(yield* fromRelay(relay, inOrder), expected)

        const atOnce = yield* watched("at-once")
        yield* atOnce.receive(changes)
        assert.deepStrictEqual(yield* view(atOnce.doc), expected)
        checkOverlaps(yield* atOnce.edits)

        // One at a time, shuffled and repeated, to a replica that saves them,
        // and to a relay whose subscriber takes what it sends. Behind the
        // relay, a replica that's reopened from what it saved now and then,
        // so it starts from texts saved partway.
        yield* Effect.scoped(Effect.gen(function*() {
          const oneByOne = yield* watched("one-by-one")
          yield* HistoryStore.persist(oneByOne.doc, saved)
          let reopened = 0
          const open = Effect.fnUntraced(function*() {
            const scope = yield* Scope.make()
            const doc = yield* make(`behind-${reopened++}`)
            yield* HistoryStore.persist(doc, behind).pipe(Scope.provide(scope))
            return { doc, scope }
          })
          let back = yield* open()
          for (const change of yield* Random.shuffle([...changes, ...changes])) {
            yield* oneByOne.receive([change])
            yield* Effect.exit(push(relay, shuffled, [change]))
            yield* pull(back.doc, relay, shuffled)
            // Saved before the next arrives.
            yield* Effect.yieldNow
            if (yield* Random.nextBoolean) {
              yield* Scope.close(back.scope, Exit.void)
              back = yield* open()
            }
          }
          assert.deepStrictEqual(yield* view(oneByOne.doc), expected)
          checkOverlaps(yield* oneByOne.edits)
          yield* Scope.close(back.scope, Exit.void)
        })).pipe(Random.withSeed(seed))
        const fromShuffled = yield* fromRelay(relay, shuffled)
        assert.deepStrictEqual(fromShuffled, expected)
        const subscriber = yield* make("subscriber")
        yield* catchUp(subscriber, updates, fromShuffled.version)
        assert.deepStrictEqual(yield* view(subscriber), expected)
        return expected
      }).pipe(Effect.provide(Layer.fresh(Relay.layer)), Random.withSeed(seed))

      // Started again from what they stored.
      for (const document of [saved, behind]) {
        const reopened = yield* Effect.scoped(Effect.gen(function*() {
          const doc = yield* make(`reopened-${document}`)
          yield* HistoryStore.persist(doc, document)
          return yield* view(doc)
        }))
        assert.deepStrictEqual(reopened, expected, document)
      }
      yield* Effect.gen(function*() {
        const relay = yield* Relay.Relay
        for (const document of [inOrder, shuffled]) assert.deepStrictEqual(yield* fromRelay(relay, document), expected)
      }).pipe(Effect.provide(Layer.fresh(Relay.layer)))
    }, (effect, { json }) =>
      effect.pipe(
        Effect.scoped,
        Effect.provide(
          json
            ? HistoryStore.layerKeyValueStore({ prefix: "forged" }).pipe(Layer.provide(KeyValueStore.layerMemory))
            : HistoryStore.layerMemory
        ),
        Effect.provideService(Sync.CurrentUser, "forger")
      )),
    { arbitrary: forgedRuns }
  )
})
