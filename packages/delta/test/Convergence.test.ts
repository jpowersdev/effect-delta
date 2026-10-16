import { assert, describe, it } from "@effect/vitest"
import { Effect, Equal, Exit, Layer, Option, Predicate, Queue, Random, Schema, Scope, Stream } from "effect"
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
import type * as InternalDocument from "effect-delta/internal/document"
import * as InternalShape from "effect-delta/internal/shape"
import { KeyValueStore } from "effect/persistence"
import {
  appliedBy,
  asOld,
  checkTextOverlaps,
  editProject,
  EditSession,
  expectedOverlaps,
  forgedRuns,
  ForgedSession,
  forgedTimeout,
  forgeSession,
  hop,
  initialProject,
  OldProject,
  Project,
  throughJson
} from "./utils/forged.ts"
import {
  Board,
  documentRuns,
  DocumentSession,
  editPlanner,
  forgedPlannerOps,
  Fraction,
  initialBoard,
  initialPlanner,
  letters,
  makeBoards,
  OldPlanner,
  Planner,
  PlannerSession,
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
    "replicas converge whatever their ids, even ones every object has a property for",
    { session: DocumentSession },
    Effect.fnUntraced(function*({ session }) {
      const docs = yield* makeBoards(strategy, ["constructor", "toString", "valueOf"])
      for (const command of session) yield* stepBoards(docs, command)
      yield* syncAll(docs)
      const [first, ...rest] = yield* Effect.forEach(docs, viewOf)
      for (const view of rest) assert.deepStrictEqual(view, first)
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

// Checks the overlaps of a merge, just after it, against the value it came
// with and against their rules, for what the replica had before the merge
// (`before`) and received in it (up to `after`), in the parts its schema
// shows. A removal's entry or item doesn't show, and the value and removal
// overlaps are the ones their rule gives. A text overlap's anchors are at its
// range; with the tree strategy, the text overlaps are the ones their rule
// gives too, and the replay strategy reports the same as tree for the same
// edits. Which overlaps a merge reports depends on what the replica had, so
// they aren't the same however the changes arrive.
const checkOverlaps = Effect.fnUntraced(function*<A>(
  doc: ReplicatedDocument.ReplicatedDocument<A>,
  edit: ReplicatedDocument.Edit<A>,
  merge: {
    readonly before: VersionVector.VersionVector
    readonly after: VersionVector.VersionVector
    readonly shape: InternalShape.Shape
    readonly strategy: Replica.Strategy
  }
) {
  if (edit._tag !== "Remote") return
  const { after, before, shape, strategy } = merge
  const changes = [...((yield* Replica.delta(doc, VersionVector.empty)) as InternalDelta.DeltaImpl).changes.values()]
  const earlier = new Set(appliedBy(changes, before))
  const received = appliedBy(changes, after).filter((change) => !earlier.has(change))
  const receiving = `receiving ${JSON.stringify(received)}`
  for (const overlap of edit.overlaps) {
    if (overlap._tag === "Removed") assert.isUndefined(partAt(edit.value, overlap.path), JSON.stringify(overlap))
  }
  const sorted = (overlaps: ReadonlyArray<Replica.Overlap | InternalDocument.Overlap>) =>
    overlaps.filter(({ _tag }) => _tag !== "Text").map((overlap) => JSON.stringify(overlap)).sort()
  assert.deepStrictEqual(
    sorted(edit.overlaps),
    sorted(
      expectedOverlaps(changes, before, after, InternalShape.reading(shape)).filter((overlap) =>
        InternalShape.shows(shape, overlap)
      )
    ),
    receiving
  )

  const textAt = (path: ReadonlyArray<string>) =>
    InternalShape.shows(shape, { _tag: "Text", path, start: 0, end: 0, anchors: everything, replicas: [] })
      ? ReplicatedDocument.text(doc, path as never)
      : undefined
  const texts = edit.overlaps.flatMap((overlap) => overlap._tag === "Text" ? [overlap] : [])
  for (const { anchors, path, range } of texts) {
    const text = textAt(path)!
    assert.deepStrictEqual(
      [yield* ReplicatedText.locate(text, anchors.start), yield* ReplicatedText.locate(text, anchors.end)],
      [Option.some(range.start), Option.some(range.end)],
      `${JSON.stringify(path)} ${JSON.stringify(range)}`
    )
  }
  if (strategy === "replay") {
    // Where text the replica had met text it received.
    const edited = (changes: Iterable<InternalDocument.AnyChange>) =>
      new Set(
        [...changes].flatMap(({ ops }) =>
          ops.flatMap((op) => textOps.has(op._tag) ? [JSON.stringify(op.path ?? [])] : [])
        )
      )
    const [had, got] = [edited(earlier), edited(received)]
    for (const overlap of texts) {
      const k = JSON.stringify(overlap.path)
      assert.isTrue(had.has(k) && got.has(k), `${JSON.stringify(overlap)} ${receiving}`)
    }
    return
  }
  yield* checkTextOverlaps(texts, { changes, before, after, textAt })
})

const everything = { start: { _tag: "Start" }, end: { _tag: "End" } } as const

const textOps = new Set(["Insert", "Remove", "InsertAt", "RemoveAt"])

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
  // A replica that edits and receives, failing or not, checking the overlaps
  // of each merge as it happens. `told` is what its subscribers were told.
  const watched = <A>(doc: ReplicatedDocument.ReplicatedDocument<A>, schema: Schema.Top) =>
    Effect.gen(function*() {
      const edits = yield* Stream.toQueue((yield* ReplicatedDocument.subscribe(doc)).edits, { capacity: "unbounded" })
      const shape = InternalShape.compile(schema)
      const told: Array<ReplicatedDocument.Edit<unknown>> = []
      const tracked = <B, E, R>(effect: Effect.Effect<B, E, R>) =>
        Effect.gen(function*() {
          const before = yield* Replica.version(doc)
          const exit = yield* Effect.exit(effect)
          const after = yield* Replica.version(doc)
          // Each edit or merge that changes the replica is one edit to tell.
          if (VersionVector.compare(after, before) !== "equal") {
            const edit = yield* Queue.take(edits)
            yield* checkOverlaps(doc, edit, { before, after, shape, strategy })
            told.push(edit)
          }
          return exit
        })
      return {
        doc,
        tracked,
        // Received one hop away.
        receive: (delta: Delta.Delta) => Effect.flatMap(hop(delta), (sent) => tracked(Replica.receive(doc, sent))),
        told: told as ReadonlyArray<ReplicatedDocument.Edit<unknown>>
      }
    })
  const inOrder = Replica.DocumentId.make("in-order")
  const shuffled = Replica.DocumentId.make("shuffled")
  const saved = Replica.DocumentId.make("saved")
  const behind = Replica.DocumentId.make("behind")

  it.effect.prop(
    "replicas, relays and reloads accept the same changes, and show the same value whatever the order",
    // Stored in memory, or as JSON. With `resend`, the changes that wait are
    // sent again with every batch, as a peer sends what it hasn't heard back
    // about.
    { session: ForgedSession, seed: Schema.Int, json: Schema.Boolean, resend: Schema.Boolean },
    Effect.fnUntraced(function*({ resend, seed, session }) {
      const expected = yield* Effect.gen(function*() {
        const relay = yield* Relay.Relay
        const updates = yield* Stream.toQueue(subscribe(relay, shuffled, VersionVector.empty), {
          capacity: "unbounded"
        })
        // As the session sends them, a replica and a relay accept the same
        // ones.
        const doc = yield* watched(yield* make("in-order"), Project)
        let waiting: ReadonlyArray<InternalDocument.AnyChange> = []
        const changes = yield* forgeSession(strategy, session, (batch) =>
          Effect.gen(function*() {
            const sending = resend ? [...waiting, ...batch] : batch
            const received = yield* doc.receive(InternalDelta.fromChanges(sending))
            const pushed = yield* Effect.exit(push(relay, inOrder, sending))
            assert.strictEqual(Exit.isSuccess(pushed), Exit.isSuccess(received), JSON.stringify(sending))
            const kept = (yield* Replica.delta(doc.doc, VersionVector.empty)) as InternalDelta.DeltaImpl
            const applied = new Set(appliedBy([...kept.changes.values()], yield* Replica.version(doc.doc)))
            waiting = [...kept.changes.values()].filter((change) => !applied.has(change))
            return (change: InternalDocument.AnyChange) => kept.changes.has(key(change))
          }))
        const expected = yield* view(doc.doc)
        assert.deepStrictEqual(yield* fromRelay(relay, inOrder), expected)

        const atOnce = yield* watched(yield* make("at-once"), Project)
        yield* atOnce.receive(InternalDelta.fromChanges(changes))
        assert.deepStrictEqual(yield* view(atOnce.doc), expected)

        // One at a time, shuffled and repeated, to a replica that saves them,
        // and to a relay whose subscriber takes what it sends: they accept the
        // same ones. Behind the relay, a replica that's reopened from what it
        // saved now and then, so it starts from texts saved partway.
        yield* Effect.scoped(Effect.gen(function*() {
          const oneByOne = yield* watched(yield* make("one-by-one"), Project)
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
            const received = yield* oneByOne.receive(InternalDelta.fromChanges([change]))
            const pushed = yield* Effect.exit(push(relay, shuffled, [change]))
            assert.strictEqual(Exit.isSuccess(pushed), Exit.isSuccess(received), JSON.stringify(change))
            yield* pull(back.doc, relay, shuffled)
            // Saved before the next arrives.
            yield* Effect.yieldNow
            if (yield* Random.nextBoolean) {
              yield* Scope.close(back.scope, Exit.void)
              back = yield* open()
            }
          }
          assert.deepStrictEqual(yield* view(oneByOne.doc), expected)
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
    { arbitrary: forgedRuns, timeout: forgedTimeout }
  )

  it.effect.prop(
    "direct peers, one on an older schema, accept the same changes and agree, whatever their write times",
    { session: ForgedSession, edits: EditSession, seed: Schema.Int },
    Effect.fnUntraced(function*({ edits, seed, session }) {
      yield* Effect.gen(function*() {
        const makeOld = (replica: string) =>
          ReplicatedDocument.make(OldProject, {
            initial: initialProject,
            strategy,
            replica: Replica.ReplicaId.make(replica)
          })
        // Two peers on the schema and one on an older version of it, editing
        // among forged changes, some stamped far ahead, which they take
        // directly. And one that takes only the forged changes.
        const peers = [yield* watched(yield* make("a"), Project), yield* watched(yield* make("b"), Project)] as const
        const old = yield* watched(yield* makeOld("old"), OldProject)
        const everyone = [...peers, old] as const
        const forged = yield* make("forged")
        const exchange = Effect.fnUntraced(function*(i: number, j: number) {
          const p = everyone[i % 3]!
          const q = everyone[j % 3]!
          const toP = yield* Replica.delta(q.doc, yield* Replica.version(p.doc))
          const toQ = yield* Replica.delta(p.doc, yield* Replica.version(q.doc))
          // What one honest peer made, another accepts.
          for (const exit of [yield* p.receive(toP), yield* q.receive(toQ)]) {
            assert.isTrue(Exit.isSuccess(exit), String(exit))
          }
        })
        let next = 0
        // An edit by the next peer, or for `Wait` and `Reopen`, an exchange
        // with the one after it.
        const step = Effect.fnUntraced(function*(command: (typeof edits)[number]) {
          const at = next++
          if (command._tag === "Wait" || command._tag === "Reopen") return yield* exchange(at, at + 1)
          const peer = everyone[at % 3]!
          const edited = yield* peer.tracked(editProject(peer.doc, command, { wait: Effect.void, reopen: Effect.void }))
          assert.isTrue(Exit.isSuccess(edited))
          // A newer one also says when it's due, which the older one can't.
          const newer = peers.find((other) => other === peer)
          if (command._tag === "Status" && newer !== undefined) {
            const due = command.status === "done" ? "fri" : "mon"
            yield* newer.tracked(ReplicatedDocument.update(newer.doc, (project) => ({ ...project, due })))
          }
        })

        yield* forgeSession(strategy, session, (batch) =>
          Effect.gen(function*() {
            if (next < edits.length) yield* step(edits[next]!)
            const delta = InternalDelta.fromChanges(batch)
            const [received, ...others] = yield* Effect.forEach(everyone, (peer) => peer.receive(delta))
            for (const exit of others) assert.strictEqual(Exit.isSuccess(exit), Exit.isSuccess(received!), String(exit))
            yield* Effect.exit(Replica.receive(forged, yield* hop(delta)))
            const kept = (yield* Replica.delta(forged, VersionVector.empty)) as InternalDelta.DeltaImpl
            return (change: InternalDocument.AnyChange) => kept.changes.has(key(change))
          }), { ahead: true })
        for (const command of edits.slice(next)) yield* step(command)
        for (let round = 0; round < 2; round++) {
          for (const [i, j] of [[0, 1], [0, 2], [1, 2]] as const) yield* exchange(i, j)
        }

        const [a, b] = yield* Effect.forEach(peers, (peer) => view(peer.doc))
        assert.deepStrictEqual(b, a)
        // The older one shows the parts it describes, and keeps the rest.
        assert.deepStrictEqual(yield* ReplicatedDocument.get(old.doc), asOld(a!.value))
        assert.deepStrictEqual(yield* Replica.version(old.doc), a!.version)
        const loaded = yield* make("loaded")
        yield* Replica.receive(loaded, throughJson(yield* Replica.delta(old.doc, VersionVector.empty)))
        assert.deepStrictEqual(yield* view(loaded), a)
        // The parts only the newer schema has.
        const isNewer = (path: ReadonlyArray<string>) => path[0] === "due" || path[0] === "meta" || path[2] === "extra"
        // The older one's edits leave them alone.
        const fromOld = (yield* Replica.delta(old.doc, VersionVector.empty)) as InternalDelta.DeltaImpl
        for (const change of fromOld.changes.values()) {
          if (change.replica !== "old") continue
          for (const op of change.ops) assert.isFalse(isNewer(op.path ?? []), JSON.stringify(op))
        }
        // And it isn't told of overlaps in them.
        for (const edit of old.told) {
          for (const { path } of edit._tag === "Remote" ? edit.overlaps : []) {
            assert.isFalse(isNewer(path), JSON.stringify(path))
          }
        }
      }).pipe(Random.withSeed(seed))
    }),
    { arbitrary: { ...forgedRuns, runs: 50 }, timeout: forgedTimeout }
  )
})

// Schema versions ---------------------------------------------------------

// Replicas on an older and a newer version of a schema, editing and syncing
// among changes a client of the newer one makes directly, which they take
// only when an authority on the newer one lets them through.

describe("schema versions", () => {
  // What a value shows at `path`, as JSON. List items are found by their `id`.
  const shownAt = (value: unknown, path: ReadonlyArray<string>): string | undefined =>
    JSON.stringify(partAt(value, path))
  const ids = (value: unknown, path: ReadonlyArray<string>) =>
    JSON.stringify((partAt(value, path) as ReadonlyArray<{ readonly id: string }> | undefined)?.map(({ id }) => id))

  it.effect.prop(
    "replicas on each version write only what they change, and agree with each other and the authority",
    { session: PlannerSession },
    Effect.fnUntraced(function*({ session }) {
      // Every change the authority took, with what its check read.
      const proposals: Array<{ readonly proposed: ReplicatedDocument.Proposed<Planner>; readonly delta: Delta.Delta }> =
        []
      const Reading = Authority.layer({
        check: (proposal) =>
          proposal._tag === "Text"
            ? Effect.fail(new Authority.Rejected({ message: "Planners aren't texts" }))
            : Effect.map(
              ReplicatedDocument.readProposal(proposal, Planner, { initial: initialPlanner }),
              (proposed) => {
                proposals.push({ proposed, delta: proposal.delta })
              }
            )
      }).pipe(Layer.provide(HistoryStore.layerMemory))
      yield* Effect.gen(function*() {
        const authority = yield* Authority.Authority
        const document = Replica.DocumentId.make("planner")
        const push = (delta: Delta.Delta) => authority.push({ format: Sync.FORMAT, document, delta })
        const newer = (replica: string) =>
          ReplicatedDocument.make(Planner, { initial: initialPlanner, replica: Replica.ReplicaId.make(replica) })
        const older = (replica: string) =>
          ReplicatedDocument.make(OldPlanner, { initial: initialPlanner, replica: Replica.ReplicaId.make(replica) })
        const old = yield* older("old")
        // Edited alike: each writes what its version can.
        const replicas = [
          { doc: yield* newer("a"), schema: Planner, version: "new" },
          { doc: yield* newer("b"), schema: Planner, version: "new" },
          { doc: old as unknown as ReplicatedDocument.ReplicatedDocument<Planner>, schema: OldPlanner, version: "old" }
        ] as const
        const docs = replicas.map(({ doc }) => doc)

        let forged = 0
        for (const command of session) {
          switch (command._tag) {
            case "Sync":
              if (command.from !== command.to) yield* Replica.exchange(docs[command.from]!, docs[command.to]!)
              break
            case "Forge": {
              const doc = docs[command.replica]!
              const has = (yield* Replica.delta(doc, VersionVector.empty)) as InternalDelta.DeltaImpl
              const ops = forgedPlannerOps(command.ops)
              const delta = InternalDelta.fromChanges([{
                replica: `forger-${forged++}`,
                seq: 0,
                length: ops.length,
                // On top of everything the replica has.
                parents: Object.entries(yield* Replica.version(doc)).map(([replica, next]) => ({
                  replica,
                  seq: next - 1
                })),
                time: Math.max(0, ...[...has.changes.values()].map(({ time }) => time)) + 1,
                ops
              }])
              if ((yield* push(delta))._tag === "Accepted") yield* Replica.receive(doc, delta)
              break
            }
            default: {
              const { doc, schema, version } = replicas[command.replica]!
              const before = yield* ReplicatedDocument.get(doc)
              const since = yield* Replica.version(doc)
              const value = command._tag === "Same" ? before : editPlanner(version, before, command)
              yield* ReplicatedDocument.set(doc, value)
              // It shows what it's given, as its schema reads it.
              const after = yield* ReplicatedDocument.get(doc)
              assert.deepStrictEqual(after, Schema.decodeUnknownSync(Schema.toType(schema))(value))
              // It writes only what changed in what it shows: nothing of what
              // it can't read, nor a default it shows. A move changes where
              // the item is, and an entry or item that appears is written
              // over what was kept of it.
              const delta = (yield* Replica.delta(doc, since)) as InternalDelta.DeltaImpl
              for (const op of [...delta.changes.values()].flatMap(({ ops }) => ops)) {
                const path = op.path ?? []
                const appears = path.some((_, i) =>
                  partAt(before, path.slice(0, i + 1)) === undefined &&
                  partAt(after, path.slice(0, i + 1)) !== undefined
                )
                assert.isTrue(
                  appears || shownAt(before, path) !== shownAt(after, path) ||
                    (op._tag === "Place" && ids(before, path.slice(0, -1)) !== ids(after, path.slice(0, -1))),
                  `${version} wrote ${JSON.stringify(op)}`
                )
              }
              // And the authority, on the newer version, takes it.
              if (delta.changes.size > 0) assert.strictEqual((yield* push(delta))._tag, "Accepted")
            }
          }
        }
        yield* syncAll(docs)

        const value = yield* ReplicatedDocument.get(docs[0]!)
        assert.deepStrictEqual(yield* ReplicatedDocument.get(docs[1]!), value)
        const version = yield* Replica.version(docs[0]!)
        for (const doc of docs) {
          assert.deepStrictEqual(yield* Replica.version(doc), version)
          // Setting what it shows writes nothing.
          yield* ReplicatedDocument.set(doc, yield* ReplicatedDocument.get(doc))
          assert.deepStrictEqual(yield* Replica.version(doc), version)
        }
        // A replica on the older version that takes it all at once, as JSON,
        // shows what the older one does.
        const fresh = yield* older("fresh")
        yield* Replica.receive(fresh, throughJson(yield* Replica.delta(old, VersionVector.empty)))
        assert.deepStrictEqual(yield* ReplicatedDocument.get(fresh), yield* ReplicatedDocument.get(old))
        // The check read each change as a replica on the newer version shows
        // it, given them in the order it checked them.
        const replica = yield* newer("replica")
        for (const { delta, proposed } of proposals) {
          assert.deepStrictEqual(proposed.before, yield* ReplicatedDocument.get(replica))
          yield* Replica.receive(replica, delta)
          assert.deepStrictEqual(proposed.after, yield* ReplicatedDocument.get(replica))
        }
        assert.deepStrictEqual(yield* ReplicatedDocument.get(replica), value)
      }).pipe(Effect.provide(Reading))
    }),
    { arbitrary: documentRuns }
  )
})
