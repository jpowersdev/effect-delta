import { assert, describe, it } from "@effect/vitest"
import { Effect, Equal, Random, Schema } from "effect"
import { Delta, Replica, ReplicatedText, VersionVector } from "effect-delta"
import * as InternalDelta from "effect-delta/internal/delta"

// Random editing sessions across three replicas, with partial syncs along the
// way. Positions are fractions of the current text, so every generated command
// is valid whatever the text is by then.

const Which = Schema.Literals([0, 1, 2])
const Fraction = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 1000 }))

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

const letters = (s: string) => [...s].map((c) => "abc"[c.charCodeAt(0) % 3]!).join("")

const run = Effect.fnUntraced(function*(session: typeof Session.Type) {
  const replicas = [
    yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("r0") }),
    yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("r1") }),
    yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("r2") })
  ] as const
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

const syncAll = Effect.fnUntraced(function*(replicas: ReadonlyArray<ReplicatedText.ReplicatedText>) {
  for (let round = 0; round < 2; round++) {
    for (let i = 0; i < replicas.length; i++) {
      for (let j = i + 1; j < replicas.length; j++) yield* Replica.exchange(replicas[i]!, replicas[j]!)
    }
  }
})

describe("convergence", () => {
  it.effect.prop(
    "replicas that have exchanged everything show the same text",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const replicas = yield* run(session)
      yield* syncAll(replicas)
      const [first, ...rest] = yield* Effect.forEach(replicas, ReplicatedText.get)
      for (const text of rest) assert.strictEqual(text, first)
      const [version, ...versions] = yield* Effect.forEach(replicas, Replica.version)
      for (const other of versions) assert.deepStrictEqual(other, version)
    }),
    { arbitrary: { runs: 200 } }
  )

  it.effect.prop(
    "receiving is order-independent and safe to repeat",
    { session: Session, seed: Schema.Int },
    Effect.fnUntraced(function*({ session, seed }) {
      const replicas = yield* run(session)
      const deltas = yield* Effect.forEach(replicas, (replica) => Replica.delta(replica, VersionVector.empty))
      const everything = deltas.reduce(Delta.combine, Delta.empty)

      const atOnce = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("at-once") })
      yield* Replica.receive(atOnce, everything)

      const changes = [...(everything as InternalDelta.DeltaImpl).changes.values()]
      const shuffled = yield* Random.shuffle([...changes, ...changes]).pipe(Random.withSeed(seed))
      const oneByOne = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("one-by-one") })
      for (const change of shuffled) yield* Replica.receive(oneByOne, InternalDelta.fromChanges([change]))

      yield* syncAll(replicas)
      const expected = yield* ReplicatedText.get(replicas[0])
      assert.strictEqual(yield* ReplicatedText.get(atOnce), expected)
      assert.strictEqual(yield* ReplicatedText.get(oneByOne), expected)
    }),
    { arbitrary: { runs: 100 } }
  )

  it.effect.prop(
    "combining deltas is commutative, associative and idempotent",
    { session: Session },
    Effect.fnUntraced(function*({ session }) {
      const replicas = yield* run(session)
      const [a, b, c] = yield* Effect.forEach(replicas, (replica) => Replica.delta(replica, VersionVector.empty))
      assert.isTrue(Equal.equals(Delta.combine(a!, b!), Delta.combine(b!, a!)))
      assert.isTrue(Equal.equals(Delta.combine(Delta.combine(a!, b!), c!), Delta.combine(a!, Delta.combine(b!, c!))))
      assert.isTrue(Equal.equals(Delta.combine(a!, a!), a!))
      assert.isTrue(Equal.equals(Delta.combine(a!, Delta.empty), a!))
    }),
    { arbitrary: { runs: 100 } }
  )
})

// The interleaving cases from "The Art of the Fugue": two people typing at the
// same spot without seeing each other must each get a contiguous passage.
describe("non-interleaving", () => {
  const typeForward = Effect.fnUntraced(function*(replica: ReplicatedText.ReplicatedText, at: number, text: string) {
    for (let i = 0; i < text.length; i++) yield* ReplicatedText.insert(replica, at + i, text[i]!)
  })
  const typeBackward = Effect.fnUntraced(function*(replica: ReplicatedText.ReplicatedText, at: number, text: string) {
    for (let i = text.length - 1; i >= 0; i--) yield* ReplicatedText.insert(replica, at, text[i]!)
  })

  const setup = Effect.gen(function*() {
    const alice = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("alice") })
    const bob = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("bob") })
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
      const reloaded = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("alice-2") })
      yield* Replica.exchange(alice, reloaded)
      yield* typeBackward(reloaded, 1, "mi")
      yield* typeBackward(bob, 1, "eggs")
      yield* Replica.exchange(reloaded, bob)
      assertOneOf(yield* ReplicatedText.get(bob), ["[milkeggs]", "[eggsmilk]"])
    }))
})
