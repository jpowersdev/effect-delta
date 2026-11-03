import { NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Presence, Replica, ReplicatedText, Sync } from "@jpowersdev/effect-delta"
import { Effect, Exit, Layer, Option, Scope, Stream } from "effect"
import { Headers, HttpRouter, HttpServer } from "effect/http"
import type * as NetAddress from "effect/net/NetAddress"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"

const notes = Replica.RoomId.make("notes")
const other = Replica.RoomId.make("other")
const named = (name: string): Presence.State => ({ selection: Option.none(), data: { name } })

const as = (user: string) => Effect.provideService(Sync.CurrentUser, user)

const seeing = (self: Presence.Joined, predicate: (peers: ReadonlyArray<Presence.Peer>) => boolean) =>
  self.peers.pipe(Stream.filter(predicate), Stream.runHead, Effect.map(Option.getOrThrow))

describe("Presence", () => {
  it.effect("everyone sees who's here and what they've said, but not themselves", () =>
    Effect.gen(function*() {
      const ada = yield* Presence.join({ room: notes }).pipe(as("ada"))
      const ben = yield* Presence.join({ room: notes }).pipe(as("ben"))
      assert.deepStrictEqual((yield* seeing(ada, (peers) => peers.length === 1)).map((peer) => peer.user), ["ben"])
      yield* ada.set(named("Ada"))
      const peers = yield* seeing(ben, (peers) => peers.some((peer) => peer.state.data !== null))
      assert.deepStrictEqual(peers.map(({ state, user }) => [user, state.data]), [["ada", { name: "Ada" }]])
    }).pipe(Effect.scoped, Effect.provide(Presence.layer)))

  it.effect("a selection is an anchor another replica can find in its own text", () =>
    Effect.gen(function*() {
      const mine = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("mine") })
      const theirs = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("theirs") })
      yield* ReplicatedText.insert(mine, 0, "Hello world")
      yield* Replica.exchange(mine, theirs)
      const selection = yield* ReplicatedText.anchorRange(mine, { start: 6, end: 11 })

      const ada = yield* Presence.join({ room: notes }).pipe(as("ada"))
      const ben = yield* Presence.join({ room: notes }).pipe(as("ben"))
      yield* ada.set({ selection: Option.some(selection), data: null })
      const [peer] = yield* seeing(ben, (peers) => peers.some((peer) => Option.isSome(peer.state.selection)))

      yield* ReplicatedText.insert(theirs, 0, ">> ")
      assert.deepStrictEqual(
        yield* ReplicatedText.locateRange(theirs, Option.getOrThrow(peer!.state.selection)),
        Option.some({ start: 9, end: 14 })
      )
    }).pipe(Effect.scoped, Effect.provide(Presence.layer)))

  it.effect("someone who leaves is gone for everyone, and rooms don't mix", () =>
    Effect.gen(function*() {
      const ada = yield* Presence.join({ room: notes }).pipe(as("ada"))
      const elsewhere = yield* Presence.join({ room: other }).pipe(as("eve"))
      const scope = yield* Scope.make()
      yield* Presence.join({ room: notes }).pipe(as("ben"), Scope.provide(scope))
      assert.strictEqual((yield* seeing(ada, (peers) => peers.length === 1))[0]!.user, "ben")
      assert.deepStrictEqual(yield* seeing(elsewhere, () => true), [])
      yield* Scope.close(scope, Exit.void)
      yield* seeing(ada, (peers) => peers.length === 0)
    }).pipe(Effect.scoped, Effect.provide(Presence.layer)))

  it.effect("one person in two tabs is two peers", () =>
    Effect.gen(function*() {
      const first = yield* Presence.join({ room: notes }).pipe(as("ada"))
      yield* Presence.join({ room: notes }).pipe(as("ada"))
      const peers = yield* seeing(first, (peers) => peers.length === 1)
      assert.strictEqual(peers[0]!.user, "ada")
    }).pipe(Effect.scoped, Effect.provide(Presence.layer)))

  it.effect("a client can't change another's state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.Presence
      const ada = yield* Presence.join({ room: notes }).pipe(as("ada"))
      yield* Presence.join({ room: notes }).pipe(as("ben"))
      const [ben] = yield* seeing(ada, (peers) => peers.length === 1)
      const forged = yield* presence.set({ room: notes, session: ben!.session, state: named("Fake") }).pipe(
        as("mallory"),
        Effect.flip
      )
      assert.strictEqual(forged.reason._tag, "Forbidden")
      // Saying something about a connection that's gone does nothing.
      yield* presence.set({ room: notes, session: 999, state: named("Late") }).pipe(as("ada"))
    }).pipe(Effect.scoped, Effect.provide(Presence.layer)))

  it.effect("state said before the server answered still arrives", () =>
    Effect.gen(function*() {
      const ada = yield* Presence.join({ room: notes }).pipe(as("ada"))
      yield* ada.set(named("Early"))
      const ben = yield* Presence.join({ room: notes }).pipe(as("ben"))
      const [peer] = yield* seeing(ben, (peers) => peers.some((peer) => peer.state.data !== null))
      assert.deepStrictEqual(peer!.state.data, { name: "Early" })
    }).pipe(Effect.scoped, Effect.provide(Presence.layer)))

  describe("over a WebSocket", () => {
    const Users = Layer.succeed(Sync.Authorization)({
      identify: (headers) =>
        Option.match(Headers.get(headers, "x-user"), {
          onNone: () => Effect.fail(new Sync.Forbidden({ message: "Sign in first" })),
          onSome: Effect.succeed
        }),
      authorize: ({ room, user }) =>
        room === other && user !== "ada"
          ? Effect.fail(new Sync.Forbidden({ message: "Not for you" }))
          : Effect.void
    })

    const Server = HttpRouter.serve(
      RpcServer.layer(Presence.Rpcs).pipe(
        Layer.provide(Presence.layerHandlers),
        Layer.provide([Presence.layer, Users]),
        Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/presence" }))
      ),
      { disableListenLog: true, disableLogger: true }
    ).pipe(
      Layer.provideMerge(NodeHttpServer.layerTest),
      Layer.provide(RpcSerialization.layerNdjson)
    )

    const clientFor = (user: string) =>
      Layer.build(Layer.fresh(Presence.layerRemote.pipe(
        Layer.provide(RpcClient.layerProtocolSocket({ retryTransientErrors: true })),
        Layer.provide(Layer.unwrap(Effect.gen(function*() {
          const server = yield* HttpServer.HttpServer
          const address = server.address as NetAddress.InetAddress
          return NodeSocket.layerWebSocket(`http://127.0.0.1:${address.port}/presence`, { headers: { "x-user": user } })
        }))),
        Layer.provide(RpcSerialization.layerNdjson)
      )))

    it.live("the server says who someone is, and who may see a room", () =>
      Effect.gen(function*() {
        const ada = yield* Presence.join({ room: notes }).pipe(Effect.provideContext(yield* clientFor("ada")))
        const ben = yield* Presence.join({ room: notes }).pipe(Effect.provideContext(yield* clientFor("ben")))
        yield* ada.set(named("Ada"))
        const peers = yield* seeing(ben, (peers) => peers.some((peer) => peer.state.data !== null))
        assert.deepStrictEqual(peers.map(({ user }) => user), ["ada"])

        const forbidden = yield* Effect.flatMap(
          Presence.Presence,
          (presence) => Stream.runHead(presence.join({ room: other }))
        ).pipe(Effect.flip, Effect.provideContext(yield* clientFor("ben")))
        assert.strictEqual(forbidden.reason._tag, "Forbidden")
      }).pipe(Effect.scoped, Effect.provide(Server)))
  })
})
