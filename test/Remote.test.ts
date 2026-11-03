import { NodeCrypto, NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Authority, HistoryStore, Relay, Replica, ReplicatedText, Sync } from "@jpowersdev/effect-delta"
import { Effect, Layer, Option, Stream } from "effect"
import { Headers, HttpRouter, HttpServer } from "effect/http"
import type * as NetAddress from "effect/net/NetAddress"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"

// A server and its clients talking over a real WebSocket, with JSON framing.

const spec = Replica.makeRoom("spec")
const drafts = Replica.makeRoom("drafts")

// Users sign in with a header. Only editors may edit, and only editors may
// read drafts.
const Users = Layer.succeed(Sync.Authorization)({
  identify: (headers) =>
    Option.match(Headers.get(headers, "x-user"), {
      onNone: () => Effect.fail(new Sync.Forbidden({ message: "Sign in first" })),
      onSome: Effect.succeed
    }),
  authorize: ({ room, user }) =>
    room === drafts && !user.startsWith("editor")
      ? Effect.fail(new Sync.Forbidden({ message: "Drafts are for editors" }))
      : Effect.void
})

const EditorsOnly = Authority.layer({
  check: ({ user }) =>
    user.startsWith("editor")
      ? Effect.void
      : Effect.fail(new Authority.Rejected({ message: "Reviewers can comment but not edit" }))
}).pipe(Layer.provide(HistoryStore.layerMemory))

const serve = <E, R>(handlers: Layer.Layer<never, E, R>) =>
  HttpRouter.serve(handlers, { disableListenLog: true, disableLogger: true }).pipe(
    Layer.provideMerge(NodeHttpServer.layerTest),
    Layer.provide(RpcSerialization.layerNdjson)
  )

const AuthorityServer = serve(
  RpcServer.layer(Sync.Rpcs).pipe(
    Layer.provide(Authority.layerHandlers),
    Layer.provide([EditorsOnly, Users]),
    Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/sync" }))
  )
)

const RelayServer = serve(
  RpcServer.layer(Sync.Rpcs).pipe(
    Layer.provide(Relay.layerHandlers),
    Layer.provide([Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory)), Users]),
    Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/sync" }))
  )
)

// One client's connection to the test server, signed in as `user`.
const protocolFor = (user: string) =>
  RpcClient.layerProtocolSocket().pipe(
    Layer.provide(Layer.unwrap(Effect.gen(function*() {
      const server = yield* HttpServer.HttpServer
      const address = server.address as NetAddress.InetAddress
      return NodeSocket.layerWebSocket(`http://127.0.0.1:${address.port}/sync`, { headers: { "x-user": user } })
    }))),
    Layer.provide(RpcSerialization.layerNdjson)
  )

const authorityFor = (user: string) =>
  Layer.build(Layer.fresh(Authority.layerRemote.pipe(Layer.provide(protocolFor(user)))))
const relayFor = (user: string) => Layer.build(Layer.fresh(Relay.layerRemote.pipe(Layer.provide(protocolFor(user)))))

const showing = (self: ReplicatedText.ReplicatedText, text: string) =>
  ReplicatedText.changes(self).pipe(Stream.filter((current) => current === text), Stream.runHead, Effect.asVoid)

describe("over a WebSocket", () => {
  it.live("an authority accepts editors' changes and rejects reviewers'", () =>
    Effect.gen(function*() {
      const editor = yield* ReplicatedText.makeConfirmed({ room: spec }).pipe(
        Effect.provideContext(yield* authorityFor("editor-1"))
      )
      const reviewer = yield* ReplicatedText.makeConfirmed({ room: spec }).pipe(
        Effect.provideContext(yield* authorityFor("reviewer-1"))
      )
      const { edits } = yield* ReplicatedText.subscribe(reviewer)

      yield* ReplicatedText.insert(editor, 0, "Spec v1")
      yield* showing(reviewer, "Spec v1")

      yield* ReplicatedText.insert(reviewer, 0, "LGTM ")
      const rejected = yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
      assert.deepStrictEqual(Option.getOrThrow(rejected), {
        _tag: "Rejected",
        patch: [{ _tag: "Remove", length: 5 }],
        message: "Reviewers can comment but not edit"
      })
      assert.strictEqual(yield* ReplicatedText.get(reviewer), "Spec v1")
    }).pipe(Effect.scoped, Effect.provide([AuthorityServer, NodeCrypto.layer])))

  it.live("a connection the server forbids gives up", () =>
    Effect.gen(function*() {
      const reviewer = yield* ReplicatedText.makeConfirmed({ room: drafts }).pipe(
        Effect.provideContext(yield* authorityFor("reviewer-1"))
      )
      const error = yield* Effect.flip(reviewer.connection.await)
      assert.deepStrictEqual(error.reason, new Sync.Forbidden({ message: "Drafts are for editors" }))
    }).pipe(Effect.scoped, Effect.provide([AuthorityServer, NodeCrypto.layer])))

  it.live("a relay passes changes between users and keeps their ids apart", () =>
    Effect.gen(function*() {
      const alice = yield* relayFor("alice")
      const bob = yield* relayFor("bob")
      const laptop = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
      const phone = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("phone") })
      yield* Relay.connect(laptop, { room: spec }).pipe(Effect.provideContext(alice))
      yield* Relay.connect(phone, { room: spec }).pipe(Effect.provideContext(bob))

      yield* ReplicatedText.insert(laptop, 0, "Hello")
      yield* showing(phone, "Hello")
      yield* ReplicatedText.insert(phone, 5, ", world")
      yield* showing(laptop, "Hello, world")

      // Bob can't push changes under Alice's replica id.
      const relay = yield* Effect.provideContext(Effect.service(Relay.Relay), bob)
      const delta = yield* Replica.delta(laptop, { phone: 7 })
      const error = yield* Effect.flip(relay.push({ format: Sync.FORMAT, room: spec, delta }))
      assert.strictEqual(error.reason._tag, "Forbidden")
    }).pipe(Effect.scoped, Effect.provide(RelayServer)))
})
