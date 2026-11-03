/**
 * A spec server that accepts edits only from editors. A reviewer's edit is
 * rejected and rolled back on their screen, and never reaches anyone else.
 *
 * Strategy: tree, with an authority.
 *
 *   pnpm add @jpowersdev/effect-delta @effect/platform-node effect
 *   node spec-review.ts
 */
import { NodeHttpServer, NodeRuntime, NodeServices, NodeSocket } from "@effect/platform-node"
import { Authority, HistoryStore, Replica, ReplicatedText, Sync } from "@jpowersdev/effect-delta"
import { Effect, Layer, Option, Stream } from "effect"
import { SecureRandom } from "effect/crypto"
import { Headers, HttpRouter } from "effect/http"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import { createServer } from "node:http"

// The app's own records: who holds each session token, and each person's role.
const sessions = new Map([["token-ada", "ada"], ["token-ben", "ben"]])
const roles = new Map([["ada", "editor"], ["ben", "reviewer"]])

// Server -------------------------------------------------------------------

// Who's connecting, from the session token their app sent when the socket
// opened. Anyone signed in may open specs.
const SignedIn = Layer.succeed(Sync.Authorization)({
  identify: (headers) =>
    Headers.get(headers, "authorization").pipe(
      Option.flatMap((value) => Option.fromNullishOr(sessions.get(value.replace(/^Bearer /, "")))),
      Option.match({
        onNone: () => Effect.fail(new Sync.Forbidden({ message: "Sign in first" })),
        onSome: Effect.succeed
      })
    ),
  authorize: () => Effect.void
})

// The rule: only editors edit.
const EditorsOnly = Authority.layer({
  check: ({ user }) =>
    roles.get(user) === "editor"
      ? Effect.void
      : Effect.fail(new Authority.Rejected({ message: "Reviewers can comment but not edit" }))
}).pipe(Layer.provide(HistoryStore.layerMemory))

const ServerLive = HttpRouter.serve(
  RpcServer.layer(Sync.Rpcs).pipe(
    Layer.provide(Authority.layerHandlers),
    Layer.provide([EditorsOnly, SignedIn]),
    Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/sync" }))
  ),
  { disableLogger: true }
).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: 3000 })),
  Layer.provide(RpcSerialization.layerNdjson)
)

// Clients ------------------------------------------------------------------

// What each person's app provides: the authority on the server, over a
// WebSocket that carries their session token.
const AuthorityRemote = (token: string) =>
  Authority.layerRemote.pipe(
    Layer.provide(RpcClient.layerProtocolSocket()),
    Layer.provide(
      NodeSocket.layerWebSocket("ws://localhost:3000/sync", { headers: { authorization: `Bearer ${token}` } })
    ),
    Layer.provide(RpcSerialization.layerNdjson)
  )

const spec = Replica.DocumentId.make("spec-42")

// Opens the spec, and waits until it has caught up with the server.
const openSpec = Effect.gen(function*() {
  const text = yield* ReplicatedText.makeConfirmed({ document: spec })
  yield* text.connection.status.pipe(Stream.filter((status) => status._tag === "Synced"), Stream.runHead)
  return text
})

// Ada, an editor, writes the spec, and waits until the server has it.
const ada = Effect.gen(function*() {
  const text = yield* openSpec
  yield* ReplicatedText.insert(text, 0, "Sessions last 24 hours.")
  yield* text.connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)
}).pipe(Effect.scoped, Effect.provide(AuthorityRemote("token-ada")))

// Ben, a reviewer, tries to approve it in the text.
const ben = Effect.gen(function*() {
  const text = yield* openSpec
  const { edits } = yield* ReplicatedText.subscribe(text)
  yield* ReplicatedText.insert(text, 0, "LGTM ")
  yield* Effect.log("Ben typed:", yield* ReplicatedText.get(text))

  yield* edits.pipe(
    Stream.filter((edit) => edit._tag === "Rejected"),
    Stream.take(1),
    Stream.runForEach((edit) => Effect.log("Rejected:", edit.message))
  )
  yield* Effect.log("Ben sees: ", yield* ReplicatedText.get(text))
}).pipe(Effect.scoped, Effect.provide(AuthorityRemote("token-ben")))

// Ada opens the spec again later.
const adaAgain = Effect.gen(function*() {
  const text = yield* openSpec
  yield* Effect.log("Ada sees: ", yield* ReplicatedText.get(text))
}).pipe(Effect.scoped, Effect.provide(AuthorityRemote("token-ada")))

Effect.all([ada, ben, adaAgain]).pipe(
  Effect.provide([ServerLive, NodeServices.layer, SecureRandom.layerWebCrypto]),
  NodeRuntime.runMain
)
