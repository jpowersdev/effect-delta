---
title: "Syncing"
description: "Sync replicas directly, through a relay, or through a server that decides what's accepted."
sidebar:
  order: 4
---

Replicas sync in three ways. Each runs over any transport, and none changes the code that edits or observes a text.

| Way                | When                                                       | Replica made with |
| ------------------ | ---------------------------------------------------------- | ----------------- |
| `Replica.exchange` | Two replicas you hold, such as in tests or over your own channel | `make`      |
| `Relay`            | A server passes changes along and stores them              | `make`            |
| `Authority`        | A server decides which changes are accepted                | `makeConfirmed`   |

A `make` replica can sync with several things at once, so connecting is explicit. A `makeConfirmed` replica has exactly one
path, its authority. It connects when it's made, and `Replica.exchange` and `Replica.receive` from anything else fail with
`AuthorityOnly`.

## Exchange directly

`Replica.delta` is what one replica has that another lacks, and `Replica.receive` takes it in. Receiving is safe to repeat,
in any order, and changes that arrive before what they build on wait inside the replica. So you can move deltas over any
channel you have. `Delta.Delta` is a `Schema`, so they encode to JSON.

```ts import.meta.vitest
import { Delta, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect, Schema } from "effect"

const program = Effect.gen(function*() {
  const laptop = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
  const phone = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("phone") })
  yield* ReplicatedText.insert(laptop, 0, "Hello")

  // Encoded on the laptop, sent however you like, and decoded on the phone
  const sent = Schema.encodeSync(Delta.Delta)(yield* Replica.delta(laptop, yield* Replica.version(phone)))
  yield* Replica.receive(phone, Schema.decodeUnknownSync(Delta.Delta)(sent))

  return yield* ReplicatedText.get(phone)
})

const text = await Effect.runPromise(program) // => "Hello"
console.log(text)
```

## Serve a relay

`Relay` and `Authority` are each one service with three layers:

| Layer           | What it is                                                                                                    |
| --------------- | ------------------------------------------------------------------------------------------------------------- |
| `layer`         | The real thing, in this process: on a server, or given straight to replicas in tests and single-process apps. |
| `layerRemote`   | The same service on a client, reaching a server over RPC.                                                     |
| `layerHandlers` | Serves the in-process service over RPC, checking each request with `Sync.Authorization`.                      |

[Getting started](../../getting-started/#a-first-program) uses `layer`. This program serves a relay over a WebSocket and
connects a client to it. It also needs `@effect/platform-node`:

```sh
pnpm add @effect/platform-node
```

```ts import.meta.vitest
import { NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { HistoryStore, Relay, Replica, ReplicatedText, Sync } from "@jpowersdev/effect-delta"
import { Effect, Layer, Stream } from "effect"
import { HttpRouter } from "effect/http"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import { createServer } from "node:http"

const ServerLive = HttpRouter.serve(
  RpcServer.layer(Sync.Rpcs).pipe(
    Layer.provide(Relay.layerHandlers),
    Layer.provide(Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))),
    // Anyone may connect. Provide your own `Sync.Authorization` for a real server.
    Layer.provide(Sync.layerAuthorizationAllowAll),
    Layer.provide(RpcServer.layerProtocolWebsocket({ path: "/sync" }))
  ),
  { disableLogger: true }
).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: 3101 })),
  Layer.provide(RpcSerialization.layerNdjson)
)

const RelayRemote = Relay.layerRemote.pipe(
  Layer.provide(RpcClient.layerProtocolSocket()),
  Layer.provide(NodeSocket.layerWebSocket("ws://localhost:3101/sync")),
  Layer.provide(RpcSerialization.layerNdjson)
)

const notes = Replica.DocumentId.make("notes")

const program = Effect.gen(function*() {
  const laptop = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
  const phone = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("phone") })
  yield* Relay.connect(laptop, { document: notes })
  yield* Relay.connect(phone, { document: notes })

  yield* ReplicatedText.insert(laptop, 0, "Hello")
  yield* ReplicatedText.changes(phone).pipe(Stream.filter((text) => text === "Hello"), Stream.runHead)
  return yield* ReplicatedText.get(phone)
})

const text = await Effect.runPromise(program.pipe(
  Effect.scoped,
  Effect.provide(RelayRemote),
  Effect.provide(ServerLive)
)) // => "Hello"
console.log(text)
```

The server and client share a process here so the example runs by itself. On a real deployment `ServerLive` is one program
and `RelayRemote` another. For peer-to-peer, one peer runs the relay and the others connect to it.

## Say who's connecting

`Sync.Authorization` runs on every request. `identify` decides who it's from, from its headers, so cookies and tokens work.
`authorize` decides read and write access per document. A replica id belongs to the first user who writes with it, and a push
with changes under an id owned by someone else is `Forbidden`.

```ts import.meta.vitest
import { Replica, Sync } from "@jpowersdev/effect-delta"
import { Effect, Layer, Option } from "effect"
import { Headers } from "effect/http"

const sessions = new Map([["token-ada", "ada"]])

const SignedIn = Layer.succeed(Sync.Authorization)({
  identify: (headers) =>
    Headers.get(headers, "authorization").pipe(
      Option.flatMap((value) => Option.fromNullishOr(sessions.get(value.replace(/^Bearer /, "")))),
      Option.match({
        onNone: () => Effect.fail(new Sync.Forbidden({ message: "Sign in first" })),
        onSome: Effect.succeed
      })
    ),
  authorize: ({ access }) =>
    access === "read" ? Effect.void : Effect.fail(new Sync.Forbidden({ message: "Read only" }))
})

const program = Effect.gen(function*() {
  const authorization = yield* Sync.Authorization
  const user = yield* authorization.identify(Headers.fromInput({ authorization: "Bearer token-ada" }))
  yield* authorization.authorize({ user, document: Replica.DocumentId.make("notes"), access: "read" })
  return user
})

const user = await Effect.runPromise(program.pipe(Effect.provide(SignedIn))) // => "ada"
console.log(user)
```

## Decide which changes are accepted

An authority checks each change, and the replica applies its own edits immediately, unconfirmed until the server answers.
It checks a push's changes in order and accepts the longest acceptable prefix. The confirmed replica then drops the rejected
change and every unconfirmed change after it, emits a `Rejected` edit with the patch that rolls the text back, and switches
to a new replica id.

```ts import.meta.vitest
import { Authority, HistoryStore, Replica, ReplicatedText, Sync } from "@jpowersdev/effect-delta"
import { Effect, Layer, Option, Stream } from "effect"
import { SecureRandom } from "effect/crypto"

const EditorsOnly = Authority.layer({
  check: ({ user }) =>
    user === "editor" ? Effect.void : Effect.fail(new Authority.Rejected({ message: "Read only" }))
}).pipe(Layer.provide(HistoryStore.layerMemory))

const program = Effect.gen(function*() {
  const spec = yield* ReplicatedText.makeConfirmed({ document: Replica.DocumentId.make("spec") })
  const { edits } = yield* ReplicatedText.subscribe(spec)

  yield* ReplicatedText.insert(spec, 0, "LGTM")
  const rejected = yield* edits.pipe(Stream.filter((edit) => edit._tag === "Rejected"), Stream.runHead)
  return Option.getOrThrow(rejected).message
})

const message = await Effect.runPromise(program.pipe(
  Effect.scoped,
  Effect.provideService(Sync.CurrentUser, "reviewer"),
  Effect.provide([EditorsOnly, SecureRandom.layerWebCrypto])
)) // => "Read only"
console.log(message)
```

Over a network, serve it the same way as a relay, with `Authority.layerHandlers` and `Authority.layerRemote`.
[`spec-review.ts`](https://github.com/jpowersdev/effect-delta/blob/main/examples/spec-review.ts) is a complete run with
session tokens and roles.

## Watch a connection

`Relay.connect` and a confirmed replica's `connection` report on the sync:

- `status` streams `Connecting`, `Syncing`, `Synced` and `Offline`.
- `unconfirmed` streams how many of this replica's changes the server hasn't acknowledged.
- `sync` waits until this replica holds everything the server had when it was asked.
- `await` fails if the connection gives up.

On `Disconnected` or `Unavailable` a connection retries with its `retry` schedule and resubscribes from where it is, so nothing is
lost. `SyncError` has a `reason` of `Forbidden`, `ProtocolViolation`, `Disconnected`, `Unavailable` or a `DeltaError`.

```ts import.meta.vitest
import { HistoryStore, Relay, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect, Layer, Stream } from "effect"

const program = Effect.gen(function*() {
  const notes = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
  const connection = yield* Relay.connect(notes, { document: Replica.DocumentId.make("notes") })
  yield* connection.status.pipe(Stream.filter((status) => status._tag === "Synced"), Stream.runHead)

  yield* ReplicatedText.insert(notes, 0, "Hello")
  yield* connection.unconfirmed.pipe(Stream.filter((count) => count === 0), Stream.runHead)
  return "The relay has every change"
})

const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))

const result = await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(RelayLive))) // => "The relay has every change"
console.log(result)
```

## What servers guarantee

- **They save before they share.** A pushed change is checked, stored, then sent to subscribers. If storing fails, the push
  fails with `Unavailable`, every subscription to that document ends with it, and the document is loaded again from the store.
- **A push that has started finishes,** even if its client goes away, so a store or `check` that never returns holds up that
  document's other pushes.
- **A change that builds on something the server doesn't have yet** waits on a relay. An authority can't check it, so it
  refuses the push as `Malformed`.
- **A document has one strategy,** taken from what's stored, or else from the first replica to subscribe or push. The other
  strategy is refused.

## Encoding

Wire and storage formats are JSON (`RpcSerialization.layerNdjson` or `layerJson`). Every RPC payload and stored record carries
a format version, and nothing encoded contains the library's name.
