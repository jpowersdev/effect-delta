---
title: "Presence"
description: "Who's in a document, and what they've selected."
sidebar:
  order: 6
---

`Presence.join` joins a document for the life of the scope. `set` publishes this session's selection and any JSON you like,
and `peers` streams everyone in the document.

```ts import.meta.vitest
import { Presence, Replica, Sync } from "@jpowersdev/effect-delta"
import { Effect, Option, Stream } from "effect"

const notes = Replica.DocumentId.make("notes")

const program = Effect.gen(function*() {
  const ada = yield* Presence.join({ document: notes }).pipe(
    Effect.provideService(Sync.CurrentUser, "ada")
  )
  const ben = yield* Presence.join({ document: notes }).pipe(
    Effect.provideService(Sync.CurrentUser, "ben")
  )

  yield* ada.set({ selection: Option.none(), data: { name: "Ada" } })

  const seen = yield* ben.peers.pipe(
    Stream.filter((peers) => peers.some((peer) => peer.state.data !== null)),
    Stream.runHead
  )
  const users = Option.getOrThrow(seen).map((peer) => peer.user)
  yield* Effect.log(users)
  return users
})

await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(Presence.layer))) // => ["ada"]
```

Presence is never stored or merged. It lives on the server while a client stays connected.

Selections are anchors, so a peer finds one in its own text with `ReplicatedText.locateRange`, which is `None` until it has
received those characters.

Over a network, `Presence` has `layerHandlers` and `layerRemote`, like [`Relay`](../syncing/#serve-a-relay). Merge
`Presence.Rpcs` with `Sync.Rpcs` to serve both from one server. A client opens a connection for each.
