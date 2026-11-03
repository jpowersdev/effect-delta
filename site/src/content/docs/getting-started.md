---
title: "Getting started"
description: "Install effect-delta and sync two replicas."
---

:::caution
Unstable: every module may change in any minor release.
:::

## Install

```sh
pnpm add @jpowersdev/effect-delta effect
```

`effect` is a peer dependency, `^4.1.1`.

## A first program

Every example in these docs is a complete program. Save this as `main.ts` and run `node main.ts`, on Node.js 24 or later
in a package with `"type": "module"`. An example that needs another package says so.

```ts import.meta.vitest
import { HistoryStore, Relay, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect, Layer, Stream } from "effect"

const notes = Replica.DocumentId.make("notes")

const program = Effect.gen(function*() {
  const laptop = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("laptop")
  })
  const phone = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("phone") })

  // Each syncs through the relay for as long as the scope is open
  yield* Relay.connect(laptop, { document: notes })
  yield* Relay.connect(phone, { document: notes })

  yield* ReplicatedText.insert(laptop, 0, "Hello")

  // Wait until the phone shows it
  yield* ReplicatedText.changes(phone).pipe(
    Stream.filter((text) => text === "Hello"),
    Stream.runHead
  )
  yield* Effect.log(yield* ReplicatedText.get(phone))
})

const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))

await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(RelayLive)))
```

A replica is a handle you pass to module functions, like a `Queue` or `SubscriptionRef`. Functions with two or more
arguments also work in `.pipe`.

What a replica syncs through is a service, with one layer that runs in this process and another that reaches a server. The
same code runs in both cases, so this program gets its relay in-process, and [Syncing](../guides/syncing/) moves it to a
server.

## Where to next

- [Concepts](../guides/concepts/) defines the terms these docs use, and what other libraries call them.
- [Strategies](../guides/strategies/) helps you choose how text merges and what a server does.
- The [API reference](../api/) covers every module.
- [`examples/`](https://github.com/jpowersdev/effect-delta/tree/main/examples) has a longer program for each strategy.
