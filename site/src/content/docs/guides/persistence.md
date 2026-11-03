---
title: "Persistence"
description: "Keep a replica's history between sessions."
sidebar:
  order: 5
---

`HistoryStore.persist` loads saved history into a replica, then saves every change for the life of the scope. It saves once
more when the scope closes, so the last edits aren't lost.

```ts import.meta.vitest
import { HistoryStore, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect, Layer } from "effect"
import { KeyValueStore } from "effect/persistence"

const notes = Replica.DocumentId.make("notes")

const open = (session: string) =>
  Effect.gen(function*() {
    const text = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make(session) })
    yield* HistoryStore.persist(text, notes)
    return text
  })

const program = Effect.gen(function*() {
  yield* Effect.scoped(Effect.gen(function*() {
    const first = yield* open("session-1")
    yield* ReplicatedText.insert(first, 0, "Saved")
  }))
  const text = yield* Effect.scoped(Effect.flatMap(open("session-2"), ReplicatedText.get))
  yield* Effect.log(text)
  return text
})

const StoreLive = HistoryStore.layerKeyValueStore({ prefix: "notes" }).pipe(
  Layer.provide(KeyValueStore.layerMemory)
)

await Effect.runPromise(program.pipe(Effect.provide(StoreLive))) // => "Saved"
```

## Stores

- `HistoryStore.layerMemory` keeps everything in memory, for tests and relays that don't need to outlive their process.
- `HistoryStore.layerKeyValueStore` keeps it in any `KeyValueStore`, under your `prefix`, including
  `KeyValueStore.layerFileSystem`.
  [`manuscript.ts`](https://github.com/jpowersdev/effect-delta/blob/main/examples/manuscript.ts) uses files.
- For anything else, implement the `HistoryStore` service.

Each document can have one writer per store, so two tabs can't persist the same document to the same store.

## Replay snapshots

For the `replay` strategy, `persist` also writes a snapshot when its scope closes, and after every 4,096 characters of
history. Loading takes the text from the snapshot and merges only the changes saved after it.

## Confirmed replicas

A confirmed replica's unconfirmed changes are stored separately and rewritten after a rejection, so a rejected change never
comes back on reload.
