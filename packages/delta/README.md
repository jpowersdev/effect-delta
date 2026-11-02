# effect-delta

Collaborative text for [Effect](https://effect.website): replicas that accept edits immediately and merge without
conflicts, a sync protocol over any `RpcClient` transport, and presence.

```ts
import { Effect, Layer, Stream } from "effect"
import { HistoryStore, Relay, Replica, ReplicatedText } from "effect-delta"

const notes = Replica.DocumentId.make("notes")

const program = Effect.gen(function*() {
  const laptop = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })
  const phone = yield* ReplicatedText.make({ replica: Replica.ReplicaId.make("phone") })
  yield* Relay.connect(laptop, { document: notes })
  yield* Relay.connect(phone, { document: notes })

  yield* ReplicatedText.insert(laptop, 0, "Hello")
  return yield* ReplicatedText.changes(phone).pipe(Stream.filter((text) => text === "Hello"), Stream.runHead)
})

const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))
await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(RelayLive)))
```

[`examples/`](examples) has fuller cases, including a server that rejects edits.

## Modules

| Module           | What it is                                                            |
| ---------------- | --------------------------------------------------------------------- |
| `ReplicatedText` | A text that merges concurrent edits, and anchors that follow it       |
| `TextPatch`      | Edits as retain, insert and remove, which editors can apply directly  |
| `Replica`        | What every replicated value has: an id, a version, deltas and merging |
| `Delta`          | The changes one replica sends another                                 |
| `VersionVector`  | What a replica has seen                                               |
| `Sync`           | The sync protocol, connections and authorization                      |
| `Relay`          | A server that passes changes between replicas and keeps them          |
| `Authority`      | A server that decides which changes are accepted                      |
| `HistoryStore`   | Where a document's history is kept                                    |
| `Presence`       | Who else is in a document, and their selections                       |

Text merges with one of two strategies: `tree` (the default) keeps a tree of every character, and `replay` keeps only
the text and its history. Authorities work with `tree` only. Locating anchors under `replay` gets slow as history
grows, about 3 s at 50 small edits to a 47 KB text.

## Status

Unstable: every module is `@stability unstable` and may change in any minor release. Schema-derived documents aren't
built yet.

It's written to fit Effect's repository as `effect/delta`: replacing `effect-delta` with `effect/delta` in imports would
be the only change for a consumer, and stored data doesn't name the library. See [`docs/`](../../docs) for the design.

## Installation

```sh
pnpm add effect-delta effect
```

`effect` is a peer dependency, `~4.0.4`.

## License

MIT
