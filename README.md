# effect-delta

Collaborative text for [Effect](https://effect.website): replicas that accept edits immediately and merge without
conflicts, a sync protocol over any `RpcClient` transport, and presence.

```sh
pnpm add @jpowersdev/effect-delta effect
```

`effect` is a peer dependency, `^4.1.1`.

## Use

Save this as `main.ts` and run `node main.ts`, on Node.js 24 or later in a package with `"type": "module"`.

```ts import.meta.vitest
import { HistoryStore, Relay, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect, Layer, Stream } from "effect"

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

const RelayLive = Relay.layer.pipe(Layer.provide(HistoryStore.layerMemory))

const text = await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(RelayLive))) // => "Hello"
console.log(text)
```

The [documentation](https://jpowersdev.github.io/effect-delta/) has guides and the API reference, and
[`examples/`](examples) has fuller programs, including a server that rejects edits.

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

`ReplicatedText` merges with one of two strategies: `tree`, the default, keeps a tree of every character, and `replay`
keeps only the text and its history. `Authority` works with `tree` only. Locating anchors under `replay` is slow on long
histories: about 3 s at 50 small edits to a 47 KB text.

## Status

Unstable: every module may change in any minor release.

## License

MIT
