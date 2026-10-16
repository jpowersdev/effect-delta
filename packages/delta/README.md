# effect-delta

Replicated data types, merge rules, and a transport-agnostic sync protocol for
collaborative state, built on [Effect](https://effect.website).

This package is written as if it will be upstreamed into Effect as the
`effect/delta` subpath. Every module is `@stability unstable` and may change in
any minor release.

> [!WARNING]
> Work in progress. Replicated text with the tree strategy works, offline and
> through relays and authorities over any `RpcClient` protocol. The replay
> strategy is next. See `examples/` and the design notes in `docs/`.

## Installation

```sh
pnpm add effect-delta effect
```

`effect` is a peer dependency.

## License

MIT
