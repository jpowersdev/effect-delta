# effect-delta design notes

## Goal

Effect-native components for tracking and merging changes to shared state —
"Yjs, but Effect". The first target is **real-time collaborative text** for
local-first apps whose replicas sync with each other (peer-to-peer) or through
a server.

## Decisions so far

- **Written as if it will be upstreamed into Effect.** If it is, consumers
  upgrade with a find-and-replace of `effect-delta` → `effect/delta` in their
  imports, and nothing else changes for them: same exports, same behaviour,
  and data they've stored or sent still loads (the library name never appears
  in anything encoded). Same tooling, configuration, directory structure, and
  doc conventions as the Effect monorepo (v4).
- **Build on Effect concepts instead of replacing them.** Reuse `Schema`,
  `Differ`, `eventlog`, `Stream`/`PubSub`, `Socket`/`Rpc`,
  `KeyValueStore`, etc. wherever they fit.
- **Correct, readable, idiomatic first.** Performance later, but data layouts
  should not rule it out.
- **Examples show usage**, as simple as possible while still realistic. At
  least one per supported strategy.
- **Scope:** replicated data types, delta/merge rules, encoding, and a
  transport-agnostic sync protocol. Transports and storage are thin adapters
  over existing Effect modules. Presence/awareness comes later.
- **Format:** our own, Schema-defined. Interop with Yjs/Automerge formats is a
  possible later adapter, not a goal.

## Phases

1. Explore the ecosystem and agree on vocabulary → [`vocabulary.md`](./vocabulary.md) (decided)
2. Pick the strategies we support → [`strategies.md`](./strategies.md) (decided: tree, authority, replay)
3. Design the public API → [`api.md`](./api.md) (decided)
4. Build, with examples

Raw research notes live in [`research/`](./research).
