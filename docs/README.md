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
  in anything encoded). Same tooling, configuration and doc conventions as Effect
  (v4), in a single package.
- **Build on Effect concepts instead of replacing them.** Reuse `Schema`,
  `Differ`, `eventlog`, `Stream`/`PubSub`, `Socket`/`Rpc`,
  `KeyValueStore`, etc. wherever they fit.
- **Correct, readable, idiomatic first.** Performance later, but data layouts
  should not rule it out.
- **Examples show usage**, as simple as possible while still realistic. At
  least one per supported strategy.
- **Scope:** replicated data types, delta/merge rules, encoding, a
  transport-agnostic sync protocol and presence. Transports and storage are thin
  adapters over existing Effect modules.
- **Format:** our own, Schema-defined. Interop with Yjs/Automerge formats is a
  possible later adapter, not a goal.

## Documents

- [`vocabulary.md`](./vocabulary.md): the terms these notes use
- [`strategies.md`](./strategies.md): how text merges: tree, authority and replay
- [`api.md`](./api.md): the public API and why it's shaped this way
- [`research/`](./research): raw notes from before those were decided

Tree, authority and replay are built. Schema-derived documents are next.
