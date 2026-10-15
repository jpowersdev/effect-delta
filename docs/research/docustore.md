# Prior art: docustore's `Replica`

Source: `/Users/jonathan/github.com/jpowersdev/docustore` (`packages/replica`, `docs/architecture.md`,
`docs/effect.md`), October 2026, Effect 4.0.1 + TypeScript 7. An internal prototype built for an
agent-first document store. **Flavor, not authority.** These notes record what we learned there; nothing
here is a decision for effect-delta.

## What it is

`Replica.optimistic(schema, { device, initial })` makes a local copy of a value. The merge rules are
**derived from an ordinary Schema** by walking its AST:

| Schema shape                                       | Merge rule                                                              |
| -------------------------------------------------- | ----------------------------------------------------------------------- |
| `Struct`                                           | field by field                                                          |
| `Record`                                           | entry by entry; removals kept as tombstones (`present: false` register) |
| `Schema.String` annotated with `Replica.text(...)` | character-level sequence CRDT (Fugue-style tree)                        |
| anything else                                      | last-writer-wins register, stamped by a hybrid logical clock            |

Public surface (`src/Replica.ts`):

```ts
interface Replica<A> extends Pipeable {
  readonly device: string
  readonly get: Effect<A>
  readonly changes: Stream<A> // current value, then each new value
  update(f: (value: A) => A): Effect<void> // local write, never waits on the network
  anchor(path, range): Effect<Option<Anchor>> // stable range: first + last character ids
  locate(path, anchor): Effect<Option<Range>>
  readonly version: Effect<Version> // version vector, opaque brand
  delta(since: Version): Effect<Delta> // only what `since` lacks
  receive(delta: Delta): Effect<ReadonlyArray<Overlap>>
}
Replica.sync(self, that) // in-memory two-step exchange
Replica.emptyVersion
Replica.isEmpty(delta)
```

Internally it's a `SubscriptionRef` holding `{ state, seen, value, last }`.

## Ideas worth keeping

- **Effect's `Combiner`/`Reducer` as the merge algebra.** `Combiner.max(order)` gives a last-writer-wins
  register, `Record.makeReducerUnion` gives a grow-only map or a version-vector join, and
  `Struct.makeCombiner` merges field by field. A state-based CRDT is a `Combiner` that is commutative,
  associative and idempotent. Those three laws can be stated as `it.prop` properties with Schema-generated
  inputs. A deliberately wrong rule ("latest sync wins") failed them, which shows the properties have
  teeth.
- **Merge rules derived from a Schema, with opt-in annotations.** Users write a plain Schema, and only
  fields that need special merging get marked (`Replica.text(Schema.String)`). That's very Effect-shaped.
- **Delta-state sync.** Copies exchange a `version` (version vector), then send a `delta` with the same
  shape as the state, omitting the parts the other side already has. A full state is also a valid delta.
  Property tests checked that merging deltas gives exactly what merging whole states gives.
- **Hybrid logical clock stamps** `{ time, counter, device }` with a total `Order`. A device with a wrong
  clock can't win forever. Time comes from `Clock`, so `TestClock` makes everything deterministic.
- **Merges report overlaps.** Any text CRDT converges, but it doesn't guarantee a sensible result:
  "24 hours" edited to "8 hours" on one device and "12 hours" on another merges to "812 hours". `receive`
  returns `Text` / `Value` / `Removed` overlaps wherever two copies changed the same thing without seeing
  each other's change. Detection is exact, because each character records who inserted and who deleted it,
  and the version vectors say which writes each side hadn't seen. Automerge exposes concurrent writes to
  one map key (`getConflicts`), but as far as we know none of Yjs, Automerge or Loro reports overlapping
  *text* edits as a merge result (unverified). It's a strong differentiator.
- **Stable positions** stored as the ids of the first and last characters (`Anchor`), with `locate`
  returning `Option<Range>`.
- **Merge the plain-language naming with Effect's.** It used "copy", "device", "overlap", "anchor" rather
  than "replica", "site", "conflict", "relative position". Some of those were chosen for docustore's
  audience. We need names that read well inside Effect core.
- **The house style guide** (`docs/effect.md`): `Effect.fn` / `fnUntraced`, `dual`, `Schema.TaggedError`
  with `reason`, `Context.Service`, `Clock`/`Random` instead of globals, `it.effect.prop`, and "break it
  once on purpose". It's consistent with Effect's own AGENTS.md. Worth reusing, checked against the
  Effect repo's own guidance.

## Lessons and things to rethink

- **Effect's `JsonPatch`/`Differ` don't help with merging.** Patches address array items by position, so
  replaying one device's patch on a document another device already changed lands in the wrong place.
  `Differ` describes local change. It doesn't describe concurrent merging.
- **Effect's `eventlog` doesn't merge.** When two devices change the same thing it hands both versions to
  app code. Its sync and transport layer could still carry our changes.
- **Writes are value-in, diff-out.** `update(f)` diffs the old string against the new one (by line, then
  by word, with a bounded Myers search) to infer the edit. That's great for agents doing find-and-replace,
  but it's wrong as the primitive for real-time editors:
  - diffs are ambiguous: in `"aa"` → `"aaa"`, which `a` was typed?
  - they cost time proportional to the text
  - they lose the user's intent at the cursor

  Explicit `insert(index, text)` / `delete(index, length)` should be the primitive, with value diffing as
  a convenience on top.
- **Size.** About 180 bytes per character of state (5.4 MB for a 30 KB document), and a one-sentence edit
  sent about 13 KB. One node per UTF-16 code unit, keyed by a string id, inside a plain object.
  **Run-length encoding** (one entry per run of characters typed together) is the known fix. Yjs, Loro and
  Diamond Types all do it. We should design the data model for runs from day one, even if the first
  implementation is simple.
- **Per-operation copying.** Each local write copied the whole character map. Each merge copied the node
  map (about 20 ms at 100 KB). Caching a tree index by state identity (a `WeakMap`) and patching it in
  place made realistic sizes workable. That's a sign the persistent data structure needs choosing
  deliberately (for example, a B-tree or rope of runs).
- **A shared initial value is assumed.** All copies start from the same `initial` value with a genesis
  stamp, so a device joining without it needs a snapshot. Better for text: documents start empty and the
  initial content is itself a change.
- **One node per UTF-16 code unit** means concurrent edits can split a surrogate pair. Decide on a unit
  (code unit vs. code point vs. grapheme) and how to keep strings well-formed.
- **Record removals win** over a concurrent edit inside the entry. It's reported, but a fixed policy may
  not suit every data shape.
- **Device-id keys in plain objects** needed own-property lookups, so that an id like `"constructor"`
  works. Prefer `Map`/`HashMap`.
- **Still open there:**
  - a guard against far-future clocks
  - transports (server, peer-to-peer)
  - `Replica.consistent` (authority-confirmed writes)
  - whether changes are signed by their authors, which affects the change format
  - lists and arrays (no sequence type for non-text yet)

## Relevance to effect-delta

The strongest carry-overs:

- `Combiner`/`Reducer` as the merge foundation, with law tests
- Schema-derived documents with annotations
- delta-state sync over version vectors
- HLC via `Clock`
- overlaps as a first-class merge result
- the house style

The main things to change for real-time collaborative text:

- explicit operations as the primitive instead of value diffs
- run-length entries and a deliberate persistent structure
- no shared-initial-value assumption
- probably a non-interleaving algorithm (FugueMax or Eg-walker) chosen on purpose rather than by default
