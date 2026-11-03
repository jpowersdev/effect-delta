---
title: "Editing text"
description: "Create a replica, edit it, follow its changes and keep stable positions."
sidebar:
  order: 3
---

## Create a replica

`ReplicatedText.make` makes a replica that can sync with anything. It needs `SecureRandom`, from `effect/crypto`, unless
you give it a replica id. `SecureRandom.layerWebCrypto` provides it on Node.js, browsers and Workers.

```ts import.meta.vitest
import { ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect } from "effect"
import { SecureRandom } from "effect/crypto"

const program = Effect.gen(function*() {
  const notes = yield* ReplicatedText.make()
  yield* ReplicatedText.insert(notes, 0, "Hello")
  yield* ReplicatedText.insert(notes, 5, ", world")
  yield* ReplicatedText.remove(notes, 0, 7)
  yield* Effect.log(yield* ReplicatedText.get(notes))
})

await Effect.runPromise(program.pipe(Effect.provide(SecureRandom.layerWebCrypto)))
```

`ReplicatedText.makeConfirmed` makes a replica whose changes an authority confirms or rejects. See
[Syncing](../syncing/#decide-which-changes-are-accepted).

Indexes are UTF-16 code units, as in JavaScript strings. An edit outside the text, or one that would split an emoji in two,
fails with a `ReplicatedTextError`.

## Whole strings

`set` and `update` are for code that holds whole strings, such as find-and-replace or form fields. They turn the new string
into one replacement: the shortest stretch between the longest common prefix and suffix.

That decides how concurrent edits merge. Changing "24 hours" to "8 hours" on one replica and to "12 hours" on another
keeps both replacements:

```ts import.meta.vitest
import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect } from "effect"

const program = Effect.gen(function*() {
  const alice = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("alice")
  })
  const bob = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("bob")
  })
  yield* ReplicatedText.set(alice, "Sessions last 24 hours.")
  yield* Replica.exchange(alice, bob)

  yield* ReplicatedText.update(alice, (text) => text.replace("24", "8"))
  yield* ReplicatedText.update(bob, (text) => text.replace("24", "12"))

  yield* Replica.exchange(alice, bob)
  yield* Effect.log(yield* ReplicatedText.get(alice))
})

await Effect.runPromise(program)
```

Editors should use `insert`, `remove` and `applyPatch`, which say exactly what the user did.

## Edit from the current text

`modify` applies a patch worked out from the current text, with no edit or merge landing in between. Use it when an edit
depends on what's there, because other changes can land between a separate read and write.

```ts import.meta.vitest
import { Replica, ReplicatedText, TextPatch } from "@jpowersdev/effect-delta"
import { Effect } from "effect"

const program = Effect.gen(function*() {
  const notes = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("laptop")
  })
  yield* ReplicatedText.set(notes, "Hello")
  yield* ReplicatedText.modify(
    notes,
    ({ length }) => [undefined, TextPatch.insert(length, "!")]
  )
  yield* Effect.log(yield* ReplicatedText.get(notes))
})

await Effect.runPromise(program)
```

## Follow changes

`changes` streams the current text, then each new one. `subscribe` gives the text at that moment and every edit after it,
as patches.

An editor binding loads `text`, then applies `Remote` and `Rejected` patches and skips `Local` ones, which it made itself.
If the editor has keystrokes it hasn't handed to the replica yet, move the remote patch past them with
`TextPatch.transform`.

```ts import.meta.vitest
import { Replica, ReplicatedText, TextPatch } from "@jpowersdev/effect-delta"
import { Effect, Stream } from "effect"

const program = Effect.gen(function*() {
  const mine = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("mine")
  })
  const theirs = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("theirs")
  })
  yield* ReplicatedText.insert(theirs, 0, "Hello")

  // What an editor would show
  const { text, edits } = yield* ReplicatedText.subscribe(mine)
  let shown = text

  yield* Replica.exchange(mine, theirs)
  yield* edits.pipe(
    Stream.filter((edit) => edit._tag === "Remote"),
    Stream.take(1),
    Stream.runForEach((edit) =>
      Effect.sync(() => {
        shown = TextPatch.apply(edit.patch, shown)
      })
    )
  )
  yield* Effect.log(shown)
})

await Effect.runPromise(program.pipe(Effect.scoped))
```

## Keep a position

An index stops being right as soon as the text changes. An anchor attaches to a character and follows it through edits and
merges, and its side decides where text typed exactly at it goes.

```ts import.meta.vitest
import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect } from "effect"

const program = Effect.gen(function*() {
  const notes = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("laptop")
  })
  yield* ReplicatedText.insert(notes, 0, "world")
  const anchor = yield* ReplicatedText.anchor(notes, 0)
  yield* ReplicatedText.insert(notes, 0, "Hello, ")
  yield* Effect.log(yield* ReplicatedText.locate(notes, anchor))
})

await Effect.runPromise(program)
```

`locate` is `None` if this replica hasn't received that character yet, or the character was rolled back. `anchorRange` and
`locateRange` do the same for a selection, which is also `None` once all of it is deleted.

## See where edits overlap

Concurrent edits to the same stretch of text merge, but can read badly. `Replica.exchange` and `Replica.receive` return each
overlap, and `subscribe` reports them on `Remote` edits, so an editor can point them out.

```ts import.meta.vitest
import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect, Option } from "effect"

const program = Effect.gen(function*() {
  const alice = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("alice")
  })
  const bob = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("bob")
  })
  yield* ReplicatedText.set(alice, "Sessions last 24 hours.")
  yield* Replica.exchange(alice, bob)

  yield* ReplicatedText.update(alice, (text) => text.replace("24", "8"))
  yield* ReplicatedText.update(bob, (text) => text.replace("24", "12"))
  const overlaps = yield* Replica.exchange(alice, bob)

  const text = yield* ReplicatedText.get(alice)
  const range = Option.getOrThrow(
    yield* ReplicatedText.locateRange(alice, overlaps[0]!.anchors)
  )
  yield* Effect.log(text.slice(range.start, range.end))
})

await Effect.runPromise(program)
```

## See who typed what

`authorship` splits the current text into stretches, each with the replica that inserted it.

```ts import.meta.vitest
import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import { Effect } from "effect"

const program = Effect.gen(function*() {
  const owner = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("owner")
  })
  const agent = yield* ReplicatedText.make({
    replica: Replica.ReplicaId.make("agent")
  })
  yield* ReplicatedText.insert(owner, 0, "Ship it Friday.")
  yield* Replica.exchange(owner, agent)
  yield* ReplicatedText.insert(agent, 14, " after the review")
  yield* Replica.exchange(owner, agent)

  const authored = yield* ReplicatedText.authorship(owner)
  const stretches = authored.map(({ range, replica }) =>
    `${replica} ${range.start}-${range.end}`
  )
  yield* Effect.log(stretches)
})

await Effect.runPromise(program)
```
