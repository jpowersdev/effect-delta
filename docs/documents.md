# Documents

The design for documents: replicas whose shape is an ordinary `Schema`, such as a design doc with a title, a
body, a status and comments. It takes the same three steps text did (vocabulary, how things merge, then the
API) and builds on what's decided in [`vocabulary.md`](./vocabulary.md), [`strategies.md`](./strategies.md)
and [`api.md`](./api.md).

**Status: decided and built** (2026-10-15), with lists. Building changed a few details; they're listed under
[Changed while building](#changed-while-building), and the sections below show the result.

## In one paragraph

A document's merge rules come from its schema. A struct merges field by field, and a record merges entry by
entry. A string marked as text merges character by character, with the document's strategy (tree or
replay). A number marked as a counter merges by adding, an array marked as a list merges item by item, and
anything else takes the latest write. The rest
works as it does for text: one replica per device, changes with parents, deltas, version vectors, relays,
authorities and history stores. A document is a second kind of replica, not a second sync stack (API
decision 1). Its text fields are edited, watched and anchored with the same `ReplicatedText` functions as a
standalone text, so one editor binding works for both.

```ts
const Comment = Schema.Struct({
  author: Schema.String,             // latest write
  range: ReplicatedText.AnchorRange, // latest write: the stretch of the body it's about
  text: ReplicatedDocument.Text      // character by character
})

const DesignDoc = Schema.Struct({
  title: ReplicatedDocument.Text,
  body: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  votes: ReplicatedDocument.Counter,
  comments: Schema.Record(Schema.String, Comment) // entry by entry
})
```

## Vocabulary

New terms only; the rest are in [`vocabulary.md`](./vocabulary.md).

| Term              | Meaning                                                                                                                                                   | Elsewhere                                                   | Builds on                                     |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------- |
| **Field**         | A named part of a struct: `title`, `status`.                                                                                                              | Yjs and Automerge map keys, Loro container keys             | `Schema.Struct` fields                        |
| **Entry**         | A key and its content in a record. Entries are added and removed.                                                                                         | Automerge map key, Riak map entry                           | `Schema.Record`; Effect's `Record` module     |
| **Path**          | Where a part of a document is: field names, record keys and list item keys from the root, such as `["comments", "c1", "text"]`. The root is `[]`. | Automerge path, JSON Pointer, json-joy path                 | `ReadonlyArray<string>`, like Schema's issue paths |
| **Merge rule**    | How one part of a document combines concurrent changes, worked out from its schema: fields, entries, text, counter, items or latest write.                | CRDT type, Loro container type, Collabs "Collab"            | `SchemaAST`, walked like `Schema.toEquivalence` |
| **Latest write**  | The default rule: the value written last shows. "Last" is by write time, then replica id.                                                                 | Last-writer-wins (LWW) register                             | `Order`, `Combiner.max`                       |
| **Write time**    | When a change was made, in milliseconds from `Clock`, moved later when needed so a change is always later than everything its replica had seen.            | Hybrid logical clock (HLC); Automerge's change `time`       | `Clock`                                       |
| **Initial value** | What a document shows for parts nobody has written yet. Part of the document's definition, like its schema, so it's the same everywhere.                    | docustore `initial`; json-joy schema defaults               | —                                             |
| **Counter**       | A whole number that merges by adding: two concurrent +1s make +2.                                                                                          | PN-counter; Automerge and Loro counters                     | —                                             |
| **List**          | An ordered collection whose items can be inserted, removed and moved concurrently.                                                                         | Y.Array, Automerge list, Loro MovableList                   | `Schema.Array`                                |
| **Item**          | One element of a list, known by its key: a field the app sets, or for a list of values, the id of the write that added it.                                 | Automerge list element, Loro MovableList element            | —                                             |

**Words we avoid**, in addition to the existing list:

| Word                       | Why                                                                                       | What we say instead                     |
| -------------------------- | ----------------------------------------------------------------------------------------- | --------------------------------------- |
| "register"                 | Academic.                                                                                 | **value**, **latest write**             |
| "container"                | Loro's word, and vague in Effect.                                                         | **part**, or field, entry, text         |
| "map" for a record         | Effect's `HashMap` and JavaScript's `Map` are other types; the schema says `Record`.      | **record**, **entry**                   |
| "property"                 | Schema's AST says property signature, but Effect's user-facing word is field (`Struct.fields`). | **field**                          |
| "timestamp", "clock"       | Effect's `Clock` is wall-clock time, and a write time isn't quite that.                   | **write time**                          |

## Merge rules

| Schema                                                                     | Rule         | Concurrent changes                                                   |
| -------------------------------------------------------------------------- | ------------ | -------------------------------------------------------------------- |
| `Struct`, `Class`                                                          | fields       | each field merges on its own                                         |
| `Record(String, X)`                                                        | entries      | each entry merges on its own; adding and removing entries are writes |
| an optional field (`optionalKey`, `optional`)                              | entry        | like a record entry with a fixed key                                 |
| `ReplicatedDocument.Text`                                                  | text         | character by character, with the document's strategy                 |
| `ReplicatedDocument.Counter`                                               | counter      | the increments add up                                                |
| `ReplicatedDocument.List(item, { key })`, `ReplicatedDocument.List(value)` | items        | inserts, removals and moves all survive; items merge by their schema |
| anything else: strings, numbers, literals, unions, arrays, `Option`, `Date` | latest write | the later write shows, and the merge reports an overlap              |

The root must be a struct, a class or a record. A document that's one text is a `ReplicatedText`.

### Latest write

A write is ordered by its change's write time, then its replica id, then its sequence number. A value shows
its largest write.

A change's write time is later than every change its replica had seen. So a write always beats the writes it
was made on top of, even when the device's clock is behind. Only writes made without seeing each other are
ordered by the clock, and then the one made later wins, which is what people expect. Writes in the same
millisecond go to the larger replica id: arbitrary, but the same everywhere. When concurrent writes put
different values, the merge reports a `Value` overlap that says whose write it kept. As with text, an
overlap is between a write the replica had and one it received, so loading a document reports none.

The alternatives:
- **Lamport order** (Automerge, Loro): the write with more history behind it wins. It needs no clock, but a
  change made yesterday at the end of a long offline stretch beats one made today, which looks random.
- **Replica id order** (Yjs maps): arbitrary, and the same replica wins every time.

What time costs: a device whose clock is far ahead wins concurrent writes until the other clocks catch up,
and replicas that see its changes stamp their own later changes after them. Changes stay correctly ordered.
What's off is which concurrent write wins and, later, the times a history view shows. Relays and
authorities reject changes stamped more than a day ahead of their own clock, so a broken clock (or a
forged time) can't spread from them; peers that exchange directly trust each other's, up to the latest
time a clock reads.

### Fields

Each field merges by its own rule, so concurrent edits to different fields never meet. A `Class` merges like
the struct it's made of, and reading builds instances of the class.

### Entries

An entry has two parts, merged separately:
- **Whether it exists.** That's a value, set by adding or removing the entry, and the latest write wins.
- **Its content**, merged by the entry's schema.

That gives two behaviours:
- **A removal wins over edits made inside the entry without seeing it**, edits to an entry that's a text
  or a counter included. An edit inside an entry says nothing about whether the entry exists, so a removal
  still removes it. The merge reports a `Removed` overlap. Most apps treat a deleted card or comment the
  same way.
- **Content is kept when an entry is removed**, everyone's edits included, even ones that arrive after the
  removal. So undoing a removal (later) can bring it all back. Adding the key again with `update` writes
  the content it's given over what was kept, as an edit to it: texts are compared with the kept text, and
  values are written again. Likewise, a key that two replicas add concurrently is one entry whose contents
  merge. Two entries would leave one hidden behind the other, the problem Loro fixed with mergeable
  containers.

The alternative is **edits keep an entry**: an edit inside counts as adding it, so a later edit brings back an
entry removed earlier. We don't recommend it, because something you just deleted can reappear because of an
edit you never saw.

An optional field works like an entry with a fixed key. `undefined` counts as absent.

### Text fields

A text field uses the document's strategy (`strategy: "tree" | "replay"`). Ids for its characters come from
the document's sequence numbers, so each character id is unique across the document. Anchors and overlaps
work per field. With replay, a field replays only the changes that touched it.

### Counters

A counter holds whole numbers. Fractions added in a different order on each replica can round differently in
floating point, so replicas would disagree. An `update` that takes a counter from 5 to 7 records +2, and
concurrent +2 and +3 make 10. Totals are kept exactly, because whole numbers past 2^53 round too; a total
past the safe integers reads as the nearest one, the same on every replica, so it's still an `Int`. `update`
records the difference from the exact total, in as many increments as it takes, so the counter then shows
the value it was given.

### Lists

`ReplicatedDocument.List(Task, { key: "id" })` is an array whose items are identified by a field the app
sets, usually a random id, as React keys are. `update` compares the old and new arrays by key:
- new keys are inserts;
- missing keys are removals;
- keys in a different order are moves (the fewest that give the new order);
- keys in both merge their content by the item's schema.

An item whose key field doesn't say its own key (which `update` never writes) is left out, as an item that
can't be read is, and a constructor default never fills a key in.

Underneath, a list is a record (an entry per key) plus each item's position. A position is a node in a
tree, ordered by the tree strategy's rule, so items inserted concurrently at the same spot don't
interleave. Moving an item gives it a new position, and its latest position wins (Kleppmann, "Moving
elements in list CRDTs", 2020), so two people moving the same item never duplicate it; the merge reports a
`Value` overlap for it. A removal wins over concurrent edits inside the item, as with entries.

`ReplicatedDocument.List(Schema.String)` is a list of values, compared by value. A changed value counts as
removing one item and inserting another. An item is known by the id of the write that added it.

Lists use the tree rule whatever the document's strategy: a list's tree has one node per placement, not
per character.

### Whole values

Anything without its own rule is written and replaced whole.
- **Arrays** that aren't lists: if two people each add an item, one write wins and the merge reports it.
  Comparing two arrays can't tell an edited item from a replaced one, so item-by-item merging needs ids for
  items, which a list provides.
- **Unions** (a block that's a paragraph or an image), `Option`, `Date`, sets and maps are replaced whole.
- Text and counters can't sit inside a whole value (a union, an array, an `Option`), because they'd be
  replaced whole too. `make` refuses such a schema, as a defect.

### Checks

- **On a value, checks always hold.** The merged value is always one of the writes, and each write was checked
  when it was made and when it was received.
- **On a merged part, they can't.** A merge combines writes that nobody checked together. That covers a check
  across a struct's fields, a text's length, a record's size, or a counter's range. `make` refuses such a
  schema, as a defect. Rules like these belong in an authority's check, which sees every change before anyone
  else does.

### The initial value

`make` takes `initial`: what the document shows for parts nobody has written. It's never sent, so every
replica must use the same one. Define it next to the schema, never from local data. To create a document
with content, the replica creating it writes the content once, as a change like any other.

Texts, records and lists start empty in `initial`, and optional fields start absent; `make` refuses an
`initial` that says otherwise, as a defect. Starting text is a change, so two replicas can't each insert the
same starting text and end up with it twice, as Yjs apps sometimes do. And whether an entry, item or
optional field exists is then always its latest write, never `initial`, so a server's JSON (which has no
`initial`) says the same.

Entries, items and optional fields have no initial value. One that's missing a required field (written by an
older version of the schema) is left out, unless the field has a constructor default
(`Schema.withConstructorDefault`), which fills it in. Every replica fills it in as it reads, so the default
must be a constant: one that reads the clock or makes a random id would show differently on each replica.
An optional field can't have one, because it would show while the field is absent; `make` refuses it.

The alternatives:
- **Defaults in the schema** (`Schema.withDecodingDefaultTypeKey` on every field): pure Schema, but verbose
  for every field.
- **Writing the initial value on creation**: every replica opening a new document would write it,
  duplicating its text.

### When the schema changes

Apps upgrade one device at a time, so replicas on different versions of a schema sync with each other.
- **Adding a field:** documents that never wrote it show its initial value. Add it to `initial`. A field
  added to an entry's or item's schema should be optional or have a constant constructor default.
- **Parts an older schema doesn't describe** (a new field, a value it can't decode, such as a new literal)
  are kept, stored and sent on, but don't show in its value, and merges don't report overlaps in them. A
  value whose latest write it can't decode shows the latest write it can. After the upgrade, they show.
- **Renaming or removing a field, or changing its rule** (a value becoming text) isn't supported, because
  paths are field names. Lenses (Cambria) are later. A replica keeps changes its schema has another rule
  for, and passes them on, but they don't show: a path keeps a text, values and a counter apart, and each
  replica reads the one its schema says.

Replicas on the same version always show the same value. Replicas on different versions have the same history,
but can show different values for the parts only one of them understands.

## How changes look

The change envelope gains a write time, on every change, texts included:
`{ replica, seq, length, parents, time, ops }`. Receivers check that it's later than each parent's, and
no later than any clock reads (a `Date`'s last millisecond) unless it's a millisecond after its parents'
(`Malformed` otherwise).

Every op names a path. An absent path means the root, so a `ReplicatedText`'s changes look exactly as
they do now, plus `time`.

| Op                                                      | Sequence numbers | What it does                                                              |
| ------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------- |
| `Insert`, `Remove` (tree); `InsertAt`, `RemoveAt` (replay) | one per character | Edits the text at the path, as now.                                     |
| `Set { path, value }`                                   | 1                | Writes a value, encoded with its schema's JSON codec. On an entry, item or optional field, it also means it exists. |
| `Add { path }`                                          | 1                | The entry, item or optional field at the path exists (for content that isn't a value). |
| `Unset { path }`                                        | 1                | Removes the entry, item or optional field at the path.                    |
| `Increment { path, by }`                                | 1                | Adds to the counter at the path.                                          |
| `Place { path, parent, side, rightOrigin? }`            | 1                | Gives the list item at the path a new position, placed as a tree `Insert` places a character. |

A change's ops on one text come together, each op takes at least one sequence number, and its parents are the
last ids of changes. Receivers check all three (`Malformed` otherwise), and check that text ops and positions
name only characters and positions their author had seen at their path, and put nothing on the left of the
start, so whether a change is accepted doesn't depend on what else the receiver has.

**Servers don't need the schema.** Ops say what they do, so a relay or an authority applies a document's
changes without the app's code. Values are JSON, texts are texts, counters are numbers. A server checks what
it can (the envelope, write times, text ops against that path's text) and stores the rest as it is. One
relay serves every app, and every version of each app's schema.

**Replicas and relays don't reject a change for not fitting a schema.** Any one replica that did would
refuse what the others accept, and a relay that did would cut off everyone it had passed the change to.
Replicas keep such changes and don't show them, as they do for parts a newer schema adds. An authority is
where a change can be turned down, before anyone else has it: it rejects ops that would make one path hold
two kinds of thing (a text and a value, a value with parts under it, or a list and a struct) or remove
something nobody added, and `readProposal` rejects anything else that doesn't fit the app's schema,
including a list item added without a place in its list, which the authority couldn't tell from an entry.

**Texts and documents shouldn't share a document id.** If they do, each keeps the other's changes without
showing them. A change with no text ops fits either strategy; servers learn a document's strategy from its
first text op, or from the replica that subscribes.

## API

### Example: design review

Covers fields, entries, a text field, an anchor stored in a document, and a `Value` overlap. It becomes
`examples/design-review.ts`.

```ts
/**
 * A design doc with a title, a body, a status, and comments pinned to the
 * body. Two people work on it offline. Their edits merge, and the merge says
 * where they both changed the same thing.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Effect, Option, Schema } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"

const Comment = Schema.Struct({
  author: Schema.String,
  range: ReplicatedText.AnchorRange,
  text: ReplicatedDocument.Text
})

const DesignDoc = Schema.Struct({
  title: ReplicatedDocument.Text,
  body: ReplicatedDocument.Text,
  status: Schema.Literals(["draft", "review", "approved"]),
  comments: Schema.Record(Schema.String, Comment)
})

// What a design doc shows before anyone writes to it. The same on every device.
const initial: typeof DesignDoc.Type = { title: "", body: "", status: "draft", comments: {} }

const program = Effect.gen(function*() {
  const ada = yield* ReplicatedDocument.make(DesignDoc, { initial })
  yield* ReplicatedDocument.update(ada, (doc) => ({
    ...doc,
    title: "Sessions",
    body: "Sessions last 24 hours. Tokens refresh hourly."
  }))

  // Ben's laptop gets a copy before they both go offline.
  const ben = yield* ReplicatedDocument.make(DesignDoc, { initial })
  yield* Replica.exchange(ada, ben)

  // Ben comments on the second sentence and approves.
  const range = yield* ReplicatedText.anchorRange(ReplicatedDocument.text(ben, ["body"]), { start: 24, end: 46 })
  yield* ReplicatedDocument.update(ben, (doc) => ({
    ...doc,
    status: "approved",
    comments: { ...doc.comments, c1: { author: "ben", range, text: "Hourly seems often." } }
  }))

  // Ada changes that sentence, and sends the doc back for review.
  const body = ReplicatedDocument.text(ada, ["body"])
  yield* ReplicatedText.update(body, (text) => text.replace("hourly", "daily"))
  yield* ReplicatedDocument.update(ada, (doc) => ({ ...doc, status: "review" }))

  // Back online.
  const overlaps = yield* Replica.exchange(ada, ben)

  const doc = yield* ReplicatedDocument.get(ada)
  const pinned = yield* ReplicatedText.locateRange(body, doc.comments.c1!.range)
  yield* Console.log(Option.getOrUndefined(Option.map(pinned, ({ end, start }) => doc.body.slice(start, end))))
  // Tokens refresh daily.    the comment still points at its sentence

  yield* Console.log(doc.status, overlaps)
  // review [{ _tag: "Value", path: ["status"], replicas: [...], kept: <Ada's id> }]
  //   (whichever status was written later; the same on both laptops)
})

program.pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain)
```

### ReplicatedDocument

```ts
interface ReplicatedDocument<A> extends Replica.Replica<A> {}
interface Confirmed<A> extends ReplicatedDocument<A> { readonly connection: Sync.Connection }

interface Options<A> {
  readonly initial: A                                // what parts nobody has written show; the same everywhere
  readonly strategy?: "tree" | "replay" | undefined  // for text fields; default "tree"
  readonly replica?: Replica.ReplicaId | undefined   // default: a fresh random id
}

declare const make: <A>(schema: Schema.Schema<A>, options: Options<A>) => Effect<ReplicatedDocument<A>, never, Crypto>

// Answers to an authority, like ReplicatedText.makeConfirmed. Tree-only.
declare const makeConfirmed: <A>(schema: Schema.Schema<A>, options: {
  readonly initial: A
  readonly document: Replica.DocumentId
  readonly replica?: Replica.ReplicaId | undefined
  readonly retry?: Schedule | undefined
}) => Effect<Confirmed<A>, never, Authority | Crypto | Scope>

// Marking parts that don't take the latest write
declare const Text: Schema.String // merges character by character
declare const Counter: Schema.Int // a whole number that merges by adding
declare const List: {             // an array that merges item by item
  <S extends Schema.Top>(item: S, options: { readonly key: KeyOf<S["Type"]> }): Schema.$Array<S>
  <S extends Schema.Top>(item: S): Schema.$Array<S> // a list of values
}

// Reading
declare const get: <A>(self: ReplicatedDocument<A>) => Effect<A>
declare const changes: <A>(self: ReplicatedDocument<A>) => Stream<A> // current value, then each new one
declare const subscribe: <A>(self: ReplicatedDocument<A>) => Effect<Subscription<A>, never, Scope>

interface Subscription<A> {
  readonly value: A               // the value at the moment of subscribing
  readonly edits: Stream<Edit<A>> // every edit after it, none missed
}

type Edit<A> =
  | { readonly _tag: "Local"; readonly value: A }
  | { readonly _tag: "Remote"; readonly value: A; readonly overlaps: ReadonlyArray<Replica.Overlap> }
  | { readonly _tag: "Rejected"; readonly value: A; readonly message: string }

// Writing: each call is one change. Dual.
declare const update: <A>(self: ReplicatedDocument<A>, f: (value: A) => A) => Effect<void, Schema.SchemaError>
declare const set: <A>(self: ReplicatedDocument<A>, value: A) => Effect<void, Schema.SchemaError>

// A text field, for the ReplicatedText functions. Dual.
declare const text: <A>(self: ReplicatedDocument<A>, path: TextPath<A>) => ReplicatedText.Editable
type TextPath<A> = /* paths through fields, record keys and list item keys that end at a string */

// For an authority's check (below)
declare const readProposal: <A>(
  proposal: Authority.DocumentProposal,
  schema: Schema.Schema<A>,
  options: { readonly initial: A }
) => Effect<Proposed<A>, Authority.Rejected> // rejects a change that doesn't fit the schema
interface Proposed<A> { readonly before: A; readonly after: A; readonly paths: ReadonlyArray<Replica.Path> }
```

**`update` and `set` compare the old and new values part by part**, and write only what changed:
- values, with `Schema.toEquivalence`;
- text fields, with the one-replacement rule `ReplicatedText.set` uses;
- counters, by the difference;
- records, by which keys appear and disappear, then the content of entries in both; as decoding does, keys the
  record's key schema doesn't take are left out;
- lists, as [Lists](#lists) says.

The whole call is one change. It fails with `Schema.SchemaError` when a new value doesn't encode, for example
when it fails a check, or encodes to something JSON can't carry (`Schema.Any` passes anything on), or to JSON
that reads back as another value (a union of a number and a string writes the string `"NaN"` as the number's
JSON), and when two items of a list have the same key. Values are
written as JSON sends them, so `-0` is written as `0`, and a `-0` in JSON text, which other languages send,
is read as `0`.

**Text fields are handles.** `ReplicatedDocument.text(doc, path)` returns a `ReplicatedText.Editable`. Every
`ReplicatedText` function except the constructors takes one: `insert`, `remove`, `applyPatch`, `set`,
`update`, `get`, `changes`, `subscribe` and the anchor functions. A `ReplicatedText` is an `Editable` that is
also a replica. Editors should edit text fields through the handle, which says exactly what the user did;
`update` on the whole document has to diff. A handle on an entry that doesn't exist, or was removed, still
works: its text is kept (see [Entries](#entries)). The path is typed to string fields, and a path that isn't
a text field is a defect.

**`changes` keeps the identity of unchanged parts.** A new value shares every part that didn't change with the
previous one, so a UI can skip re-rendering them.

**Reading never fails.** Parts the schema doesn't describe are left out, and so are entries, items and optional
fields it can't read. Parts nobody wrote come from `initial`.

### Overlaps

`Replica.Overlap` becomes a union, and every overlap has a path:

```ts
type Path = ReadonlyArray<string>

type Overlap =
  | { readonly _tag: "Text"; readonly path: Path; readonly range: Range; readonly anchors: AnchorRange; readonly replicas: ReadonlyArray<ReplicaId> }
  // Both wrote different values here, both moved this list item, or one removed it; `kept` is whose write shows.
  | { readonly _tag: "Value"; readonly path: Path; readonly replicas: ReadonlyArray<ReplicaId>; readonly kept: ReplicaId }
  // One removed this entry, item or optional field while the other changed something inside it, or moved
  // it. It stays removed.
  | { readonly _tag: "Removed"; readonly path: Path; readonly replicas: ReadonlyArray<ReplicaId> }
```

A `ReplicatedText`'s overlaps are all `Text`, with path `[]`. Concurrent increments aren't overlaps,
because nothing is lost.

### Sync, storage and servers

- `Replica.exchange`, `Replica.receive`, `Relay.connect` and `HistoryStore.persist` take documents as they
  are.
- **Relays** need nothing new. They don't need the schema.
- **Authorities** check each change, as now. `Authority.Proposal` becomes a union. A `DocumentProposal` has
  the document before and after the change as JSON (only the parts that have been written), which entries,
  items and optional fields exist in each, the paths the change touched, and the change itself. `ReplicatedDocument.readProposal` reads them with the app's schema
  and initial value, and rejects a change that doesn't fit the schema: replicas wouldn't show it, so the
  JSON would show the check something they don't. The app picks the schema from the document id, as it
  already decides who may open which document:

  ```ts
  const OwnerApproves = Authority.layer({
    check: (proposal) =>
      Effect.gen(function*() {
        if (proposal._tag === "Text") return yield* new Authority.Rejected({ message: "Design docs aren't texts" })
        const { after, before } = yield* ReplicatedDocument.readProposal(proposal, DesignDoc, { initial })
        if (after.status === "approved" && before.status !== "approved" && proposal.user !== ownerOf(proposal.document)) {
          return yield* new Authority.Rejected({ message: "Only the owner can approve" })
        }
      })
  })
  ```

- **Snapshots** hold the text of every text field, by path: `{ version, texts: [{ path, text }] }`. Values and
  counters load quickly from history.

## Changes to what was built for text

Nothing has been released, so the format stays 1. None of these changes how a text is edited.

1. **Every change has a write time.** `ReplicatedText` starts reading `Clock`, which every Effect program
   has, so no requirement changes.
2. **Ops can name a path.** Texts never do.
3. **`Replica.Overlap` is a union, and `Text` overlaps have a `path`.**
4. **`ReplicatedText` functions take `ReplicatedText.Editable`:** a `ReplicatedText` or a document's text
   field. It's a wider parameter, so existing calls compile as they are.
5. **`Authority.Proposal` is a union** tagged `"Text"` or `"Document"`. Checks that only read `user`, like
   the spec-review example's, compile as they are.
6. **`HistoryStore.Snapshot` holds texts by path.**

## Decisions

Decided (2026-10-15):

1. **`ReplicatedDocument`**, made from a schema and an initial value, as the second kind of replica. It names
   the special part first, like `ReplicatedText`. *Alternatives:* `ReplicatedValue` (vague),
   `ReplicatedStruct` (too narrow: the root can be a record).
2. **Merge rules come from the schema**, with two markers, `ReplicatedDocument.Text` and
   `ReplicatedDocument.Counter`, and lists made with `ReplicatedDocument.List`. Everything else is a field,
   an entry, or a value that takes the latest write.
3. **The latest write is decided by write time** (wall clock, never earlier than what the replica has seen),
   then replica id. *Alternative:* Lamport order, which needs no clock but lets the longer history win.
4. **An entry's content is kept when it's removed, and a removal wins over edits it hadn't seen.** The merge
   reports it. *Alternative:* edits keep an entry.
5. **An explicit initial value**, the same everywhere. Text fields start empty. *Alternative:* decoding
   defaults on every field.
6. **Arrays are whole values unless they're lists.** `ReplicatedDocument.List` merges item by item, with
   items keyed by a field.
7. **Checks only on values.** `make` refuses checks on merged parts.
8. **Parts an older schema doesn't describe are kept, not shown, and never rejected.**
9. **One change format, and servers don't need the schema:** every change has a write time, and every op can
   name a path.
10. **Text fields are `ReplicatedText.Editable`**, so one editor binding serves both. *Alternative names:*
    `ReplicatedText.Field`.
11. **Counters are whole numbers.**
12. **Authorities check documents through a `DocumentProposal`**, read with the app's schema.

## Build order

All built, each with tests and an example:

1. Write times and paths in changes, a document state servers can apply without the schema, and
   `ReplicatedDocument` with the tree strategy: fields, entries, text fields, counters, values and overlaps.
   Relays and `HistoryStore` take documents. `examples/design-review.ts`.
2. Replay text fields. `examples/wiki.ts`.
3. Authorities for documents: the `Proposal` union, `readProposal` and `makeConfirmed`.
   `examples/approvals.ts`.
4. Lists. `examples/task-board.ts`.

## How we'll know it's correct

- **Convergence.** Random sessions over a test schema that uses every rule: three replicas, random delivery,
  both strategies. Every replica ends with the same value, and tree and replay give the same one.
- **A text field is a `ReplicatedText`.** The same edits on a field and on a standalone text give the same
  text, anchors and overlaps, step by step.
- **Latest write against a model.** Each value is its largest write, and a `Value` overlap is reported exactly
  when concurrent writes differ.
- **Entries.** A removal against concurrent edits, adding a removed entry back, and concurrent adds of one key.
- **Two versions of a schema.** Replicas on each sync through a relay. Both keep everything, the newer one
  shows its new field, and their histories are equal.
- **Servers without the schema.** A relay stores documents it can't read, and replicas reload them intact.
- **Authorities.** Checks see typed values before and after each change; a rejection rolls the change back.
  Among forged changes, an authority takes or turns down each change as a replica given the same changes in
  the same order does, and each proposal shows the document as that replica has it before and after.
- **Forged changes.** Changes made directly, many malformed or not fitting the schema, among well-formed ones
  from extra replicas, sent together, late, again and as JSON. Replicas, relays and authorities accept the
  same ones and show the same value however they arrive, and every merge reports the `Value` and `Removed`
  overlaps a model of the rule gives. With the tree strategy, it reports the `Text` overlaps the rule gives
  too, for these and for honest sessions, and replay reports what tree does.
- **Break it on purpose,** as before: drop the replica-id tiebreak, let edits keep entries, and so on, and
  confirm a property fails.

## Known limits

- **`update` compares the whole document,** so its cost grows with the document. Path-scoped writes come later.
- **An authority reads the whole document for every change it checks:** its JSON and which parts exist,
  before and after, so a check's cost grows with the document too.
- **`update` can't always tell which item moved.** When one item moves past a single neighbour, moving
  either gives the new order, and `update` picks one. Every replica still agrees; only a concurrent move of
  one of them shows the difference. An explicit move, like the text handles, comes with path-scoped writes.
- **An authority that loses changes it accepted** (a store restored from a backup, say) leaves confirmed
  replicas to recover one change at a time: a change it has lost is rolled back when it's pushed again,
  and one built on another replica's lost change is refused as malformed, which ends the connection of the
  replica that pushed it; opened again, it's refused again until that other replica has pushed first. A
  replica that had saved a lost change as confirmed brings it back each time it reopens, until it's pushed
  and rolled back again, with any edits made on top of it in the meantime.
- **Counters are exact only within the safe integers.** A counter's increments add up exactly, but it reads
  as a JavaScript number, so a total (with its initial value) beyond 2^53 reads as the nearest safe
  integer.
- **Values are written whole.** A large array that isn't a list changes as a whole.
- **A clock far ahead** decides concurrent writes until other clocks catch up (see
  [Latest write](#latest-write)).
- **Removed entries and items keep their content** until pruning exists.
- **Replicas on different schema versions** can show different values for the parts only one understands.
- **An authority whose check doesn't read proposals with the schema** keeps what the first write to a path
  says it holds: a client could claim a path with the wrong kind of write and leave later writes there
  rejected. `readProposal` rejects such writes, so a check that uses it has no such gap.
- **A replica whose clock ran more than a day ahead can't sync with servers** until their clocks reach the
  times it stamped. Fixing its clock doesn't help: each later change is stamped after the ones it made. The
  same goes for peers that took its changes in a direct exchange.
- **An authority needs the newest schema.** `readProposal` rejects what its schema can't read, so an
  authority on an older schema rejects the newer apps' changes to the parts it doesn't know. Upgrade the
  authority first.
- **A server forgets a strategy it only heard from a subscriber.** A document with no text ops yet takes the
  strategy of whoever subscribes first, and after the server reloads it, the next subscriber's.
- **A replay text finds its own parents by walking the history back** to the last change that touched it.
  A text edited rarely, in a document edited often, walks a long way. Remembering each change's latest
  text parts would fix that.
- **One long edit is slow to merge.** Trees are one node per character, so inserting or removing a long run
  costs its length times the tree's depth (see the text limits in [`api.md`](./api.md#known-limits)).

## Changed while building

1. **`initial` has no entries, items or optional fields.** Texts, records and lists start empty and optional
   fields start absent, so whether something exists is always its latest write. Otherwise a server's JSON,
   which has no `initial`, couldn't say that an entry in `initial` had been removed.
2. **Entries, items and optional fields that can't be read are left out**, such as one missing a required
   field, unless the field has a constructor default.
3. **Lists are `ReplicatedDocument.List(item, { key })`**, a constructor like `Schema.Array`, instead of
   `Schema.Array(item).pipe(list(...))`. Items are placed with a `Place` op, and a list of values knows each
   item by the id of the write that added it.
4. **Existence is `Set` for values and `Add` for everything else,** texts and counters included: an
   optional text field is added, then edited.
5. **Changes must keep one text's ops together, take a sequence number or more for each op, and name the
   last ids of changes as parents.** All three hold for every change a replica makes; receivers reject others
   as `Malformed`.
6. **A replay text's parents are found by walking the change graph,** not from version vectors. The
   Eg-walker conformance data has replica ids whose changes aren't in sequence order, which version vectors
   can't describe.
7. **`readProposal` is an `Effect` of a `Proposed<A>`,** and fails with `Authority.Rejected` when the change
   doesn't fit the schema, so a check reads the document as replicas will show it. Reading JSON
   alone let a client hide a write from a check: writing over it with a value replicas can't read, or
   removing a required field, which replicas ignore.
8. **Changes that don't fit a schema are kept, not rejected,** by replicas and relays alike (see
   [How changes look](#how-changes-look)). Rejecting them let one bad change cut every replica off from a
   relay for good. Authorities still keep each path to one kind of thing.
9. **Servers reject write times more than a day ahead of their clock.** A change stamped far in the future
   would leave everyone who saw it unable to write anything later.
10. **Every receiver refuses a write time later than any clock reads** (a `Date`'s last millisecond),
    unless it's a millisecond after its parents', as the changes of a replica that saw one are. A change
    stamped at the last whole numbers a change can carry left peers that took it directly unable to stamp a
    change after it, so nobody accepted their changes.

## Later

### Other things later

- **Sets** (`ReadonlySet`, `HashSet`) that merge element by element: a record of which elements are present.
- **Tagged unions** that merge field by field while their tag stays the same.
- **Values merged with a `Reducer`** the app gives, such as `Number.ReducerMax` for "last seen". Only for
  reducers that are commutative and idempotent, checked by law tests.
- **Path-scoped writes**, through `Optic`, so a write needn't compare the whole document, including an
  explicit list move.
- **Recursive schemas** (`Schema.suspend`), such as nested folders.
- **Renaming fields**, with lenses.
- **Starting text in `initial`.**
