import * as Equal from "effect/Equal"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import type * as Replica from "../Replica.ts"
import * as TextPatch from "../TextPatch.ts"
import * as VersionVector from "../VersionVector.ts"
import type { AnchorRange } from "./anchor.ts"
import {
  type Change,
  Change as ChangeSchema,
  compareReplica,
  compareWrites,
  type Id,
  Id as IdSchema,
  idKey,
  key,
  lastId
} from "./change.ts"
import * as Fugue from "./fugue.ts"
import * as History from "./history.ts"
import * as Path from "./path.ts"
import * as ReplayText from "./replayText.ts"
import * as Text from "./text.ts"
import * as TreeText from "./treeText.ts"

// A document's state, worked out from its changes without its schema, so
// replicas and servers share it. Every op names a path and says what it does,
// and each path keeps what its ops wrote:
//
// - a text, merged by the document's strategy;
// - values, of which the latest write shows;
// - whether it exists (for record entries, optional fields and list items),
//   which is a value too;
// - a counter, the sum of its increments;
// - for a list, the positions of its items.
//
// A path is a text, a counter or a value, or it has parts under it. Ops that
// mix those up are malformed. Replicas read the state with their schema
// (shape.ts); servers only check it, store it, and show it as JSON.

// Ops ------------------------------------------------------------------------

/** @internal */
export interface SetOp {
  readonly _tag: "Set"
  readonly path: Path.Path
  readonly value: Schema.Json
}

/** @internal */
export interface AddOp {
  readonly _tag: "Add"
  readonly path: Path.Path
}

/** @internal */
export interface UnsetOp {
  readonly _tag: "Unset"
  readonly path: Path.Path
}

/** @internal */
export interface IncrementOp {
  readonly _tag: "Increment"
  readonly path: Path.Path
  readonly by: number
}

/**
 * Gives the list item at `path` a new position in its list, placed as a tree
 * strategy character is.
 *
 * @internal
 */
export interface PlaceOp {
  readonly _tag: "Place"
  readonly path: Path.Path
  readonly parent: Id | null
  readonly side: "L" | "R"
  readonly rightOrigin?: Id | null
}

/** @internal */
export type Op = Text.Op | SetOp | AddOp | UnsetOp | IncrementOp | PlaceOp

/**
 * A value as JSON sends it: JSON has no -0, which `JSON.stringify` sends as 0.
 *
 * @internal
 */
export const asSent = (json: Schema.Json): Schema.Json => JSON.parse(JSON.stringify(json))

// JSON text can say -0 all the same. Decoded as it's sent on, a value shows
// the same on the replica that decodes it and those it sends it to.
const SentJson = Schema.Json.pipe(
  Schema.decode({ decode: SchemaGetter.transform(asSent), encode: SchemaGetter.passthrough() })
)

/** @internal */
export const Op: Schema.Codec<Op> = Schema.Union([
  TreeText.Op,
  ReplayText.Op,
  Schema.TaggedStruct("Set", { path: Path.Path, value: SentJson }),
  Schema.TaggedStruct("Add", { path: Path.Path }),
  Schema.TaggedStruct("Unset", { path: Path.Path }),
  Schema.TaggedStruct("Increment", { path: Path.Path, by: Schema.Int }),
  Schema.TaggedStruct("Place", {
    path: Path.Path,
    parent: Schema.NullOr(IdSchema),
    side: Schema.Literals(["L", "R"]),
    rightOrigin: Schema.optionalKey(Schema.NullOr(IdSchema))
  })
])

/** @internal */
export const AnyChange = ChangeSchema(Op)

/** @internal */
export type AnyChange = Change<Op>

/** @internal */
export const pathOf = (op: Op): Path.Path => op.path ?? Path.root

/** @internal */
export const opLength = (op: Op): number => Text.isOp(op) ? Text.opLength(op) : 1

/**
 * The strategy of the first text op in `changes`, if any.
 *
 * @internal
 */
export const strategyOf = (changes: Iterable<AnyChange>): Replica.Strategy | undefined => {
  for (const change of changes) {
    for (const op of change.ops) if (Text.isOp(op)) return Text.strategyOfOp(op)
  }
  return undefined
}

/**
 * Whether every text op in a change was made with `strategy`. A change without
 * text ops fits any.
 *
 * @internal
 */
export const fitsStrategy = (change: AnyChange, strategy: Replica.Strategy | undefined): boolean =>
  change.ops.every((op) => !Text.isOp(op) || Text.strategyOfOp(op) === strategy)

// State ----------------------------------------------------------------------

/**
 * One write: its op's sequence number, its change's write time, and what its
 * author had seen.
 *
 * @internal
 */
export interface Write {
  readonly replica: string
  readonly seq: number
  readonly time: number
  readonly past: VersionVector.VersionVector
}

/** @internal */
export interface ValueWrite extends Write {
  readonly value: Schema.Json
}

interface PresenceWrite extends Write {
  readonly exists: boolean
}

interface PlaceWrite extends Write {
  readonly node: Fugue.Node<Item>
}

// Which item a list position belongs to.
interface Item {
  readonly key: string
}

// What a path holds. A path that holds none of these has parts under it.
type Kind = "text" | "counter" | "value"

// What a path holds, judging by what's been written there. Replicas and
// relays keep every kind written to a path, and each reader takes the one its
// schema has; an authority keeps one.
const kindOf = (found: Slot | undefined): Kind | undefined =>
  found === undefined
    ? undefined
    : found.text !== undefined
    ? "text"
    : found.counted
    ? "counter"
    : found.values.length > 0
    ? "value"
    : undefined

/**
 * What one path holds. Mutable: its document serializes every call.
 *
 * @internal
 */
export interface Slot {
  readonly path: Path.Path
  /** Whether it was ever incremented. */
  counted: boolean
  /** Whether it was ever added: an entry or item whose content isn't a value. */
  added: boolean
  /** The names of the paths just under this one. */
  readonly children: Set<string>
  text: Text.Text | undefined
  /** Exact, so increments add up to the same total in any order. */
  counter: bigint
  /** Every value written, earliest write first. */
  readonly values: Array<ValueWrite>
  /** Every write of whether this path exists, earliest first. */
  readonly presences: Array<PresenceWrite>
  /** The latest of them. */
  presence: PresenceWrite | undefined
  /**
   * Per replica, the sequence number after its last edit inside this path: an
   * op under it, or on the text or counter it holds.
   */
  readonly inside: Record<string, number>
  /** Per replica, the sequence number after its last write of a value, presence or position here. */
  readonly written: Record<string, number>
  /** For a list: its items' positions. */
  list: Fugue.Tree<Item> | undefined
  /** For a list item: every position it was given, earliest write first. */
  readonly places: Array<PlaceWrite>
}

/**
 * A document's history and state. Mutable: its owner serializes every call.
 *
 * @internal
 */
export interface Document {
  /** Undefined on a server until a text op or a subscriber says which. */
  strategy: Replica.Strategy | undefined
  readonly history: History.History<Op>
  readonly slots: Map<string, Slot>
}

/** @internal */
export const make = (strategy: Replica.Strategy | undefined): Document => ({
  strategy,
  history: History.make(),
  slots: new Map()
})

/**
 * Starts over from `changes`, in the order they were applied.
 *
 * @internal
 */
export const rebuild = (strategy: Replica.Strategy | undefined, changes: Iterable<AnyChange>): Document => {
  const document = make(strategy)
  integrate(document, [...changes])
  return document
}

/** @internal */
export const slot = (document: Document, path: Path.Path): Slot | undefined => document.slots.get(Path.key(path))

const ensureSlot = (document: Document, path: Path.Path): Slot => {
  const k = Path.key(path)
  let found = document.slots.get(k)
  if (found === undefined) {
    found = {
      path,
      counted: false,
      added: false,
      children: new Set(),
      text: undefined,
      counter: BigInt(0),
      values: [],
      presences: [],
      presence: undefined,
      inside: {},
      written: {},
      list: undefined,
      places: []
    }
    document.slots.set(k, found)
    if (!Path.isRoot(path)) ensureSlot(document, Path.parent(path)).children.add(path[path.length - 1]!)
  }
  return found
}

// Whether `by` had seen `write`. A replica's own writes come in order, and a
// change's ops see the ones before them.
const seen = (by: Write, write: Write): boolean =>
  by.replica === write.replica ? write.seq < by.seq : VersionVector.get(by.past, write.replica) > write.seq

// The writes in `writes`, at `at`, that `write` hadn't seen: the ones it met.
// Usually it had seen every write there, which `at.written` says without
// looking at each.
const unseenBy = <W extends Write>(at: Slot, write: Write, writes: ReadonlyArray<W>): Array<W> => {
  const sawAll = Object.entries(at.written).every(([replica, next]) =>
    replica === write.replica || VersionVector.get(write.past, replica) >= next
  )
  return sawAll ? [] : writes.filter((other) => other !== write && !seen(write, other))
}

// A value is history: every reader shares it, so nobody may change it in place.
const freeze = (json: Schema.Json): Schema.Json => {
  if (typeof json === "object" && json !== null && !Object.isFrozen(json)) {
    Object.freeze(json)
    for (const part of Object.values(json) as Array<Schema.Json>) freeze(part)
  }
  return json
}

// Adds a write to `writes`, at `at`, in order.
const addWrite = <W extends Write>(at: Slot, writes: Array<W>, write: W): void => {
  let i = writes.length
  while (i > 0 && compareWrites(writes[i - 1]!, write) > 0) i--
  writes.splice(i, 0, write)
  at.written[write.replica] = Math.max(VersionVector.get(at.written, write.replica), write.seq + 1)
}

// Reading --------------------------------------------------------------------

/**
 * The text at `path`: empty if nothing has been written to it.
 *
 * @internal
 */
export const textAt = (document: Document, path: Path.Path): string => slot(document, path)?.text?.value ?? ""

/**
 * The values written at `path`, earliest write first.
 *
 * @internal
 */
export const valuesAt = (document: Document, path: Path.Path): ReadonlyArray<ValueWrite> =>
  slot(document, path)?.values ?? []

/**
 * Whether the record entry, optional field or list item at `path` exists.
 *
 * @internal
 */
export const exists = (document: Document, path: Path.Path): boolean => slot(document, path)?.presence?.exists ?? false

/** @internal */
export const counterAt = (document: Document, path: Path.Path): number => Number(totalAt(document, path))

/**
 * The exact sum of the counter at `path`'s increments.
 *
 * @internal
 */
export const totalAt = (document: Document, path: Path.Path): bigint => slot(document, path)?.counter ?? BigInt(0)

/**
 * The names of the paths written under `path`, in order.
 *
 * @internal
 */
export const childrenAt = (document: Document, path: Path.Path): ReadonlyArray<string> => {
  const found = slot(document, path)
  return found === undefined ? [] : [...found.children].sort(compareReplica)
}

/**
 * The items of the list at `path` that show, in order.
 *
 * @internal
 */
export const itemsAt = (document: Document, path: Path.Path): ReadonlyArray<string> => {
  const list = slot(document, path)?.list
  if (list === undefined) return []
  const keys: Array<string> = []
  for (const node of Fugue.nodes(list)) if (!node.removed) keys.push(node.insertedBy.key)
  return keys
}

/**
 * The paths of every text, with their texts.
 *
 * @internal
 */
export const texts = (document: Document): ReadonlyArray<{ readonly path: Path.Path; readonly text: string }> =>
  [...document.slots.values()].flatMap((found) =>
    found.text === undefined ? [] : [{ path: found.path, text: found.text.value }]
  )

/**
 * The document as JSON: its texts, counters and latest values where they are,
 * without what's been removed. For servers, which have no schema to read it
 * with.
 *
 * With `existing`, it also adds the path of every record entry, optional
 * field and list item that exists. JSON alone can't tell one nobody added,
 * but with something written inside, from a struct field.
 *
 * @internal
 */
export const toJson = (document: Document, existing?: Array<Path.Path>): Schema.Json =>
  jsonAt(document, Path.root, existing) ?? {}

const jsonAt = (
  document: Document,
  path: Path.Path,
  existing: Array<Path.Path> | undefined
): Schema.Json | undefined => {
  const found = slot(document, path)
  if (found === undefined || found.presence?.exists === false) return undefined
  // What exists always has JSON: a value, or what was added.
  if (found.presence?.exists === true) existing?.push(path)
  switch (kindOf(found)) {
    case "text":
      return found.text!.value
    case "counter":
      return Number(found.counter)
    case "value":
      return found.values.at(-1)!.value
  }
  // A list shows its items, as a replica's list does: anything else under it
  // is a part no schema has there.
  if (found.list !== undefined) {
    const items: Array<Schema.Json> = []
    for (const item of itemsAt(document, path)) {
      const json = jsonAt(document, [...path, item], existing)
      if (json !== undefined) items.push(json)
    }
    return items
  }
  const entries: Array<[string, Schema.Json]> = []
  for (const name of childrenAt(document, path)) {
    const json = jsonAt(document, [...path, name], existing)
    if (json !== undefined) entries.push([name, json])
  }
  // Defined rather than assigned, so a key such as `__proto__` is a key.
  return found.added || entries.length > 0 ? Object.fromEntries(entries) : undefined
}

// Overlaps -------------------------------------------------------------------

/**
 * Where a merge brought together changes that hadn't seen each other.
 *
 * @internal
 */
export type Overlap =
  | {
    readonly _tag: "Text"
    readonly path: Path.Path
    readonly start: number
    readonly end: number
    readonly anchors: AnchorRange
    readonly replicas: ReadonlyArray<string>
  }
  | {
    readonly _tag: "Value"
    readonly path: Path.Path
    readonly replicas: ReadonlyArray<string>
    readonly kept: string
  }
  | { readonly _tag: "Removed"; readonly path: Path.Path; readonly replicas: ReadonlyArray<string> }

/**
 * How a replica reads what's written at each path, so that the overlaps it's
 * told of are ones its value shows: a value it can't read meets no other
 * value, two it reads as the same don't differ, whether a part exists meets
 * nothing where the part is always there, and a position meets nothing where
 * the part isn't a list item.
 *
 * @internal
 */
export interface Reading {
  /** Whether it can read a value written at `path`. */
  readonly reads: (path: Path.Path, value: Schema.Json) => boolean
  /** Whether two values written at `path`, both of which it can read, differ as it reads them. */
  readonly differ: (path: Path.Path, a: Schema.Json, b: Schema.Json) => boolean
  /** Whether the part at `path` comes and goes: an optional field, a record entry or a list item. */
  readonly comesAndGoes: (path: Path.Path) => boolean
  /** Whether the part at `path` moves: a list item. */
  readonly moves: (path: Path.Path) => boolean
}

// What concurrent writes met over: a value, whether something exists, or a
// list item's position.
type Over = "value" | "presence" | "place"

// Edits inside a part, by `editor` with sequence numbers from `from` up to
// `next`, that met a removal of it by `remover`.
interface Lost {
  readonly remover: string
  readonly editor: string
  readonly from: number
  readonly next: number
}

// Collects value and removal overlaps by path, so each path has at most one of
// each however many writes met there.
class Found {
  private readonly found = new Map<
    string,
    {
      tag: "Value" | "Removed"
      path: Path.Path
      replicas: Set<string>
      over: Set<Over>
      lost: Array<Lost>
    }
  >()
  // Writes meet only what was here before the merge, as text edits do: two
  // that both arrive in it, as when a document loads, don't count.
  private readonly fresh = new Set<Write>()
  private readonly insideBefore = new Map<Slot, Record<string, number>>()
  readonly reading: Reading
  constructor(reading: Reading) {
    this.reading = reading
  }
  /** Notes a write the merge made. */
  wrote(write: Write): void {
    this.fresh.add(write)
  }
  /** Whether a write was here before the merge. */
  had(write: Write): boolean {
    return !this.fresh.has(write)
  }
  /** What had been edited inside a path before the merge. */
  inside(at: Slot): Record<string, number> {
    return this.insideBefore.get(at) ?? at.inside
  }
  /** Notes what's inside a path, before the merge first edits inside it. */
  editingInside(at: Slot): void {
    if (!this.insideBefore.has(at)) this.insideBefore.set(at, { ...at.inside })
  }
  private entry(tag: "Value" | "Removed", path: Path.Path) {
    const k = `${tag}${Path.key(path)}`
    let entry = this.found.get(k)
    if (entry === undefined) {
      entry = { tag, path, replicas: new Set(), over: new Set(), lost: [] }
      this.found.set(k, entry)
    }
    return entry
  }
  /** Notes writes that met at a path over a value, its existence or its place. */
  add(path: Path.Path, replicas: Iterable<string>, over: Over): void {
    const entry = this.entry("Value", path)
    for (const replica of replicas) entry.replicas.add(replica)
    entry.over.add(over)
  }
  /** Notes edits inside a part that met a removal of it. */
  removed(path: Path.Path, lost: Lost): void {
    this.entry("Removed", path).lost.push(lost)
  }
  list(document: Document): Array<Overlap> {
    return [...this.found.values()].flatMap(({ lost, over, path, replicas, tag }): Array<Overlap> => {
      if (tag === "Value") {
        const kept = keptAt(slot(document, path)!, over, this.reading)
        return [{ _tag: "Value", path, replicas: [...replicas].sort(compareReplica), kept }]
      }
      // Edits lost to the removal the part ends removed by, made
      // concurrently with it: a removal they met may have been undone since,
      // and an edit made after seeing the one that wins wasn't lost to it.
      const removal = slot(document, path)?.presence
      if (removal === undefined || removal.exists) return []
      // Worked out once for each change, which can make many of the edits.
      const edits = new Map<AnyChange, boolean>()
      const editsHere = (change: AnyChange) => {
        let found = edits.get(change)
        if (found === undefined) {
          found = change.ops.some((op) => editsInside(op, path, this.reading))
          edits.set(change, found)
        }
        return found
      }
      const concurrent = lost.filter((each) =>
        each.editor !== removal.replica && editedConcurrently(document.history, each, removal, editsHere)
      )
      if (concurrent.length === 0) return []
      const met = new Set(concurrent.flatMap(({ editor, remover }) => [editor, remover]))
      return [{ _tag: "Removed", path, replicas: [...met].sort(compareReplica) }]
    })
  }
}

// Whether any of the edits `lost` names was made concurrently with `removal`:
// neither had seen the other. `edits` says whether a change edits inside the
// part.
const editedConcurrently = (
  history: History.History<Op>,
  lost: Lost,
  removal: Write,
  edits: (change: AnyChange) => boolean
): boolean => {
  for (let seq = Math.max(lost.from, VersionVector.get(removal.past, lost.editor)); seq < lost.next;) {
    const change = History.find(history, { replica: lost.editor, seq })!
    // Each change says what it was made on top of, which needn't include
    // what its replica's earlier ones had seen.
    const sawRemoval = VersionVector.get(history.pasts.get(key(change))!, removal.replica) > removal.seq
    if (!sawRemoval && edits(change)) return true
    seq = change.seq + change.length
  }
  return false
}

// Whether an op edits inside the part at `path`, as `noteInside` and moves
// count it: under it, or on the text or counter it is, or moving it.
const editsInside = (op: Op, path: Path.Path, reading: Reading): boolean => {
  const at = pathOf(op)
  return at.length > path.length
    ? Path.equals(at.slice(0, path.length), path)
    : Path.equals(at, path) &&
      (Text.isOp(op) || op._tag === "Increment" || (op._tag === "Place" && reading.moves(path)))
}

// Whose write shows at a path where writes met: a removal if it won, else the
// latest of the kind of write they met over (of values, the latest the
// replica reads).
const keptAt = (found: Slot, over: ReadonlySet<Over>, reading: Reading): string => {
  const presence = reading.comesAndGoes(found.path) ? found.presence : undefined
  if (presence !== undefined && (!presence.exists || (over.size === 1 && over.has("presence")))) {
    return presence.replica
  }
  if (!over.has("value")) return found.places.at(-1)!.replica
  // Values it reads met, so there's one.
  let i = found.values.length - 1
  while (!reading.reads(found.path, found.values[i]!.value)) i--
  return found.values[i]!.replica
}

// Merging --------------------------------------------------------------------

/** @internal */
export interface Integrated {
  /** How many of the changes were applied. */
  readonly applied: number
  /** How each text changed, by path key. */
  readonly texts: ReadonlyMap<string, TextPatch.TextPatch>
  readonly overlaps: ReadonlyArray<Overlap>
  /** What was wrong with each bad change, by its key. */
  readonly problems: ReadonlyArray<{ readonly key: string; readonly message: string }>
}

/**
 * Applies changes whose parents are all applied (or come earlier in
 * `changes`), in order, checking each one first. A bad one isn't applied, nor
 * is anything made on top of it. Says what's wrong with the first bad one.
 *
 * @internal
 */
export const integrate = (
  document: Document,
  changes: ReadonlyArray<AnyChange>
): { readonly texts: Integrated["texts"]; readonly problem: string | undefined } => {
  const { bad, texts } = applyChanges(document, changes, undefined)
  return { texts, problem: bad[0]?.message }
}

/**
 * Applies every pending change that's ready, as `integrate` does, in one
 * merge: a bad change is dropped, the changes ready behind it are applied,
 * and the ones made on top of it keep waiting. So what a document shows
 * doesn't depend on how its changes arrived, and replicas and relays drop the
 * same changes. With `overlaps`, how the replica reads what's written, it
 * collects the overlaps it's told of.
 *
 * @internal
 */
export const integrateReady = (
  document: Document,
  options: { readonly overlaps: Reading | undefined }
): Integrated => {
  const { history } = document
  // All in one merge, so changes that arrive together never meet, whatever
  // bad change is among them.
  const found = options.overlaps === undefined ? undefined : new Found(options.overlaps)
  const { applied, bad, textOverlaps, texts, waiting } = applyChanges(document, History.takeReady(history), found)
  for (const { change } of bad) history.dropped.set(key(change), change)
  for (const change of waiting) history.pending.set(key(change), change)
  return {
    applied,
    texts,
    overlaps: [...(found?.list(document) ?? []), ...textOverlaps],
    problems: bad.map(({ change, message }) => ({ key: key(change), message }))
  }
}

type TextOverlap = Extract<Overlap, { readonly _tag: "Text" }>

// Applies changes as `integrate` says, returning the bad ones and the ones
// that wait behind them. With `found`, it collects value and removal overlaps
// there, and returns the text overlaps.
const applyChanges = (
  document: Document,
  changes: ReadonlyArray<AnyChange>,
  found: Found | undefined
): {
  readonly applied: number
  readonly texts: Map<string, TextPatch.TextPatch>
  readonly textOverlaps: ReadonlyArray<TextOverlap>
  readonly bad: ReadonlyArray<{ readonly change: AnyChange; readonly message: string }>
  readonly waiting: ReadonlyArray<AnyChange>
} => {
  const contexts = contextsOf(document, changes)
  // Every text's parts of the batch, in order, for its merge.
  const incoming = new Map<string, Array<TextPart>>()
  for (const { parts } of contexts) {
    for (const part of parts) {
      const k = Path.key(part.path)
      const ofText = incoming.get(k)
      if (ofText === undefined) incoming.set(k, [part])
      else ofText.push(part)
    }
  }
  // A text nobody has written yet joins the document when a part of it is
  // applied, so a change that's refused leaves nothing behind.
  const merges = new Map<string, { readonly path: Path.Path; readonly text: Text.Text; readonly merge: Text.Merge }>()
  const mergeOf = (path: Path.Path, strategy: Replica.Strategy) => {
    const k = Path.key(path)
    let merge = merges.get(k)
    if (merge === undefined) {
      const text = slot(document, path)?.text ?? Text.make(strategy)
      merge = { path, text, merge: text.merge(incoming.get(k) ?? []) }
      merges.set(k, merge)
    }
    return merge
  }

  const texts = new Map<string, TextPatch.TextPatch>()
  const bad: Array<{ readonly change: AnyChange; readonly message: string }> = []
  const waiting: Array<AnyChange> = []
  // Once a change isn't applied, neither are its replica's later changes, nor
  // changes made on top of any of them: by replica, the first sequence number
  // that won't be applied.
  const missing = new Map<string, number>()
  const has = (id: Id) => (missing.get(id.replica) ?? Infinity) > id.seq
  const skip = (change: AnyChange) =>
    missing.set(change.replica, Math.min(missing.get(change.replica) ?? Infinity, change.seq))
  let applied = 0
  for (const [i, change] of changes.entries()) {
    if (!has(change) || !change.parents.every(has)) {
      skip(change)
      waiting.push(change)
      continue
    }
    const context = contexts[i]!
    const problem = check(document, change, context, (path, strategy) => mergeOf(path, strategy).merge)
    if (problem !== undefined) {
      bad.push({ change, message: problem })
      skip(change)
      continue
    }
    let seq = change.seq
    for (const op of change.ops) {
      const write: Write = { replica: change.replica, seq, time: change.time, past: context.past }
      if (Text.isOp(op)) {
        const part = context.parts.find((part) => part.seq === seq)
        if (part !== undefined) {
          const strategy = Text.strategyOfOp(op)
          document.strategy ??= strategy
          const { merge, text } = mergeOf(part.path, strategy)
          const patch = merge.apply(part)
          ensureSlot(document, part.path).text = text
          composeText(texts, part.path, patch)
        }
      } else {
        applyOp(document, op, write, found)
      }
      noteInside(document, op, write, found)
      seq += opLength(op)
    }
    History.record(document.history, change, context.past)
    applied++
  }

  const textOverlaps: Array<TextOverlap> = []
  if (found !== undefined) {
    for (const { merge, path } of merges.values()) {
      for (const overlap of merge.overlaps()) textOverlaps.push({ _tag: "Text", path, ...overlap })
    }
  }
  return { applied, texts, textOverlaps, bad, waiting }
}

const composeText = (texts: Map<string, TextPatch.TextPatch>, path: Path.Path, patch: TextPatch.TextPatch): void => {
  const k = Path.key(path)
  texts.set(k, TextPatch.compose(texts.get(k) ?? TextPatch.empty, patch))
}

type TextPart = Text.Part & { readonly path: Path.Path }

interface Context {
  /** What the change's author had seen. */
  readonly past: VersionVector.VersionVector
  /** The latest write time among its parents. */
  readonly parentTime: number
  /** Whether every parent is the last id of a change, as replicas make them. */
  readonly parentsAreChanges: boolean
  readonly parts: ReadonlyArray<TextPart>
}

// What each change was made on top of, for changes whose parents are applied
// or come earlier in the batch.
const contextsOf = (document: Document, changes: ReadonlyArray<AnyChange>): Array<Context> => {
  const { history } = document
  type Entry = { readonly change: AnyChange; readonly past: VersionVector.VersionVector }
  const batch = new Map<string, Array<Entry>>()
  // By last id, which is what parents name.
  const ends = new Map<string, Entry>()
  // The texts earlier changes in the batch have parts of.
  const touched = new Set<string>()
  const owner = (id: Id) => {
    const applied = History.find(history, id)
    if (applied !== undefined) return { change: applied, past: history.pasts.get(key(applied))! }
    return ends.get(idKey(id)) ??
      batch.get(id.replica)?.find(({ change }) => change.seq <= id.seq && id.seq < change.seq + change.length)
  }
  return changes.map((change) => {
    const past: Record<string, number> = {}
    let parentTime = -1
    let parentsAreChanges = true
    for (const parent of change.parents) {
      const found = owner(parent)!
      for (const [replica, seen] of Object.entries(found.past)) {
        past[replica] = Math.max(VersionVector.get(past, replica), seen)
      }
      past[parent.replica] = Math.max(VersionVector.get(past, parent.replica), parent.seq + 1)
      parentTime = Math.max(parentTime, found.change.time)
      parentsAreChanges &&= found.change.seq + found.change.length - 1 === parent.seq
    }
    const entry = { change, past }
    const ofReplica = batch.get(change.replica) ?? []
    ofReplica.push(entry)
    batch.set(change.replica, ofReplica)
    ends.set(idKey(lastId(change)), entry)
    // A text nothing applied or earlier in the batch has touched has no
    // parts to be made on top of.
    const parts = textParts(
      change,
      past,
      (path) =>
        touched.has(Path.key(path)) || slot(document, path)?.text !== undefined
          ? textParents(change, path, (id) => owner(id)?.change)
          : []
    )
    for (const part of parts) touched.add(Path.key(part.path))
    return { past, parentTime, parentsAreChanges, parts }
  })
}

// A change's text ops, grouped into one part per run of ops on the same text.
// Only a replay text merges by its parts' parents, which can take a walk
// through the history to find.
const textParts = (
  change: AnyChange,
  past: VersionVector.VersionVector,
  parentsOf: (path: Path.Path) => ReadonlyArray<Id>
): Array<TextPart> => {
  const parts: Array<TextPart> = []
  let current: { path: Path.Path; seq: number; length: number; ops: Array<Text.Op> } | undefined
  const close = () => {
    if (current !== undefined) {
      const parents = current.ops.some(ReplayText.isOp) ? parentsOf(current.path) : []
      parts.push({ replica: change.replica, past, parents, ...current })
    }
    current = undefined
  }
  let seq = change.seq
  for (const op of change.ops) {
    if (!Text.isOp(op)) close()
    else {
      const path = pathOf(op)
      if (current !== undefined && !Path.equals(current.path, path)) close()
      current ??= { path, seq, length: 0, ops: [] }
      current.ops.push(op)
      current.length += Text.opLength(op)
    }
    seq += opLength(op)
  }
  close()
  return parts
}

// The latest parts of the text at `path` a change was made on top of: walks
// back from its parents, stopping at changes that edited the text.
const textParents = (
  change: AnyChange,
  path: Path.Path,
  owner: (id: Id) => AnyChange | undefined
): Array<Id> => {
  const k = Path.key(path)
  const parents: Array<Id> = []
  const visited = new Set<AnyChange>()
  const stack = [...change.parents]
  while (stack.length > 0) {
    const found = owner(stack.pop()!)
    if (found === undefined || visited.has(found)) continue
    visited.add(found)
    const end = textEnds(found).get(k)
    if (end !== undefined) parents.push(end)
    else stack.push(...found.parents)
  }
  return parents.sort((a, b) => compareReplica(a.replica, b.replica) || a.seq - b.seq)
}

// The last id of each text's ops in a change, by path key.
const textEnds = (change: AnyChange): ReadonlyMap<string, Id> => {
  let ends = endsCache.get(change)
  if (ends === undefined) {
    const found = new Map<string, Id>()
    let seq = change.seq
    for (const op of change.ops) {
      seq += opLength(op)
      if (Text.isOp(op)) found.set(Path.key(pathOf(op)), { replica: change.replica, seq: seq - 1 })
    }
    ends = found
    endsCache.set(change, ends)
  }
  return ends
}

const endsCache = new WeakMap<AnyChange, ReadonlyMap<string, Id>>()

/**
 * Takes in changes as they arrive, the same way for every receiver, before
 * any is applied. A change under the key of another the document has, even
 * one dropped as bad, reuses its replica id, and nothing is taken: `reused`
 * says whose. Otherwise a change that's bad on its own is refused, and
 * `refused` says why; the rest that are new wait as pending, for
 * `integrateReady`, and are `fresh`.
 *
 * @internal
 */
export const arrive = (
  document: Document,
  changes: ReadonlyArray<AnyChange>
): {
  readonly reused: string | undefined
  readonly refused: string | undefined
  readonly fresh: ReadonlyArray<AnyChange>
} => {
  const { history } = document
  // A store keeps one change per key, so the other is another change too.
  const reused = changes.find((change) => {
    const existing = History.get(history, key(change))
    return existing !== undefined && !Equal.equals(existing, change)
  })
  if (reused !== undefined) return { reused: reused.replica, refused: undefined, fresh: [] }
  let refused: string | undefined
  const fresh: Array<AnyChange> = []
  for (const change of changes) {
    const problem = checkOnArrival(change)
    if (problem !== undefined) refused ??= problem
    else if (!History.has(history, change)) fresh.push(change)
  }
  for (const change of fresh) history.pending.set(key(change), change)
  return { reused: undefined, refused, fresh }
}

// What's wrong with a change that the change alone shows: it takes no sequence
// numbers, which version vectors can't say whether anyone has, or more than
// they can count, or it starts, or names a parent, before any change. It's
// refused before it can wait or be saved: nothing checks a waiting change's
// ops against its length, and servers count changes that wait as held.
const checkOnArrival = (change: AnyChange): string | undefined => {
  const name = `Change ${key(change)}`
  if (change.length < 1) return `${name} takes no sequence numbers`
  if (!Number.isSafeInteger(change.seq + change.length)) {
    return `${name} takes more sequence numbers than can be counted`
  }
  if (change.seq < 0) return `${name} starts before any change`
  if (change.parents.some((parent) => parent.seq < 0)) return `${name} has a parent before any change`
  return undefined
}

// The last millisecond a `Date` holds. No clock reads later, so a change
// stamped later is forged, unless it's a millisecond after what it was made on
// top of, as replicas that saw one stamp theirs. Otherwise changes stamped
// ever later could run out of whole numbers a change can carry.
const latestTime = 8.64e15

// Checks a change before applying any of it, so a bad change can't leave the
// document half-updated.
const check = (
  document: Document,
  change: AnyChange,
  { parentTime, parentsAreChanges, parts, past }: Context,
  mergeOf: (path: Path.Path, strategy: Replica.Strategy) => Text.Merge
): string | undefined => {
  const name = `Change ${key(change)}`
  if (change.ops.length === 0) return `${name} has no ops`
  // Each op's sequence numbers follow the last op's, so one that takes none,
  // or fewer, would give ops ids that others have.
  if (
    change.ops.some((op) => opLength(op) < 1 || (op._tag === "Remove" && op.targets.some(({ length }) => length < 1)))
  ) {
    return `${name} has an op that takes no sequence numbers`
  }
  const total = change.ops.reduce((sum, op) => sum + opLength(op), 0)
  if (total !== change.length) return `${name} says it has ${change.length} sequence numbers but has ${total}`
  if (!parentsAreChanges) return `${name} has a parent in the middle of a change`
  if (change.time <= parentTime) return `${name} has a write time no later than a change it was made on top of`
  if (change.time > Math.max(latestTime, parentTime + 1)) return `${name} is stamped later than any clock reads`
  const strategy = document.strategy ?? strategyOf([change])
  if (!fitsStrategy(change, strategy)) return `${name} mixes strategies`

  const problem = checkPlaces(document, change, past)
  if (problem !== undefined) return problem
  const textPaths = new Set<string>()
  for (const part of parts) {
    const k = Path.key(part.path)
    if (textPaths.has(k)) return `${name} edits one text in more than one run of ops`
    textPaths.add(k)
  }
  for (const part of parts) {
    const found = mergeOf(part.path, strategy!).check(part)
    if (found !== undefined) return `${name} ${found}`
  }
  return undefined
}

/**
 * Whether a change's ops agree with what's at their paths: a text, a counter
 * and a value have nothing under them, a value is never added (writing it
 * says it exists), nothing is removed that nobody added, the root is a text
 * or has parts, and a list holds items that are values or have parts.
 * Without a schema, this is what an authority can check so its JSON of a
 * document shows what replicas do; replicas and relays keep such changes,
 * and readers ignore what their schema doesn't have.
 *
 * @internal
 */
export const checkPaths = (document: Document, change: AnyChange): string | undefined => {
  const name = `Change ${key(change)}`
  const kinds = new Map<string, Kind>()
  const added = new Set<string>()
  const existed = new Set<string>()
  const parents = new Set<string>()
  const placed = new Set<string>()
  const kindAt = (path: Path.Path) => kinds.get(Path.key(path)) ?? kindOf(slot(document, path))
  const addedAt = (path: Path.Path) => added.has(Path.key(path)) || (slot(document, path)?.added ?? false)
  const existedAt = (path: Path.Path) =>
    existed.has(Path.key(path)) || (slot(document, path)?.presences.length ?? 0) > 0
  const hasChildren = (path: Path.Path) => parents.has(Path.key(path)) || (slot(document, path)?.children.size ?? 0) > 0
  const placedAt = (path: Path.Path) => placed.has(Path.key(path)) || (slot(document, path)?.places.length ?? 0) > 0
  for (const op of change.ops) {
    const path = pathOf(op)
    const k = Path.key(path)
    if (Path.isRoot(path) && !Text.isOp(op)) return `${name} writes to the root of a document`
    for (const prefix of Path.prefixes(path)) {
      const above = kindAt(prefix)
      if (above !== undefined) return `${name} writes inside a ${above}`
    }
    const here = kindAt(path)
    if (Text.isOp(op) || op._tag === "Increment" || op._tag === "Set") {
      const kind: Kind = Text.isOp(op) ? "text" : op._tag === "Increment" ? "counter" : "value"
      if ((here !== undefined && here !== kind) || hasChildren(path) || (kind === "value" && addedAt(path))) {
        return `${name} writes a ${kind} where there's ${here === undefined ? "something else" : `a ${here}`}`
      }
      if (kind !== "value" && placedAt(path)) return `${name} writes a ${kind} where there's a list item`
      kinds.set(k, kind)
      if (kind === "value") existed.add(k)
    } else if (op._tag === "Add") {
      if (here === "value") return `${name} adds a value`
      added.add(k)
      existed.add(k)
    } else if (op._tag === "Unset") {
      // Replicas only remove what they show. Removing what was never there
      // would leave a list item, or what looks like an entry, where no list
      // could start.
      if (!existedAt(path)) return `${name} removes something nobody added`
    } else if (op._tag === "Place") {
      if (here === "text" || here === "counter") return `${name} places a ${here} in a list`
      // A path is a list from its first position on, so a struct or record
      // with parts never becomes one. Text typed into an item nobody has
      // added yet is under the list too, but nothing is written at the item.
      const listPath = Path.parent(path)
      if (Path.isRoot(listPath)) return `${name} places a list item in the root`
      const list = slot(document, listPath)
      if (
        list !== undefined && list.list === undefined &&
        [...list.children].some((child) => isWritten(slot(document, [...listPath, child])!))
      ) {
        return `${name} places a list item among parts that aren't items`
      }
      placed.add(k)
    }
    for (const prefix of Path.prefixes(path)) parents.add(Path.key(prefix))
  }
  return undefined
}

// Whether anything was written at a path itself, rather than only under it.
const isWritten = (found: Slot): boolean => found.presences.length > 0 || kindOf(found) !== undefined

// A position goes next to positions already in its list, or placed in the same
// list earlier in the change.
const checkPlaces = (
  document: Document,
  change: AnyChange,
  past: VersionVector.VersionVector
): string | undefined => {
  // The positions placed so far, with their lists.
  const placed = new Set<string>()
  let seq = change.seq
  for (const op of change.ops) {
    if (op._tag === "Place") {
      const listPath = Path.parent(op.path)
      const list = slot(document, listPath)?.list
      const inList = (id: Id) => `${Path.key(listPath)}${idKey(id)}`
      const known = (id: Id | null | undefined) =>
        id === null || id === undefined || placed.has(inList(id)) ||
        // Only what its author had seen, so it doesn't matter what else is here.
        (VersionVector.get(past, id.replica) > id.seq && list !== undefined && Fugue.find(list, id) !== undefined)
      if (op.side === "R" ? op.rightOrigin === undefined : op.rightOrigin !== undefined) {
        return `Change ${key(change)} has a position with a mismatched side and right origin`
      }
      // As for a text's characters.
      if (op.parent === null && op.side === "L") {
        return `Change ${key(change)} places an item before the start of its list`
      }
      if (!known(op.parent) || !known(op.rightOrigin)) {
        return `Change ${key(change)} places an item next to a position it hasn't seen`
      }
      placed.add(inList({ replica: change.replica, seq }))
    }
    seq += opLength(op)
  }
  return undefined
}

// Applies an op that isn't a text op. `found` collects overlaps, if wanted.
const applyOp = (document: Document, op: Exclude<Op, Text.Op>, write: Write, found: Found | undefined): void => {
  const at = ensureSlot(document, op.path)
  switch (op._tag) {
    case "Set": {
      const value: ValueWrite = { ...write, value: freeze(op.value) }
      addWrite(at, at.values, value)
      if (found !== undefined) {
        found.wrote(value)
        // Only values the replica reads meet, as it reads them.
        const { reading } = found
        const differing = reading.reads(op.path, value.value)
          ? unseenBy(at, value, at.values).filter((other) =>
            found.had(other) && reading.reads(op.path, other.value) &&
            reading.differ(op.path, other.value, value.value)
          )
          : []
        if (differing.length > 0) {
          found.add(op.path, [value.replica, ...differing.map((other) => other.replica)], "value")
        }
      }
      setPresence(document, at, { ...write, exists: true }, found)
      break
    }
    case "Add":
      at.added = true
      setPresence(document, at, { ...write, exists: true }, found)
      break
    case "Unset":
      setPresence(document, at, { ...write, exists: false }, found)
      break
    case "Increment":
      at.counted = true
      at.counter += BigInt(op.by)
      break
    case "Place": {
      const placement: Fugue.Placement = op.side === "R"
        ? { parent: op.parent ?? undefined, side: "R", rightOrigin: op.rightOrigin ?? null }
        : { parent: op.parent ?? undefined, side: "L" }
      const id = { replica: write.replica, seq: write.seq }
      const node = Fugue.insert(listOf(document, op.path), placement, id, "\u0000", {
        key: op.path[op.path.length - 1]!
      })
      const place: PlaceWrite = { ...write, node }
      addWrite(at, at.places, place)
      // Where the part isn't a list item, its positions are nothing the
      // replica shows.
      if (found !== undefined) {
        found.wrote(place)
        if (found.reading.moves(op.path)) {
          const moved = unseenBy(at, place, at.places).filter((other) => found.had(other))
          if (moved.length > 0) {
            found.add(op.path, [place.replica, ...moved.map((other) => other.replica)], "place")
          }
          reportRemovals(found, at, place)
        }
      }
      refreshPosition(at)
      break
    }
  }
}

const setPresence = (document: Document, at: Slot, write: PresenceWrite, found: Found | undefined): void => {
  addWrite(at, at.presences, write)
  at.presence = at.presences.at(-1)
  if (found !== undefined) {
    found.wrote(write)
    // Where the part is always there, whether it exists is nothing the
    // replica shows.
    const differing = found.reading.comesAndGoes(at.path)
      ? unseenBy(at, write, at.presences).filter((other) => found.had(other) && other.exists !== write.exists)
      : []
    if (differing.length > 0) {
      found.add(at.path, [write.replica, ...differing.map((other) => other.replica)], "presence")
    }
    // A removal over edits inside the entry, or moves of the item, it hadn't
    // seen.
    if (!write.exists) {
      for (const [editor, next] of Object.entries(found.inside(at))) {
        const from = VersionVector.get(write.past, editor)
        if (editor !== write.replica && next > from) {
          found.removed(at.path, { remover: write.replica, editor, from, next })
        }
      }
      for (const place of found.reading.moves(at.path) ? unseenBy(at, write, at.places) : []) {
        if (found.had(place)) {
          found.removed(at.path, {
            remover: write.replica,
            editor: place.replica,
            from: place.seq,
            next: place.seq + 1
          })
        }
      }
    }
  }
  if (!Path.isRoot(at.path) && slot(document, Path.parent(at.path))?.list !== undefined) refreshPosition(at)
}

// Records an op as an edit inside every path above it, and for a text or
// counter op, inside its own: what an entry, item or optional field that's a
// text or a counter holds is its content. Reports it against the removals it
// hadn't seen that may win over it.
const noteInside = (document: Document, op: Op, write: Write, found: Found | undefined): void => {
  const path = pathOf(op)
  const paths = Text.isOp(op) || op._tag === "Increment" ? [...Path.prefixes(path), path] : Path.prefixes(path)
  for (const inside of paths) {
    const at = ensureSlot(document, inside)
    found?.editingInside(at)
    at.inside[write.replica] = Math.max(VersionVector.get(at.inside, write.replica), write.seq + opLength(op))
    if (found !== undefined) reportRemovals(found, at, write)
  }
}

// Reports a write at or under `at` against every removal there it hadn't
// seen, whichever is latest now: the merge lists it if `at` ends removed by a
// removal made concurrently with it.
const reportRemovals = (found: Found, at: Slot, write: Write): void => {
  for (const removal of unseenBy(at, write, at.presences)) {
    if (!removal.exists && found.had(removal)) {
      found.removed(at.path, { remover: removal.replica, editor: write.replica, from: write.seq, next: write.seq + 1 })
    }
  }
}

// The positions in the list that the item at `path` is in.
const listOf = (document: Document, path: Path.Path): Fugue.Tree<Item> =>
  ensureSlot(document, Path.parent(path)).list ??= Fugue.make<Item>({ key: "" })

// A list item shows at its latest position, while it exists.
const refreshPosition = (item: Slot): void => {
  const latest = item.places.at(-1)
  const shows = item.presence?.exists ?? false
  for (const place of item.places) Fugue.setRemoved(place.node, !(shows && place === latest))
}

// Local edits ----------------------------------------------------------------

/**
 * What a local edit does, before it has sequence numbers.
 *
 * - `Text`: edits the text at `path`.
 * - `Set`, `Add`, `Unset`, `Increment`: as the ops.
 * - `Place`: moves the list item at `path` to just after the item `after` (or
 *   to the start), which must show.
 * - `Insert`: adds a value to the list at `path`, just after `after`: an item's
 *   key, or the index of an earlier `Insert` in the same edit.
 *
 * @internal
 */
export type Intent =
  | { readonly _tag: "Text"; readonly path: Path.Path; readonly patch: TextPatch.TextPatch }
  | { readonly _tag: "Set"; readonly path: Path.Path; readonly value: Schema.Json }
  | { readonly _tag: "Add"; readonly path: Path.Path }
  | { readonly _tag: "Unset"; readonly path: Path.Path }
  | { readonly _tag: "Increment"; readonly path: Path.Path; readonly by: number }
  | { readonly _tag: "Place"; readonly path: Path.Path; readonly after: string | undefined }
  | {
    readonly _tag: "Insert"
    readonly path: Path.Path
    readonly value: Schema.Json
    readonly after: string | number | undefined
  }

/**
 * Makes a local edit as one change, with sequence numbers from `seq`. Every
 * intent must fit the document as the ones before it leave it. Undefined if it
 * changes nothing.
 *
 * @internal
 */
export const edit = (
  document: Document,
  options: { readonly replica: string; readonly seq: number; readonly time: number },
  intents: ReadonlyArray<Intent>
): { readonly change: AnyChange; readonly texts: Integrated["texts"] } | undefined => {
  const { history } = document
  const past = { ...history.version }
  const ops: Array<Op> = []
  const texts = new Map<string, TextPatch.TextPatch>()
  const inserted = new Map<number, string>()
  let seq = options.seq
  const write = (op: Exclude<Op, Text.Op>) => {
    const done: Write = { replica: options.replica, seq, time: options.time, past }
    applyOp(document, op, done, undefined)
    noteInside(document, op, done, undefined)
    ops.push(op)
    seq++
  }
  const place = (path: Path.Path, after: string | undefined) => {
    const list = listOf(document, path)
    // Its current position goes as soon as this one is made.
    for (const { node } of ensureSlot(document, path).places) Fugue.setRemoved(node, true)
    const index = after === undefined ? 0 : Fugue.indexOf(shownPosition(document, [...Path.parent(path), after])) + 1
    const placement = Fugue.place(list, index)
    write(
      placement.side === "R"
        ? { _tag: "Place", path, parent: placement.parent ?? null, side: "R", rightOrigin: placement.rightOrigin }
        : { _tag: "Place", path, parent: placement.parent ?? null, side: "L" }
    )
  }
  for (const [index, intent] of intents.entries()) {
    switch (intent._tag) {
      case "Text": {
        // As in a merge, a text joins the document only once it's written.
        const text = slot(document, intent.path)?.text ?? Text.make(document.strategy!)
        const edited = text.edit(intent.patch, { replica: options.replica, seq }, past)
        if (edited === undefined) break
        ensureSlot(document, intent.path).text = text
        const pathed = Path.isRoot(intent.path) ? {} : { path: intent.path }
        for (const op of edited.ops) {
          const pathedOp = { ...op, ...pathed }
          ops.push(pathedOp)
          noteInside(document, pathedOp, { replica: options.replica, seq, time: options.time, past }, undefined)
          seq += Text.opLength(op)
        }
        composeText(texts, intent.path, edited.patch)
        break
      }
      case "Place":
        place(intent.path, intent.after)
        break
      case "Insert": {
        // A value in a list is known by the id of the write that added it.
        const item = idKey({ replica: options.replica, seq })
        inserted.set(index, item)
        write({ _tag: "Set", path: [...intent.path, item], value: intent.value })
        place([...intent.path, item], typeof intent.after === "number" ? inserted.get(intent.after) : intent.after)
        break
      }
      default:
        write(intent)
    }
  }
  if (ops.length === 0) return undefined
  const change: AnyChange = {
    replica: options.replica,
    seq: options.seq,
    length: seq - options.seq,
    parents: history.frontier,
    time: options.time,
    ops
  }
  History.record(history, change, past)
  return { change, texts }
}

const shownPosition = (document: Document, path: Path.Path): Fugue.Node<Item> => {
  const node = ensureSlot(document, path).places.at(-1)?.node
  if (node === undefined || node.removed) throw new Error(`List item ${Path.key(path)} isn't showing`)
  return node
}

// Loading --------------------------------------------------------------------

/**
 * Starts a replay document from saved history without replaying its texts:
 * `changes` are exactly the ones in `version`, and `texts` are the texts they
 * make. Undefined if they don't fit together, or the strategy can't start
 * from texts.
 *
 * @internal
 */
export const load = (
  strategy: Replica.Strategy,
  changes: ReadonlyArray<AnyChange>,
  version: VersionVector.VersionVector,
  saved: ReadonlyArray<{ readonly path: Path.Path; readonly text: string }>
): Document | undefined => {
  if (strategy !== "replay") return undefined
  const document = make(strategy)
  const { history } = document
  for (const change of changes) history.pending.set(key(change), change)
  const parts = new Map<string, { path: Path.Path; parts: Array<Text.Part> }>()
  for (const change of History.takeReady(history)) {
    const past = History.pastOf(history, change.parents)
    let seq = change.seq
    for (const op of change.ops) {
      const write: Write = { replica: change.replica, seq, time: change.time, past }
      if (!Text.isOp(op)) applyOp(document, op, write, undefined)
      noteInside(document, op, write, undefined)
      seq += opLength(op)
    }
    const owner = (id: Id) => History.find(history, id)
    const parentsOf = (path: Path.Path) => parts.has(Path.key(path)) ? textParents(change, path, owner) : []
    for (const part of textParts(change, past, parentsOf)) {
      const k = Path.key(part.path)
      const entry = parts.get(k) ?? { path: part.path, parts: [] }
      entry.parts.push(part)
      parts.set(k, entry)
    }
    History.record(history, change, past)
  }
  // Changes that don't make up the version can't go with its texts. Any left
  // waiting once they do are behind their replica's sequence numbers, as
  // forged ones can be, so they could never be applied: they wait here too.
  if (VersionVector.compare(history.version, version) !== "equal") return undefined
  const byPath = new Map(saved.map(({ path, text }) => [Path.key(path), text]))
  for (const [k, entry] of parts) {
    const text = byPath.get(k)
    const at = ensureSlot(document, entry.path)
    at.text = Text.make(strategy)
    if (text === undefined || !at.text.load(entry.parts, text)) return undefined
  }
  return document
}
