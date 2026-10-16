import * as Equal from "effect/Equal"
import * as Schema from "effect/Schema"
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
  key
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

/** @internal */
export const Op: Schema.Codec<Op> = Schema.Union([
  TreeText.Op,
  ReplayText.Op,
  Schema.TaggedStruct("Set", { path: Path.Path, value: Schema.Json }),
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

/**
 * What one path holds. Mutable: its document serializes every call.
 *
 * @internal
 */
export interface Slot {
  readonly path: Path.Path
  /** Set by the first op that says what the path is. */
  kind: Kind | undefined
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
  /** Per replica, the sequence number after its last op under this path. */
  readonly below: Record<string, number>
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
  integrate(document, [...changes], { overlaps: false })
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
      kind: undefined,
      added: false,
      children: new Set(),
      text: undefined,
      counter: BigInt(0),
      values: [],
      presences: [],
      presence: undefined,
      below: {},
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
  by.replica === write.replica ? write.seq < by.seq : (by.past[write.replica] ?? 0) > write.seq

// The writes in `writes` that `write` hadn't seen: the ones it met.
const unseenBy = <W extends Write>(write: Write, writes: ReadonlyArray<W>): Array<W> =>
  writes.filter((other) => other !== write && !seen(write, other))

const insertSorted = <W extends Write>(writes: Array<W>, write: W): void => {
  let i = writes.length
  while (i > 0 && compareWrites(writes[i - 1]!, write) > 0) i--
  writes.splice(i, 0, write)
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
export const counterAt = (document: Document, path: Path.Path): number => Number(slot(document, path)?.counter ?? 0)

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
 * @internal
 */
export const toJson = (document: Document): Schema.Json => jsonAt(document, Path.root) ?? {}

const jsonAt = (document: Document, path: Path.Path): Schema.Json | undefined => {
  const found = slot(document, path)
  if (found === undefined || found.presence?.exists === false) return undefined
  switch (found.kind) {
    case "text":
      return found.text?.value ?? ""
    case "counter":
      return Number(found.counter)
    case "value":
      return found.values.length === 0 ? undefined : found.values[found.values.length - 1]!.value
  }
  if (found.list !== undefined) {
    const items: Array<Schema.Json> = []
    for (const item of itemsAt(document, path)) {
      const json = jsonAt(document, [...path, item])
      if (json !== undefined) items.push(json)
    }
    return items
  }
  const entries: Array<[string, Schema.Json]> = []
  for (const name of childrenAt(document, path)) {
    const json = jsonAt(document, [...path, name])
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

// What concurrent writes met over: a value, whether something exists, or a
// list item's position.
type Over = "value" | "presence" | "place"

// Collects value and removal overlaps by path, so each path has at most one of
// each however many writes met there.
class Found {
  private readonly found = new Map<
    string,
    { tag: "Value" | "Removed"; path: Path.Path; replicas: Set<string>; over: Set<Over> }
  >()
  add(tag: "Value" | "Removed", path: Path.Path, replicas: Iterable<string>, over: Over): void {
    const k = `${tag}${Path.key(path)}`
    let entry = this.found.get(k)
    if (entry === undefined) {
      entry = { tag, path, replicas: new Set(), over: new Set() }
      this.found.set(k, entry)
    }
    for (const replica of replicas) entry.replicas.add(replica)
    entry.over.add(over)
  }
  list(document: Document): Array<Overlap> {
    // A removal can win partway through a batch and lose to a later write.
    return [...this.found.values()].filter(({ path, tag }) => tag === "Value" || !exists(document, path)).map(
      ({ over, path, replicas, tag }) => {
        const sorted = [...replicas].sort(compareReplica)
        return tag === "Removed"
          ? { _tag: "Removed", path, replicas: sorted }
          : { _tag: "Value", path, replicas: sorted, kept: kept(slot(document, path)!, over) }
      }
    )
  }
}

// Whose write shows at a path where writes met: a removal if it won, else the
// latest of the kind of write they met over.
const kept = (found: Slot, over: ReadonlySet<Over>): string => {
  const { presence } = found
  if (presence !== undefined && (!presence.exists || (over.size === 1 && over.has("presence")))) {
    return presence.replica
  }
  return (over.has("value") ? found.values.at(-1) : found.places.at(-1))!.replica
}

// Merging --------------------------------------------------------------------

/** @internal */
export interface Integrated {
  /** How many of the changes were applied. */
  readonly applied: number
  /** How each text changed, by path key. */
  readonly texts: ReadonlyMap<string, TextPatch.TextPatch>
  readonly overlaps: ReadonlyArray<Overlap>
  /** What's wrong with the first change that wasn't applied, if any. */
  readonly problem: string | undefined
}

/**
 * Applies changes whose parents are all applied (or come earlier in
 * `changes`), in order, checking each one first, and with `check` if given.
 * Stops at the first bad one, keeping the ones before it.
 *
 * @internal
 */
export const integrate = (
  document: Document,
  changes: ReadonlyArray<AnyChange>,
  options: { readonly overlaps: boolean; readonly check?: ((change: AnyChange) => string | undefined) | undefined }
): Integrated => {
  const contexts = contextsOf(document.history, changes)
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
  // applied, so a change that's rejected leaves nothing behind.
  const merges = new Map<string, { readonly path: Path.Path; readonly text: Text.Text; readonly merge: Text.Merge }>()
  const mergeOf = (path: Path.Path, strategy: Replica.Strategy) => {
    const k = Path.key(path)
    let found = merges.get(k)
    if (found === undefined) {
      const text = slot(document, path)?.text ?? Text.make(strategy)
      found = { path, text, merge: text.merge(incoming.get(k) ?? []) }
      merges.set(k, found)
    }
    return found
  }

  const found = options.overlaps ? new Found() : undefined
  const texts = new Map<string, TextPatch.TextPatch>()
  let problem: string | undefined
  let applied = 0
  for (const [i, change] of changes.entries()) {
    const context = contexts[i]!
    problem = options.check?.(change) ??
      check(document, change, context, (path, strategy) => mergeOf(path, strategy).merge)
    if (problem !== undefined) break
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
          const at = ensureSlot(document, part.path)
          at.kind = "text"
          at.text = text
          composeText(texts, part.path, patch)
        }
      } else {
        applyOp(document, op, write, found)
      }
      noteBelow(document, pathOf(op), write, opLength(op), found)
      seq += opLength(op)
    }
    History.record(document.history, change, context.past)
    applied++
  }

  const overlaps = found?.list(document) ?? []
  if (options.overlaps) {
    for (const { merge, path } of merges.values()) {
      for (const overlap of merge.overlaps()) overlaps.push({ _tag: "Text", path, ...overlap })
    }
  }
  return { applied, texts, overlaps, problem }
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
const contextsOf = (history: History.History<Op>, changes: ReadonlyArray<AnyChange>): Array<Context> => {
  const batch = new Map<string, Array<{ readonly change: AnyChange; readonly past: VersionVector.VersionVector }>>()
  const owner = (id: Id) => {
    const applied = History.find(history, id)
    if (applied !== undefined) return { change: applied, past: history.pasts.get(key(applied))! }
    return batch.get(id.replica)?.find(({ change }) => change.seq <= id.seq && id.seq < change.seq + change.length)
  }
  return changes.map((change) => {
    const past: Record<string, number> = {}
    let parentTime = -1
    let parentsAreChanges = true
    for (const parent of change.parents) {
      const found = owner(parent)!
      for (const [replica, seen] of Object.entries(found.past)) past[replica] = Math.max(past[replica] ?? 0, seen)
      past[parent.replica] = Math.max(past[parent.replica] ?? 0, parent.seq + 1)
      parentTime = Math.max(parentTime, found.change.time)
      parentsAreChanges &&= found.change.seq + found.change.length - 1 === parent.seq
    }
    const ofReplica = batch.get(change.replica) ?? []
    ofReplica.push({ change, past })
    batch.set(change.replica, ofReplica)
    const parts = textParts(change, past, (path) => textParents(change, path, (id) => owner(id)?.change))
    return { past, parentTime, parentsAreChanges, parts }
  })
}

// A change's text ops, grouped into one part per run of ops on the same text.
const textParts = (
  change: AnyChange,
  past: VersionVector.VersionVector,
  parentsOf: (path: Path.Path) => ReadonlyArray<Id>
): Array<TextPart> => {
  const parts: Array<TextPart> = []
  let current: { path: Path.Path; seq: number; length: number; ops: Array<Text.Op> } | undefined
  const close = () => {
    if (current !== undefined) {
      parts.push({ replica: change.replica, past, parents: parentsOf(current.path), ...current })
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

// Checks a change before applying any of it, so a bad change can't leave the
// document half-updated.
const check = (
  document: Document,
  change: AnyChange,
  { parentTime, parentsAreChanges, parts }: Context,
  mergeOf: (path: Path.Path, strategy: Replica.Strategy) => Text.Merge
): string | undefined => {
  const name = `Change ${key(change)}`
  if (change.ops.length === 0) return `${name} has no ops`
  const total = change.ops.reduce((sum, op) => sum + opLength(op), 0)
  if (total !== change.length) return `${name} says it has ${change.length} sequence numbers but has ${total}`
  if (!parentsAreChanges) return `${name} has a parent in the middle of a change`
  if (change.time <= parentTime) return `${name} has a write time no later than a change it was made on top of`
  const strategy = document.strategy ?? strategyOf([change])
  if (!fitsStrategy(change, strategy)) return `${name} mixes strategies`

  const problem = checkPaths(document, change) ?? checkPlaces(document, change)
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

// Every op's path must agree with what's there: a text, a counter and a value
// have nothing under them, a value is never added (writing it says it exists),
// and the root is a text or has parts.
const checkPaths = (document: Document, change: AnyChange): string | undefined => {
  const name = `Change ${key(change)}`
  const kinds = new Map<string, Kind>()
  const added = new Set<string>()
  const parents = new Set<string>()
  const kindAt = (path: Path.Path) => kinds.get(Path.key(path)) ?? slot(document, path)?.kind
  const addedAt = (path: Path.Path) => added.has(Path.key(path)) || (slot(document, path)?.added ?? false)
  const hasChildren = (path: Path.Path) => parents.has(Path.key(path)) || (slot(document, path)?.children.size ?? 0) > 0
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
      kinds.set(k, kind)
    } else if (op._tag === "Add") {
      if (here === "value") return `${name} adds a value`
      added.add(k)
    } else if (op._tag === "Place" && (here === "text" || here === "counter")) {
      return `${name} places a ${here} in a list`
    }
    for (const prefix of Path.prefixes(path)) parents.add(Path.key(prefix))
  }
  return undefined
}

// A position goes next to positions already in its list, or placed in the same
// list earlier in the change.
const checkPlaces = (document: Document, change: AnyChange): string | undefined => {
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
        (list !== undefined && Fugue.find(list, id) !== undefined)
      if (op.side === "R" ? op.rightOrigin === undefined : op.rightOrigin !== undefined) {
        return `Change ${key(change)} has a position with a mismatched side and right origin`
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
      at.kind = "value"
      const value: ValueWrite = { ...write, value: op.value }
      insertSorted(at.values, value)
      const differing = unseenBy(value, at.values).filter((other) => !Equal.equals(other.value, value.value))
      if (found !== undefined && differing.length > 0) {
        found.add("Value", op.path, [value.replica, ...differing.map((other) => other.replica)], "value")
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
      at.kind = "counter"
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
      insertSorted(at.places, place)
      const moved = unseenBy(place, at.places)
      if (found !== undefined && moved.length > 0) {
        found.add("Value", op.path, [place.replica, ...moved.map((other) => other.replica)], "place")
      }
      // A move that a removal it hadn't seen wins over.
      if (found !== undefined && at.presence?.exists === false && !seen(place, at.presence)) {
        found.add("Removed", op.path, [at.presence.replica, place.replica], "presence")
      }
      refreshPosition(at)
      break
    }
  }
}

const setPresence = (document: Document, at: Slot, write: PresenceWrite, found: Found | undefined): void => {
  insertSorted(at.presences, write)
  at.presence = at.presences.at(-1)
  const differing = unseenBy(write, at.presences).filter((other) => other.exists !== write.exists)
  if (found !== undefined && differing.length > 0) {
    found.add("Value", at.path, [write.replica, ...differing.map((other) => other.replica)], "presence")
  }
  // A removal that wins over edits under the entry, or moves of the item, it
  // hadn't seen.
  if (found !== undefined && !write.exists && at.presence === write) {
    const editors = [
      ...Object.entries(at.below).filter(([replica, next]) =>
        replica !== write.replica && next > (write.past[replica] ?? 0)
      ),
      ...unseenBy(write, at.places).map((other) => [other.replica] as const)
    ]
    if (editors.length > 0) {
      found.add("Removed", at.path, [write.replica, ...editors.map(([replica]) => replica)], "presence")
    }
  }
  if (!Path.isRoot(at.path) && slot(document, Path.parent(at.path))?.list !== undefined) refreshPosition(at)
}

// Records an op of `length` sequence numbers under every path above it, and
// reports an edit inside an entry that a removal it hadn't seen wins over.
const noteBelow = (
  document: Document,
  path: Path.Path,
  write: Write,
  length: number,
  found: Found | undefined
): void => {
  for (const prefix of Path.prefixes(path)) {
    const above = ensureSlot(document, prefix)
    above.below[write.replica] = Math.max(above.below[write.replica] ?? 0, write.seq + length)
    const presence = above.presence
    if (found !== undefined && presence !== undefined && !presence.exists && !seen(write, presence)) {
      found.add("Removed", prefix, [presence.replica, write.replica], "presence")
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
    noteBelow(document, op.path, done, 1, undefined)
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
        const at = ensureSlot(document, intent.path)
        at.kind = "text"
        at.text = text
        const pathed = Path.isRoot(intent.path) ? {} : { path: intent.path }
        for (const op of edited.ops) {
          ops.push({ ...op, ...pathed })
          const done: Write = { replica: options.replica, seq, time: options.time, past }
          noteBelow(document, intent.path, done, Text.opLength(op), undefined)
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
      noteBelow(document, pathOf(op), write, opLength(op), undefined)
      seq += opLength(op)
    }
    const owner = (id: Id) => History.find(history, id)
    for (const part of textParts(change, past, (path) => textParents(change, path, owner))) {
      const k = Path.key(part.path)
      const entry = parts.get(k) ?? { path: part.path, parts: [] }
      entry.parts.push(part)
      parts.set(k, entry)
    }
    History.record(history, change, past)
  }
  // Changes that don't make up the version can't go with its texts.
  if (history.pending.size > 0 || VersionVector.compare(history.version, version) !== "equal") return undefined
  const byPath = new Map(saved.map(({ path, text }) => [Path.key(path), text]))
  for (const [k, entry] of parts) {
    const text = byPath.get(k)
    const at = ensureSlot(document, entry.path)
    at.kind = "text"
    at.text = Text.make(strategy)
    if (text === undefined || !at.text.load(entry.parts, text)) return undefined
  }
  return document
}
