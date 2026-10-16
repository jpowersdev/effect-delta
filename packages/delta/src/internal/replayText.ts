import * as Schema from "effect/Schema"
import * as TextPatch from "../TextPatch.ts"
import { type Change, type Id, idKey, key, lastId } from "./change.ts"
import * as Fugue from "./fugue.ts"
import type * as TreeText from "./treeText.ts"

// The replay strategy (Eg-walker, Gentle & Kleppmann, EuroSys 2025). Ops say
// what the user did, as indexes into the text they saw. A replica keeps only
// the text and its history.
//
// A change made on top of everything a replica has applies to its text as is.
// One made concurrently with changes the replica has needs placing: the
// walker replays the history through a temporary FugueMax tree, looking at
// the tree as each change's author saw it, and works out where each change
// lands in the text as it is. Each change becomes exactly the tree op its
// author's replica would have made under the tree strategy, so both
// strategies give the same text.

/**
 * An op of the replay strategy, applied in order to the text its change was
 * made on top of.
 *
 * @internal
 */
export type Op =
  | { readonly _tag: "InsertAt"; readonly index: number; readonly text: string }
  | { readonly _tag: "RemoveAt"; readonly index: number; readonly length: number }

/** @internal */
export const Op: Schema.Codec<Op> = Schema.Union([
  Schema.TaggedStruct("InsertAt", { index: Schema.Int, text: Schema.String }),
  Schema.TaggedStruct("RemoveAt", { index: Schema.Int, length: Schema.Int })
])

/** @internal */
export const isOp = (op: { readonly _tag: string }): op is Op => op._tag === "InsertAt" || op._tag === "RemoveAt"

/** @internal */
export const opLength = (op: Op): number => op._tag === "InsertAt" ? op.text.length : op.length

/**
 * Checks that a change's ops fit a text of `length` characters. Returns what's
 * wrong, if anything.
 *
 * @internal
 */
export const check = (change: Change<Op>, length: number): string | undefined => {
  let current = length
  for (const op of change.ops) {
    if (op.index < 0 || op.index > current || (op._tag === "RemoveAt" && op.index + op.length > current)) {
      return `Change ${key(change)} edits outside the text it was made on`
    }
    current += op._tag === "InsertAt" ? op.text.length : -op.length
  }
  return undefined
}

/**
 * Applies a change's ops to `text`, which must be the text it was made on.
 *
 * @internal
 */
export const apply = (
  text: string,
  change: Change<Op>
): { readonly text: string; readonly patch: TextPatch.TextPatch } => {
  let patch = TextPatch.empty
  for (const op of change.ops) {
    if (op._tag === "InsertAt") {
      text = text.slice(0, op.index) + op.text + text.slice(op.index)
      patch = TextPatch.compose(patch, TextPatch.insert(op.index, op.text))
    } else {
      text = text.slice(0, op.index) + text.slice(op.index + op.length)
      patch = TextPatch.compose(patch, TextPatch.remove(op.index, op.length))
    }
  }
  return { text, patch }
}

// The walker -----------------------------------------------------------------

/**
 * A temporary tree with every replayed character, looked at as the author of
 * the next change saw it.
 *
 * @internal
 */
export interface Walker {
  readonly tree: Fugue.Tree<TreeText.Info>
  readonly seen: Map<Fugue.Node<TreeText.Info>, Seen>
  readonly view: Fugue.View<TreeText.Info>
  /** Replayed changes, by the key of their last id, which is how parents name them. */
  readonly ends: Map<string, Replayed>
  /** The frontier of what the tree is being looked at with. */
  frontier: ReadonlyArray<Replayed>
}

interface Seen {
  inserted: boolean
  removals: number
  /** Characters showing in this node's subtree. */
  size: number
}

interface Replayed {
  readonly change: Change<Op>
  /** Its place in the replay, which respects what each change was made on top of. */
  readonly order: number
  readonly parents: ReadonlyArray<Replayed>
  readonly inserted: Array<Fugue.Node<TreeText.Info>>
  readonly removed: Array<Fugue.Node<TreeText.Info>>
}

const rootInfo: TreeText.Info = { replica: "", seq: 0, length: 0, past: {} }

/**
 * Makes a walker that starts from a text of `length` characters. Their history
 * isn't replayed, so the changes replayed must all have been made on top of
 * that text. It can be longer than the real text, as long as the extra
 * characters are at the end, where no change made on top of it can reach.
 *
 * @internal
 */
export const makeWalker = (length: number): Walker => {
  const tree = Fugue.make(rootInfo)
  const seen = new Map<Fugue.Node<TreeText.Info>, Seen>([[tree.root, { inserted: true, removals: 1, size: length }]])
  for (const node of Fugue.fill(tree, length, rootInfo)) {
    seen.set(node, { inserted: true, removals: 0, size: node.size })
  }
  return {
    tree,
    seen,
    view: {
      has: (node) => seen.get(node)!.inserted,
      shows: (node) => shows(seen.get(node)!),
      size: (node) => seen.get(node)!.size
    },
    ends: new Map(),
    frontier: []
  }
}

const shows = (seen: Seen): boolean => seen.inserted && seen.removals === 0

/**
 * Looks at the tree as the author of a change made on top of `parents` saw
 * it: retreats the replayed changes outside their past, and advances the ones
 * inside it. Parents that weren't replayed are part of the starting text.
 *
 * @internal
 */
export const moveTo = (walker: Walker, parents: ReadonlyArray<Id>): void => {
  const target = parents.flatMap((parent) => walker.ends.get(idKey(parent)) ?? [])
  const { onlyA: retreat, onlyB: advance } = diff(walker.frontier, target)
  for (const replayed of retreat) toggle(walker, replayed, false)
  for (const replayed of advance.reverse()) toggle(walker, replayed, true)
  walker.frontier = target
}

/**
 * Replays a change on top of its parents, which must have been replayed. With
 * `emit`, also returns how it changes the text as it is (everything replayed
 * so far); otherwise the patch is empty.
 *
 * @internal
 */
export const replay = (
  walker: Walker,
  change: Change<Op>,
  info: TreeText.Info,
  emit: boolean
): TextPatch.TextPatch => {
  moveTo(walker, change.parents)
  const replayed: Replayed = {
    change,
    order: walker.ends.size,
    parents: walker.frontier,
    inserted: [],
    removed: []
  }
  let patch = TextPatch.empty
  let seq = change.seq
  for (const op of change.ops) {
    if (op._tag === "InsertAt") {
      const spot = Fugue.spot(walker.tree, op.index, walker.view)
      const first = Fugue.insertAt(walker.tree, spot, { replica: change.replica, seq }, op.text, info)
      const nodes = Array.from(
        op.text,
        (_, offset) => Fugue.find(walker.tree, { replica: change.replica, seq: seq + offset })!
      )
      for (const node of nodes) walker.seen.set(node, { inserted: true, removals: 0, size: 0 })
      for (const node of nodes) resize(walker, node, 1)
      replayed.inserted.push(...nodes)
      if (emit) patch = TextPatch.compose(patch, TextPatch.insert(Fugue.indexOf(first), op.text))
      seq += op.text.length
    } else {
      for (let offset = 0; offset < op.length; offset++) {
        // Each removal hides the character, so the next one is at the same index.
        const node = Fugue.nodeAt(walker.tree, op.index, walker.view)
        walker.seen.get(node)!.removals++
        resize(walker, node, -1)
        replayed.removed.push(node)
        const index = Fugue.indexOf(node)
        if (Fugue.remove(node, info) && emit) patch = TextPatch.compose(patch, TextPatch.remove(index, 1))
      }
      seq += op.length
    }
  }
  walker.ends.set(idKey(lastId(change)), replayed)
  walker.frontier = [replayed]
  return patch
}

/**
 * How many characters show in the view: the length of the text the next
 * change was made on.
 *
 * @internal
 */
export const viewLength = (walker: Walker): number => walker.seen.get(walker.tree.root)!.size

// The replayed changes in the past of `a` but not `b`, and of `b` but not `a`,
// latest first. Walks back from both frontiers, latest change first, until
// everything left to walk is in both pasts (Eg-walker's diff).
const diff = (
  a: ReadonlyArray<Replayed>,
  b: ReadonlyArray<Replayed>
): { readonly onlyA: Array<Replayed>; readonly onlyB: Array<Replayed> } => {
  type Flag = "a" | "b" | "both"
  const flags = new Map<Replayed, Flag>()
  const queue: Array<Replayed> = []
  let shared = 0
  const enqueue = (replayed: Replayed, flag: Flag) => {
    const existing = flags.get(replayed)
    if (existing === undefined) {
      queue.push(replayed)
      flags.set(replayed, flag)
      if (flag === "both") shared++
    } else if (existing !== flag && existing !== "both") {
      flags.set(replayed, "both")
      shared++
    }
  }
  for (const replayed of a) enqueue(replayed, "a")
  for (const replayed of b) enqueue(replayed, "b")
  const onlyA: Array<Replayed> = []
  const onlyB: Array<Replayed> = []
  while (queue.length > shared) {
    let latest = 0
    for (let i = 1; i < queue.length; i++) if (queue[i]!.order > queue[latest]!.order) latest = i
    const [replayed] = queue.splice(latest, 1)
    const flag = flags.get(replayed!)!
    if (flag === "both") shared--
    else (flag === "a" ? onlyA : onlyB).push(replayed!)
    for (const parent of replayed!.parents) enqueue(parent, flag)
  }
  return { onlyA, onlyB }
}

const toggle = (walker: Walker, replayed: Replayed, on: boolean): void => {
  for (const node of replayed.inserted) {
    const seen = walker.seen.get(node)!
    const before = shows(seen)
    seen.inserted = on
    if (shows(seen) !== before) resize(walker, node, on ? 1 : -1)
  }
  for (const node of replayed.removed) {
    const seen = walker.seen.get(node)!
    const before = shows(seen)
    seen.removals += on ? 1 : -1
    if (shows(seen) !== before) resize(walker, node, on ? -1 : 1)
  }
}

const resize = (walker: Walker, node: Fugue.Node<TreeText.Info>, change: number): void => {
  for (let current: Fugue.Node<TreeText.Info> | undefined = node; current !== undefined; current = current.parent) {
    walker.seen.get(current)!.size += change
  }
}
