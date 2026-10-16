import type * as Option from "effect/Option"
import type * as Replica from "../Replica.ts"
import * as TextPatch from "../TextPatch.ts"
import type * as VersionVector from "../VersionVector.ts"
import type { Anchor } from "./anchor.ts"
import { type Change, type Id, idKey, key, lastId } from "./change.ts"
import * as Fugue from "./fugue.ts"
import * as History from "./history.ts"
import * as ReplayText from "./replayText.ts"
import * as TreeText from "./treeText.ts"

// One text in a document, under one strategy. The document keeps the history
// and hands each text the part of every change that touched it: its ops, the
// sequence numbers they use, and what the change's author had seen. A
// replicated text is a document whose only text is at the root.

/** @internal */
export type Op = TreeText.Op | ReplayText.Op

/** @internal */
export const isOp = (op: { readonly _tag: string }): op is Op =>
  op._tag === "Insert" || op._tag === "Remove" || ReplayText.isOp(op)

/** @internal */
export const opLength = (op: Op): number =>
  ReplayText.isOp(op) ? ReplayText.opLength(op) : TreeText.opLength(op as TreeText.Op)

/** @internal */
export const strategyOfOp = (op: Op): Replica.Strategy => ReplayText.isOp(op) ? "replay" : "tree"

/**
 * The part of a change that touched one text: its ops on the text, which are
 * consecutive, starting at sequence number `seq`.
 *
 * @internal
 */
export interface Part {
  readonly replica: string
  readonly seq: number
  readonly length: number
  readonly ops: ReadonlyArray<Op>
  /** What the change's author had seen, across the whole document. */
  readonly past: VersionVector.VersionVector
  /** The last ids of the latest parts of this text the change was made on top of. */
  readonly parents: ReadonlyArray<Id>
}

/**
 * A text. Mutable: its document serializes every call.
 *
 * @internal
 */
export interface Text {
  readonly strategy: Replica.Strategy
  /** The current text. */
  readonly value: string
  /**
   * Makes a local edit, with ids starting at `first`, on top of everything in
   * `past`. The patch must fit the current text. Undefined if it changes
   * nothing.
   */
  readonly edit: (patch: TextPatch.TextPatch, first: Id, past: VersionVector.VersionVector) =>
    | { readonly ops: ReadonlyArray<Op>; readonly patch: TextPatch.TextPatch }
    | undefined
  /** Starts merging the parts of a batch of changes, which are `incoming`, in order. */
  readonly merge: (incoming: ReadonlyArray<Part>) => Merge
  readonly anchor: (index: number, side: TextPatch.Side) => Anchor
  readonly locate: (anchor: Anchor) => Option.Option<number>
  /**
   * Starts an empty text from saved history: `parts` are every part it has,
   * in order, and `text` is the text they make. Strategies that need to work
   * the text out themselves return false and change nothing.
   */
  readonly load: (parts: ReadonlyArray<Part>, text: string) => boolean
}

/**
 * A merge in progress. Each part is checked, and if it's fine, applied.
 *
 * @internal
 */
export interface Merge {
  /**
   * What's wrong with the part, if anything, said of its change ("inserts
   * next to a character it hasn't seen"). Changes nothing.
   */
  readonly check: (part: Part) => string | undefined
  /** Applies a part that passed `check`, returning how it changed the text. */
  readonly apply: (part: Part) => TextPatch.TextPatch
  /** Where the applied parts met parts their authors hadn't seen. */
  readonly overlaps: () => ReadonlyArray<TreeText.Overlap>
}

/** @internal */
export const make = (strategy: Replica.Strategy): Text => strategy === "tree" ? makeTree() : makeReplay()

// Inserting or removing nothing makes no op: every op takes sequence numbers.
const withoutNoOps = (patch: TextPatch.TextPatch): TextPatch.TextPatch =>
  patch.filter((operation) => operation._tag === "Insert" ? operation.text !== "" : operation.length !== 0)

// Tree -----------------------------------------------------------------------

const makeTree = (): Text => {
  const state = TreeText.make()
  return {
    strategy: "tree",
    get value() {
      return state.text
    },
    edit: (patch, first, past) => {
      const info: TreeText.Info = { replica: first.replica, seq: first.seq, length: 0, past }
      const ops: Array<TreeText.Op> = []
      let applied = TextPatch.empty
      let position = 0
      for (const operation of withoutNoOps(patch)) {
        switch (operation._tag) {
          case "Retain":
            position += operation.length
            break
          case "Insert": {
            const id = { replica: first.replica, seq: first.seq + info.length }
            const [op, done] = TreeText.insert(state, position, operation.text, id, info)
            ops.push(op)
            applied = TextPatch.compose(applied, done)
            info.length += operation.text.length
            position += operation.text.length
            break
          }
          case "Remove": {
            const [op, done] = TreeText.remove(state, position, operation.length, info)
            ops.push(op)
            applied = TextPatch.compose(applied, done)
            info.length += operation.length
            break
          }
        }
      }
      return ops.length === 0 ? undefined : { ops, patch: applied }
    },
    merge: () => {
      const added = new Set<TreeText.Info>()
      return {
        check: (part) => checkTree(state, part),
        apply: (part) => {
          const info: TreeText.Info = { replica: part.replica, seq: part.seq, length: part.length, past: part.past }
          let patch = TextPatch.empty
          let seq = part.seq
          for (const op of part.ops as ReadonlyArray<TreeText.Op>) {
            patch = TextPatch.compose(patch, TreeText.apply(state, op, { replica: part.replica, seq }, info))
            seq += TreeText.opLength(op)
          }
          added.add(info)
          return patch
        },
        overlaps: () => TreeText.overlaps(state, added)
      }
    },
    anchor: (index, side) => TreeText.anchor(state, index, side),
    locate: (anchor) => TreeText.locate(state, anchor),
    // The tree has to be built from the history.
    load: () => false
  }
}

// Checks a tree part before applying any of it, so a bad change can't leave
// the tree half-updated. Its ops may refer to characters inserted earlier in
// the same part.
const checkTree = (state: TreeText.State, part: Part): string | undefined => {
  const inserted = new Set<string>()
  const known = (id: Id | null | undefined) =>
    id === null || id === undefined || inserted.has(idKey(id)) ||
    // Only what its author had seen, so it doesn't matter what else is here.
    ((part.past[id.replica] ?? 0) > id.seq && TreeText.has(state, id))
  let seq = part.seq
  for (const op of part.ops) {
    if (op._tag === "Insert") {
      if (op.side === "R" ? op.rightOrigin === undefined : op.rightOrigin !== undefined) {
        return "has an insert with a mismatched side and right origin"
      }
      // Replicas put what goes at the start on the right of nothing. On its
      // left, it would leave the next character typed after it with nothing
      // to name as its right origin.
      if (op.parent === null && op.side === "L") return "inserts before the start of the text"
      if (!known(op.parent) || !known(op.rightOrigin)) {
        return "inserts next to a character it hasn't seen"
      }
      for (let offset = 0; offset < op.text.length; offset++) {
        inserted.add(idKey({ replica: part.replica, seq: seq + offset }))
      }
    } else if (op._tag === "Remove") {
      for (const span of op.targets) {
        for (let offset = 0; offset < span.length; offset++) {
          if (!known({ replica: span.replica, seq: span.seq + offset })) {
            return "removes a character it hasn't seen"
          }
        }
      }
    }
    seq += opLength(op)
  }
  return undefined
}

// Replay ---------------------------------------------------------------------

type ReplayChange = Change<ReplayText.Op>

// A replay text keeps its own history: the parts that touched it, each with
// the latest parts before it as parents. The rest of the document doesn't
// change the text, so replaying only these gives the same text.
const makeReplay = (): Text => {
  let value = ""
  const history = History.make<Op>()
  // A walker over the whole history, for anchors. Dropped on every change.
  let cached: ReplayText.Walker | undefined

  const infoOf = (change: ReplayChange, infos: ReadonlyMap<string, TreeText.Info>): TreeText.Info =>
    infos.get(key(change)) ??
      { replica: change.replica, seq: change.seq, length: change.length, past: history.pasts.get(key(change))! }

  // Replays the whole history, using `infos` for changes that have one.
  const walkAll = (infos: ReadonlyMap<string, TreeText.Info>): ReplayText.Walker => {
    const walker = ReplayText.makeWalker(0)
    for (const change of history.changes.values() as Iterable<ReplayChange>) {
      ReplayText.replay(walker, change, infoOf(change, infos), false)
    }
    return walker
  }

  // A walker ready to merge `incoming`: it starts from the text at the latest
  // part that they and every applied part after it were made on top of, and
  // replays only the applied parts after that. Also says how many of its
  // stand-ins for that text weren't in it.
  const walkFrom = (
    incoming: ReadonlyArray<ReplayChange>,
    infos: ReadonlyMap<string, TreeText.Info>
  ): { readonly walker: ReplayText.Walker; readonly extra: number } => {
    const after = changesAfterBase(history, incoming)
    // The text at the base can't be longer than the text now plus everything
    // removed since; any extra stand-ins sit at the end, out of reach.
    const removed = after.reduce(
      (sum, change) => sum + change.ops.reduce((n, op) => n + (op._tag === "RemoveAt" ? op.length : 0), 0),
      0
    )
    const walker = ReplayText.makeWalker(value.length + removed)
    for (const change of after) ReplayText.replay(walker, change, infoOf(change, infos), false)
    // Out of reach, they end every view, so the view of everything has them
    // past the text.
    ReplayText.moveTo(walker, history.frontier)
    return { walker, extra: ReplayText.viewLength(walker) - value.length }
  }

  return {
    strategy: "replay",
    get value() {
      return value
    },
    edit: (patch, first, past) => {
      const ops: Array<ReplayText.Op> = []
      let position = 0
      for (const operation of withoutNoOps(patch)) {
        switch (operation._tag) {
          case "Retain":
            position += operation.length
            break
          case "Insert":
            ops.push({ _tag: "InsertAt", index: position, text: operation.text })
            position += operation.text.length
            break
          case "Remove":
            ops.push({ _tag: "RemoveAt", index: position, length: operation.length })
            break
        }
      }
      if (ops.length === 0) return undefined
      // Made on top of every part of this text, so its parents are the latest.
      const change: ReplayChange = {
        replica: first.replica,
        seq: first.seq,
        length: ops.reduce((sum, op) => sum + ReplayText.opLength(op), 0),
        parents: history.frontier,
        time: 0,
        ops
      }
      const applied = ReplayText.apply(value, change)
      History.record(history, change, past)
      value = applied.text
      cached = undefined
      return { ops, patch: applied.patch }
    },
    merge: (parts) => {
      cached = undefined
      const incoming = parts.map(toChange)
      const changeOf = (part: Part): ReplayChange => incoming[parts.indexOf(part)]!
      const infos = new Map<string, TreeText.Info>()
      let walker: ReplayText.Walker | undefined
      let extra = 0
      let applied = 0
      return {
        check: (part) => {
          const change = changeOf(part)
          if (walker === undefined && sameIds(change.parents, history.frontier)) {
            // Made on top of exactly this text: it applies as is.
            return ReplayText.check(change, value.length)
          }
          // Concurrent with parts here: place it by replaying. Once there's a
          // walker, the rest of the batch goes through it too.
          if (walker === undefined) ({ extra, walker } = walkFrom(incoming.slice(applied), infos))
          ReplayText.moveTo(walker, change.parents)
          // Measured without the extra stand-ins, which a change mustn't reach.
          return ReplayText.check(change, ReplayText.viewLength(walker) - extra)
        },
        apply: (part) => {
          const change = changeOf(part)
          const info: TreeText.Info = { replica: part.replica, seq: part.seq, length: part.length, past: part.past }
          let patch: TextPatch.TextPatch
          if (walker === undefined) {
            const done = ReplayText.apply(value, change)
            value = done.text
            patch = done.patch
          } else {
            patch = ReplayText.replay(walker, change, info, true)
            value = TextPatch.apply(patch, value)
          }
          History.record(history, change, part.past)
          infos.set(key(change), info)
          applied++
          return patch
        },
        overlaps: () => {
          if (walker === undefined) return []
          const added = new Set(infos.values())
          const found = TreeText.overlaps({ tree: walker.tree, text: value }, added)
          // An overlap that's all removed characters is anchored on them, and
          // stand-ins have no ids. Those need the whole history replayed.
          return found.some((overlap) =>
              isStandIn(walker!, overlap.anchors.start) || isStandIn(walker!, overlap.anchors.end)
            )
            ? TreeText.overlaps({ tree: walkAll(infos).tree, text: value }, added)
            : found
        }
      }
    },
    anchor: (index, side) => {
      cached ??= walkAll(new Map())
      return TreeText.anchor({ tree: cached.tree, text: value }, index, side)
    },
    locate: (anchor) => {
      cached ??= walkAll(new Map())
      return TreeText.locate({ tree: cached.tree, text: value }, anchor)
    },
    // Records the history without replaying anything: the text is given.
    load: (parts, text) => {
      if (history.changes.size > 0) return false
      for (const part of parts) History.record(history, toChange(part), part.past)
      value = text
      cached = undefined
      return true
    }
  }
}

const toChange = (part: Part): ReplayChange => ({
  replica: part.replica,
  seq: part.seq,
  length: part.length,
  parents: part.parents,
  time: 0,
  ops: part.ops as ReadonlyArray<ReplayText.Op>
})

// The applied changes to replay to merge `incoming`, in the order they were
// applied. Walks back from the frontier and the incoming changes, latest
// first, until one applied change is left that everything walked was made on
// top of (Eg-walker's search for a critical version). That change and
// everything before it stay plain text.
const changesAfterBase = (
  history: History.History<Op>,
  incoming: ReadonlyArray<ReplayChange>
): Array<ReplayChange> => {
  const applied = [...history.changes.values()] as Array<ReplayChange>
  const order = new Map<ReplayChange, number>()
  const byEnd = new Map<string, ReplayChange>()
  for (const change of [...applied, ...incoming]) {
    order.set(change, order.size)
    byEnd.set(idKey(lastId(change)), change)
  }
  // The empty text, which changes with no parents were made on top of. It
  // comes before everything, so it's walked last.
  const start = "start"
  const pending = new Set(incoming)
  const queue = new Set<ReplayChange | typeof start>(incoming)
  for (const tip of history.frontier) queue.add(byEnd.get(idKey(tip))!)
  if (queue.size === 0) queue.add(start)
  const walked = new Set<ReplayChange>()
  while (queue.size > 1 || [...queue].some((change) => change !== start && pending.has(change))) {
    let latest: ReplayChange | undefined
    for (const change of queue) {
      if (change !== start && (latest === undefined || order.get(change)! > order.get(latest)!)) latest = change
    }
    queue.delete(latest!)
    walked.add(latest!)
    if (latest!.parents.length === 0) queue.add(start)
    for (const parent of latest!.parents) {
      const change = byEnd.get(idKey(parent))
      if (change !== undefined && !walked.has(change)) queue.add(change)
    }
  }
  return applied.filter((change) => walked.has(change))
}

const isStandIn = (walker: ReplayText.Walker, anchor: Anchor): boolean =>
  anchor._tag === "Character" && Fugue.find(walker.tree, anchor) === undefined

const sameIds = (a: ReadonlyArray<Id>, b: ReadonlyArray<Id>): boolean =>
  a.length === b.length && a.every((id) => b.some((other) => other.replica === id.replica && other.seq === id.seq))
