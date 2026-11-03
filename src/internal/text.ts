import type * as Option from "effect/Option"
import type * as Replica from "../Replica.ts"
import * as TextPatch from "../TextPatch.ts"
import * as VersionVector from "../VersionVector.ts"
import type { Anchor } from "./anchor.ts"
import { type Change, type Id, idKey, key, lastId } from "./change.ts"
import * as Fugue from "./fugue.ts"
import * as History from "./history.ts"
import * as ReplayText from "./replayText.ts"
import * as TreeText from "./treeText.ts"

// A text and its history, under one strategy. Replicas and servers keep one per
// document and go through this, so neither depends on how the strategy works.

/** @internal */
export type Op = TreeText.Op | ReplayText.Op

/** @internal */
export type AnyChange = Change<Op>

/**
 * A text and its history. Mutable: its owner serializes every call.
 *
 * @internal
 */
export interface Text {
  readonly strategy: Replica.Strategy
  /** The current text. */
  readonly value: string
  readonly history: History.History<Op>
  /**
   * Makes a local edit as one change, with ids starting at `first`. The patch
   * must fit the current text. Undefined if it changes nothing.
   */
  readonly edit: (patch: TextPatch.TextPatch, first: Id) =>
    | { readonly change: AnyChange; readonly patch: TextPatch.TextPatch }
    | undefined
  /**
   * Applies changes whose parents are all applied, in order, checking each one
   * first. Stops at the first bad one, keeping the ones before it.
   */
  readonly integrate: (changes: ReadonlyArray<AnyChange>, options: { readonly overlaps: boolean }) => Integrated
  readonly anchor: (index: number, side: TextPatch.Side) => Anchor
  readonly locate: (anchor: Anchor) => Option.Option<number>
  /** Who typed each part of the current text. */
  readonly authorship: () => ReadonlyArray<TreeText.Run>
  /**
   * Starts an empty text from saved history: `changes` are exactly the ones in
   * `version`, and `text` is the text they make. Strategies that need to work
   * the text out themselves return false and change nothing.
   */
  readonly load: (changes: ReadonlyArray<AnyChange>, version: VersionVector.VersionVector, text: string) => boolean
}

/** @internal */
export interface Integrated {
  /** How the applied changes changed the text. */
  readonly patch: TextPatch.TextPatch
  readonly overlaps: ReadonlyArray<TreeText.Overlap>
  /** What's wrong with the first change that wasn't applied, if any. */
  readonly problem: string | undefined
}

/** @internal */
export const make = (strategy: Replica.Strategy): Text => strategy === "tree" ? makeTree() : makeReplay()

/**
 * Starts over from `changes`, in the order they were applied.
 *
 * @internal
 */
export const rebuild = (strategy: Replica.Strategy, changes: Iterable<AnyChange>): Text => {
  const text = make(strategy)
  text.integrate([...changes], { overlaps: false })
  return text
}

/**
 * Which strategy made a change, judging by its ops. Undefined if it mixes them
 * or has none.
 *
 * @internal
 */
export const strategyOf = (change: AnyChange): Replica.Strategy | undefined => {
  if (change.ops.length === 0) return undefined
  const replay = change.ops.filter(ReplayText.isOp).length
  return replay === change.ops.length ? "replay" : replay === 0 ? "tree" : undefined
}

// Both strategies count one sequence number per character inserted or removed.
const checkLength = (change: AnyChange, opLength: (op: Op) => number): string | undefined => {
  const total = change.ops.reduce((sum, op) => sum + opLength(op), 0)
  return total === change.length
    ? undefined
    : `Change ${key(change)} says it has ${change.length} characters but has ${total}`
}

// Tree -----------------------------------------------------------------------

type TreeChange = Change<TreeText.Op>

const treeOpLength = (op: Op) => TreeText.opLength(op as TreeText.Op)

// A batch this many characters long is applied in one pass over the tree, which
// costs the same however much it holds. Smaller ones, such as a keystroke, walk
// up the tree per character instead, which is cheaper for a few characters.
const batchCharacters = 256

const makeTree = (): Text => {
  const state = TreeText.make()
  const history = History.make<Op>()

  const integrateOne = (change: TreeChange): { readonly patch: TextPatch.TextPatch; readonly info: TreeText.Info } => {
    const past = History.pastOf(history, change.parents)
    const info: TreeText.Info = { replica: change.replica, seq: change.seq, length: change.length, past }
    let patch = TextPatch.empty
    let seq = change.seq
    for (const op of change.ops) {
      patch = TextPatch.compose(patch, TreeText.apply(state, op, { replica: change.replica, seq }, info))
      seq += TreeText.opLength(op)
    }
    History.record(history, change, past)
    return { patch, info }
  }

  const integrateLater = (change: TreeChange): TreeText.Info => {
    const past = History.pastOf(history, change.parents)
    const info: TreeText.Info = { replica: change.replica, seq: change.seq, length: change.length, past }
    let seq = change.seq
    for (const op of change.ops) {
      TreeText.applyLater(state, op, { replica: change.replica, seq }, info)
      seq += TreeText.opLength(op)
    }
    History.record(history, change, past)
    return info
  }

  return {
    strategy: "tree",
    get value() {
      return state.text
    },
    history,
    edit: (patch, first) => {
      const info: TreeText.Info = { replica: first.replica, seq: first.seq, length: 0, past: { ...history.version } }
      const ops: Array<TreeText.Op> = []
      let applied = TextPatch.empty
      let position = 0
      for (const operation of patch) {
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
      if (ops.length === 0) return undefined
      const change: TreeChange = {
        replica: first.replica,
        seq: first.seq,
        length: info.length,
        parents: history.frontier,
        ops
      }
      History.record(history, change, info.past)
      return { change, patch: applied }
    },
    integrate: (changes, options) => {
      const added = new Set<TreeText.Info>()
      const later = changes.reduce((sum, change) => sum + change.length, 0) >= batchCharacters
      let patch = TextPatch.empty
      let problem: string | undefined
      for (const change of changes as ReadonlyArray<TreeChange>) {
        problem = checkLength(change, treeOpLength) ?? checkTree(state, change)
        if (problem !== undefined) break
        if (later) {
          added.add(integrateLater(change))
        } else {
          const applied = integrateOne(change)
          patch = TextPatch.compose(patch, applied.patch)
          added.add(applied.info)
        }
      }
      if (later) patch = TreeText.settle(state, added)
      return { patch, overlaps: options.overlaps ? TreeText.overlaps(state, added) : [], problem }
    },
    anchor: (index, side) => TreeText.anchor(state, index, side),
    locate: (anchor) => TreeText.locate(state, anchor),
    authorship: () => TreeText.authorship(state),
    // The tree has to be built from the history.
    load: () => false
  }
}

// Checks a tree change before applying any of it, so a bad change can't leave
// the tree half-updated. Its ops may refer to characters inserted earlier in
// the same change.
const checkTree = (state: TreeText.State, change: TreeChange): string | undefined => {
  const inserted = new Set<string>()
  const known = (id: Id | null | undefined) =>
    id === null || id === undefined || inserted.has(idKey(id)) || TreeText.has(state, id)
  let seq = change.seq
  for (const op of change.ops) {
    if (op._tag === "Insert") {
      if (op.side === "R" ? op.rightOrigin === undefined : op.rightOrigin !== undefined) {
        return `Change ${key(change)} has an insert with a mismatched side and right origin`
      }
      if (!known(op.parent) || !known(op.rightOrigin)) {
        return `Change ${key(change)} inserts next to a character it hasn't seen`
      }
      for (let offset = 0; offset < op.text.length; offset++) {
        inserted.add(idKey({ replica: change.replica, seq: seq + offset }))
      }
    } else {
      for (const span of op.targets) {
        for (let offset = 0; offset < span.length; offset++) {
          if (!known({ replica: span.replica, seq: span.seq + offset })) {
            return `Change ${key(change)} removes a character it hasn't seen`
          }
        }
      }
    }
    seq += TreeText.opLength(op)
  }
  return undefined
}

// Replay ---------------------------------------------------------------------

type ReplayChange = Change<ReplayText.Op>

const replayOpLength = (op: Op) => ReplayText.opLength(op as ReplayText.Op)

const makeReplay = (): Text => {
  let value = ""
  let history = History.make<Op>()
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
  // change that they and every applied change after it were made on top of,
  // and replays only the applied changes after that.
  const walkFrom = (
    incoming: ReadonlyArray<ReplayChange>,
    infos: ReadonlyMap<string, TreeText.Info>
  ): ReplayText.Walker => {
    const after = changesAfterBase(history, incoming)
    // The text at the base can't be longer than the text now plus everything
    // removed since; any extra stand-ins sit at the end, out of reach.
    const removed = after.reduce(
      (sum, change) => sum + change.ops.reduce((n, op) => n + (op._tag === "RemoveAt" ? op.length : 0), 0),
      0
    )
    const walker = ReplayText.makeWalker(value.length + removed)
    for (const change of after) ReplayText.replay(walker, change, infoOf(change, infos), false)
    return walker
  }

  return {
    strategy: "replay",
    get value() {
      return value
    },
    get history() {
      return history
    },
    edit: (patch, first) => {
      const ops: Array<ReplayText.Op> = []
      let position = 0
      for (const operation of patch) {
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
      const change: ReplayChange = {
        replica: first.replica,
        seq: first.seq,
        length: ops.reduce((sum, op) => sum + ReplayText.opLength(op), 0),
        parents: history.frontier,
        ops
      }
      const applied = ReplayText.apply(value, change)
      History.record(history, change, { ...history.version })
      value = applied.text
      cached = undefined
      return { change, patch: applied.patch }
    },
    integrate: (changes, options) => {
      cached = undefined
      const infos = new Map<string, TreeText.Info>()
      let walker: ReplayText.Walker | undefined
      let patch = TextPatch.empty
      let problem: string | undefined
      for (const [index, change] of (changes as ReadonlyArray<ReplayChange>).entries()) {
        const past = History.pastOf(history, change.parents)
        const info: TreeText.Info = { replica: change.replica, seq: change.seq, length: change.length, past }
        problem = checkLength(change, replayOpLength) ?? checkParents(history, change)
        if (problem !== undefined) break
        if (walker === undefined && sameIds(change.parents, history.frontier)) {
          // Made on top of exactly this text: it applies as is.
          problem = ReplayText.check(change, value.length)
          if (problem !== undefined) break
          const applied = ReplayText.apply(value, change)
          value = applied.text
          patch = TextPatch.compose(patch, applied.patch)
        } else {
          // Concurrent with changes here: place it by replaying. Once there's
          // a walker, the rest of the batch goes through it too.
          walker ??= walkFrom(changes.slice(index) as ReadonlyArray<ReplayChange>, infos)
          ReplayText.moveTo(walker, change.parents)
          problem = ReplayText.check(change, ReplayText.viewLength(walker))
          if (problem !== undefined) break
          const applied = ReplayText.replay(walker, change, info, true)
          value = TextPatch.apply(applied, value)
          patch = TextPatch.compose(patch, applied)
        }
        History.record(history, change, past)
        infos.set(key(change), info)
      }
      let overlaps = options.overlaps && walker !== undefined
        ? TreeText.overlaps({ tree: walker.tree, text: value }, new Set(infos.values()))
        : []
      // An overlap that's all removed characters is anchored on them, and
      // stand-ins have no ids. Those need the whole history replayed.
      if (
        overlaps.some((overlap) => isStandIn(walker!, overlap.anchors.start) || isStandIn(walker!, overlap.anchors.end))
      ) {
        overlaps = TreeText.overlaps({ tree: walkAll(infos).tree, text: value }, new Set(infos.values()))
      }
      return { patch, overlaps, problem }
    },
    anchor: (index, side) => {
      cached ??= walkAll(new Map())
      return TreeText.anchor({ tree: cached.tree, text: value }, index, side)
    },
    locate: (anchor) => {
      cached ??= walkAll(new Map())
      return TreeText.locate({ tree: cached.tree, text: value }, anchor)
    },
    authorship: () => {
      cached ??= walkAll(new Map())
      return TreeText.authorship({ tree: cached.tree, text: value })
    },
    // Records the history without replaying anything: the text is given.
    load: (changes, version, text) => {
      if (history.changes.size > 0 || history.pending.size > 0) return false
      const loaded = History.make<Op>()
      for (const change of changes) loaded.pending.set(key(change), change)
      for (const change of History.takeReady(loaded)) {
        History.record(loaded, change, History.pastOf(loaded, change.parents))
      }
      // Changes that don't make up the version can't go with its text.
      if (loaded.pending.size > 0 || VersionVector.compare(loaded.version, version) !== "equal") return false
      history = loaded
      value = text
      return true
    }
  }
}

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

// The walker finds changes by their last ids, which is what honest replicas
// make parents of.
const checkParents = (history: History.History<Op>, change: AnyChange): string | undefined =>
  change.parents.every((parent) => History.endsAt(history, parent))
    ? undefined
    : `Change ${key(change)} has a parent in the middle of a change`

const sameIds = (a: ReadonlyArray<Id>, b: ReadonlyArray<Id>): boolean =>
  a.length === b.length && a.every((id) => b.some((other) => other.replica === id.replica && other.seq === id.seq))
