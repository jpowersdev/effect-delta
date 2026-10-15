import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as TextPatch from "../TextPatch.ts"
import type * as VersionVector from "../VersionVector.ts"
import type { Anchor } from "./anchor.ts"
import { compareReplica, type Id, Id as IdSchema, type Span, Span as SpanSchema } from "./change.ts"
import * as Fugue from "./fugue.ts"

// The tree strategy: text as a FugueMax tree. Ops name characters by id, so a
// replica can apply another replica's op without knowing what else was
// happening when it was made.

/**
 * An op of the tree strategy. `Insert` adds a run of characters typed together;
 * the first goes at `parent`/`side` (with `rightOrigin` for right children) and
 * each later one follows the one before. `Remove` removes characters by id.
 *
 * @internal
 */
export type Op =
  | {
    readonly _tag: "Insert"
    readonly text: string
    readonly parent: Id | null
    readonly side: "L" | "R"
    readonly rightOrigin?: Id | null
  }
  | { readonly _tag: "Remove"; readonly targets: ReadonlyArray<Span> }

/** @internal */
export const Op: Schema.Codec<Op> = Schema.Union([
  Schema.TaggedStruct("Insert", {
    text: Schema.String,
    parent: Schema.NullOr(IdSchema),
    side: Schema.Literals(["L", "R"]),
    rightOrigin: Schema.optionalKey(Schema.NullOr(IdSchema))
  }),
  Schema.TaggedStruct("Remove", { targets: Schema.Array(SpanSchema) })
])

/** @internal */
export const opLength = (op: Op): number =>
  op._tag === "Insert" ? op.text.length : op.targets.reduce((sum, span) => sum + span.length, 0)

/**
 * What the tree records about the change behind each character, so a merge
 * can tell which edits were concurrent.
 *
 * @internal
 */
export interface Info {
  readonly replica: string
  readonly seq: number
  length: number
  /** What the change's author had seen when making it. */
  readonly past: VersionVector.VersionVector
}

/** @internal */
export interface State {
  readonly tree: Fugue.Tree<Info>
  text: string
}

const rootInfo: Info = { replica: "", seq: 0, length: 0, past: {} }

/** @internal */
export const make = (): State => ({ tree: Fugue.make(rootInfo), text: "" })

// Local edits --------------------------------------------------------------

/**
 * Inserts `text` at `index` as a local edit, with ids starting at `first`.
 *
 * @internal
 */
export const insert = (state: State, index: number, text: string, first: Id, info: Info): [Op, TextPatch.TextPatch] => {
  const placement = Fugue.place(state.tree, index)
  const op: Op = placement.side === "R"
    ? { _tag: "Insert", text, parent: placement.parent ?? null, side: "R", rightOrigin: placement.rightOrigin }
    : { _tag: "Insert", text, parent: placement.parent ?? null, side: "L" }
  return [op, apply(state, op, first, info)]
}

/**
 * Removes `length` characters at `index` as a local edit.
 *
 * @internal
 */
export const remove = (state: State, index: number, length: number, info: Info): [Op, TextPatch.TextPatch] => {
  const targets: Array<Span> = []
  for (let offset = 0; offset < length; offset++) {
    const id = Fugue.nodeAt(state.tree, index + offset).id!
    const last = targets[targets.length - 1]
    if (last !== undefined && last.replica === id.replica && last.seq + last.length === id.seq) {
      targets[targets.length - 1] = { ...last, length: last.length + 1 }
    } else {
      targets.push({ replica: id.replica, seq: id.seq, length: 1 })
    }
  }
  const op: Op = { _tag: "Remove", targets }
  return [op, apply(state, op, { replica: info.replica, seq: 0 }, info)]
}

// Applying ops -------------------------------------------------------------

/**
 * Whether the character `id` is in the tree, removed or not.
 *
 * @internal
 */
export const has = (state: State, id: Id): boolean => Fugue.find(state.tree, id) !== undefined

/**
 * Applies an op (the replica's own or another's) and returns how it changed the
 * visible text. `first` is the id of the op's first character.
 *
 * @internal
 */
export const apply = (state: State, op: Op, first: Id, info: Info): TextPatch.TextPatch => {
  if (op._tag === "Insert") {
    const placement: Fugue.Placement = op.side === "R"
      ? { parent: op.parent ?? undefined, side: "R", rightOrigin: op.rightOrigin ?? null }
      : { parent: op.parent ?? undefined, side: "L" }
    const node = Fugue.insert(state.tree, placement, first, op.text, info)
    const index = Fugue.indexOf(node)
    const patch = TextPatch.insert(index, op.text)
    state.text = state.text.slice(0, index) + op.text + state.text.slice(index)
    return patch
  }
  let patch = TextPatch.empty
  for (const span of op.targets) {
    for (let offset = 0; offset < span.length; offset++) {
      const node = Fugue.find(state.tree, { replica: span.replica, seq: span.seq + offset })!
      const index = Fugue.indexOf(node)
      if (Fugue.remove(node, info)) {
        patch = TextPatch.compose(patch, TextPatch.remove(index, 1))
        state.text = state.text.slice(0, index) + state.text.slice(index + 1)
      }
    }
  }
  return patch
}

// Anchors ------------------------------------------------------------------

/** @internal */
export const anchor = (state: State, index: number, side: TextPatch.Side): Anchor => {
  if (side === "before") {
    if (index === state.text.length) return { _tag: "End" }
    const id = Fugue.nodeAt(state.tree, index).id!
    return { _tag: "Character", replica: id.replica, seq: id.seq, side }
  }
  if (index === 0) return { _tag: "Start" }
  const id = Fugue.nodeAt(state.tree, index - 1).id!
  return { _tag: "Character", replica: id.replica, seq: id.seq, side }
}

/**
 * Where an anchor is now. A removed character's anchors sit where it was. None
 * if the character isn't known here.
 *
 * @internal
 */
export const locate = (state: State, anchor: Anchor): Option.Option<number> => {
  switch (anchor._tag) {
    case "Start":
      return Option.some(0)
    case "End":
      return Option.some(state.text.length)
    case "Character": {
      const node = Fugue.find(state.tree, anchor)
      if (node === undefined) return Option.none()
      const index = Fugue.indexOf(node)
      return Option.some(node.removed || anchor.side === "before" ? index : index + 1)
    }
  }
}

// Overlaps -----------------------------------------------------------------

/**
 * A stretch of merged text where changes that hadn't seen each other touched.
 *
 * @internal
 */
export interface Overlap {
  readonly start: number
  readonly end: number
  readonly anchors: { readonly start: Anchor; readonly end: Anchor }
  readonly replicas: ReadonlyArray<string>
}

/**
 * Finds the overlaps a merge produced. `added` are the changes just applied.
 * The text is split at visible characters that nothing concurrent touched; a
 * stretch where a just-applied change and an earlier change it hadn't seen both
 * inserted or removed characters is an overlap.
 *
 * @internal
 */
export const overlaps = (state: State, added: ReadonlySet<Info>): Array<Overlap> => {
  if (added.size === 0) return []
  const unseenBy = (earlier: Info, later: Info): boolean =>
    (later.past[earlier.replica] ?? 0) < earlier.seq + earlier.length
  const concurrent = new Map<Info, boolean>()
  const isConcurrent = (info: Info): boolean => {
    let result = concurrent.get(info)
    if (result === undefined) {
      result = [...added].some((later) => unseenBy(info, later))
      concurrent.set(info, result)
    }
    return result
  }

  const found: Array<Overlap> = []
  let stretch: Stretch | undefined
  let visible = 0
  const close = () => {
    if (stretch !== undefined && isOverlap(stretch, unseenBy)) found.push(toOverlap(stretch))
    stretch = undefined
  }
  for (const node of Fugue.nodes(state.tree)) {
    const touches = [node.insertedBy, ...node.removedBy]
    const fresh = touches.filter((info) => added.has(info))
    const earlier = touches.filter((info) => !added.has(info) && info !== rootInfo && isConcurrent(info))
    if (fresh.length === 0 && earlier.length === 0) {
      if (!node.removed) {
        close()
        visible++
      }
      continue
    }
    stretch ??= { start: visible, visible: 0, first: node, last: node, fresh: new Set(), earlier: new Set() }
    stretch.last = node
    if (!node.removed) {
      stretch.firstVisible ??= node
      stretch.lastVisible = node
      stretch.visible++
      visible++
    }
    for (const info of fresh) stretch.fresh.add(info)
    for (const info of earlier) stretch.earlier.add(info)
  }
  close()
  return found
}

interface Stretch {
  readonly start: number
  visible: number
  readonly first: Fugue.Node<Info>
  last: Fugue.Node<Info>
  firstVisible?: Fugue.Node<Info>
  lastVisible?: Fugue.Node<Info>
  readonly fresh: Set<Info>
  readonly earlier: Set<Info>
}

const isOverlap = (stretch: Stretch, unseenBy: (earlier: Info, later: Info) => boolean): boolean => {
  for (const earlier of stretch.earlier) {
    for (const later of stretch.fresh) {
      if (unseenBy(earlier, later)) return true
    }
  }
  return false
}

const toOverlap = (stretch: Stretch): Overlap => {
  const first = stretch.firstVisible ?? stretch.first
  const last = stretch.lastVisible ?? stretch.last
  const replicas = new Set<string>()
  for (const info of [...stretch.fresh, ...stretch.earlier]) replicas.add(info.replica)
  return {
    start: stretch.start,
    end: stretch.start + stretch.visible,
    anchors: {
      start: { _tag: "Character", replica: first.id!.replica, seq: first.id!.seq, side: "before" },
      end: { _tag: "Character", replica: last.id!.replica, seq: last.id!.seq, side: "after" }
    },
    replicas: [...replicas].sort(compareReplica)
  }
}
