import { compareReplica, type Id, idKey } from "./change.ts"

// The FugueMax tree (Weidner & Kleppmann, "The Art of the Fugue").
//
// Every character ever inserted is a node with a permanent id. A node is the
// left or right child of a node it was typed next to, and the text is an
// in-order walk of the tree: left children, the node, right children. Removing
// a character only marks its node, because other characters may be placed
// relative to it. Concurrent inserts at the same spot become siblings, and
// the sibling order is what keeps passages typed concurrently from
// interleaving.
//
// This follows the paper's reference implementation (fugue-max-simple) closely,
// with one node per UTF-16 code unit. Runs, B-trees and other speedups can
// replace it later without changing what any replica computes.
//
// The tree is mutable. Its owner (a replica) serializes every change to it.

/**
 * Which side of its parent a node hangs on.
 *
 * @internal
 */
export type Side = "L" | "R"

/** @internal */
export interface Node<Info> {
  /** Undefined only for the root. */
  readonly id: Id | undefined
  readonly char: string
  readonly parent: Node<Info> | undefined
  readonly side: Side
  /**
   * For right children: the node that came next in the list when this one was
   * inserted, or `null` for the end of the list. Undefined for left children.
   */
  readonly rightOrigin: Node<Info> | null | undefined
  readonly leftChildren: Array<Node<Info>>
  readonly rightChildren: Array<Node<Info>>
  /** Visible characters in this node's subtree. */
  size: number
  removed: boolean
  /** The change that inserted this character. */
  readonly insertedBy: Info
  /** The changes that removed it. More than one means they removed it concurrently. */
  readonly removedBy: Array<Info>
}

/** @internal */
export interface Tree<Info> {
  readonly root: Node<Info>
  readonly nodes: Map<string, Node<Info>>
}

/**
 * Where a new character goes: which node it hangs from, on which side, and
 * (for right children) what came next when it was typed. Ops record it by id.
 *
 * @internal
 */
export type Placement =
  | { readonly parent: Id | undefined; readonly side: "L" }
  | { readonly parent: Id | undefined; readonly side: "R"; readonly rightOrigin: Id | null }

/**
 * A placement by node, for nodes that may have no id.
 *
 * @internal
 */
export type Spot<Info> =
  | { readonly parent: Node<Info>; readonly side: "L" }
  | { readonly parent: Node<Info>; readonly side: "R"; readonly rightOrigin: Node<Info> | null }

/**
 * A way of looking at the tree: which nodes are in it, which characters show,
 * and how many show in each subtree. `current` is the tree as it is. The
 * replay strategy also looks at it as a replica saw it when making a change,
 * without the characters that replica hadn't received.
 *
 * @internal
 */
export interface View<Info> {
  readonly has: (node: Node<Info>) => boolean
  readonly shows: (node: Node<Info>) => boolean
  readonly size: (node: Node<Info>) => number
}

/**
 * The tree as it is: every node, with removed characters hidden.
 *
 * @internal
 */
export const current: View<any> = {
  has: () => true,
  shows: (node) => !node.removed,
  size: (node) => node.size
}

/** @internal */
export const make = <Info>(rootInfo: Info): Tree<Info> => ({
  root: {
    id: undefined,
    char: "",
    parent: undefined,
    side: "R",
    rightOrigin: undefined,
    leftChildren: [],
    rightChildren: [],
    size: 0,
    removed: true,
    insertedBy: rootInfo,
    removedBy: []
  },
  nodes: new Map()
})

/** @internal */
export const find = <Info>(tree: Tree<Info>, id: Id): Node<Info> | undefined => tree.nodes.get(idKey(id))

/**
 * Fills an empty tree with `length` stand-in characters: a text whose
 * characters' ids and history don't matter, only their order. They're laid out
 * as a balanced subtree, so finding one is quick, and they get no ids (`find`
 * never returns them).
 *
 * A stand-in is never a sibling of a character inserted later: a new character
 * becomes a right child only of a node with no right children, and a left child
 * only of a node with no left children. So how they're laid out never affects
 * how later characters are ordered.
 *
 * @internal
 */
export const fill = <Info>(tree: Tree<Info>, length: number, info: Info): Array<Node<Info>> => {
  const filled: Array<Node<Info>> = []
  const build = (start: number, end: number, parent: Node<Info>, side: Side): Node<Info> | undefined => {
    if (start >= end) return undefined
    const middle = (start + end) >> 1
    const node: Node<Info> = {
      id: { replica: "", seq: middle },
      char: "",
      parent,
      side,
      rightOrigin: side === "R" ? null : undefined,
      leftChildren: [],
      rightChildren: [],
      size: end - start,
      removed: false,
      insertedBy: info,
      removedBy: []
    }
    const left = build(start, middle, node, "L")
    if (left !== undefined) node.leftChildren.push(left)
    filled[middle] = node
    const right = build(middle + 1, end, node, "R")
    if (right !== undefined) node.rightChildren.push(right)
    return node
  }
  const top = build(0, length, tree.root, "R")
  if (top !== undefined) {
    tree.root.rightChildren.push(top)
    tree.root.size += length
  }
  return filled
}

/**
 * Where a character typed at visible `index` goes. The new character goes
 * right after the character before the cursor (its left origin): as that
 * character's right child if it has none yet, otherwise as the left child of
 * whatever comes next, removed characters included.
 *
 * @internal
 */
export const place = <Info>(tree: Tree<Info>, index: number): Placement => {
  const found = spot(tree, index, current)
  return found.side === "R"
    ? { parent: found.parent.id, side: "R", rightOrigin: found.rightOrigin === null ? null : found.rightOrigin.id! }
    : { parent: found.parent.id, side: "L" }
}

/**
 * Where a character typed at visible `index` goes, as `view` sees the tree.
 *
 * @internal
 */
export const spot = <Info>(tree: Tree<Info>, index: number, view: View<Info>): Spot<Info> => {
  const leftOrigin = index === 0 ? tree.root : nodeAt(tree, index - 1, view)
  const firstRight = leftOrigin.rightChildren.find(view.has)
  if (firstRight === undefined) {
    return { parent: leftOrigin, side: "R", rightOrigin: nextNonDescendant(leftOrigin, view) ?? null }
  }
  return { parent: leftmostDescendant(firstRight, view), side: "L" }
}

/**
 * Adds a run of characters typed together, starting at `placement`. Its parent
 * and right origin must already be in the tree.
 *
 * @internal
 */
export const insert = <Info>(
  tree: Tree<Info>,
  placement: Placement,
  first: Id,
  text: string,
  info: Info
): Node<Info> =>
  insertAt(
    tree,
    placement.side === "R"
      ? {
        parent: lookup(tree, placement.parent),
        side: "R",
        rightOrigin: placement.rightOrigin === null ? null : lookup(tree, placement.rightOrigin)
      }
      : { parent: lookup(tree, placement.parent), side: "L" },
    first,
    text,
    info
  )

/**
 * Adds a run of characters typed together at `spot`. Each character after the
 * first is the right child of the one before it, with the same right origin a
 * local insert would have given it. Returns the first new node.
 *
 * @internal
 */
export const insertAt = <Info>(
  tree: Tree<Info>,
  spot: Spot<Info>,
  first: Id,
  text: string,
  info: Info
): Node<Info> => {
  let parent = spot.parent
  let side: Side = spot.side
  let rightOrigin: Node<Info> | null | undefined = spot.side === "R" ? spot.rightOrigin : undefined
  // Typing on from the first character never has a right child to go around,
  // so every later character's right origin is what followed the first one.
  const chainRightOrigin = spot.side === "R" ? rightOrigin : parent
  let firstNode: Node<Info> | undefined
  for (let offset = 0; offset < text.length; offset++) {
    const node: Node<Info> = {
      id: { replica: first.replica, seq: first.seq + offset },
      char: text[offset]!,
      parent,
      side,
      rightOrigin,
      leftChildren: [],
      rightChildren: [],
      size: 0,
      removed: false,
      insertedBy: info,
      removedBy: []
    }
    insertIntoSiblings(node)
    tree.nodes.set(idKey(node.id!), node)
    updateSize(node, 1)
    firstNode ??= node
    parent = node
    side = "R"
    rightOrigin = chainRightOrigin
  }
  return firstNode!
}

/**
 * Marks a character removed by `info`. Returns whether it was visible until now;
 * a character removed concurrently by two changes is only hidden once.
 *
 * @internal
 */
export const remove = <Info>(node: Node<Info>, info: Info): boolean => {
  node.removedBy.push(info)
  if (node.removed) return false
  node.removed = true
  updateSize(node, -1)
  return true
}

/**
 * Hides or shows a node, for trees whose nodes come and go: a list's
 * positions, where only each item's latest position shows.
 *
 * @internal
 */
export const setRemoved = <Info>(node: Node<Info>, removed: boolean): void => {
  if (node.removed === removed) return
  node.removed = removed
  updateSize(node, removed ? -1 : 1)
}

/**
 * The visible character at `index`.
 *
 * @internal
 */
export const nodeAt = <Info>(tree: Tree<Info>, index: number, view: View<Info> = current): Node<Info> => {
  let node = tree.root
  let remaining = index
  // Iterative, because ordinary typing builds very deep trees.
  descend: while (true) {
    for (const child of node.leftChildren) {
      const size = view.size(child)
      if (remaining < size) {
        node = child
        continue descend
      }
      remaining -= size
    }
    if (view.shows(node)) {
      if (remaining === 0) return node
      remaining--
    }
    for (const child of node.rightChildren) {
      const size = view.size(child)
      if (remaining < size) {
        node = child
        continue descend
      }
      remaining -= size
    }
    throw new Error(`Index ${index} is outside the text`)
  }
}

/**
 * How many visible characters come before `node`.
 *
 * @internal
 */
export const indexOf = <Info>(node: Node<Info>): number => {
  let count = 0
  for (const child of node.leftChildren) count += child.size
  let current = node
  while (current.parent !== undefined) {
    const parent: Node<Info> = current.parent
    if (current.side === "R") {
      for (const child of parent.leftChildren) count += child.size
      if (!parent.removed) count++
      for (const child of parent.rightChildren) {
        if (child === current) break
        count += child.size
      }
    } else {
      for (const child of parent.leftChildren) {
        if (child === current) break
        count += child.size
      }
    }
    current = parent
  }
  return count
}

/**
 * Every node in text order, removed ones included, without the root.
 *
 * @internal
 */
export const nodes = function*<Info>(tree: Tree<Info>): Generator<Node<Info>> {
  // Each frame is a node and how far its children have been walked.
  const stack: Array<{ node: Node<Info>; side: Side; child: number }> = [{ node: tree.root, side: "L", child: 0 }]
  while (stack.length > 0) {
    const frame = stack[stack.length - 1]!
    const children = frame.side === "L" ? frame.node.leftChildren : frame.node.rightChildren
    if (frame.child < children.length) {
      stack.push({ node: children[frame.child++]!, side: "L", child: 0 })
    } else if (frame.side === "L") {
      if (frame.node !== tree.root) yield frame.node
      frame.side = "R"
      frame.child = 0
    } else {
      stack.pop()
    }
  }
}

const lookup = <Info>(tree: Tree<Info>, id: Id | undefined): Node<Info> => {
  if (id === undefined) return tree.root
  const node = find(tree, id)
  if (node === undefined) throw new Error(`Character ${idKey(id)} isn't in the tree`)
  return node
}

const compareIds = (a: Id, b: Id): number => compareReplica(a.replica, b.replica) || a.seq - b.seq

// Right siblings are ordered in *reverse* list order of their right origins,
// then by id; left siblings by id. This is FugueMax's rule, and it's what
// makes it maximally non-interleaving.
const insertIntoSiblings = <Info>(node: Node<Info>): void => {
  const parent = node.parent!
  if (node.side === "R") {
    const siblings = parent.rightChildren
    let i = 0
    while (
      i < siblings.length && (
        isBefore(node.rightOrigin!, siblings[i]!.rightOrigin!) ||
        (node.rightOrigin === siblings[i]!.rightOrigin && compareIds(node.id!, siblings[i]!.id!) > 0)
      )
    ) i++
    siblings.splice(i, 0, node)
  } else {
    const siblings = parent.leftChildren
    let i = 0
    while (i < siblings.length && compareIds(node.id!, siblings[i]!.id!) > 0) i++
    siblings.splice(i, 0, node)
  }
}

// Whether `a` comes before `b` in the current list order, where `null` is the
// end of the list.
const isBefore = <Info>(a: Node<Info> | null, b: Node<Info> | null): boolean => {
  if (a === b || a === null) return false
  if (b === null) return true
  let aAncestor = a
  let bAncestor = b
  const aDepth = depth(a)
  const bDepth = depth(b)
  // Bring both to the same depth. If one is the other's ancestor, the side of
  // the subtree the descendant is in decides.
  let lastSide: Side = "R"
  for (let d = aDepth; d > bDepth; d--) {
    lastSide = aAncestor.side
    aAncestor = aAncestor.parent!
  }
  if (aDepth > bDepth && aAncestor === b) return lastSide === "L"
  for (let d = bDepth; d > aDepth; d--) {
    lastSide = bAncestor.side
    bAncestor = bAncestor.parent!
  }
  if (bDepth > aDepth && bAncestor === a) return lastSide === "R"
  // Climb together to siblings of a common parent.
  while (aAncestor.parent !== bAncestor.parent) {
    aAncestor = aAncestor.parent!
    bAncestor = bAncestor.parent!
  }
  if (aAncestor.side !== bAncestor.side) return aAncestor.side === "L"
  const siblings = aAncestor.side === "L" ? aAncestor.parent!.leftChildren : aAncestor.parent!.rightChildren
  return siblings.indexOf(aAncestor) < siblings.indexOf(bAncestor)
}

const depth = <Info>(node: Node<Info>): number => {
  let d = 0
  for (let current = node; current.parent !== undefined; current = current.parent) d++
  return d
}

const updateSize = <Info>(node: Node<Info>, change: number): void => {
  for (let current: Node<Info> | undefined = node; current !== undefined; current = current.parent) {
    current.size += change
  }
}

// Nodes outside a view never have descendants inside it, because a replica
// always has the characters a character it has was typed next to. So skipping
// them, subtrees and all, is enough.

const leftmostDescendant = <Info>(node: Node<Info>, view: View<Info>): Node<Info> => {
  let current = node
  for (let next = current.leftChildren.find(view.has); next !== undefined; next = current.leftChildren.find(view.has)) {
    current = next
  }
  return current
}

// The next node in text order that isn't in `node`'s subtree, removed ones
// included; undefined at the end of the list.
const nextNonDescendant = <Info>(node: Node<Info>, view: View<Info>): Node<Info> | undefined => {
  let current = node
  while (current.parent !== undefined) {
    const siblings = current.side === "L" ? current.parent.leftChildren : current.parent.rightChildren
    const next = siblings.slice(siblings.indexOf(current) + 1).find(view.has)
    if (next !== undefined) return leftmostDescendant(next, view)
    if (current.side === "L") return current.parent
    current = current.parent
  }
  return undefined
}
