import * as TextPatch from "../TextPatch.ts"
import { type Change, key } from "./change.ts"
import * as History from "./history.ts"
import * as TreeText from "./treeText.ts"

// Applying tree-strategy changes to a text and its history, shared by
// replicas and by the authority, which keeps the text to check changes
// against.

/**
 * Applies one change whose parents are all applied, and records it.
 *
 * @internal
 */
export const integrate = (
  text: TreeText.State,
  history: History.History<TreeText.Op>,
  change: Change<TreeText.Op>
): { readonly patch: TextPatch.TextPatch; readonly info: TreeText.Info } => {
  const past = History.pastOf(history, change.parents)
  const info: TreeText.Info = { replica: change.replica, seq: change.seq, length: change.length, past }
  let patch = TextPatch.empty
  let seq = change.seq
  for (const op of change.ops) {
    patch = TextPatch.compose(patch, TreeText.apply(text, op, { replica: change.replica, seq }, info))
    seq += TreeText.opLength(op)
  }
  History.record(history, change, past)
  return { patch, info }
}

/**
 * Starts over from `changes`, in the order they were applied.
 *
 * @internal
 */
export const rebuild = (changes: ReadonlyArray<Change<TreeText.Op>>): {
  readonly text: TreeText.State
  readonly history: History.History<TreeText.Op>
} => {
  const text = TreeText.make()
  const history = History.make<TreeText.Op>()
  for (const change of changes) integrate(text, history, change)
  return { text, history }
}

/**
 * Checks a change before applying any of it, so a bad change can't leave the
 * tree half-updated. Its ops may refer to characters inserted earlier in the
 * same change. Returns what's wrong, if anything.
 *
 * @internal
 */
export const check = (text: TreeText.State, change: Change<TreeText.Op>): string | undefined => {
  const total = change.ops.reduce((sum, op) => sum + TreeText.opLength(op), 0)
  if (total !== change.length) return `Change ${key(change)} says it has ${change.length} characters but has ${total}`
  const inserted = new Set<string>()
  const known = (id: { readonly replica: string; readonly seq: number } | null | undefined) =>
    id === null || id === undefined || inserted.has(`${id.seq}@${id.replica}`) ||
    TreeText.has(text, id)
  let seq = change.seq
  for (const op of change.ops) {
    if (op._tag === "Insert") {
      if (op.side === "R" ? op.rightOrigin === undefined : op.rightOrigin !== undefined) {
        return `Change ${key(change)} has an insert with a mismatched side and right origin`
      }
      if (!known(op.parent) || !known(op.rightOrigin)) {
        return `Change ${key(change)} inserts next to a character it hasn't seen`
      }
      for (let offset = 0; offset < op.text.length; offset++) inserted.add(`${seq + offset}@${change.replica}`)
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
