import type * as Authority from "../Authority.ts"
import type * as Delta from "../Delta.ts"
import type * as Replica from "../Replica.ts"
import * as InternalDocument from "./document.ts"
import * as Path from "./path.ts"

// What a document proposal's JSON can't say: which record entries, list items
// and optional fields exist. Without the schema, an entry nobody added but
// someone wrote inside looks like a struct field. The authority notes which
// parts exist, by their paths in the JSON, and readProposal reads only those,
// as replicas do.

/**
 * A document before or after a change, as a proposal has it: its JSON, and
 * the paths in the JSON of the parts that exist.
 *
 * @internal
 */
export interface Side {
  readonly json: Authority.DocumentProposal["before"]
  readonly existing: ReadonlySet<string>
}

const sides = new WeakMap<Authority.DocumentProposal, { readonly before: Side; readonly after: Side }>()

/** @internal */
export const sideOf = (document: InternalDocument.Document): Side => {
  const existing = new Set<string>()
  return { json: InternalDocument.toJson(document, existing), existing }
}

/** @internal */
export const make = (
  proposal: {
    readonly document: Replica.DocumentId
    readonly user: string
    readonly before: Side
    readonly after: Side
    readonly paths: ReadonlyArray<Replica.Path>
    readonly delta: Delta.Delta
  }
): Authority.DocumentProposal => {
  const made: Authority.DocumentProposal = {
    _tag: "Document",
    document: proposal.document,
    user: proposal.user,
    before: proposal.before.json,
    after: proposal.after.json,
    paths: proposal.paths,
    delta: proposal.delta
  }
  sides.set(made, { before: proposal.before, after: proposal.after })
  return made
}

/**
 * Whether the entry, item or optional field at a path in the proposal's JSON
 * (list items by index) exists before or after the change, if the proposal
 * says. Undefined for a proposal an authority didn't make.
 *
 * @internal
 */
export const existsIn = (
  proposal: Authority.DocumentProposal,
  side: "before" | "after"
): ((path: Path.Path) => boolean) | undefined => {
  const found = sides.get(proposal)?.[side].existing
  return found === undefined ? undefined : (path) => found.has(Path.key(path))
}
