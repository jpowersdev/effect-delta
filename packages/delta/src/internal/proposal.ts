import type * as Authority from "../Authority.ts"
import type * as Delta from "../Delta.ts"
import type * as Replica from "../Replica.ts"
import type * as InternalDocument from "./document.ts"
import * as Path from "./path.ts"

// What a document proposal's JSON can't say: which record entries, list items
// and optional fields exist. Without the schema, an entry nobody added but
// someone wrote inside looks like a struct field. The authority notes which
// parts exist, and readProposal reads only those, as replicas do.

const existing = new WeakMap<Authority.DocumentProposal, {
  readonly before: ReadonlySet<string>
  readonly after: ReadonlySet<string>
}>()

/**
 * The paths whose latest write says they exist.
 *
 * @internal
 */
export const existingIn = (document: InternalDocument.Document): ReadonlySet<string> => {
  const found = new Set<string>()
  for (const [k, slot] of document.slots) if (slot.presence?.exists) found.add(k)
  return found
}

/** @internal */
export const make = (
  proposal: {
    readonly document: Replica.DocumentId
    readonly user: string
    readonly before: { readonly json: Authority.DocumentProposal["before"]; readonly existing: ReadonlySet<string> }
    readonly after: { readonly json: Authority.DocumentProposal["after"]; readonly existing: ReadonlySet<string> }
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
  existing.set(made, { before: proposal.before.existing, after: proposal.after.existing })
  return made
}

/**
 * Whether the entry, item or optional field at a path exists before or after
 * the change, if the proposal says. Undefined for a proposal an authority
 * didn't make.
 *
 * @internal
 */
export const existsIn = (
  proposal: Authority.DocumentProposal,
  side: "before" | "after"
): ((path: Path.Path) => boolean) | undefined => {
  const found = existing.get(proposal)?.[side]
  return found === undefined ? undefined : (path) => found.has(Path.key(path))
}
