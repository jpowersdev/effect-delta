import * as Inspectable from "effect/Inspectable"
import { pipeArguments } from "effect/Pipeable"
import { hasProperty } from "effect/Predicate"
import type * as ReplicatedText from "../ReplicatedText.ts"
import * as InternalDocument from "./document.ts"
import type * as InternalEngine from "./engine.ts"
import type { Path } from "./path.ts"

// A text that can be edited: a replicated text's own, at the root, or a text
// field of a document. Both are a path into a replica's engine.

// Must match the EditableTypeId in ReplicatedText.ts.
/** @internal */
export const TypeId = "~effect-delta/ReplicatedText/Editable"

/** @internal */
export interface EditableImpl extends ReplicatedText.Editable {
  readonly engine: InternalEngine.Engine
  readonly path: Path
}

/** @internal */
export const isEditable = (u: unknown): u is ReplicatedText.Editable => hasProperty(u, TypeId)

/** @internal */
export const text = (self: ReplicatedText.Editable): string => {
  const { engine, path } = self as EditableImpl
  return InternalDocument.textAt(engine.state.document, path)
}

const Proto = {
  ...Inspectable.BaseProto,
  [TypeId]: TypeId,
  pipe() {
    return pipeArguments(this, arguments)
  },
  toJSON(this: EditableImpl) {
    return { _id: "ReplicatedText.Editable", path: this.path, text: text(this) }
  }
}

/**
 * The text at `path` in a replica's engine.
 *
 * @internal
 */
export const make = (engine: InternalEngine.Engine, path: Path): ReplicatedText.Editable =>
  Object.assign(Object.create(Proto), { engine, path })
