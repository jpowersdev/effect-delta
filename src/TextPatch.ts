/**
 * The `TextPatch` module describes how a string changed, as a sequence of
 * operations that keep, insert and remove text. A patch applies in order to
 * the string it was made for, and encodes to the Quill delta shape, so editors
 * that speak Quill deltas (or the Yjs and Loro "delta" format) can apply it
 * directly.
 *
 * Patches are sequential, like `JsonPatch` and `Differ`: composing two patches
 * means applying one after the other. They don't merge concurrent edits. That's
 * what replicas and deltas are for.
 *
 * @stability unstable
 * @since 0.1.0
 */
import type * as Differ_ from "effect/Differ"
import { dual } from "effect/Function"
import * as Schema from "effect/Schema"
import * as SchemaTransformation from "effect/SchemaTransformation"

/**
 * One step of a text patch: keep `length` characters, insert `text`, or remove
 * `length` characters. Lengths count UTF-16 code units, as JavaScript string
 * indexes do.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type TextPatchOperation =
  | { readonly _tag: "Retain"; readonly length: number }
  | { readonly _tag: "Insert"; readonly text: string }
  | { readonly _tag: "Remove"; readonly length: number }

/**
 * A sequence of operations that turns one string into another. Characters past
 * the last operation are kept.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type TextPatch = ReadonlyArray<TextPatchOperation>

/**
 * One operation of a Quill delta, the encoded form of a text patch. Formatting
 * attributes are ignored when decoding.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type QuillOperation =
  | { readonly retain: number }
  | { readonly insert: string }
  | { readonly delete: number }

const Length = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))

/**
 * Schema for a text patch, encoded as a Quill delta: `{ retain }`, `{ insert }`
 * and `{ delete }` operations.
 *
 * **Example** (Decoding a Quill delta)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 * import { Schema } from "effect"
 *
 * const patch = Schema.decodeUnknownSync(TextPatch.TextPatch)([{
 *   retain: 6
 * }, {
 *   insert: "big "
 * }])
 * TextPatch.apply(patch, "a fox is here") // => "a fox big is here"
 * ```
 *
 * @stability unstable
 * @category schemas
 * @since 0.1.0
 */
export const TextPatch: Schema.Codec<TextPatch, ReadonlyArray<QuillOperation>> = Schema.Array(
  Schema.Union([
    Schema.Struct({ retain: Length }),
    Schema.Struct({ insert: Schema.String }),
    Schema.Struct({ delete: Length })
  ])
).pipe(
  Schema.decodeTo(
    Schema.Array(Schema.Union([
      Schema.TaggedStruct("Retain", { length: Length }),
      Schema.TaggedStruct("Insert", { text: Schema.String }),
      Schema.TaggedStruct("Remove", { length: Length })
    ])),
    SchemaTransformation.transform<TextPatch, ReadonlyArray<QuillOperation>>({
      decode: (operations) => operations.map(fromQuill),
      encode: (operations) => operations.map(toQuill)
    })
  )
)

function fromQuill(operation: QuillOperation): TextPatchOperation {
  if ("insert" in operation) return { _tag: "Insert", text: operation.insert }
  if ("retain" in operation) return { _tag: "Retain", length: operation.retain }
  return { _tag: "Remove", length: operation.delete }
}

function toQuill(operation: TextPatchOperation): QuillOperation {
  switch (operation._tag) {
    case "Retain":
      return { retain: operation.length }
    case "Insert":
      return { insert: operation.text }
    case "Remove":
      return { delete: operation.length }
  }
}

/**
 * The patch that changes nothing.
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const empty: TextPatch = []

/**
 * Creates a patch that inserts `text` at `index`.
 *
 * **Example** (Inserting text)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * TextPatch.apply(TextPatch.insert(5, ","), "Hello world") // => "Hello, world"
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const insert = (index: number, text: string): TextPatch => normalize([retain(index), insertOp(text)])

/**
 * Creates a patch that removes `length` characters starting at `index`.
 *
 * **Example** (Removing text)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * TextPatch.apply(TextPatch.remove(5, 6), "Hello world") // => "Hello"
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const remove = (index: number, length: number): TextPatch => normalize([retain(index), removeOp(length)])

/**
 * Returns the patch that turns `before` into `after` with one replacement:
 * the shortest stretch between their longest common prefix and suffix, moved
 * so it never splits a surrogate pair (half of an emoji). Replicated texts use
 * the same rule when given a whole new string.
 *
 * **Example** (Finding the replacement)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * const patch = TextPatch.get("Sessions last 24 hours", "Sessions last 8 hours")
 * patch // => [{ _tag: "Retain", length: 14 }, { _tag: "Insert", text: "8" }, { _tag: "Remove", length: 2 }]
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const get = (before: string, after: string): TextPatch => {
  const limit = Math.min(before.length, after.length)
  let prefix = 0
  while (prefix < limit && before.charCodeAt(prefix) === after.charCodeAt(prefix)) prefix++
  if (prefix > 0 && isHighSurrogate(before.charCodeAt(prefix - 1))) prefix--
  let suffix = 0
  while (
    suffix < limit - prefix &&
    before.charCodeAt(before.length - 1 - suffix) === after.charCodeAt(after.length - 1 - suffix)
  ) suffix++
  if (suffix > 0 && isLowSurrogate(before.charCodeAt(before.length - suffix))) suffix--
  return normalize([
    retain(prefix),
    insertOp(after.slice(prefix, after.length - suffix)),
    removeOp(before.length - prefix - suffix)
  ])
}

/**
 * Applies a patch to the string it was made for.
 *
 * **Gotchas**
 *
 * Throws if the patch keeps or removes more characters than `before` has.
 *
 * **Example** (Applying a patch)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * TextPatch.apply(TextPatch.get("cat", "cart"), "cat") // => "cart"
 * ```
 *
 * @stability unstable
 * @category combinators
 * @since 0.1.0
 */
export const apply: {
  (before: string): (patch: TextPatch) => string
  (patch: TextPatch, before: string): string
} = dual(2, (patch: TextPatch, before: string): string => {
  let out = ""
  let position = 0
  for (const operation of patch) {
    switch (operation._tag) {
      case "Retain":
        if (position + operation.length > before.length) {
          throw new RangeError("The patch keeps past the end of the text")
        }
        out += before.slice(position, position + operation.length)
        position += operation.length
        break
      case "Insert":
        out += operation.text
        break
      case "Remove":
        if (position + operation.length > before.length) {
          throw new RangeError("The patch removes past the end of the text")
        }
        position += operation.length
        break
    }
  }
  return out + before.slice(position)
})

/**
 * Combines two patches into one that applies `first` and then `second`.
 *
 * **Example** (Composing edits)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * const both = TextPatch.compose(TextPatch.insert(0, "Hello"), TextPatch.insert(5, " world"))
 * TextPatch.apply(both, "") // => "Hello world"
 * ```
 *
 * @stability unstable
 * @category combining
 * @since 0.1.0
 */
export const compose: {
  (second: TextPatch): (first: TextPatch) => TextPatch
  (first: TextPatch, second: TextPatch): TextPatch
} = dual(2, (first: TextPatch, second: TextPatch): TextPatch => {
  const a = iterate(first)
  const b = iterate(second)
  const out: Array<TextPatchOperation> = []
  while (a.hasNext() || b.hasNext()) {
    if (b.peekTag() === "Insert") {
      out.push(b.next())
    } else if (a.peekTag() === "Remove") {
      out.push(a.next())
    } else {
      const length = Math.min(a.peekLength(), b.peekLength())
      const fromA = a.next(length)
      const fromB = b.next(length)
      if (fromB._tag === "Retain") out.push(fromA)
      // Text the first patch inserted and the second removes never existed.
      else if (fromA._tag === "Retain") out.push(fromB)
    }
  }
  return normalize(out)
})

/**
 * Rewrites `patch` so it applies after `over`, when both were made for the same
 * string. Where both insert at the same spot, `side` decides whether `patch`'s
 * text goes `"before"` or `"after"` `over`'s. Editor bindings use this to move
 * a remote patch past keystrokes the replica hasn't seen yet.
 *
 * **Example** (Rebasing a remote patch over local typing)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * const local = TextPatch.insert(0, "Hi ")
 * const remote = TextPatch.insert(5, "!")
 * const rebased = TextPatch.transform(remote, local, "after")
 * TextPatch.apply(rebased, TextPatch.apply(local, "there")) // => "Hi there!"
 * ```
 *
 * @stability unstable
 * @category combinators
 * @since 0.1.0
 */
export const transform: {
  (over: TextPatch, side: Side): (patch: TextPatch) => TextPatch
  (patch: TextPatch, over: TextPatch, side: Side): TextPatch
} = dual(3, (patch: TextPatch, over: TextPatch, side: Side): TextPatch => {
  const a = iterate(over)
  const b = iterate(patch)
  const out: Array<TextPatchOperation> = []
  while (a.hasNext() || b.hasNext()) {
    if (a.peekTag() === "Insert" && (side === "after" || b.peekTag() !== "Insert")) {
      out.push(retain(textLength(a.next())))
    } else if (b.peekTag() === "Insert") {
      out.push(b.next())
    } else {
      const length = Math.min(a.peekLength(), b.peekLength())
      const fromA = a.next(length)
      const fromB = b.next(length)
      // Text `over` already removed can't be kept or removed again.
      if (fromA._tag === "Remove") continue
      out.push(fromB._tag === "Remove" ? fromB : retain(length))
    }
  }
  return normalize(out)
})

/**
 * Which way a position leans when text is inserted exactly at it: `"before"`
 * the character after it (the position moves past the new text) or `"after"`
 * the character before it (the position stays put).
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Side = "before" | "after"

/**
 * Returns where `index` ends up after `patch`. Text inserted exactly at `index`
 * goes in front of a position leaning `"before"` and behind one leaning
 * `"after"`. A position inside removed text moves to where the removal was.
 *
 * **Example** (Moving a cursor past a remote edit)
 *
 * ```ts import.meta.vitest
 * import { TextPatch } from "@jpowersdev/effect-delta"
 *
 * TextPatch.mapIndex(TextPatch.insert(0, "Hi "), 2, "before") // => 5
 * TextPatch.mapIndex(TextPatch.insert(2, "!"), 2, "after") // => 2
 * ```
 *
 * @stability unstable
 * @category combinators
 * @since 0.1.0
 */
export const mapIndex: {
  (index: number, side: Side): (patch: TextPatch) => number
  (patch: TextPatch, index: number, side: Side): number
} = dual(3, (patch: TextPatch, index: number, side: Side): number => {
  let mapped = index
  let position = 0
  for (const operation of patch) {
    if (position > index) break
    switch (operation._tag) {
      case "Retain":
        position += operation.length
        break
      case "Insert":
        if (position < index || side === "before") mapped += operation.text.length
        break
      case "Remove":
        mapped -= Math.min(operation.length, index - position)
        position += operation.length
        break
    }
  }
  return mapped
})

/**
 * A `Differ` for strings whose patches are text patches: `get`, `compose` and
 * `apply`.
 *
 * @stability unstable
 * @category instances
 * @since 0.1.0
 */
export const Differ: Differ_.Differ<string, TextPatch> = {
  empty,
  diff: get,
  combine: compose,
  patch: (before, patch) => apply(patch, before)
}

const retain = (length: number): TextPatchOperation => ({ _tag: "Retain", length })
const insertOp = (text: string): TextPatchOperation => ({ _tag: "Insert", text })
const removeOp = (length: number): TextPatchOperation => ({ _tag: "Remove", length })

const textLength = (operation: TextPatchOperation): number =>
  operation._tag === "Insert" ? operation.text.length : operation.length

const isHighSurrogate = (code: number): boolean => code >= 0xd800 && code <= 0xdbff
const isLowSurrogate = (code: number): boolean => code >= 0xdc00 && code <= 0xdfff

/**
 * Merges neighbours of the same kind, drops empty operations and the trailing
 * keep, and puts an insert before a removal next to it, so equal patches look
 * the same.
 *
 * @internal
 */
export const normalize = (operations: ReadonlyArray<TextPatchOperation>): TextPatch => {
  const out: Array<TextPatchOperation> = []
  for (const operation of operations) {
    if (textLength(operation) === 0) continue
    const last = out[out.length - 1]
    if (operation._tag === "Insert" && last?._tag === "Remove") {
      // Keep inserts ahead of removals at the same spot.
      out.pop()
      push(out, operation)
      out.push(last)
      continue
    }
    push(out, operation)
  }
  while (out.length > 0 && out[out.length - 1]!._tag === "Retain") out.pop()
  return out
}

const push = (out: Array<TextPatchOperation>, operation: TextPatchOperation): void => {
  const last = out[out.length - 1]
  if (last === undefined || last._tag !== operation._tag) {
    out.push(operation)
  } else if (operation._tag === "Insert") {
    out[out.length - 1] = insertOp((last as { readonly text: string }).text + operation.text)
  } else {
    out[out.length - 1] = {
      _tag: operation._tag,
      length: (last as { readonly length: number }).length + operation.length
    }
  }
}

// Walks a patch an operation (or part of one) at a time. Past the end, a patch
// keeps everything.
interface Iterator {
  readonly hasNext: () => boolean
  readonly peekTag: () => TextPatchOperation["_tag"]
  readonly peekLength: () => number
  readonly next: (length?: number) => TextPatchOperation
}

const iterate = (patch: TextPatch): Iterator => {
  let index = 0
  let offset = 0
  return {
    hasNext: () => index < patch.length,
    peekTag: () => index < patch.length ? patch[index]!._tag : "Retain",
    peekLength: () => index < patch.length ? textLength(patch[index]!) - offset : Infinity,
    next: (length = Infinity) => {
      const operation = patch[index]
      if (operation === undefined) return retain(length)
      const remaining = textLength(operation) - offset
      const taken = Math.min(length, remaining)
      const start = offset
      if (taken === remaining) {
        index++
        offset = 0
      } else {
        offset += taken
      }
      return operation._tag === "Insert"
        ? insertOp(operation.text.slice(start, start + taken))
        : { _tag: operation._tag, length: taken }
    }
  }
}
