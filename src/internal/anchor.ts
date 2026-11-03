import * as Schema from "effect/Schema"

/**
 * A stable position: the gap just before or just after one character, or the
 * very start or end of the text. Every strategy names characters by id, so
 * anchors mean the same thing whichever strategy made them.
 *
 * @internal
 */
export const Anchor = Schema.Union([
  Schema.TaggedStruct("Character", {
    replica: Schema.String,
    seq: Schema.Int,
    side: Schema.Literals(["before", "after"])
  }),
  Schema.TaggedStruct("Start", {}),
  Schema.TaggedStruct("End", {})
])

/** @internal */
export type Anchor = typeof Anchor.Type

/** @internal */
export const AnchorRange = Schema.Struct({ start: Anchor, end: Anchor })

/** @internal */
export type AnchorRange = typeof AnchorRange.Type
