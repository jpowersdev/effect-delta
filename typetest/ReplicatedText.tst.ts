import { Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import type { Crypto, Effect, Option } from "effect"
import { describe, expect, it } from "tstyche"

declare const notes: ReplicatedText.ReplicatedText

describe("ReplicatedText", () => {
  it("make needs Crypto only without a replica id", () => {
    expect(ReplicatedText.make()).type.toBe<Effect.Effect<ReplicatedText.ReplicatedText, never, Crypto.Crypto>>()
    expect(ReplicatedText.make({ strategy: "tree" })).type.toBe<
      Effect.Effect<ReplicatedText.ReplicatedText, never, Crypto.Crypto>
    >()
    expect(ReplicatedText.make({ replica: Replica.ReplicaId.make("laptop") })).type.toBe<
      Effect.Effect<ReplicatedText.ReplicatedText>
    >()
  })

  it("editing is pipeable", () => {
    expect(ReplicatedText.insert(notes, 0, "Hi")).type.toBe<Effect.Effect<void, ReplicatedText.ReplicatedTextError>>()
    expect(notes.pipe(ReplicatedText.insert(0, "Hi"))).type.toBe<
      Effect.Effect<void, ReplicatedText.ReplicatedTextError>
    >()
    expect(notes.pipe(ReplicatedText.set("Hi"))).type.toBe<Effect.Effect<void>>()
  })

  it("anchors take an optional side in both forms", () => {
    expect(ReplicatedText.anchor(notes, 0)).type.toBe<
      Effect.Effect<ReplicatedText.Anchor, ReplicatedText.ReplicatedTextError>
    >()
    expect(notes.pipe(ReplicatedText.anchor(0, "after"))).type.toBe<
      Effect.Effect<ReplicatedText.Anchor, ReplicatedText.ReplicatedTextError>
    >()
    expect(ReplicatedText.locate(notes, { _tag: "Start" })).type.toBe<Effect.Effect<Option.Option<number>>>()
  })

  it("a replicated text is a replica of a string", () => {
    expect(notes).type.toBeAssignableTo<Replica.Replica<string>>()
    expect(notes).type.not.toBeAssignableTo<Replica.Replica<number>>()
  })
})
