import type { HistoryStore, Sync } from "@jpowersdev/effect-delta"
import { Authority, Relay, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
import type { Effect, Layer, Scope } from "effect"
import type { SecureRandom } from "effect/crypto/SecureRandom"
import type { RpcClient } from "effect/rpc"
import { describe, expect, it } from "tstyche"

declare const notes: ReplicatedText.ReplicatedText
const room = Replica.makeRoom("notes")

// An app service an authority's rule might need.
interface Members {
  readonly _: unique symbol
}
declare const editorsOnly: Effect.Effect<void, Authority.Rejected, Members>

describe("Sync", () => {
  it("a confirmed replica needs an authority", () => {
    expect(ReplicatedText.makeConfirmed({ room })).type.toBe<
      Effect.Effect<ReplicatedText.Confirmed, never, Authority.Authority | SecureRandom | Scope.Scope>
    >()
  })

  it("connecting through a relay needs a relay, in either form", () => {
    expect(Relay.connect(notes, { room })).type.toBe<
      Effect.Effect<Sync.Connection, never, Relay.Relay | Scope.Scope>
    >()
    expect(notes.pipe(Relay.connect({ room }))).type.toBe<
      Effect.Effect<Sync.Connection, never, Relay.Relay | Scope.Scope>
    >()
  })

  it("an authority's rule passes its requirements on to the layer", () => {
    const layer = Authority.layer({ check: () => editorsOnly })
    expect(layer).type.toBe<Layer.Layer<Authority.Authority, never, HistoryStore.HistoryStore | Members>>()
  })

  it("an authority's rule may only fail with Rejected", () => {
    expect(Authority.layer).type.not.toBeCallableWith({
      check: () => null as unknown as Effect.Effect<void, Sync.Forbidden>
    })
  })

  it("the remote layers need only an RPC protocol", () => {
    expect(Relay.layerRemote).type.toBe<Layer.Layer<Relay.Relay, never, RpcClient.Protocol>>()
    expect(Authority.layerRemote).type.toBe<Layer.Layer<Authority.Authority, never, RpcClient.Protocol>>()
  })
})
