import * as SecureRandom from "effect/crypto/SecureRandom"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

/**
 * Secure randomness that gives the same bytes for the same seed, so a test's
 * replica ids don't change between runs.
 */
export const layerSeeded = (seed: string): Layer.Layer<SecureRandom.SecureRandom> =>
  Layer.effect(SecureRandom.SecureRandom)(Effect.sync(() => {
    let state = 2166136261
    for (let i = 0; i < seed.length; i++) {
      state = Math.imul(state ^ seed.charCodeAt(i), 16777619)
    }
    return SecureRandom.make((size) => {
      const bytes = new Uint8Array(size)
      for (let i = 0; i < size; i++) {
        state = Math.imul(state ^ (state >>> 15), 2246822507) + 2654435769 | 0
        bytes[i] = state >>> 24
      }
      return bytes
    })
  }))
