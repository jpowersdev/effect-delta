/**
 * An authority is a server that decides which changes count. Each change a
 * replica pushes is checked against a rule the app gives (`check`), and either
 * accepted (stored, then sent to every subscriber) or rejected. Replicas that
 * answer to an authority are made with `ReplicatedText.makeConfirmed` or
 * `ReplicatedDocument.makeConfirmed`; they roll a rejected change back.
 *
 * `Authority` is a service with three layers:
 *
 * - `layer` runs an authority in this process: on a server, or given straight
 *   to replicas in tests.
 * - `layerRemote` reaches an authority on a server, over any `RpcClient`
 *   protocol. One connection carries every document.
 * - `layerHandlers` serves an authority over `Sync.Rpcs`, checking each request
 *   with `Sync.Authorization`.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Rpc from "effect/rpc/Rpc"
import type * as RpcClient from "effect/rpc/RpcClient"
import type * as RpcGroup from "effect/rpc/RpcGroup"
import * as Schema from "effect/Schema"
import type * as Delta from "./Delta.ts"
import type * as HistoryStore from "./HistoryStore.ts"
import * as InternalServer from "./internal/server.ts"
import type * as Replica from "./Replica.ts"
import type * as Sync from "./Sync.ts"
import type * as TextPatch from "./TextPatch.ts"

/**
 * An authority: subscribing to a document, and pushing changes to it, which it
 * may reject.
 *
 * @stability unstable
 * @category services
 * @since 0.1.0
 */
export class Authority extends Context.Service<Authority, Sync.Server>()("effect-delta/Authority") {}

/**
 * A change an authority is asked to accept: who made it, and what it does to
 * the document as the authority has it. `Text` for a replicated text,
 * `Document` for a `ReplicatedDocument`.
 *
 * **Details**
 *
 * Both have `document` and `user`: the user pushing it, from
 * `Sync.Authorization.identify` (or `Sync.CurrentUser`, in this process).
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export type Proposal = TextProposal | DocumentProposal

/**
 * A change to a replicated text.
 *
 * **Details**
 *
 * - `before` and `after`: the text without and with the change.
 * - `patch`: how the change turns `before` into `after`.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface TextProposal {
  readonly _tag: "Text"
  readonly document: Replica.DocumentId
  readonly user: string
  readonly patch: TextPatch.TextPatch
  readonly before: string
  readonly after: string
}

/**
 * A change to a document. An authority has no schema, so the document is
 * JSON; `ReplicatedDocument.readProposal` reads it with the app's, and rejects
 * a change that doesn't fit.
 *
 * **Details**
 *
 * - `before` and `after`: the document without and with the change, as JSON.
 *   Only the parts that have been written are there, and removed parts
 *   aren't. Texts are strings, counters are the sum of their increments, and
 *   values are their latest write, as their type's JSON. A part added with
 *   nothing in it yet, such as an empty text in a record, is `{}`.
 * - `exists`: the paths in `before` and in `after` whose latest write says
 *   they exist: record entries, list items and optional fields that were
 *   added, and every value, which writing says exists. JSON can't say this
 *   alone: an entry nobody added, but that something was written inside, has
 *   JSON without existing.
 * - `paths`: where the change wrote.
 * - `delta`: the change itself.
 *
 * A list is an array of the items that show, in order, but in `exists` and
 * `paths` an item's path ends with its key, not its index: its key field, or
 * for a list of values, a key made from the id of the write that added it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface DocumentProposal {
  readonly _tag: "Document"
  readonly document: Replica.DocumentId
  readonly user: string
  readonly before: Schema.Json
  readonly after: Schema.Json
  readonly exists: { readonly before: ReadonlyArray<Replica.Path>; readonly after: ReadonlyArray<Replica.Path> }
  readonly paths: ReadonlyArray<Replica.Path>
  readonly delta: Delta.Delta
}

/**
 * The authority's rule turned a change down, for the reason in `message`. The
 * replica that made it rolls it back and shows `message`.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  message: Schema.String
}) {}

/**
 * An authority in this process, keeping documents in the `HistoryStore` and
 * accepting each change that `check` succeeds for.
 *
 * **Details**
 *
 * Changes are checked one at a time, in the order they were made. A push is
 * accepted up to the first change `check` rejects; that change and the rest of
 * the push are dropped, because each was made on top of the one before.
 *
 * Before `check`, the authority refuses changes it can tell are malformed, and
 * refuses a push whole if a change in it is stamped more than a day ahead of
 * its clock. It rejects, as `check` does, changes that would make a path of a
 * document hold two kinds of thing (a text and a value, say), or remove a
 * part of a document nobody added, so a `DocumentProposal` has each part
 * where replicas have it. `ReplicatedDocument.readProposal` rejects whatever
 * else doesn't fit the app's schema.
 *
 * **Gotchas**
 *
 * A check should reject the kind of proposal it doesn't expect, such as a
 * `Document` one for an app of texts. The first change to a new document
 * decides which kind of thing its root holds, so a client could otherwise
 * make a text's document hold fields, and every text change would be
 * rejected.
 *
 * **Example** (Only editors may edit)
 *
 * ```ts import.meta.vitest
 * import { Effect, Layer } from "effect"
 * import { Authority, HistoryStore } from "effect-delta"
 *
 * const EditorsOnly = Authority.layer({
 *   check: ({ _tag, user }) =>
 *     _tag !== "Text"
 *       ? Effect.fail(new Authority.Rejected({ message: "Specs are texts" }))
 *       : user.startsWith("editor:")
 *       ? Effect.void
 *       : Effect.fail(new Authority.Rejected({ message: "Reviewers can comment but not edit" }))
 * }).pipe(Layer.provide(HistoryStore.layerMemory))
 * ```
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layer = <R = never>(options: {
  readonly check: (proposal: Proposal) => Effect.Effect<void, Rejected, R>
}): Layer.Layer<Authority, never, HistoryStore.HistoryStore | R> =>
  Layer.effect(Authority)(Effect.gen(function*() {
    const context = yield* Effect.context<R>()
    return yield* InternalServer.make({
      check: (proposal) => Effect.provideContext(options.check(proposal), context)
    })
  }))

/**
 * An authority on a server, reached over `Sync.Rpcs`. Provide an
 * `RpcClient.Protocol`, such as `RpcClient.layerProtocolSocket`.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerRemote: Layer.Layer<Authority, never, RpcClient.Protocol> = Layer.effect(Authority)(
  InternalServer.remote
)

/**
 * Serves the `Authority` over `Sync.Rpcs`. Each request is identified and
 * authorized with `Sync.Authorization`, its user is the `user` that `check`
 * sees, and a push may only carry changes under replica ids that belong to its
 * user.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerHandlers: Layer.Layer<
  Rpc.ToHandler<RpcGroup.Rpcs<typeof Sync.Rpcs>>,
  never,
  Authority | Sync.Authorization
> = InternalServer.layerHandlers(Authority)
