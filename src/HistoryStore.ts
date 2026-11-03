/**
 * A history store is where replicas and servers keep each room's history
 * between sessions. `persist` connects a replica to it: it loads what was saved
 * and then saves every change for as long as its scope lasts.
 *
 * The store itself only appends deltas and remembers a few small records, so
 * it's easy to back with anything. For the replay strategy it also keeps a
 * snapshot of the text, so reopening one reads the text instead of
 * replaying its history. `layerMemory` keeps everything in memory;
 * `layerKeyValueStore` uses Effect's `KeyValueStore`, which has file system,
 * Web Storage and SQL implementations.
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as KeyValueStore from "effect/persistence/KeyValueStore"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Delta from "./Delta.ts"
import * as InternalReplica from "./internal/replica.ts"
import type * as Replica from "./Replica.ts"
import * as VersionVector from "./VersionVector.ts"

/**
 * What a store has kept for one room.
 *
 * **Details**
 *
 * - `history`: the changes, combined into one delta.
 * - `unconfirmed`: a confirmed replica's own changes that its authority hadn't
 *   acknowledged. Kept apart, so a rejected change never comes back on reload.
 * - `writers`: every replica id written under for this room here, and who
 *   it belongs to. Replicas use it to upload changes made in earlier sessions;
 *   servers use it to know whose edits are whose.
 * - `snapshot`: the text at a version, if one was saved.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Stored {
  readonly history: Delta.Delta
  readonly unconfirmed: Delta.Delta
  readonly writers: { readonly [replica: string]: string }
  readonly snapshot: Option.Option<Snapshot>
}

/**
 * The text at a version: what the changes in `version` make. Replicas that
 * can start from it (the replay strategy) don't replay those changes when
 * they load.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Snapshot {
  readonly version: VersionVector.VersionVector
  readonly text: string
}

/**
 * The store couldn't load or save.
 *
 * @stability unstable
 * @category errors
 * @since 0.1.0
 */
export class HistoryStoreError extends Schema.TaggedError<HistoryStoreError>()("HistoryStoreError", {
  message: Schema.String,
  cause: Schema.Defect()
}) {}

/**
 * Keeps each room's history. Append-only, apart from the unconfirmed
 * changes, which are replaced as a whole.
 *
 * **Gotchas**
 *
 * One writer per room per store: two replicas persisting the same room
 * to the same store at once isn't supported yet.
 *
 * @stability unstable
 * @category services
 * @since 0.1.0
 */
export class HistoryStore extends Context.Service<HistoryStore, {
  readonly load: (room: Replica.RoomId) => Effect.Effect<Option.Option<Stored>, HistoryStoreError>
  readonly append: (room: Replica.RoomId, delta: Delta.Delta) => Effect.Effect<void, HistoryStoreError>
  readonly setUnconfirmed: (room: Replica.RoomId, delta: Delta.Delta) => Effect.Effect<void, HistoryStoreError>
  readonly setSnapshot: (room: Replica.RoomId, snapshot: Snapshot) => Effect.Effect<void, HistoryStoreError>
  readonly addWriter: (
    room: Replica.RoomId,
    replica: Replica.ReplicaId,
    owner: string
  ) => Effect.Effect<void, HistoryStoreError>
}>()("effect-delta/HistoryStore") {}

/**
 * A history store in memory, lost when the process ends. For tests, and for
 * relays that only need to keep rooms while they're in use.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerMemory: Layer.Layer<HistoryStore> = Layer.sync(HistoryStore)(() => {
  const rooms = new Map<
    string,
    {
      history: Delta.Delta
      unconfirmed: Delta.Delta
      writers: Record<string, string>
      snapshot: Option.Option<Snapshot>
    }
  >()
  const entry = (room: string) => {
    let found = rooms.get(room)
    if (found === undefined) {
      found = { history: Delta.empty, unconfirmed: Delta.empty, writers: {}, snapshot: Option.none() }
      rooms.set(room, found)
    }
    return found
  }
  return HistoryStore.of({
    load: (room) =>
      Effect.sync(() => {
        const found = rooms.get(room)
        return found === undefined ? Option.none() : Option.some({ ...found, writers: { ...found.writers } })
      }),
    append: (room, delta) =>
      Effect.sync(() => {
        const found = entry(room)
        found.history = Delta.combine(found.history, delta)
      }),
    setUnconfirmed: (room, delta) =>
      Effect.sync(() => {
        entry(room).unconfirmed = delta
      }),
    setSnapshot: (room, snapshot) =>
      Effect.sync(() => {
        entry(room).snapshot = Option.some(snapshot)
      }),
    addWriter: (room, replica, owner) =>
      Effect.sync(() => {
        entry(room).writers[replica] = owner
      })
  })
})

/**
 * A history store on Effect's `KeyValueStore`, with every key under `prefix`.
 *
 * **Details**
 *
 * Appended deltas go under numbered keys, because `KeyValueStore` can't list
 * keys. Once there are `foldAfter` of them (default 64), the next append folds
 * them all into one record. Folding loses nothing. Values are JSON.
 *
 * @stability unstable
 * @category layers
 * @since 0.1.0
 */
export const layerKeyValueStore = (options: {
  readonly prefix: string
  readonly foldAfter?: number | undefined
}): Layer.Layer<HistoryStore, never, KeyValueStore.KeyValueStore> =>
  Layer.effect(HistoryStore)(Effect.gen(function*() {
    const store = yield* KeyValueStore.KeyValueStore
    const foldAfter = options.foldAfter ?? 64
    const key = (room: string, name: string) => `${options.prefix}/${encodeURIComponent(room)}/${name}`
    const fail = (message: string) => (cause: unknown) => new HistoryStoreError({ message, cause })

    const readJson = <A, I>(schema: Schema.Codec<A, I>, at: string) =>
      store.get(at).pipe(
        Effect.flatMap((json) =>
          json === undefined
            ? Effect.succeedNone
            : Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(json).pipe(Effect.map(Option.some))
        ),
        Effect.mapError(fail(`Couldn't read ${at}`))
      )
    const writeJson = <A, I>(schema: Schema.Codec<A, I>, at: string, value: A) =>
      Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
        Effect.flatMap((json) => store.set(at, json)),
        Effect.mapError(fail(`Couldn't write ${at}`))
      )

    const Count = Schema.Int
    const Writers = Schema.Record(Schema.String, Schema.String)
    const SnapshotRecord = Schema.Struct({
      format: Schema.Literal(1),
      version: VersionVector.VersionVector,
      text: Schema.String
    })

    const count = (room: string) => readJson(Count, key(room, "count")).pipe(Effect.map(Option.getOrElse(() => 0)))

    const readHistory = Effect.fnUntraced(function*(room: string) {
      const n = yield* count(room)
      let history = Delta.empty
      for (let i = 0; i < n; i++) {
        const delta = yield* readJson(Delta.Delta, key(room, `history/${i}`))
        if (Option.isSome(delta)) history = Delta.combine(history, delta.value)
      }
      return { n, history }
    })

    return HistoryStore.of({
      load: Effect.fnUntraced(function*(room) {
        const { history, n } = yield* readHistory(room)
        const unconfirmed = yield* readJson(Delta.Delta, key(room, "unconfirmed"))
        const writers = yield* readJson(Writers, key(room, "writers"))
        const snapshot = yield* readJson(SnapshotRecord, key(room, "snapshot"))
        if (n === 0 && Option.isNone(unconfirmed) && Option.isNone(writers)) return Option.none()
        return Option.some({
          history,
          unconfirmed: Option.getOrElse(unconfirmed, () => Delta.empty),
          writers: Option.getOrElse(writers, () => ({})),
          snapshot: Option.map(snapshot, ({ text, version }) => ({ version, text }))
        })
      }),
      append: Effect.fnUntraced(function*(room, delta) {
        const n = yield* count(room)
        if (n < foldAfter) {
          yield* writeJson(Delta.Delta, key(room, `history/${n}`), delta)
          yield* writeJson(Count, key(room, "count"), n + 1)
          return
        }
        // Fold everything into the first record, then drop the rest.
        const { history } = yield* readHistory(room)
        yield* writeJson(Delta.Delta, key(room, "history/0"), Delta.combine(history, delta))
        yield* writeJson(Count, key(room, "count"), 1)
        for (let i = 1; i < n; i++) {
          yield* store.remove(key(room, `history/${i}`)).pipe(Effect.mapError(fail("Couldn't fold history")))
        }
      }),
      setUnconfirmed: (room, delta) => writeJson(Delta.Delta, key(room, "unconfirmed"), delta),
      setSnapshot: (room, snapshot) => writeJson(SnapshotRecord, key(room, "snapshot"), { format: 1, ...snapshot }),
      addWriter: Effect.fnUntraced(function*(room, replica, owner) {
        const writers = yield* readJson(Writers, key(room, "writers"))
        const current = Option.getOrElse(writers, () => ({}) as Record<string, string>)
        if (current[replica] === owner) return
        yield* writeJson(Writers, key(room, "writers"), { ...current, [replica]: owner })
      })
    })
  }))

/**
 * A replica's saving to its history store.
 *
 * **Details**
 *
 * `await` fails if saving gives up. It never succeeds; saving lasts as long as
 * its scope.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Persisted {
  readonly await: Effect.Effect<never, HistoryStoreError>
}

/**
 * Loads what the store has for `room` into `replica`, then saves every
 * change the replica makes or receives, for the life of the scope.
 *
 * **Example** (Keeping a text across restarts)
 *
 * ```ts import.meta.vitest
 * import { HistoryStore, Replica, ReplicatedText } from "@jpowersdev/effect-delta"
 * import { Effect } from "effect"
 *
 * const notes = Replica.makeRoom("notes")
 * const open = (replica: string) =>
 *   Effect.gen(function*() {
 *     const text = yield* ReplicatedText.make({
 *       replica: Replica.makeId(replica)
 *     })
 *     yield* HistoryStore.persist(text, notes)
 *     return text
 *   })
 *
 * const program = Effect.gen(function*() {
 *   yield* Effect.scoped(Effect.gen(function*() {
 *     const first = yield* open("session-1")
 *     yield* ReplicatedText.insert(first, 0, "Saved")
 *   }))
 *   return yield* Effect.scoped(Effect.flatMap(open("session-2"), ReplicatedText.get))
 * })
 *
 * await Effect.runPromise(program.pipe(Effect.provide(HistoryStore.layerMemory))) // => "Saved"
 * ```
 *
 * @stability unstable
 * @category persistence
 * @since 0.1.0
 */
export const persist = Effect.fnUntraced(function*(
  replica: Replica.Replica<unknown>,
  room: Replica.RoomId
): Effect.fn.Return<Persisted, HistoryStoreError | Replica.DeltaError, HistoryStore | Scope.Scope> {
  const store = yield* HistoryStore
  const core = InternalReplica.core(replica)
  const stored = yield* store.load(room)
  if (Option.isSome(stored)) {
    yield* core.restore({
      history: stored.value.history,
      unconfirmed: stored.value.unconfirmed,
      writers: Object.keys(stored.value.writers) as Array<Replica.ReplicaId>,
      snapshot: Option.getOrUndefined(stored.value.snapshot)
    })
  }

  let saved = VersionVector.empty
  let savedWriters = new Set(Option.match(stored, { onNone: () => [], onSome: (s) => Object.keys(s.writers) }))
  // How much history the last snapshot covers, in characters.
  let snapshotAt = Option.match(Option.flatMap(stored, (s) => s.snapshot), {
    onNone: () => 0,
    onSome: (snapshot) => size(snapshot.version)
  })
  const save = (final: boolean) =>
    Effect.gen(function*() {
      const next = yield* core.toStore(saved)
      if (!Delta.isEmpty(next.history)) yield* store.append(room, next.history)
      if (core.confirmed !== undefined) yield* store.setUnconfirmed(room, next.unconfirmed)
      for (const writer of next.writers) {
        if (!savedWriters.has(writer)) yield* store.addWriter(room, writer, "")
      }
      savedWriters = new Set([...savedWriters, ...next.writers])
      saved = next.version
      // For the replay strategy: when the scope closes, and every so often in
      // between, so a reload has little to replay.
      const covered = size(next.version)
      if (replica.strategy === "replay" && covered > snapshotAt && (final || covered - snapshotAt >= snapshotEvery)) {
        yield* store.setSnapshot(room, { version: next.version, text: next.text })
        snapshotAt = covered
      }
    })

  // Subscribe before the first save, so no change slips between them.
  const changes = yield* core.changed
  yield* save(false)
  // Added before the fork, so it runs after the fork is interrupted: a last
  // save catches changes made just before the scope closed.
  yield* Effect.addFinalizer(() => Effect.ignore(save(true)))
  const fiber = yield* Effect.forkScoped(Stream.runForEach(changes, () => save(false)))
  return { await: Fiber.join(fiber).pipe(Effect.flatMap(() => Effect.never)) }
})

// Characters of history between snapshots.
const snapshotEvery = 4096

const size = (version: VersionVector.VersionVector): number =>
  Object.values(version).reduce((sum, seen) => sum + seen, 0)
