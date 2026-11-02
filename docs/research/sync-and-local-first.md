# Sync protocols, server roles, and local-first systems

Phase 1 research notes (track: sync and local-first), gathered 2026-10-15 from
docs, specs, papers, and source. Anything I could not confirm is marked
**(unverified)**. Effect's `eventlog` was read from the local clone. LiveStore
was read at commit `505332f9`.

## 1. Overview

Every system here answers the same three questions differently:

| Question | Answers |
|---|---|
| How does a replica summarize what it has? | *Version vector* (`replica → counter`): Yjs, Loro, Jazz. *Hash-DAG heads* + Bloom filters: Automerge, Kleppmann & Howard's Byzantine Eventual Consistency paper (BEC). *Server-assigned cursor*: eventlog, LiveStore, Electric, Replicache. *Set-reconciliation fingerprints*: Evolu (range-based set reconciliation, RBSR), Subduction. |
| Who orders writes? | Nobody (CRDTs commute). A server that sequences but doesn't transform: LiveStore, eventlog, prosemirror-collab. A server that transforms or re-executes: ShareDB/Jupiter OT, Replicache/Zero, Liveblocks. |
| What does a server store? | Nothing (relay). A compacted snapshot: y-sweet, Hocuspocus. Snapshot + log: Automerge Repo, y-partykit. Full op history: ShareDB, LiveStore. |

The main finding for us: **CRDT updates commute, so the server can be just a
"cloud peer".** The local-first essay recommends this, and Automerge Repo's
sync server already works that way. One symmetric protocol can then serve both
peer-to-peer and client–server. Presence, auth, and multiplexing sit beside
the document sync as separate channels.

## 2. Glossary

| Term | Meaning | Systems using it | Clashes |
|---|---|---|---|
| **peer** | A party to a sync connection | Loro, Automerge Repo, Subduction, Ditto | Loro `PeerID` is a 64-bit *op author* per doc instance. Automerge Repo `PeerId` is a *network endpoint* (one per Repo). Subduction's is an Ed25519 *public key*. Ditto has device "Small Peers" and server "Big Peer". |
| **replica** | A copy of a document that accepts writes and converges | CRDT literature, BEC, eventlog ("remote replicas") | Mostly consistent |
| **site** | Author identifier | Classic OT/CRDT papers (WOOT, Logoot) | Academic only; means the same as Yjs "client" |
| **actor** | Author identifier | Automerge `actorId` (random bytes) | Liveblocks "actor" is a *connection* number (`targetActor`) |
| **client** | Author ID, or a non-server participant | Yjs `clientID` (random per `Y.Doc` instance); Replicache/Zero `clientID` (per tab) plus `clientGroupID` (per profile); LiveStore `clientId` (per profile, shared by tabs) | A Yjs "client" is roughly a LiveStore "session" |
| **session** | One running instance or connection | Jazz `SessionID` (account × device, with its own signed log); Automerge Repo `SessionId` (random per startup, used for ephemeral dedup); LiveStore `sessionId` (per tab); Hocuspocus `sessionAwareness`; eventlog (an authenticated RPC session) | Four distinct meanings |
| **document** | The unit of history and convergence | Yjs `Y.Doc`, `Automerge.Doc`, `LoroDoc`; Jazz *CoValue*; DXOS *space* (contains objects); Evolu *owner*; eventlog and LiveStore *store* | "store" also means the storage backend (Effect `KeyValueStore`) |
| **doc id** | Address of a document | Automerge `automerge:<bs58check(UUID)>`; Jazz `co_z<base58(BLAKE3(header))>` (content-addressed); y-sweet `[A-Za-z0-9_-]` names; eventlog `StoreId` (branded string) | Random vs content-addressed vs user-chosen |
| **room** | A server-side unit of fan-out, presence, and auth | Liveblocks, PartyKit (one Durable Object per room), Loro Protocol (`(crdtType, roomId)`), y-websocket (URL path) | Usually 1:1 with a doc. A Liveblocks room holds storage, a Yjs doc, *and* presence |
| **provider** | A connector that binds a doc to the network or storage | Yjs (`y-websocket`, `y-indexeddb`, `HocuspocusProvider`); LiveStore "sync provider" (a backend implementation) | React `Provider`; Effect's `provide` |
| **adapter** | A pluggable I/O implementation | Automerge Repo `NetworkAdapter`/`StorageAdapter`; LiveStore platform adapters (`adapter-web`); Loro "adaptors" (CRDT-type plugins) | Platform vs I/O vs data type |
| **network adapter** | Moves messages between peers | Automerge Repo (`connect`/`send`/`disconnect`; `peer-candidate` events) | — |
| **storage adapter** | Persists chunks by key | Automerge Repo (`load`/`save`/`remove`/`loadRange`/`removeRange`); y-sweet (`get`/`set`/`remove`/`exists`) | — |
| **sync state** | Per-peer protocol bookkeeping | Automerge `SyncState` (`sharedHeads`, `theirHeads`, `theirNeed`, `theirHave`, `sentHashes`, `inFlight`); LiveStore `SyncState` (`pending`, `upstreamHead`, `localHead`) | y-websocket `synced` and Hocuspocus `SyncStatus` are UI flags or acks |
| **handshake** | The opening exchange | Yjs `SyncStep1`/`SyncStep2` (data); Loro `JoinRequest`/`JoinResponseOk` (versions plus permissions); eventlog `Hello`/`Authenticate` (auth only); Automerge Repo `join`/`peer` (protocol version) | Data vs auth vs transport negotiation |
| **heads** | Hashes of DAG leaves | Automerge, BEC, Git | LiveStore "head" is a *sequence number* |
| **frontier** | Op IDs of DAG leaves | Loro `Frontiers`; Braid "frontier of time" | Same idea as heads, but keyed by op ID rather than hash. Loro warns that frontiers can't be interpreted without the history |
| **state / version vector** | `author → count` | Yjs state vector; Loro `VersionVector`; Jazz "known state" (`session → txCount`) | Same concept under three names |
| **snapshot** | Some persisted state | Loro (full doc; "shallow" = trimmed history); Automerge `save()`; Secsync (encrypted state made by a client); Braid (full body, as opposed to a patch); y-partykit (a persistence mode) | **Yjs `Y.snapshot` is a version marker (state vector plus delete set) with no content** |
| **checkpoint** | A durable position | y-sweet (periodic flush, every 10 s); Loro docs (saved frontiers) | Persistence timing vs a history bookmark |
| **compaction** | Shrinking stored history | Automerge Repo (merges incremental chunks into a snapshot; lossless); eventlog `groupCompaction` (*semantic*: rewrites entries per primary key); Loro shallow snapshot (truncation); Yjs GC | Lossless storage merge vs lossy history rewrite |
| **presence** | Who is here and their ephemeral state | Liveblocks (per-connection JSON); Jazz (recommends CoFeeds) | **Ditto "presence" means mesh topology** |
| **awareness** | An ephemeral per-client state CRDT | Yjs awareness (`clock`, `null` = offline, 30 s timeout); Hocuspocus; Loro `EphemeralStore`; Automerge Repo "ephemeral" messages (stateless broadcast) | Liveblocks calls this "presence" |
| **cursor / cookie** | A resumable position in a server stream | Replicache cookie; Electric `offset`+`handle`; LiveStore pull cursor; eventlog `startSequence` | Text-editor cursors (Loro `Cursor` is a stable text position) |

## 3. Sync protocols

| Protocol | "What I have" | Handshake | Steady state | Reconnect | Missing deps | Idempotency |
|---|---|---|---|---|---|---|
| Yjs | State vector | `SyncStep1(sv)` → `SyncStep2(diff)` | `Update` frames | Re-run handshake (stateless) | Held in `store.pendingStructs` / `pendingDs` | Updates commute, associate, and are idempotent |
| Automerge | Heads + Bloom filter of changes since last sync | `generateSyncMessage`/`receiveSyncMessage` loop until both return null | Same loop; `in_flight` prevents duplicates | Encoded `SyncState` keeps only `sharedHeads` | `need` hashes; dependents of Bloom negatives are sent along | Content-addressed changes |
| Loro | Version vector (and frontiers) | Exchange `oplogVersion()`; `export({mode:"update", from})` | Broadcast `subscribeLocalUpdates` | Re-exchange version vectors | `ImportStatus.pending` ranges; fetch and re-import | Ops keyed by `(peer, counter)` |
| BEC hash graph | Heads + old heads + Bloom | Send heads | Eager `msgs` plus periodic reconciliation | `LoadHeads(peer)` | `needs` rounds | Hashes; badly signed messages are dropped |
| Braid-HTTP | `Version`/`Parents` DAG | `GET` + `Subscribe` | `209` stream of patches or snapshots | `GET` + `Subscribe` + `Parents` | `410 Gone`, `432 Version Not Found` | `PUT` with `Version` + `Parents` |
| effect/eventlog | Per-remote sequence + uncommitted set | `Hello` → challenge → `Authenticate` | `Changes` stream; `Write*` | Resume from `nextRemoteSequence` | N/A (server total order) | Dedup by UUIDv7 entry ID |
| LiveStore | Global sequence number | `pull(cursor)` | `pull({live:true})`, batched `push` | Pull from the persisted cursor | Rejected as `ServerAheadError`; the client rebases | Echo confirms pending events |

**Yjs** ([y-protocols PROTOCOL.md](https://github.com/yjs/y-protocols/blob/master/PROTOCOL.md)).
Peer-to-peer, both sides send `SyncStep1`. Client–server, the client starts
and the server replies with `SyncStep2` and then its own `SyncStep1` ("SHOULD
NOT initiate"; y-sweet does anyway). Sync and awareness are sub-protocols
behind a leading `varUint`.

**Automerge** ([sync.rs](https://github.com/automerge/automerge/blob/main/rust/automerge/src/sync.rs)).
Built on the BEC paper, assuming "a reliable in-order stream between two
peers". A message is `{heads, need, have: [{lastSync, bloom}], changes}` plus
capabilities; `MessageV2` can carry a compressed whole document for first
sync. Sync state is per peer, per doc; Automerge Repo persists it under
`[docId, "sync-state", storageId]`.

**Loro** ([sync docs](https://github.com/loro-dev/loro-docs/blob/main/pages/docs/tutorial/sync.mdx)).
Two messages are enough. The **Loro Protocol**
([protocol.md](https://github.com/loro-dev/protocol/blob/main/protocol.md)) is
the clearest multiplexing design:
- Frames are `magic(4) • roomId • type • payload`, with magics `%LOR`, `%YJS`,
  `%YAW`, `%EPH`, `%EPS`, `%FLO`.
- `JoinRequest(auth, version)` → `JoinResponseOk("read"|"write", version)` or
  `JoinError(auth_failed|version_unknown)`.
- `DocUpdate(updates[], batchId)` → `Ack(batchId, ok|permission_denied|invalid_update|payload_too_large|rate_limited)`.
- 256 KB frame cap with fragments (10 s reassembly timeout);
  `RoomError(rejoin_suggested|evicted)`; `Leave`. Text `ping`/`pong` is
  connection-scoped. No E2EE.

**BEC** ([arXiv 2012.00472](https://arxiv.org/abs/2012.00472)). Messages
contain their predecessors' hashes. Replicas exchange heads and request
unknown hashes until they reach common ancestors, which costs up to DAG-depth
round trips. The optimization stores each peer's heads from the last run
(`StoreHeads`) and sends a Bloom filter of everything since. The receiver
returns Bloom negatives *and their successors*, then falls back to `needs`
rounds for false positives. A correct replica "will not deliver messages
without a complete predecessor graph", which gives Sybil immunity for
invariants preserved under any merge (I-confluent).

**Braid-HTTP** ([draft-04](https://datatracker.ietf.org/doc/draft-toomim-httpbis-braid-http/),
split out into [versions-04](https://datatracker.ietf.org/doc/draft-toomim-httpbis-versions/);
both expired). Four HTTP extensions: versioning, patches, subscriptions,
merge types. `Current-Version` signals that the client has caught up;
`Version-Type: peer-counter; text-runs` run-length-encodes text history.
"Peers can drop history at any time."

**Other reconciliation shapes.** Jazz: `load`/`known`/`content`/`done` with
per-session `{after: N, newTransactions}`. Evolu: RBSR (`Skip | Fingerprint |
Timestamps` ranges) over encrypted messages. Subduction/Sedimentree: commits
in hash-depth strata, compared as compressed fragments.

## 4. Server roles

| Role | Examples | Stores | Merges? | Orders? |
|---|---|---|---|---|
| Pure relay | Evolu relay, eventlog encrypted server, Secsync, Keyhive `Relay` access | Ciphertext and clear metadata | No | No (Secsync numbers updates per doc) |
| Relay + persistence | y-sweet, Hocuspocus, y-partykit, Automerge Repo sync server | Snapshot, or snapshot + log | Yes (plaintext CRDT) | No |
| Authoritative sequencer | LiveStore (compare-and-append), prosemirror-collab, eventlog plaintext server | Ordered log | No transform | Yes |
| Authoritative transformer or executor | ShareDB/Jupiter/Wave OT, Replicache, Zero, Liveblocks, Jazz 2 "fate authority", Triplit (unverified) | Canonical state | Yes | Yes |

**Relay + persistence:**
- **y-sweet** (Jamsocket, now part of Modal): backend-signed doc tokens
  (`full`/`read-only`, 60 min), one socket per doc, and one merged object per
  doc (`{docId}/data.ysweet`) written at most every 10 s. Message `102` echoes
  a version counter, which doubles as a save ack and a heartbeat.
- **Hocuspocus** v4: **every frame is prefixed with the document name**, so
  docs share one socket and auth is per doc. Messages sent before auth are
  queued. Stores are debounced (2 s, 10 s max) and write a full snapshot.
  Redis pub/sub links instances, and a Redlock picks the single writer.
- **PartyKit / Durable Objects**: Cloudflare advises "one Durable Object per
  logical unit… a document". Objects are single-threaded with strongly
  consistent storage. y-partykit `snapshot` mode merges the log when the last
  client leaves; `history` mode keeps up to 10 MB of log, then snapshots.
  Hibernation keeps awareness IDs in WebSocket attachments.
- **Automerge Repo sync server**: about 60 lines. It is a Repo with a WebSocket
  adapter, filesystem storage, and `sharePolicy: () => false` (it never
  announces docs). It is just another peer; its `storageId` keys clients'
  per-server sync state.

**Authoritative variants:**
- **(a) The server transforms.** ShareDB clients send `{v, src, seq, op}`;
  the server transforms against ops since `v`, and a duplicate `src+seq`
  gets back an ack.
- **(b) The server rejects stale writes and the client rebases.** In
  prosemirror-collab, "the server never transforms"; the client confirms its
  own steps by `clientID` and rebases the rest. LiveStore has the same shape.
- **(c) The server re-executes mutators; the client rewinds and replays.**
  Replicache bumps a per-client `lastMutationID` in the same transaction as
  the mutation. Pull returns `{cookie, lastMutationIDChanges, patch}`. A
  Replicache "poke" is an empty hint; a Zero poke carries the data.
- **(d) The server applies in arrival order and corrects.** Liveblocks:
  "That order is the authority." Ops (`connectionId:opClock`) come back
  `accepted`, `ignored`, `rejected`, or `rectified`, and unacked ops are
  re-sent on reconnect.

## 5. Local-first systems

**Ink & Switch, "Local-first software" (2019)** — the 7 ideals:
1. No spinners.
2. Your work is not trapped on one device.
3. The network is optional.
4. Seamless collaboration.
5. The Long Now.
6. Security and privacy by default.
7. You retain ultimate ownership and control.

Its findings: "CRDT technology works." Growing history is the main open
problem. "Using a server for communication is fine." Servers act as "cloud
peers… not the source of truth". Access control beyond secret URLs is open
research.

| System | Data model | Sync shape | Server role | Identity / auth | E2EE |
|---|---|---|---|---|---|
| Jazz Classic | CoValues (CoMap, CoList, CoFeed, CoPlainText…), with a signed log per session | Known-state version vectors | Cloud peer (store and forward) | Accounts and Groups; roles reader/writer/manager/admin/writeOnly, enforced cryptographically by every peer | Yes: per-Group read key, rotated when a member is removed |
| Jazz 2.0 (2026, alpha) | Relational rows, each with a version DAG | Server-ordered | "Fate authority" orders writes | Row-level security | Opt-in per field |
| ElectricSQL 1.x | Postgres "Shapes" | HTTP `offset`/`handle`, long-poll or SSE; CDN-cacheable | Read path only; the app owns writes | App's own | No |
| Triplit | Collections, hybrid-logical-clock (HLC) timestamps per attribute | Query subscriptions over WebSocket (`CONNECT_QUERY`) | Re-stamps writes (unverified) | JWT roles; permissions enforced only on the server | No (unverified) |
| Evolu | SQLite, last-writer-wins per cell, HLC | RBSR | Blind relay | Owner mnemonic → (OwnerId, encryption key, write key) | Yes |
| DXOS | ECHO objects in spaces (Automerge inside; Effect Schema for schemas) | Automerge/Subduction over WebRTC (MESH) | EDGE agents act as headless devices | HALO credential chains | Secure channels (unverified beyond that) |
| Ditto | CRDT map documents, delta-state with diffs computed on demand | Query subscriptions; multi-hop Bluetooth LE/LAN/P2P Wi-Fi mesh | Ditto Server is just another peer | X.509 certificates; per-peer DQL permissions | TLS only (unverified) |

**LiveStore** (Effect 4, v0.5) is event sourcing on SQLite.
- **Events and state.** `Events.synced`/`Events.clientOnly` carry Schema
  `args`. Pure *materializers* (a dev-mode hash check catches impure ones)
  project events into a state DB, which is rematerialized from the eventlog
  when the schema changes.
- **Ordering.** Sequence numbers are `e{global}.{client}r{rebaseGeneration}`.
  **The backend accepts a push only if its first `parentSeqNum` equals the
  head**; otherwise it returns `ServerAheadError` (compare-and-append). The
  client then rebases with a pure `SyncState.merge` over
  `{pending, upstreamHead, localHead}`, rolling back with SQLite session
  changesets.
- **Provider interface.**
  ```ts
  type SyncBackend<M> = {
    connect: Effect<void, IsOfflineError | UnknownError, Scope>
    pull: (cursor: Option<{ eventSequenceNumber; metadata: Option<M> }>, options?: { live?: boolean })
      => Stream<{ batch; pageInfo: MoreUnknown | MoreKnown | NoMore }, ...>
    push: (batch: ReadonlyArray<Event.Global.Encoded>)
      => Effect<void, IsOfflineError | BackendIdMismatchError | UnknownError | ServerAheadError>
    ping; isConnected: SubscriptionRef<boolean>; metadata; supports: { pullPageInfoKnown; pullLive }
  }
  ```
- **Multi-tab.** A Web Lock elects a leader worker and a SharedWorker proxies
  every tab to it. Each tab keeps an in-memory SQLite copy for synchronous
  reads.
- **Batching.** Pushes are batched (10 local, 50 to the backend) with
  exponential retry capped at 30 s. Both queues are **unbounded**.
- **Gaps:** no eventlog compaction, no presence, no collaborative text (the
  docs say to pair it with Yjs, Automerge, or Loro), and no peer-to-peer. The
  S2 provider has no fencing, so two clients can fork the history.
  `clientId`/`sessionId` are `nanoid(5)` and collisions aren't handled.

**effect/eventlog** (`packages/effect/src/eventlog/`).
- **Data model.** An `Event` (tag, `primaryKey(payload)`, Schema payload) is
  stored as `Entry {id: UUIDv7, event, primaryKey, payload}`. The handler
  runs first; the entry commits only if it succeeds.
- **Protocol.** `EventLogRemoteRpcs` over `effect/rpc`:
  `Hello → {remoteId, challenge}`; `Authenticate` (Ed25519 signature);
  `WriteSingle`/`WriteChunked` (512 KB chunks); and
  `Changes {publicKey, storeId, startSequence}`, a stream of
  `Single | Chunked` frames. Errors: `Unauthorized`, `Forbidden`, `NotFound`,
  `InvalidRequest`, `InternalServerError`.
- **Bookkeeping and client loop.** Per `RemoteId`, a sequence cursor and an
  "uncommitted" set. Incoming changes are batched with `Queue.takeAll`, push
  triggers coalesced through `Queue.dropping(1)`, and retries use
  `exponential(200ms, 1.5)` capped at 10 s.
- **Conflicts.** Same `(event, primaryKey)` with a later timestamp →
  handed to the handler as `conflicts`; resolution is up to the app.
- **Compaction.** `groupCompaction` rewrites entries per primary key. The
  server's compacted backlog uses "representative sequences" so client
  cursors still advance.
- **Authorization.** `authorizeIdentity/Write/Read` is per operation
  (`authorizeWrite` sees the entries). `StoreMapping` maps
  `(publicKey, storeId)` to a server store.
- **Encryption.** SHA-256 over the identity's 32-byte root secret derives an
  AES-GCM key and an Ed25519 seed. The encrypted server stores opaque entries
  and can't run handlers.
- **Topology.** Strictly client → server, with no causal metadata.

## 6. Cross-cutting concerns

**Persistence and compaction.** Three approaches: one merged snapshot
(y-sweet, Hocuspocus); snapshot plus incremental chunks with threshold
compaction (Automerge Repo keys `[docId,"snapshot",headsHash]` and
`[docId,"incremental",sha256]`, compacting once the incrementals are at least
as large as the snapshot); or a log kept until the last client leaves or a
size cap is hit (y-partykit). Truncating history (Loro shallow snapshots,
Braid `410`) breaks sync for peers older than the cut — the essay's "six
months away" problem.

**Presence and awareness.**
- y-awareness entries are `(clientID, clock, state|null)`, applied only when
  `clock` is higher. Clients re-broadcast every 15 s, are dropped after 30 s,
  and broadcast `null` before leaving. **It is not authenticated.**
- Heartbeats exist at three levels: transport (y-sweet pings every 20 s with
  a 40 s timeout; Hocuspocus v4 `Ping`/`Pong`; Loro text `ping`), application
  (the y-sweet `102` echo), and presence TTL (Electric's y-durable-streams,
  15 s).
- Automerge Repo `ephemeral` messages are deduplicated by
  `(sessionId, count)`. Subduction runs ephemeral traffic as its own signed
  protocol.

**Auth and access control.**
- *Per document:* y-sweet/Liveblocks/Loro tokens with read vs write, granted
  at join.
- *Per operation:* eventlog `authorizeWrite(entries)`, Zero/Replicache
  mutator endpoints, Liveblocks `rejected`.
- *Capabilities:*
  - Automerge Repo is "know the URL".
  - **UCAN 1.0** (DAG-CBOR) delegations `{iss, aud, sub, cmd, pol, exp}` that
    may only attenuate.
  - **Keyhive** "convergent capabilities": group membership is a CRDT, with
    access levels `Relay < Read < Edit < Admin`. **BeeKEM** provides key
    agreement that needs only causal order.
  - Jazz enforces Group roles on every peer by replaying the group log.

**End-to-end encryption.** A blind relay can store and forward, reconcile on
clear metadata (Evolu timestamps, Jazz session counters, Secsync clocks,
Sedimentree hashes), check write keys or signatures, and fan out presence. It
**cannot merge, validate, index, or compact**. So:
- Clients make the snapshots (Secsync, the DXOS Epoch `snapshotCid`).
- Snapshots need signature or parent-hash chains (Snapdoc).
- Removing a member means rotating keys, and data already delivered stays
  readable to them.
- eventlog derives one key per *identity*, so there are no group keys.
  Sharing a store means sharing the identity.

**Schema evolution.** Cambria (2020) uses bidirectional lenses (`rename`,
`hoist`, `wrap`, `convert`…) composed along the shortest path in a graph of
schema versions. Writes are stored in the writer's schema and translated at
read time, and the lenses live *in the document*. Later variations: Jazz 2
migration lenses; Evolu's grow-only schema with a quarantine table for
unknown changes; LiveStore keeps unknown events and rematerializes later.

**Offline queues, backpressure, and batching.** Unconfirmed work sits in an
outbox (Triplit), unacked ops (Liveblocks), `pendingOps` with one in flight
(ShareDB), or `unconfirmed` steps (prosemirror-collab). Frame caps: Loro
256 KB, eventlog 512 KB chunks, LiveStore 900 KB / 100 events. Loro acks per
`batchId`. Hocuspocus caps its pre-auth queue. No system I read applies real
backpressure; most queues are unbounded.

**Multiplexing.** A doc-name or room-ID prefix on every frame (Hocuspocus,
Loro, Automerge Repo `documentId`). y-sweet, Liveblocks, and y-websocket use
one socket per doc or room. Keepalive should be scoped to the connection, not
the room (Loro).

**Identity.** Author IDs are random per session (Yjs `clientID`, Loro
`PeerID`). Loro warns never to use user IDs; reusing a peer ID means
reloading its state first and locking it across tabs, or `(peer, counter)`
collides. Doc IDs are random UUIDs (Automerge), content hashes (Jazz), or
names (y-sweet, Hocuspocus). Servers identify themselves too: eventlog
`RemoteId`, Automerge `storageId`, LiveStore `backendId` (a mismatch forces a
reset).

## 7. Relevance to us

**Protocol shape.** Use a **symmetric, per-document, version-vector protocol**
in the Yjs/Loro family, run as a pure state machine.
- `Hello{docId, version, capabilities}` on both sides →
  `Updates{since: theirVersion}` → a steady stream of `Update{batchId, …}` →
  optional `Ack{batchId, status}`.
- Peer-to-peer means both sides speak first. Client–server is the same
  protocol, with the server as a peer that persists.
- Updates with missing dependencies wait in a pending buffer (Yjs/Loro) and
  trigger a re-request by version.
- Persist each `(doc, remote)` peer's last-known version to speed up
  resumption (BEC `StoreHeads`, Automerge Repo sync state).
- Hash heads plus Bloom filters are the fallback if we need integrity against
  untrusted peers.
- Server-assigned cursors (eventlog, Electric) could be an optional
  *optimization* for catch-up from a server, not the source of truth.
- Keep `(docId, replicaId, counter range, deps)` in clear in the update
  envelope, so a blind E2EE relay can still answer "since version V".

**Borrow from eventlog:** Schema-defined messages with `SchemaBinary`;
`RpcGroup` as one transport binding; chunked frames; the
`Hello`/challenge/Ed25519 handshake as *optional* middleware; `RemoteId` with
per-remote cursor and "uncommitted" bookkeeping; the retry `Schedule` and
coalescing push trigger; the identity/read/write authorization split; the
encrypted-server variant. Caveat: eventlog is client → server, server-ordered,
and last-writer-wins by primary key. As it stands it is not a CRDT sync
protocol.

**Borrow from LiveStore:** the provider shape (`pull` as a `Stream` with
`live` and `pageInfo`, `push` with typed errors, `isConnected:
SubscriptionRef`, `supports` flags, opaque cursor `metadata`); `backendId`
mismatch detection; a pure, Schema-encoded merge kernel kept apart from the
effectful processors; Web Lock + SharedWorker leader election for multi-tab.
**Don't copy:** compare-and-append ordering and rebase (commuting updates
make them unnecessary), unbounded queues, forked RPC internals.

**Server.** Relay plus persistence by default, as "just another replica plus a
storage adapter" (the Automerge Repo pattern): one room per doc ID (the
Durable Object model); many docs multiplexed over one connection by doc-ID
prefix; snapshot plus incremental log with threshold compaction on
`KeyValueStore`/`Sql`; presence as a separate ephemeral channel with
y-awareness semantics (clock, `null` = offline, 15 s / 30 s), keyed by
`(docId, sessionId)`.

**Open questions:**
1. Layer on eventlog (updates as `Entry`s) or define a sibling protocol that
   reuses its pieces?
2. Version vector vs hash heads: is Byzantine integrity in scope?
3. Replica IDs: random per session (Yjs) vs a persisted, locked per-device ID
   (Loro's caveat), and the multi-tab leader question.
4. Auth model: doc tokens with read/write at join, per-operation checks, or
   capability chains (UCAN/Keyhive)? How does eventlog's `Identity` map onto
   this?
5. E2EE: which metadata stays clear, and which client compacts?
6. Should a room ever hold more than one doc plus presence (Liveblocks)?
7. Do document schemas evolve through lenses or grow-only?
8. Bounded queues and frame limits: what defaults, and is there an ack-based
   flow-control window?

## 8. Sources

- Yjs: https://github.com/yjs/y-protocols/blob/master/PROTOCOL.md ; https://docs.yjs.dev/api/document-updates ; https://github.com/yjs/yjs/blob/main/src/utils/encoding.js ; https://github.com/yjs/y-websocket
- Automerge: https://github.com/automerge/automerge/blob/main/rust/automerge/src/sync.rs ; https://automerge.org/automerge/src/automerge/sync/state.rs.html ; https://github.com/automerge/automerge-repo ; https://github.com/automerge/automerge-repo-sync-server
- Loro: https://github.com/loro-dev/loro-docs (pages/docs/tutorial/sync.mdx, concepts/{import_status,frontiers,peerid_management,shallow_snapshots}.mdx) ; https://github.com/loro-dev/protocol/blob/main/protocol.md
- BEC: https://arxiv.org/abs/2012.00472
- Braid: https://datatracker.ietf.org/doc/draft-toomim-httpbis-braid-http/ ; https://www.ietf.org/archive/id/draft-toomim-httpbis-braid-http-04.txt ; https://www.ietf.org/archive/id/draft-toomim-httpbis-versions-04.txt
- Servers: https://github.com/jamsocket/y-sweet ; https://modal.com/blog/jamsocket-is-joining-modal ; https://github.com/ueberdosis/hocuspocus ; https://liveblocks.io/docs/guides/how-conflict-resolution-works-in-liveblocks-sync ; https://liveblocks.io/docs/authentication ; https://github.com/liveblocks/liveblocks ; https://docs.partykit.io/reference/y-partykit-api/ ; https://github.com/cloudflare/partykit ; https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/ ; https://developers.cloudflare.com/durable-objects/best-practices/websockets/
- Authoritative: https://doc.replicache.dev/concepts/how-it-works ; https://doc.replicache.dev/reference/server-push ; https://doc.replicache.dev/reference/server-pull ; https://zero.rocicorp.dev/docs/mutators ; https://github.com/rocicorp/mono ; https://github.com/ProseMirror/prosemirror-collab ; https://marijnhaverbeke.nl/blog/collaborative-editing.html ; https://github.com/share/sharedb ; https://svn.apache.org/repos/asf/incubator/wave/whitepapers/operational-transform/operational-transform.html
- Local-first: https://www.inkandswitch.com/essay/local-first/ ; https://github.com/garden-co/classic-jazz ; https://github.com/garden-co/jazz ; https://electric.ax/docs/api/http ; https://electric.ax/blog/2024/07/17/electric-next ; https://github.com/durable-streams/durable-streams ; https://github.com/aspen-cloud/triplit ; https://www.evolu.dev/docs/privacy ; https://www.evolu.dev/blog/scaling-local-first-software ; https://github.com/dxos/dxos ; https://docs.ditto.live/key-concepts/syncing-data ; https://www.ditto.com/blog/dittos-delta-state-crdts
- LiveStore: https://github.com/livestorejs/livestore ; https://github.com/livestorejs/livestore-contrib ; https://docs.livestore.dev/building-with-livestore/syncing/ ; https://docs.livestore.dev/patterns/presence/ ; https://docs.livestore.dev/patterns/rich-text-editing/
- Effect eventlog: local clone `effect-ts/effect/packages/effect/src/eventlog/` (EventLog.ts, EventJournal.ts, EventLogMessage.ts, EventLogRemote.ts, EventLogServer*.ts, EventLogEncryption.ts)
- Access control, E2EE, schema: https://www.inkandswitch.com/keyhive/notebook/ ; https://github.com/inkandswitch/keyhive ; https://github.com/inkandswitch/subduction ; https://eprint.iacr.org/2026/1434.pdf ; https://github.com/ucan-wg/spec ; https://github.com/ucan-wg/delegation ; https://github.com/nikgraf/secsync ; https://www.inkandswitch.com/cambria/ ; https://github.com/inkandswitch/cambria-project
