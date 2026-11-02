# Strategies

Phase 2 output: which ways of merging collaborative text we support, why, and what each needs. Terms are
from [`vocabulary.md`](./vocabulary.md). The research behind it is in [`research/`](./research), mainly
[`algorithms.md`](./research/algorithms.md).

**Status: decided** (2026-10-15): the three strategies, their names (tree, replay, authority), and the
scope. The shared decisions near the end are as recommended.

## What a strategy is

Two separate choices sit under the word:

1. **Merge strategy** (**tree** or **replay**): how concurrent edits to the same text combine. It decides
   what a change records (character ids or indexes), what a replica keeps in memory, and how the text is
   worked out from the history.
2. **Server role** (**relay** or **authority**): whether a server just passes changes along, or has the
   final say over which changes are accepted.

Where replicas sync (directly, through a relay, or both) is not a merge-strategy choice. Both merge
strategies work over any transport, because deltas combine in any order and can be repeated safely.

## Naming

Strategies have plain names that say how they work, as Effect's `RateLimiter` has `"fixed-window"` and
`"token-bucket"`. The docs name the published algorithm behind each one, so experts can check what they're
getting and find the papers. If we ever adjust an ordering rule, the plain name stays accurate.

| Name          | Algorithm behind it                                                                                     |
| ------------- | ------------------------------------------------------------------------------------------------------- |
| **tree**      | FugueMax ordering (Weidner & Kleppmann, "The Art of the Fugue", 2025)                                   |
| **replay**    | Eg-walker (Gentle & Kleppmann, EuroSys 2025), merging through the tree strategy's ordering               |
| **authority** | Server reconciliation (Replicache, prosemirror-collab, Weidner 2025), over the tree strategy's changes  |

## Summary

| Strategy      | Use it when                                                                                       | How concurrent inserts are ordered    | Build order |
| ------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------- | ----------- |
| **tree**      | The default. Local-first apps, peer-to-peer or through a relay.                                    | FugueMax                              | 1st         |
| **authority** | A server must be able to refuse edits (permissions, validation), and clients sync only with it.   | the same as tree                      | 2nd         |
| **replay**    | Large documents, many documents loaded at once, or long offline stretches. Same text as tree.     | the same as tree, during merges only  | 3rd         |

All three share the same change envelope, version vectors, deltas and public API (the API phase confirms
the last part). An app could switch between tree and replay by changing a constructor or layer.

## Tree

**What it is.** Every character gets a permanent id and a place in a tree, next to the character it was
typed beside. The text is a walk of the tree. Deleting marks a character as a tombstone and keeps it,
because other characters may be positioned relative to it. Concurrent inserts at the same spot become
siblings in the tree, ordered by FugueMax's rule. That rule is proven never to interleave two passages
typed concurrently at the same spot, except where every possible algorithm must.

**What a change records.** Inserts name their left and right origins (the ids of the characters around
the cursor when typed). Deletes name the ids they removed. Because ops name ids, not indexes, a replica can
integrate a change without knowing anything about what else was happening when it was made.

**How it fits Effect.** The history is a set of changes, and combining two histories is set union. That
makes the merge a `Combiner` that is also commutative and idempotent. The text is a pure function of the
history, which the replica keeps up to date incrementally. Changes are `Schema` values.

**What it needs from the network.** Nothing beyond eventual delivery. Changes that arrive before the
characters they reference are held as pending until those arrive.

**What it costs.**
- A tombstone for every deleted character, forever (until pruning exists).
- An id, origins and a parent per character. Runs (characters typed together stored as one entry) keep
  this small. The optimized Fugue in the paper uses about 23 bytes per character in memory, on par with Yjs.

**Why it's first.**
- It's the most readable correct algorithm: about 435 lines in the paper's simple form.
- Overlaps, anchors and, later, formatting marks all attach naturally to character ids.
- The other two strategies reuse it. Authority uses it directly; replay uses it as its temporary merge
  state.
- An earlier prototype already proved the approach on `Combiner`, including law tests and overlap detection. Its
  tree is Fugue-style; FugueMax differs only in how it orders some right-side siblings.

**Example.** Two people edit a meeting note offline on their laptops, reconnect, and both edits survive.
Where they both rewrote the same sentence, the merge reports an overlap.

## Authority

**What it is.** The tree strategy, plus a server that decides which changes are accepted. This is the
server-reconciliation pattern from Replicache, prosemirror-collab and Weidner (2025), but run on tree
changes:

1. A client says up front that it syncs through an authority: it's made with
   `ReplicatedText.makeConfirmed`, which requires an `Authority` service. It applies its own edits
   immediately, as usual. They're **unconfirmed** until the server acknowledges them, and the
   replica refuses to exchange with anything but its authority, so a change that might be rejected never
   reaches other replicas.
2. The client sends changes only to the server. The server checks each one with a check the app supplies,
   for example "this user can edit" or "the document stays under its size limit". It adds accepted changes
   to its history and sends them to everyone.
3. A rejected change is dropped by the client, together with its later unconfirmed changes, since those
   were made on top of it. Nobody else ever saw them, so dropping them is safe. The client reports what was
   rejected, so the app can tell the user. It then continues under a **new replica id**, so sequence numbers
   are never reused and never leave a gap.

**Why the merge part is easy.** Tree changes commute, so a client doesn't need to replay its unconfirmed
changes on top of the server's, as OT and prosemirror-collab must. Its text is just the tree text of
confirmed plus unconfirmed changes. Only rejection needs work: work out the text again without the rejected
changes.

**What it adds over a relay.** The server can refuse edits. A relay can't: once one client has a change,
everyone eventually does, and a client whose change was rejected would stay diverged forever (the Yjs
problem). It also gives a total order of accepted changes, which is useful for history views and for
resuming from a sequence number.

**What it gives up.** Clients can't sync directly with each other, and for now it's tree-only (replay
with an authority comes later). Offline work is kept (unlike prosemirror-collab, which drops edits whose
surroundings changed), but it can be rejected when it reaches the server.

**Example.** A shared spec where reviewers can only comment. A reviewer's attempt to edit the body is
rejected by the server and rolled back on their screen.

## Replay

**What it is.** Each change records what the user actually did, as indexes ("insert `k` at 12"), plus the
changes it was made on top of. In steady state a replica keeps only the history and the plain text. When a
change arrives:
- **with nothing concurrent** (its parents are the replica's frontier), it's applied directly to the text
  as a patch.
- **concurrent with local changes**, the replica replays only the part of history since the two diverged
  through a temporary tree. That tells it where the change lands, and then the tree is thrown away.

This is the Eg-walker algorithm.

**What it gains.**
- Memory close to the size of the plain text.
- Loading is reading text, not rebuilding a CRDT.
- Merging long offline branches is fast. The paper's example takes 24 ms, against an hour with OT.
- History is the primary data, so time travel and diffs between versions come naturally.

**What it costs.**
- It's the most complex strategy: walking the history backward and forward, and spotting versions with
  nothing concurrent across them, where the temporary state can be dropped.
- Anchors and overlaps need a replay, because changes don't carry character ids.
- Formatting marks need extra design (Loro solved this, and we'd study how).

**Why it's third.** Its benefit is performance, and we're doing correctness first. It also uses the tree
internally, so it should come after the tree exists.

**Why include it at all.**
- It's the current state of the art for this problem.
- It's the strategy local-first apps with many or large documents eventually need.
- Testing it against tree is unusually strong. The two must produce identical text for the same edits, so
  random edit histories run through both check replay's machinery against the straightforward version.
  Eg-walker's authors "believe" it matches FugueMax. Ours merges through our own tree, so the two match by
  construction, and the tests check everything else.

**How it's built.** Changes are `InsertAt`/`RemoveAt` ops on the text their author saw. A change made on
top of the replica's whole history applies to the text as is. Otherwise the replica walks back from both
sides to the latest change that everything being merged was made on top of (Eg-walker's critical
version), lays out stand-ins for the text at that point, and replays only the changes after it through a
temporary FugueMax tree. The tree is looked at as each change's author saw it (retreating and advancing
changes, found with Eg-walker's diff), so each change becomes exactly the tree op its author's replica
would have made. Stand-ins are never siblings of characters inserted later, so their layout can't affect
the order. Tests check replay against tree step by step on random sessions, and against Eg-walker's
conformance data.

**Example.** A co-author returns after a long flight with hours of edits to a long document. Their
replica syncs and merges quickly, and memory stays close to the size of the text.

## Not supported

| Approach                                        | Why not                                                                                                                                                                                                     |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Operational transformation** (OT)             | Needs a central server to order operations, slow to merge long offline branches, and its peer-to-peer variants have a long history of published proofs being wrong. Our patches use OT's insert/retain/delete shape, so editors that speak it still plug in. |
| **RGA** (Automerge) and **YATA** (Yjs)          | Both interleave text typed backward (right to left) in some cases. The tree strategy doesn't, with the same kind of metadata.                                                                                |
| **Logoot, LSEQ**, dense ids                     | The worst interleaving of all, forward and backward, and ids that grow where people type a lot.                                                                                                             |
| **prosemirror-collab-style rebase**             | Drops offline edits whose surroundings were deleted. Authority keeps them unless the server refuses.                                                                                                        |
| **Server-assigned ids** (json-joy server clock) | Smaller encoding, but changes are no longer safe to repeat, and it rules out peer-to-peer.                                                                                                                  |
| **Interop with Yjs or Automerge formats**       | Not a goal (decided in phase 1). Possible later as an adapter.                                                                                                                                              |

## Decisions shared by all strategies

1. **Text unit: UTF-16 code units.** Indexes count the same way JavaScript strings, textareas and editor
   libraries (CodeMirror, ProseMirror, Monaco) do, so apps never convert. A local edit that would split a
   surrogate pair (half an emoji) is rejected. That's enough to keep every merged text well-formed,
   because all characters come from local edits. Grapheme clusters (an emoji with a skin tone, a letter with
   an accent) can't be protected by any algorithm under concurrent edits, so we don't try.
2. **Replica ids: random, one per replica instance** (each tab, process or session), as branded strings
   generated with `Crypto`. Reusing a replica id is only safe when the replica restores its own saved state
   and nothing else is using that id at the same time. That's a later optimization, with a lock for multiple
   tabs. Version vectors grow by one entry per session, which is fine until pruning exists. The binary
   encoding can map ids to small numbers per document later, as Yjs and Automerge do.
3. **Every change has the same envelope:** change id, parents and ops. Only the op payload differs by
   strategy: character ids for tree and authority, indexes for replay. Parents cost little (usually one) and
   give every strategy the same history, frontier and version vectors.
4. **Keep all history** for now. A snapshot is a lossless shortcut for loading. Pruning comes later, once
   we know what it's for.
5. **Runs from day one** in the data model, even if the first implementation is simple. An earlier prototype's
   one-node-per-character layout cost about 180 bytes per character.
6. **Transports needn't preserve order.** Changes that arrive early wait as pending inside the replica.
7. **Overlaps and anchors work in all three strategies.** Replay works them out during merges, using its
   temporary tree.
8. **Undo comes later.** It will be local-only (undo my changes, never someone else's), which is the
   consensus across editors. Undoing a delete inserts the text again with new ids, as Yjs does. Reviving the
   old ids would make the deletion set something other than grow-only.
9. **Formatting marks come later.** The tree's character ids are what they attach to, which is one more
   reason it's first.

## Scope: text first, then documents

Realistic apps rarely have a bare text. A note has a title, a body and maybe a status. An earlier prototype handled
this by deriving merge rules from an ordinary `Schema`:
- structs merge field by field
- records merge entry by entry
- fields annotated as text use a text strategy
- everything else takes the latest write

That's a second family of strategies: last-writer-wins values and maps. It reuses the same changes,
version vectors and deltas.

**Decided (2026-10-15): text first, with all three strategies, then Schema-derived documents.** The
first examples use a bare text. Documents come once every text strategy is solid.

## How we'll know it's correct

The same tests run against every strategy:

- **Combine laws.** Combining deltas is commutative, associative and idempotent, for generated deltas
  (`Arbitrary` with `it.prop`).
- **Convergence.** Several simulated replicas make random edits. Their changes are delivered in random
  orders, with repeats, partitions and pending changes. Once everything is delivered, every replica shows
  the same text. Uses `TestClock` and seeded `Random`.
- **Intent.** A replica shows its own edits immediately and exactly.
- **Non-interleaving.** The interleaving cases from the Fugue paper, as fixed tests.
- **Strategy against strategy.** Replay and tree must produce identical text for the same edits. The
  Eg-walker reference's conformance data is a second check.
- **Authority.** Rejecting a change leaves the client equal to the server's confirmed state plus its
  remaining unconfirmed changes, under a new replica id.
- **Break it on purpose.** When we add a property, we break the code once and confirm the property fails.
  (An earlier prototype did this with a deliberately wrong merge rule.)
