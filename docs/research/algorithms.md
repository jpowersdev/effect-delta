# Algorithms and theory behind collaborative text

Phase 1 research notes, algorithms and theory track. Sources are primary (papers, author blogs, reference code) and are listed in §10. Claims I could not check against a primary source are marked **(unverified)**.

## 1. Overview

Every collaborative text system answers two questions:

1. **How do changes travel?** As full states, deltas, operations, or an event graph, either peer-to-peer or through a server that orders them.
2. **How are concurrent edits at the same spot ordered?** This is the *sequence* problem.

There are three families:

- **CRDTs** (RGA, YATA/Yjs, Fugue, Logoot) give every character a unique ID and insert relative to IDs, so concurrent operations commute by construction.
- **Operational transformation (OT)** (Jupiter, Google Docs, ShareDB, ot.js) keeps integer indexes and *transforms* each operation against concurrent ones. Practical OT needs a central server to order operations.
- **Replay/rebase hybrids** store the original operations and recompute the merge when needed. Eg-walker does this peer-to-peer. Server reconciliation does it with a central server (prosemirror-collab, Replicache, Weidner 2025).

Results from 2019 to 2025 that change the picture:

- **Interleaving is a correctness problem.** Most published OT and CRDT algorithms can mix concurrently typed passages together. **FugueMax** is proven *maximally non-interleaving*.
- **Eg-walker** (EuroSys 2025) keeps only the plain text in steady state. It builds a temporary CRDT only while merging concurrent edits, and merges long-diverged branches in O(n log n).
- **Server-ordered "insert-after-ID" with rebase** (Weidner 2025) is a simple option when a server exists anyway.

## 2. Glossary

| Term | Precise definition | Origin | Synonyms |
|---|---|---|---|
| Op-based CRDT (CmRDT) | Replicas broadcast operations (ops). Concurrent ops commute. Each op is applied exactly once, in causal order, through two phases: *prepare* at the source, then *effect* everywhere | Shapiro et al. 2011 | commutative RDT |
| State-based CRDT (CvRDT) | The state is a join-semilattice. Merge is the join ⊔ (commutative, associative, idempotent). Replicas ship whole states | Shapiro et al. 2011 | convergent RDT |
| Delta-state CRDT | Mutators return a small *delta* in the same lattice. Deltas are joined like states, so loss, duplication, and reordering are fine. Causal consistency needs *delta-intervals* | Almeida, Shoker, Baquero 2015/18 | δ-CRDT, delta-group |
| Pure op-based CRDT | *prepare* cannot read the state (`prepare(o)=o`). The state is a partially ordered log of ops (PO-Log), compacted once ops are causally stable | Baquero et al. 2014/17 | PO-Log CRDT |
| Causal delivery | A replica receives an op only after everything that happened before it | Birman; Lamport | causal broadcast |
| Exactly-once vs. idempotent | Op-based designs assume exactly-once delivery; state/delta merges tolerate at-least-once. List CRDTs dedup on op IDs | Shapiro; Almeida | dedup |
| Happened-before (→), concurrent (∥) | a → b if a precedes b on one replica, or a was delivered before b was created, or by transitivity. a ∥ b when neither a → b nor b → a | Lamport 1978 | causal order |
| Lamport timestamp | `(counter, replicaId)`, set to `max(local, received)+1`. It gives a total order consistent with →, but it cannot detect concurrency | Lamport 1978 | logical clock |
| Version vector | A map from replica ID to the number of updates seen from that replica. It compares versions as ≤ or concurrent | Parker et al. 1983 | vector clock, state vector (Yjs) |
| Dot / causal context / dot store | A *dot* `(replicaId, seq)` names one event. A *causal context* is the set of dots seen (a version vector plus a "dot cloud"). A *dot store* (DotSet/DotFun/DotMap) holds the data tagged by dots | Almeida et al. 2018 | op ID; causal history |
| Dotted version vector | A version vector plus one dot. It tracks concurrent siblings precisely, with size bounded by the number of servers, not clients | Preguiça et al. 2010 | DVV |
| Hybrid logical clock | `(l, c)`, where *l* is the largest physical time seen and *c* is a tiebreak counter. It is consistent with →, stays near NTP time, and fits in 64 bits | Kulkarni et al. 2014 | HLC |
| Event graph / frontier | A DAG of events, each listing its parents. The *frontier* (events with no children) identifies a version exactly | Gentle & Kleppmann 2025 | causal graph, oplog; heads |
| Causal stability | An op is causally stable once nothing concurrent with it can still arrive. Its metadata can then be compacted | Baquero et al. 2017 | stable timestamp |
| Critical version | A version where every event before it happened before every event after it. Eg-walker drops its merge state there | Gentle & Kleppmann 2025 | — |
| Tombstone | A deleted element kept because others may refer to it | WOOT onward | ⊥ |
| Origin | The ID of a neighbour that an insert anchors to: the *left origin* (predecessor) or *right origin* (successor) at insert time | RGA; YATA; Fugue | reference, anchor, parent |
| Dense identifier | A position drawn from a dense total order, so a new position always fits between any two existing ones | Treedoc; Logoot | fractional index |
| Interleaving | Passages typed concurrently at the same place come out mixed. *Forward* means typed left to right; *backward* means right to left | Kleppmann et al. 2019 | — |
| Maximal non-interleaving | No interleaving except where every possible algorithm must interleave | Weidner & Kleppmann | — |
| Strong / weak list spec | Strong: one global order over all elements ever inserted. Weak: only orders that are observed together are constrained | Attiya et al. 2016 | — |
| Strong eventual consistency (SEC) | Replicas that have received the same set of updates are in the same state | Shapiro et al. 2011 | convergence |
| Intention preservation | An op's effect matches what the user meant. This is informal; the strong list spec is the formal stand-in | Sun et al. 1998 | — |
| TP1 / TP2 | TP1: `x∘T(y,x) = y∘T(x,y)`. TP2: transforming a third op gives the same result along either path. A server-ordered system needs only TP1; peer-to-peer OT needs both | Ressel et al. 1996 | CP1/CP2 |
| Rebase / server reconciliation | Roll back pending local ops, apply the server's ops, then reapply the pending ones | prosemirror-collab; Replicache | — |
| Mark / expand | A formatting span anchored to character IDs. *Expand* (before/after/both/none) decides whether text typed at a boundary joins the span | Peritext 2022 | attribute |
| Local vs. global undo | Local undo reverts *my* last op. Global undo reverts the last op by anyone | OT literature; Stewen & Kleppmann 2024 | selective undo |

## 3. Models and logical time

| Model | Ships | Network must provide | Notes |
|---|---|---|---|
| Op-based | ops | causal order; exactly-once delivery (or dedup) | Smallest messages. The sender must track which peers still need which ops |
| State-based | full state | eventual delivery only | Lattice join. Expensive for big documents |
| Delta-state | deltas | unreliable channels are OK; causal consistency needs delta-intervals | Small messages plus an idempotent merge |
| Pure op-based | the op itself | Tagged Causal Stable Broadcast, which also reports when ops become stable | Makes GC explicit |
| Event graph (Eg-walker) | events with their parents | ancestors before descendants; set union is idempotent | Described as "a simple grow-only set CRDT" of original ops (eg-walker-reference) |

All known text CRDTs are op-based (Gomes et al. 2017). In practice, Yjs "updates" behave like deltas: a state-vector diff ships what the peer is missing, and applying it twice is harmless.

**Causal stability and GC.** A timestamp τ is stable at node *i* once every later delivery at *i* has a timestamp greater than τ. A node detects this conservatively by checking that every other node has sent something after τ. Roh's vector-clock purge of RGA tombstones works the same way. This has three consequences for local-first apps:

- GC requires **known membership**.
- One replica that stays offline blocks GC indefinitely.
- Ephemeral replica IDs (one per browser tab) make membership unbounded.

A server can approximate stability with epochs that all active clients have acknowledged. I found no primary analysis of that for text **(unverified)**. Attiya et al. (PODC 2016) also prove that, for a large class of list protocols, metadata grows at least linearly with the number of deleted elements. Tombstones, or an equivalent, cannot be avoided in general.

**Logical time in practice.** Lamport timestamps break ties (RGA siblings, OpSets-style total-order replay). Version vectors answer "what am I missing?", and `(replicaId, seq)` runs compress well. Dots and dot stores are the vocabulary for maps and registers beside the text. HLCs suit human-facing ordering such as history UIs, but should never be used alone to detect concurrency.

**Causal graphs.** An event graph generalises version vectors. A version is a frontier, and happened-before is reachability. The graph supports branching, time travel, and diffs between any two versions. Content-addressed (hashed) events make history tamper-evident and Byzantine-tolerant (Kleppmann 2022).

## 4. Sequence algorithms

### WOOT (Oster et al., CSCW 2006)

Each character stores the IDs of its previous and next characters at insert time. A recursive `IntegrateIns` places it, breaking ties by ID. Characters are never removed (the first tombstone CRDT). It **forward-interleaves** (Fugue paper, A.1.6), correcting a 2019 conjecture. It is reportedly cubic in the worst case **(unverified)**.

### Treedoc (Preguiça et al., ICDCS 2009)

Treedoc's dense IDs are paths through a binary tree, plus disambiguators for concurrent inserts on the same path. Compacting the tree ("flatten") needs agreement between replicas **(unverified detail)**. Concurrent inserts walk identical paths, so Treedoc interleaves **both forward and backward**.

### Logoot (ICDCS 2009) and LSEQ (DocEng 2013)

- **IDs and deletes.** An ID is a list of `(int, replicaId, …)` tuples sorted lexicographically. A delete removes the ID, so there are **no tombstones**.
- **Delivery.** Inserts don't need causal delivery. Only deletes must wait for their insert.
- **ID size.** LSEQ picks allocation strategies adaptively to keep IDs sub-linear in length.
- **Interleaving.** Concurrent inserts draw IDs from the same gap, so both suffer **severe forward and backward interleaving**.

Dense IDs derived from Fugue avoid interleaving. Weidner's `position-strings` averages about 33 characters per ID for one user and about 111 for several.

### RGA (Roh et al., JPDC 2011) and timestamped insertion trees

Each insert references its left neighbour, so the elements form a tree. The document is a pre-order traversal with siblings in **descending timestamp order**. Attiya et al. call this a *timestamped insertion tree*; Grishchenko calls it a *causal tree*. Deletes leave tombstones.

- RGA is proven **forward non-interleaving**, but it **interleaves backward-typed text**, because right-to-left typing anchors every character to the same parent.
- Kleppmann et al. 2019 proposed a fix, but it turned out **not to converge** (Fugue paper, Appendix A.3).
- Automerge's list is RGA-based.
- RGA has a mechanised proof of strong eventual consistency (Gomes et al. 2017).

### YATA (Nicolaescu et al., GROUP 2016) / Yjs

Each item stores both a **left and a right origin**. Integration scans the items between the two origins. Origin lines may not cross, and when items share an origin, Yjs puts the lower `clientID` first.

- Yjs is forward non-interleaving (proof in progress, per the Fugue paper).
- It **interleaves backward-typed text across several replicas** (Fugue paper, Appendix A.1.9 reproduces this).
- Gentle's **YjsMod** changes the order of siblings and is conjectured to equal FugueMax.

### Fugue / FugueMax (Weidner & Kleppmann, IEEE TPDS 2025)

**Structure.** The state is a tree in which every node is a *left* or *right* child of its parent. The document is an in-order traversal.

**Inserting between L and R.** L is the left origin; R is the next node when the traversal includes tombstones.

- If L has no right children, the new node becomes L's right child.
- Otherwise it becomes R's left child.

**Siblings and deletes.** Siblings on the same side are sorted by ID. A delete sets the value to ⊥. The tombstone stays because it may still be the parent of live nodes.

**FugueMax** additionally stores the right origin on right children. It visits right-side siblings in **reverse list order of their right origins**, falling back to ID order. This technique comes from YjsMod.

**Proven properties.** Both satisfy the strong list spec. Fugue is forward non-interleaving, and FugueMax is **maximally non-interleaving**. The two differ only in cases where some interleaving is unavoidable.

**Size and performance.** Fugue Simple is 298 LOC and FugueMax Simple 435 LOC (618–685 bytes per character, "still usable"). Optimised Fugue is 1,132 LOC and on par with Yjs: 105 kB of text saves to 168 kB, at about 23 bytes per character in memory.

### Eg-walker (Gentle & Kleppmann, EuroSys 2025)

**State.** Each replica keeps three things:

1. An on-disk **event graph** of the original index-based ops: `{id, parents, insert|delete, pos, content}`.
2. The **plain text**.
3. A **transient CRDT** that exists only while merging.

**Merging.** Events are walked in topological order. A *prepare* version moves back and forth (`retreat`/`advance`), while an *effect* version only applies events. At **critical versions** the CRDT state is replaced by a placeholder. Events on a linear stretch of history are emitted unchanged, and only the part after the common ancestor is replayed.

**Ordering.** The internal CRDT is a YjsMod variant, which the reference implementation describes as "isomorphic to FugueMax".

**Performance.**

- Merging *k* and *m* events costs O((k+m) log(k+m)), with an O(n² log n) worst case.
- A document whose merge takes **1 hour with OT takes 24 ms**.
- Steady-state memory is an order of magnitude below the best CRDTs.

It needs no server, and its stored format is independent of the ordering algorithm. Diamond-types and Loro use it. The TypeScript reference (~1.6k LOC) ships a fuzzer and conformance data.

### Interleaving summary (Fugue paper, Table 1)

● = can interleave · ○ = no example found · ✓ = proven not to interleave

| | Forward | Backward (1 replica) | Backward (multi-replica) |
|---|---|---|---|
| OT: Jupiter, adOPTed, GOT, SOCT2, TTF | ● | mixed (GOT even reorders) | mixed |
| WOOT | ● | ○ | ○ |
| Logoot, LSEQ, Treedoc | ● | ● | ● |
| RGA | ✓ | ● | ● |
| Yjs | ✓ | ○ | ● |
| YjsMod, Sync9 | ○ | ○ | ○ |
| Fugue, FugueMax | ✓ | ✓ | ✓ |

### Tombstones

Every reference-based CRDT keeps deleted IDs. Run-length encoding (runs of consecutive IDs from one replica) makes this cheap; in the Fugue trace, saved size even *drops* after deletes. Yjs keeps a deleted flag but no deletion time (Gentle), and discards deleted content **(unverified detail)**. The ways out are causal-stability purging (needs membership), consensus compaction (Treedoc flatten), or Eg-walker (tombstones only during a merge, but history is kept).

## 5. Rich text, move, undo

### Peritext (Litt, Lim, Kleppmann, van Hardenberg, CSCW 2022)

Marks (`addMark`/`removeMark`) attach to the **gaps** next to character IDs, through per-character `markOpsBefore` and `markOpsAfter` sets.

- **Bold** ends on the *before* anchor of the next character, so it grows when you type at its end.
- A **link** ends on the *after* anchor of its last character, so it doesn't grow.

When marks conflict:

- Different mark types (bold and italic) both apply.
- Exclusive values (colour, bold vs. unbold) are resolved last-writer-wins by op ID.
- **Comments** can overlap.

Inserts next to tombstones look at the anchors on those tombstones, so text typed after a deleted link end stays outside the link. Peritext only covers inline formatting; blocks are future work.

Elsewhere: **Automerge** marks take `expand: none|before|after|both`, and **Loro** configures expand per style key, following Peritext. **Yjs** embeds formatting *control items* in the sequence, which Peritext shows loses information when spans overlap. **Markdown as plain text** breaks under concurrent edits (`**The **fox** jumped.**`). **Quill Delta (OT)** attaches attributes to insert and retain ops.

### Moving elements (Kleppmann, PaPoC 2020)

Implementing a move as delete plus insert duplicates the element when two users move it concurrently.

**The fix.** Each element gets a **last-writer-wins (or multi-value) register holding its position ID**. A move writes a new position into the register, and the list itself is an add-wins set of `(value, register)` pairs. The approach works on top of any list CRDT.

**The open problem for text is range moves.** If a paragraph is moved character by character, a concurrent edit inside it is left behind at the old location. This matters for future block and list moves but not for typing.

### Undo/redo

Stewen & Kleppmann (PaPoC 2024) tested six mainstream collaborative apps (Google Sheets and Slides, Microsoft Excel and PowerPoint Online, Figma, Miro). All except Miro use **local undo**:

- Undo reverts my last op.
- Redo restores the state before the matching undo.
- A new edit clears the redo stack.
- *Undo-redo neutrality*: n undos followed by n redos get back to the start.

Global undo surprises users by reverting edits they can't see. It also fails when timestamps aren't final: a remote op can arrive inside an interval that has already been undone.

Pitfalls:

1. **Undoing a delete.** Yjs re-inserts a *copy* with **new IDs**, linked to the original by `redone`. Marks, cursors, and concurrent ops that point at the old IDs have to follow that link. The alternative is to revive the original IDs, using counter-based undo as in Logoot-Undo or Yu et al. 2015. Counter-based undo cannot skip over remote ops, though, and local undo needs that.
2. **Undoing an insert** that others typed inside leaves fragments; **concurrent undos** of overlapping ops conflict.
3. **GC.** Anything on an undo stack must stay alive (Yjs `keepItem`).
4. **OT undo** needs inverse properties IP1–IP3 (Sun 2002, ANYUNDO) and `invert`/`invertWithDoc`.
5. **Grouping** keystrokes into undo steps and restoring the selection.

Loro also scopes undo to the local peer.

## 6. OT and rebasing

**Vocabulary.** A *transformation function* `T(a, b)` rewrites op *a* so it can run after op *b*. The *control algorithm* decides which ops get transformed against which. Ellis & Gibbs introduced OT with dOPT in 1989. Ressel et al. (1996) formalised TP1 and TP2. Sun et al. (1998) defined the **CCI** model: convergence, causality preservation, intention preservation.

**TP2 is a minefield.** Published TP2 claims for dOPT, adOPTed, IMOR, SOCT2, and SDT were all broken by counterexamples. That includes machine-checked "proofs" built on false assumptions (Imine et al. 2003; Oster et al. 2005, as summarised by Gomes et al.). Randolph et al. (2015) showed TP2 is unattainable in the classic formulation. Only TTF has no known counterexample.

**Server-ordered OT.**

- **Jupiter** (UIST 1995) keeps a two-party state space between each client and the server. Because the server sets the order, only TP1 is needed.
- Google Docs is based on Jupiter. Jupiter satisfies only the **weak** list spec (Wei et al. 2018).
- **Google Wave** added a rule: the client keeps one batch of ops in flight and buffers the rest until the server acknowledges it.

**ot.js.** An op is a list of components: retain is a positive integer, insert is a string, and delete is a negative integer. The client is a three-state machine: `Synchronized` → `AwaitingConfirm` → `AwaitingWithBuffer`.

**ShareDB / ottypes.**

- Each OT type defines `apply`, `transform(op1, op2, side)`, and optional `compose`, `invert`, and `transformCursor`.
- `side` breaks ties between inserts at the same index. ShareDB passes `'left'` for the incoming op.
- The `text` type uses the components `N` (skip), `"str"` (insert), and `{d:N}` (delete), and counts UTF-16 code units.
- **`text-unicode`** counts code points instead. Its `{d:"str"}` form keeps the deleted text, so the op can be inverted.
- The TP2 text variant is about 20× slower and isn't needed behind a server.

**Rebasing.**

- **prosemirror-collab** (Haverbeke 2015) uses a central authority with integer versions. A client whose push is rejected maps its unconfirmed steps through the steps it missed. Steps whose context was deleted are **dropped**. Haverbeke calls the model "useless (as is OT)" for offline or branching work.
- **Replicache** rewinds to the last server state, applies the server's patch, then re-runs pending *mutators*, tracking progress with `lastMutationID`.
- **Weidner (2025)** uses character IDs and ops of the form "insert after ID X", which the server applies literally.
  - Concurrent inserts after the same ID end up in reverse order of arrival, which matches RGA's sibling order.
  - Forward-typed text is safe; **backward-typed text can interleave**.

**When OT or rebase fits better:** a server is required anyway (authorisation, validation, rejection), divergence is short, the editor already speaks index deltas, and per-character memory must be zero. The Eg-walker paper notes that server OT is an event graph with one main branch; Eg-walker matches OT's memory profile without a server.

## 7. Correctness and testing

**Properties to state:**

1. **SEC/convergence.** Replicas with the same ops reach the same state.
2. **Strong list spec.** This is formal intention preservation for lists.
3. **Maximal non-interleaving** (Fugue paper §5).
4. **Peritext's intent rules** for marks (informal).
5. **Semilattice laws** for merges: commutative, associative, idempotent.

**Formal verification.**

- **Gomes et al. (OOPSLA 2017)** built an Isabelle/HOL framework with an explicit network model. Causal delivery is derived from axioms, which avoids the false assumptions that sank earlier OT proofs. They verified RGA, the OR-Set, and a counter from one abstract convergence theorem.
- **VeriFx (ECOOP 2023)** verifies CRDTs and OT functions automatically with an SMT solver.
- **Liquid Haskell (OOPSLA 2020)** checks the laws with refinement types and includes a text editor.

**Property-based and fuzz testing.**

- **Simulated replicas.** Run N replicas with a seeded RNG. Apply random inserts and deletes, deliver messages in random order with partitions and duplicates, then flush everything and assert the texts match. Yjs's `TestConnector` does this, and eg-walker-reference ships `fuzzer.ts`.
- **Differential checks.** FugueMax should match Eg-walker and diamond-types order, using the reference's `conformance.json`. Fugue and FugueMax should agree wherever no interleaving is forced.
- **Oracles**: local ops show immediately (strong list spec), each typing session stays contiguous (non-interleaving), and merge passes the law tests.
- **Real editing traces** (`josephg/editing-traces`) for regressions and benchmarks.

Effect v4 has `effect/testing/FastCheck` and Schema-derived arbitraries (`toArbitrary`), so op generators can come straight from our op schemas.

## 8. Strategy comparison

| Algorithm | Needs from network/server | Metadata overhead | Interleaving | Tombstones / GC | P2P | Offline / long divergence | Impl. complexity | Readable reference fit |
|---|---|---|---|---|---|---|---|---|
| **RGA** | Causal delivery; dedup by ID | ID + reference per character (runs compress) | Forward safe (proven); backward interleaves | Required; purge needs stability + membership | Yes | Good | Low–med | Good; formally verified, but its ordering is superseded |
| **YATA (Yjs)** | Causal delivery (buffers missing deps); state-vector sync | ID + left/right origin (runs) | Forward safe; multi-replica backward interleaves | Required; content GC only **(unverified)** | Yes | Good | Medium; subtle rules | Fair; hard to explain |
| **Fugue** | Causal broadcast (can be made state-based) | ID + parent + side; ~23 B/char optimised | Non-interleaving except rare unavoidable cases | Required (deleted nodes can be parents) | Yes | Good | Low (298 LOC) | **Excellent** |
| **FugueMax** | As Fugue | + right origin on right children | **Maximal (proven)** | As Fugue | Yes | Good | Low–med (435 LOC) | **Excellent**; same order as Eg-walker (conjectured) |
| **Eg-walker** | Event-graph replication, ancestors first, idempotent; no server | Memory: plain text. Disk: original ops + parents. CRDT only while merging | Maximal (YjsMod ≈ FugueMax) | No persistent tombstones; history prunable only when no concurrency can arrive | Yes | **Best**: O((k+m) log(k+m)) | High (retreat/advance, critical versions, B-trees) | Good; ~1.6k LOC TS reference + fuzzer |
| **Logoot / LSEQ** | Inserts need no causal order; deletes wait for their insert | Variable-length IDs that grow in hot spots | **Worst**: forward + backward | **None** | Yes | Fair (ID growth) | Low–med | Poor, unless IDs are Fugue-derived |
| **Server-ordered OT** | Central total order, FIFO channels, acks | None per character; server keeps op log | Forward interleaving possible (Jupiter); tie by `side` | None | No (TP2 is fragile) | **Poor**: O(n·m)+ | Low (Jupiter + text type) | Good for the narrow case |
| **Rebase** | Authoritative server, linear history, `version`/`lastMutationID` | PM: none. ID-based: ID + tombstone per character | ID-based: forward safe, backward can interleave; PM **(unverified)** | ID-based: server tombstones; GC policy open | No | PM drops work; ID-based is workable | **Low** | **Excellent**; server can validate |

## 9. Relevance to us

### Recommendation: one ordering core, three strategies

1. **FugueMax as the canonical ordering, exposed as an ID-based CRDT (the "tree" strategy).**
   - It is the only *proven* maximally non-interleaving list CRDT, and about 435 LOC of plain data and functions (good for immutable data and Schema-encoded ops).
   - Its IDs anchor Peritext marks, and it gives op-based/delta sync with readable rules.
2. **Eg-walker as the default local-first / peer-to-peer strategy.**
   - Its events are immutable, Schema-friendly records in an append-only log, which fits `Stream`/eventlog-style plumbing (the Effect track should confirm).
   - Memory and load time are close to plain text, and long offline branches merge cheaply.
   - Its internal ordering is YjsMod ≈ FugueMax, so strategies 1 and 2 should produce **identical documents**. That gives us a free differential-testing oracle, plus diamond-types conformance data.
   - The stored format is independent of the ordering algorithm, which keeps future options open.
3. **Server-authoritative rebase over ID-based ops.**
   - For apps with a server that must authorise, validate, or reject edits. Easiest to explain; reuses strategy 1's IDs and ops. No peer-to-peer.

**Skip:**

- **Logoot/LSEQ**: the worst interleaving.
- **RGA/YATA as orderings**: FugueMax dominates both. Keep them as vocabulary only.
- **Classic OT**: TP2 hazards and slow long-divergence merges. A `text-unicode`-style delta could still serve as an import/export adapter.

### Open questions

- **Character unit**: UTF-16 code units, code points, or grapheme clusters? This sets Eg-walker indexes and editor interop.
- **Replica IDs**: persistent or one per tab? Ephemeral IDs bloat version vectors and block stability-based GC.
- **Event identity**: `(agent, seq)` or content hashes (Byzantine-tolerant)?
- **History retention**: when can the Eg-walker graph be pruned or snapshotted, and how do new peers join without the full history? Can a server-declared epoch stand in for causal stability?
- **Rich text on Eg-walker**: Peritext marks anchor on IDs, but Eg-walker events use indexes. Loro combines the two, so we should study how. The Eg-walker paper leaves rich text as future work.
- **Undo**: should undoing a delete revive the original IDs or mint new ones (as Yjs does)? How does undo interact with marks and the event graph?
- **Authority under Eg-walker**: can a server reject events in a DAG, or does that need strategy 3?
- **Moves**: register-per-element moves are solved; range and block moves are not.
- **Data layout**: the core types must not rule out runs of consecutive IDs or B-tree indexes later.

## 10. Sources

- Shapiro, Preguiça, Baquero, Zawirski. CRDTs (SSS 2011) / comprehensive study (INRIA RR-7506): https://inria.hal.science/inria-00555588
- Almeida, Shoker, Baquero. Delta State Replicated Data Types: https://arxiv.org/abs/1603.01529
- Baquero, Almeida, Shoker. Pure Operation-Based Replicated Data Types: https://arxiv.org/abs/1710.04469
- Almeida. Approaches to Conflict-free Replicated Data Types: https://arxiv.org/abs/2310.18220
- Lamport. Time, Clocks, and the Ordering of Events (1978): https://lamport.azurewebsites.net/pubs/time-clocks.pdf
- Preguiça et al. Dotted Version Vectors: https://arxiv.org/abs/1011.5808
- Kulkarni et al. Logical Physical Clocks (HLC): https://cse.buffalo.edu/tech-reports/2014-04.pdf
- Kleppmann. Making CRDTs Byzantine Fault Tolerant (PaPoC 2022): https://martin.kleppmann.com/papers/bft-crdt-papoc22.pdf
- Oster et al. WOOT (CSCW 2006): https://hal.archives-ouvertes.fr/file/index/docid/108523/filename/OsterCSCW06.pdf
- Preguiça et al. Treedoc (ICDCS 2009): https://doi.org/10.1109/ICDCS.2009.20 (DOI unverified)
- Weiss, Urso, Molli. Logoot (ICDCS 2009): https://doi.org/10.1109/ICDCS.2009.75 (DOI unverified)
- Nédelec et al. LSEQ (DocEng 2013): https://github.com/parchemins/paper-lseq-tex
- Roh et al. RGA (JPDC 2011): https://csl.skku.edu/papers/jpdc11.pdf
- Attiya et al. Specification and Complexity of Collaborative Text Editing (PODC 2016): https://dl.acm.org/doi/10.1145/2933057.2933090
- Nicolaescu et al. YATA (GROUP 2016): https://dl.acm.org/doi/10.1145/2957276.2957310
- Kleppmann et al. Interleaving anomalies (PaPoC 2019): https://martin.kleppmann.com/papers/interleaving-papoc19.pdf
- Weidner, Kleppmann. The Art of the Fugue (IEEE TPDS 2025): https://arxiv.org/abs/2305.00583
- Fugue implementations: https://github.com/mweidner037/fugue
- Weidner. position-strings: https://github.com/mweidner037/position-strings
- Gentle, Kleppmann. Eg-walker (EuroSys 2025): https://arxiv.org/abs/2409.14252
- Gentle. eg-walker-reference: https://github.com/josephg/eg-walker-reference
- Gentle. 5000x faster CRDTs: https://josephg.com/blog/crdts-go-brrr/
- Gentle. editing-traces: https://github.com/josephg/editing-traces
- Litt et al. Peritext (CSCW 2022): https://www.inkandswitch.com/peritext/
- Automerge rich text: https://automerge.org/docs/reference/documents/rich-text/
- Loro rich text: https://loro.dev/blog/loro-richtext
- Loro undo: https://loro.dev/docs/advanced/undo
- Loro Eg-walker: https://loro.dev/docs/concepts/event_graph_walker
- Kleppmann. Moving Elements in List CRDTs (PaPoC 2020): https://martin.kleppmann.com/papers/list-move-papoc20.pdf
- Stewen, Kleppmann. Undo and Redo for Replicated Registers (PaPoC 2024): https://arxiv.org/abs/2404.11308
- Sun. Undo as Concurrent Inverse in Group Editors (TOCHI 2002): https://dl.acm.org/doi/10.1145/586081.586085
- Yu, Elvinger, Ignat. CRDT Supporting Selective Undo (DAIS 2015): https://members.loria.fr/CIgnat/files/pdf/YuDAIS15.pdf
- Yjs UndoManager: https://github.com/yjs/yjs/blob/main/src/utils/UndoManager.js
- Yjs Item.integrate (YATA): https://github.com/yjs/yjs/blob/main/src/structs/Item.js
- Yjs test harness: https://github.com/yjs/yjs/blob/main/tests/testHelper.js
- Ressel et al. adOPTed / TP1–TP2 (CSCW 1996): https://www.semanticscholar.org/paper/6a313d12c90b01efae531e70f8d0cd1d1e8565ae
- Wei, Huang, Lu. Jupiter Protocol Revisited (OPODIS 2018): https://arxiv.org/abs/1708.04754
- Wang, Mah, Lassen. Google Wave OT: https://svn.apache.org/repos/asf/incubator/wave/whitepapers/operational-transform/operational-transform.html
- ot.js: https://github.com/Operational-Transformation/ot.js
- ottypes spec: https://github.com/ottypes/docs
- ottypes text-unicode: https://github.com/ottypes/text-unicode
- ShareDB ot.js: https://github.com/share/sharedb/blob/master/lib/ot.js
- Haverbeke. Collaborative Editing in ProseMirror: https://marijnhaverbeke.nl/blog/collaborative-editing.html
- Replicache. How it works: https://doc.replicache.dev/concepts/how-it-works
- Weidner. Collaborative Text Editing without CRDTs or OT: https://mattweidner.com/2025/05/21/text-without-crdts.html
- Weidner. CRDT Survey: https://mattweidner.com/2023/09/26/crdt-survey-1.html
- Gomes et al. Verifying Strong Eventual Consistency (OOPSLA 2017): https://arxiv.org/abs/1707.01747
- De Porre et al. VeriFx (ECOOP 2023): https://arxiv.org/abs/2207.02502
- Liu et al. Liquid Haskell RDTs (OOPSLA 2020): https://dl.acm.org/doi/10.1145/3428284
- Effect v4 (FastCheck, Differ, eventlog): https://github.com/Effect-TS/effect-smol
