---
title: "Strategies"
description: "Choosing how text merges and what a server does."
sidebar:
  order: 2
---

There are two separate choices:

1. **Merge strategy**, `tree` or `replay`: how concurrent edits to the same text combine, and what a replica keeps in
   memory. It's an option on `ReplicatedText.make`, and the code that edits or observes a text doesn't change with it.
2. **Server role**, relay or authority: whether a server just passes changes along, or decides which are accepted.

Where replicas sync doesn't depend on the merge strategy. Both work over any transport.

| Use               | When                                                                  | Runnable example                                                                                     |
| ----------------- | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `tree`            | The default. Local-first apps, syncing directly or through a relay.   | [`offline-notes.ts`](https://github.com/jpowersdev/effect-delta/blob/main/examples/offline-notes.ts) |
| `tree`, authority | A server must be able to refuse edits, and clients sync only with it. | [`spec-review.ts`](https://github.com/jpowersdev/effect-delta/blob/main/examples/spec-review.ts)     |
| `replay`          | Large texts, many texts loaded at once, or long offline stretches.    | [`manuscript.ts`](https://github.com/jpowersdev/effect-delta/blob/main/examples/manuscript.ts)       |

`tree` and `replay` produce the same text for the same edits.

## Tree

Every character gets a permanent id and a place in a tree, next to the character it was typed beside. Deleting a character
keeps it as a tombstone, because others may be positioned relative to it. Concurrent inserts at the same spot are ordered
by FugueMax (Weidner & Kleppmann, "The Art of the Fugue", 2025), which keeps two passages typed at the same spot from
interleaving, except where every possible algorithm must.

It needs nothing from the network beyond eventual delivery. Its memory grows with every character ever typed, including
deleted ones.

## Authority

The tree strategy, plus a server that decides which changes are accepted. This is server reconciliation, as in Replicache
and prosemirror-collab.

1. A client is made with `ReplicatedText.makeConfirmed`, which requires an `Authority` service. It applies its own edits
   immediately. They're unconfirmed until the server acknowledges them, and the replica refuses to exchange with anything
   else, so a change that might be rejected never reaches other replicas.
2. The server checks each change with a check the app supplies, such as "this user can edit" or "the room stays under
   its size limit". It stores accepted changes and sends them to everyone.
3. A rejected change is dropped by the client, along with its later unconfirmed changes, since those were made on top of
   it. The client reports what was rejected, and continues under a new replica id.

Clients can't sync directly with each other. Offline work is kept, but can be rejected when it reaches the server. A relay
can't refuse edits: once one client has a change, everyone eventually does.

## Replay

Each change records what the user did, as indexes ("insert `k` at 12"), plus the changes it was made on top of. A replica
keeps the history and the plain text. A change with nothing concurrent applies directly to the text. One concurrent with
local changes is merged by replaying the history since the two diverged through a temporary tree, which is then thrown
away. This is Eg-walker (Gentle & Kleppmann, EuroSys 2025), merging with the tree strategy's ordering.

Memory stays close to the size of the text, loading reads text rather than rebuilding a tree, and long offline branches
merge quickly. Anchors are the cost: changes don't carry character ids, so locating an anchor needs a replay, which is slow
on long histories. Authorities don't work with it.

## True of every strategy

- **Indexes are UTF-16 code units.** A local edit that would split a surrogate pair (half an emoji) is rejected, which keeps
  every merged text well-formed. Grapheme clusters, such as an emoji with a skin tone, can still be split by concurrent
  edits.
- **Replica ids are random, one per replica instance** (each tab, process or session), generated with `SecureRandom`. Reuse an id
  only when a replica is restoring its own saved state and nothing else is using that id.
- **History is kept forever.** Nothing prunes it.
- **Transports needn't preserve order.** Changes that arrive early wait inside the replica.
- **Overlaps and anchors work everywhere.**
