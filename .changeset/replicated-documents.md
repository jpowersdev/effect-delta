---
"effect-delta": minor
---

Add `ReplicatedDocument`: replicas whose value is an ordinary `Schema`, merged by rules from the schema. Structs merge field by field, records entry by entry, `ReplicatedDocument.Text` character by character (tree or replay), `ReplicatedDocument.Counter` by adding and `ReplicatedDocument.List` item by item; everything else takes the latest write. Documents sync through relays, history stores and authorities as texts do, and their text fields work with every `ReplicatedText` function but the constructors.

- `Replica.Overlap` is a union of `Text` (now with a `path`), `Value` and `Removed` overlaps; narrow on `_tag` before reading `range` or `anchors`.
- `Authority.Proposal` is a union of `TextProposal` and `DocumentProposal`; narrow on `_tag` before reading `before`, `after` or `patch`.
- `HistoryStore.Snapshot` holds `texts`, one per path, instead of `text`.
- Changes carry a write time, so deltas and histories stored by earlier versions don't load. Relays and authorities refuse changes stamped more than a day ahead of their clock, a text's included.
- `Replica.ReplicaId` and the `VersionVector` codec refuse the replica id `__proto__`.
