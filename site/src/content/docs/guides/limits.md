---
title: "Limits"
description: "What costs more than you might expect, and what isn't built."
sidebar:
  order: 7
---

## Costs

- **Replay merges cost time in proportion to the text.**
- **Replay anchors are slow on long histories.** `anchor`, `locate` and overlaps made only of removed text replay the whole
  history, once per change to the text, and the result is cached until the next one. That's about 3 s at 50 small edits to a
  47 KB text.
- **The tree is one node per character,** and typing builds deep chains, so tree edits cost time in proportion to their depth.
- **History is kept forever.** Nothing prunes it.

## Not built

- Schema-derived documents with several fields
- A symmetric peer-to-peer mesh
- Undo and formatting marks
- Pruning history
- Binary encoding
- WebRTC and BroadcastChannel transports
- Sharing one replica id across tabs
- End-to-end encryption
- Confirmed replicas for the `replay` strategy
