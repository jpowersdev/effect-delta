---
title: "Concepts"
description: "The model, the terms these docs use, and what other libraries call them."
sidebar:
  order: 1
---

A **document** is shared by many **replicas**, one per device or session. Each has a **replica id**. When an app edits
a replica's text, the edit is recorded as a **change**, along with the **parents** it was made on top of. All the changes
a replica knows form its **history**, and a **version vector** summarizes what it has seen from each replica.

To sync, two replicas exchange version vectors, and each sends a **delta**: the changes the other lacks. Receiving a delta
**combines** it into the history. Combining is order-independent and safe to repeat, so every replica that has received
the same changes shows the same text.

Apps observe each change to the text as a **patch**. They keep stable positions as **anchors**, and are told about
**overlaps**: places where two replicas edited the same stretch of text without seeing each other's edit.

## Documents and replicas

| Term       | Meaning                                                                                                     |
| ---------- | ----------------------------------------------------------------------------------------------------------- |
| Document   | The shared text being edited, identified by a document id.                                                  |
| Replica    | One copy of a document in one place (a tab, a device, a server). It accepts edits at once and syncs later.  |
| Replica id | Identifies the stream of changes one replica writes. Never reused. It isn't a user or a network connection. |

## Changes and history

| Term           | Meaning                                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------------------- |
| Change         | The edits one replica committed together. The unit that is stored and sent, and is never modified.      |
| Parents        | What a change was made on top of: the changes its replica had seen.                                     |
| History        | Every change a replica knows. Append-only. The current text is derived from it.                         |
| Version vector | For each replica id, how much of it a replica has seen. Compares as before, after, equal or concurrent. |
| Concurrent     | Two changes are concurrent when neither's replica had seen the other when it was made.                  |

## Syncing

| Term          | Meaning                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| Delta         | A set of changes sent between replicas. Deltas combine in any order, and repeating one is harmless.      |
| Pending       | Changes received before their parents, held until the parents arrive. Transports needn't preserve order. |
| Unconfirmed   | A replica's own changes that its authority hasn't acknowledged. They may still be rejected.              |
| Relay         | A server that passes changes between replicas and stores them. It never rejects anything.                |
| Authority     | A server that checks each change, and can reject it.                                                     |
| History store | Where a replica or server keeps history between sessions.                                                |
| Snapshot      | The text at one version, saved so a `replay` replica can load without replaying its history.             |
| Presence      | Short-lived state that isn't part of the document: who's here, and their selections.                     |

## Text and positions

| Term    | Meaning                                                                                                         |
| ------- | --------------------------------------------------------------------------------------------------------------- |
| Index   | A position in the current text, counted in UTF-16 code units, as JavaScript strings and editor libraries count. |
| Patch   | How a text changed, as retain, insert and remove, in Quill's shape. Applied in order to one particular text.    |
| Anchor  | A position that follows the text through edits and merges, attached to one character.                           |
| Side    | Which way an anchor leans, `before` or `after` its character. This decides where text typed exactly at it goes. |
| Overlap | A stretch of merged text where changes that hadn't seen each other touch ("24 hours" becoming "812 hours").     |

A **delta** and a **patch** are different. Deltas are what replicas send each other, and combine in any order. A patch is
what an editor applies to show new text. Yjs, Quill and Loro call the second one a delta.

## Coming from another library

| Here           | Yjs                      | Automerge              | Loro              | Eg-walker / Diamond Types |
| -------------- | ------------------------ | ---------------------- | ----------------- | ------------------------- |
| Replica        | Y.Doc instance           | doc instance           | LoroDoc instance  | branch + oplog            |
| Replica id     | clientID                 | actor                  | PeerID            | agent                     |
| Change         | item runs in an update   | change                 | change            | event (run)               |
| History        | struct store             | change graph           | OpLog             | event graph / oplog       |
| Version vector | state vector             | —                      | version vector    | —                         |
| Delta          | update                   | sync message / changes | update            | patch bytes               |
| Patch          | delta                    | patch (splice)         | delta (text diff) | xf ops                    |
| Anchor + side  | RelativePosition + assoc | cursor + move          | Cursor + Side     | —                         |
| Presence       | awareness                | presence               | ephemeral store   | —                         |
