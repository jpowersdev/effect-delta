/**
 * A team's task board. Two people reorder and edit it offline, both moving
 * the same task. Back online, that task is in one place, every edit to it
 * survives, and the merge says they both moved it.
 *
 * Strategy: tree.
 *
 *   node packages/delta/examples/task-board.ts
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node"
import { Console, Effect, Schema } from "effect"
import { Replica, ReplicatedDocument, ReplicatedText } from "effect-delta"

const Task = Schema.Struct({
  id: Schema.String,
  title: ReplicatedDocument.Text,
  done: Schema.Boolean
})

const Board = Schema.Struct({
  name: Schema.String,
  tasks: ReplicatedDocument.List(Task, { key: "id" })
})
type Board = typeof Board.Type

// What a board shows before anyone writes to it. The same on every device.
const initial: Board = { name: "Launch", tasks: [] }

// Moves a task to `index`, as dragging it on the board would.
const move = (board: Board, id: string, index: number): Board => {
  const task = board.tasks.find((task) => task.id === id)!
  const rest = board.tasks.filter((other) => other !== task)
  return { ...board, tasks: [...rest.slice(0, index), task, ...rest.slice(index)] }
}

const show = (board: Board) => board.tasks.map((task) => `${task.done ? "[x]" : "[ ]"} ${task.title}`).join("\n")

const program = Effect.gen(function*() {
  const ada = yield* ReplicatedDocument.make(Board, { initial })
  yield* ReplicatedDocument.update(ada, (board) => ({
    ...board,
    tasks: [
      { id: "announce", title: "Write the announcement", done: false },
      { id: "pricing", title: "Update the pricing page", done: false },
      { id: "login", title: "Fix the login bug", done: false },
      { id: "docs", title: "Publish the docs", done: false }
    ]
  }))

  // Ben's laptop gets a copy before they both go offline.
  const ben = yield* ReplicatedDocument.make(Board, { initial })
  yield* Replica.exchange(ada, ben)

  // Ada moves the bug to the top, and adds a task at the bottom.
  yield* ReplicatedDocument.update(ada, (board) => {
    const moved = move(board, "login", 0)
    return { ...moved, tasks: [...moved.tasks, { id: "support", title: "Brief support", done: false }] }
  })

  // Ben moves the bug to the bottom, says which browser it's on, and ticks off
  // the pricing page.
  yield* ReplicatedDocument.update(ben, (board) => {
    const moved = move(board, "login", 3)
    return { ...moved, tasks: moved.tasks.map((task) => task.id === "pricing" ? { ...task, done: true } : task) }
  })
  const title = ReplicatedDocument.text(ben, ["tasks", "login", "title"])
  yield* ReplicatedText.update(title, (text) => `${text} on Safari`)

  // Back online.
  const overlaps = yield* Replica.exchange(ada, ben)

  yield* Console.log(show(yield* ReplicatedDocument.get(ada)))
  // [ ] Write the announcement
  // [x] Update the pricing page
  // [ ] Publish the docs
  // [ ] Brief support
  // [ ] Fix the login bug on Safari    at the bottom, or at the top: wherever
  //                                    it was moved later; the same on both laptops

  yield* Console.log(overlaps)
  // [{ _tag: "Value", path: ["tasks", "login"], replicas: [...], kept: <whoever moved it later> }]
})

program.pipe(Effect.provide(NodeServices.layer), NodeRuntime.runMain)
