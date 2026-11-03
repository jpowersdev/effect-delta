// Writes every runnable example in the docs to a directory, as a reader would paste it:
// the fences doctest runs, from the README, the site's pages and the source's doc comments.
import { glob, mkdir, readFile, writeFile } from "node:fs/promises"
import { join } from "node:path"

const [root, target] = process.argv.slice(2)
const fence = /```ts import\.meta\.vitest[^\n]*\n([\s\S]*?)\n\s*(?:\* )?```/g
const pages = ["README.md", "site/src/content/docs/**/*.{md,mdx}", "src/*.ts"]

await mkdir(target, { recursive: true })
let count = 0
for (const page of pages) {
  for await (const file of glob(page, { cwd: root, exclude: (name) => name === "api" })) {
    const text = await readFile(join(root, file), "utf8")
    for (const [, body] of text.matchAll(fence)) {
      const code = file.endsWith(".ts") ? body.replace(/^\s*\* ?/gm, "") : body
      count++
      await writeFile(join(target, `${String(count).padStart(3, "0")}-${file.replace(/\W+/g, "-")}.ts`), `${code}\n`)
    }
  }
}
console.log(`Wrote ${count} examples to ${target}`)
