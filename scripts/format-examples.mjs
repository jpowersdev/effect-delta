// Puts every object property in the docs' examples on its own line. dprint keeps an object
// on several lines once it's written that way, but can't be told to, so this writes it that
// way: in the README, the site's pages, the source's doc comments and examples/.
//
//   node scripts/format-examples.mjs          rewrite them, then format with dprint
//   node scripts/format-examples.mjs --check  fail if any would change
import { parse } from "@babel/parser"
import { execFileSync } from "node:child_process"
import { globSync, readFileSync, writeFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const check = process.argv.includes("--check")

const options = {
  sourceType: "module",
  plugins: ["typescript"],
  allowAwaitOutsideFunction: true,
  allowReturnOutsideFunction: true
}

const parseCode = (code) => {
  try {
    return { ast: parse(code, options), offset: 0 }
  } catch {
    // A fragment, with `yield*` at its top level.
    const prefix = "function* fragment() {\n"
    return { ast: parse(`${prefix}${code}\n}`, options), offset: prefix.length }
  }
}

const objects = (node, found = []) => {
  if (Array.isArray(node)) {
    for (const child of node) objects(child, found)
  } else if (node && typeof node.type === "string") {
    if (node.type === "ObjectExpression" && node.properties.length > 0) found.push(node)
    for (const key of Object.keys(node)) {
      if (key !== "loc" && key !== "extra" && !key.endsWith("Comments")) objects(node[key], found)
    }
  }
  return found
}

/** The code with each object's properties on their own lines. */
const expand = (code) => {
  const { ast, offset } = parseCode(code)
  const edits = []
  const between = (from, to) => {
    if (!code.slice(from, to).includes("\n")) edits.push({ from, to })
  }
  for (const node of objects(ast.program)) {
    const start = node.start - offset
    const end = node.end - offset
    const properties = node.properties.map((property) => ({
      start: property.start - offset,
      end: property.end - offset
    }))
    between(start + 1, properties[0].start)
    properties.slice(1).forEach((property, index) => {
      const comma = code.indexOf(",", properties[index].end) + 1
      between(comma, property.start)
    })
    const last = properties.at(-1).end
    const tail = code.slice(last, end - 1)
    between(tail.trimStart().startsWith(",") ? code.indexOf(",", last) + 1 : last, end - 1)
  }
  return edits
    .toSorted((a, b) => b.from - a.from)
    .reduce((text, { from, to }) => `${text.slice(0, from)}\n${text.slice(to)}`, code)
}

const dprint = (code, config) =>
  execFileSync(join(root, "node_modules/.bin/dprint"), ["fmt", "--config", config, "--stdin", "example.ts"], {
    cwd: root,
    input: code,
    encoding: "utf8"
  }).trimEnd()

const fence = (prefix) => new RegExp(`^(${prefix})\`\`\`ts([^\\n]*)\\n([\\s\\S]*?)\\n${prefix}\`\`\``, "gm")

const inFences = (text, pattern, config) =>
  text.replace(pattern, (whole, indent, info, body) => {
    // A blank line in a doc comment is ` *`, without the prefix's trailing space.
    const code = body.split("\n")
      .map((line) => line.startsWith(indent) ? line.slice(indent.length) : line.trimEnd() === indent.trimEnd() ? "" : line)
      .join("\n")
    const expanded = expand(code)
    if (expanded === code) return whole
    const formatted = check ? expanded : dprint(expanded, config)
    const lines = formatted.split("\n").map((line) => `${indent}${line}`.trimEnd())
    return `${indent}\`\`\`ts${info}\n${lines.join("\n")}\n${indent}\`\`\``
  })

const update = (file, next) => {
  const path = join(root, file)
  const text = readFileSync(path, "utf8")
  const changed = next(text)
  if (changed === text) return 0
  if (check) console.error(`${file}: an object has properties that aren't on their own lines`)
  else writeFileSync(path, changed)
  return 1
}

const pages = globSync("site/src/content/docs/**/*.{md,mdx}", { cwd: root }).filter((file) => !file.includes("/api/"))
const site = "site/dprint.json"
const count = update("README.md", (text) => inFences(text, fence(" *"), "dprint.json"))
  + pages.map((file) => update(file, (text) => inFences(text, fence(" *"), site))).reduce((a, b) => a + b, 0)
  + globSync("src/*.ts", { cwd: root })
    .map((file) => update(file, (text) => inFences(text, fence(" \\* "), "dprint.json"))).reduce((a, b) => a + b, 0)
  + globSync("examples/*.ts", { cwd: root })
    .map((file) =>
      update(file, (text) => {
        const expanded = expand(text)
        return expanded === text || check ? expanded : `${dprint(expanded, "dprint.json")}\n`
      })
    ).reduce((a, b) => a + b, 0)

if (check && count > 0) process.exit(1)
if (!check) console.log(`Updated ${count} files`)
