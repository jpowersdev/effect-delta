// Turns docgen's markdown (site/.docgen, written by `pnpm docgen` at the repo
// root) into Starlight pages under src/content/docs/api.
//
// docgen writes for Jekyll (just-the-docs): its own front matter, a table of
// contents, category headings at h1 and source links without the module's
// directory. Each page gets Starlight front matter, loses the table of
// contents (Starlight renders one), has its headings moved down a level and
// its source links fixed.
//
// docgen's page for the barrel (index.ts) lists re-exports by file name, so the
// overview is written here instead, from the barrel: its module comment, then its
// modules with their summaries.
//
// docgen formats code with Prettier at 120 columns. Pages are formatted again with
// dprint, like the guides and the source, at 88, the width the content column fits
// without scrolling.
import { execFileSync } from "node:child_process"
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { basename, dirname, join, relative } from "node:path"
import { fileURLToPath } from "node:url"

const site = join(dirname(fileURLToPath(import.meta.url)), "..")
const input = join(site, ".docgen", "modules")
const output = join(site, "src", "content", "docs", "api")
const repo = "https://github.com/jpowersdev/effect-delta/blob/main"
const packageName = "@jpowersdev/effect-delta"

if (!existsSync(input)) {
  console.error(`${relative(process.cwd(), input)} is missing: run \`pnpm docgen\` at the repository root first.`)
  process.exit(1)
}

const files = (dir: string): Array<string> =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
    entry.isDirectory() ? files(join(dir, entry.name)) : entry.name.endsWith(".ts.md") ? [join(dir, entry.name)] : [],
  )

const width = 88

// Prettier leaves comments as they are, so one-line doc comments are wrapped here
const wrapComments = (markdown: string) =>
  markdown.replace(/^( *)\/\*\* (.*) \*\/$/gm, (line, indent: string, text: string) => {
    if (line.length <= width) return line
    const lines: Array<string> = []
    let current = ""
    for (const word of text.split(" ")) {
      if (current !== "" && indent.length + 3 + current.length + 1 + word.length > width) {
        lines.push(current)
        current = word
      } else current = current === "" ? word : `${current} ${word}`
    }
    lines.push(current)
    return [`${indent}/**`, ...lines.map((l) => `${indent} * ${l}`), `${indent} */`].join("\n")
  })

const dprint = join(site, "..", "node_modules", ".bin", "dprint")

const format = (markdown: string) =>
  wrapComments(
    execFileSync(dprint, ["fmt", "--config", join(site, "dprint.json"), "--stdin", "page.md"], {
      input: markdown,
      encoding: "utf8",
    }),
  )

const yamlString = (s: string) => JSON.stringify(s)

const frontMatter = (fields: { title: string; description?: string; order?: number }) =>
  [
    "---",
    `title: ${yamlString(fields.title)}`,
    ...(fields.description === undefined ? [] : [`description: ${yamlString(fields.description)}`]),
    ...(fields.order === undefined ? [] : ["sidebar:", `  order: ${fields.order}`]),
    "editUrl: false",
    "---",
  ].join("\n")

const firstSentence = (paragraph: string) => paragraph.replace(/\s+/g, " ").split(/(?<=\.) /)[0]!

interface Page {
  readonly title: string
  readonly summary: string | undefined
  readonly markdown: string
}

const convert = (file: string, module: string): Page => {
  const text = readFileSync(file, "utf8")
  const body = text.replace(/^---\n[\s\S]*?\n---\n/, "")
  const title = basename(module)

  const lines = body.split("\n")
  const out: Array<string> = []
  let fenced = false
  let skippingToc = false
  for (const line of lines) {
    if (/^(```|~~~)/.test(line)) fenced = !fenced
    if (fenced || /^(```|~~~)/.test(line)) {
      out.push(line)
      continue
    }
    if (line === "## Exports Grouped by Category") {
      skippingToc = true
      continue
    }
    if (skippingToc) {
      if (line.startsWith("# ")) skippingToc = false
      else continue
    }
    if (/^## \S+\.ts overview$/.test(line) || line === "---") continue
    // A category: capitalized, as a section title
    if (line.startsWith("# ")) {
      out.push(`## ${line[2]!.toUpperCase()}${line.slice(3)}`)
      continue
    }
    if (/^#{2,5} /.test(line)) {
      out.push(`#${line}`)
      continue
    }
    out.push(
      // docgen's links don't always name the module's own file
      line.replace(/\[Source\]\(https:\/\/github\.com\/[^)]*?\/src\/[^/#)]+\.ts(#L\d+)?\)/g, (_m, anchor = "") =>
        `[Source](${repo}/src/${module}.ts${anchor})`,
      ),
    )
  }

  const content = out.join("\n").replace(/\n{3,}/g, "\n\n").trim()
  const first = content.split("\n\n")[0]!
  const summary = /^[#`]|^Since /.test(first) ? undefined : firstSentence(first)
  return { title, summary, markdown: `${frontMatter({ title, ...(summary && { description: summary }) })}\n\n${content}\n` }
}

// The barrel's module comment, as markdown
const barrelComment = (source: string): string => {
  const block = source.match(/^\/\*\*([\s\S]*?)\*\//)?.[1] ?? ""
  return block
    .split("\n")
    .map((line) => line.replace(/^\s*\* ?/, ""))
    .filter((line) => !line.startsWith("@"))
    .join("\n")
    .trim()
}

const overview = (pages: ReadonlyMap<string, Page>): string => {
  const source = readFileSync(join(site, "..", "src", "index.ts"), "utf8")
  const intro = barrelComment(source)
  const modules = Array.from(source.matchAll(/^export \* as (\w+) from "\.\/(\w+)\.ts"/gm), ([, namespace, file]) => ({
    namespace: namespace!,
    file: file!,
    page: pages.get(file!),
  }))
  return [
    frontMatter({ title: "Overview", description: firstSentence(intro.split("\n\n")[0]!), order: 0 }),
    "",
    intro,
    "",
    "```ts",
    `import { ${modules.map((m) => m.namespace).join(", ")} } from "${packageName}"`,
    "```",
    "",
    "## Modules",
    "",
    ...modules.map(({ namespace, file, page }) => `- [${namespace}](./${file.toLowerCase()}/): ${page?.summary ?? ""}`),
    "",
  ].join("\n")
}

rmSync(output, { recursive: true, force: true })
mkdirSync(output, { recursive: true })
const pages = new Map<string, Page>()
for (const file of files(input)) {
  const module = relative(input, file).replace(/\.ts\.md$/, "")
  if (module === "index") continue
  const page = convert(file, module)
  writeFileSync(join(output, `${module}.md`), format(page.markdown))
  pages.set(module, page)
}
writeFileSync(join(output, "index.md"), format(overview(pages)))
console.log(`Wrote ${pages.size} API pages and an overview to ${relative(process.cwd(), output)}`)
