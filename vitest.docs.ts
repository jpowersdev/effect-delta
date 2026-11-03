import * as Doctest from "@effect/doctest/Plugin"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vitest/config"

const src = fileURLToPath(new URL("./src", import.meta.url))

export default defineConfig({
  plugins: [Doctest.plugin()],
  resolve: {
    // The site's pages sit outside tsconfig.json, so its paths don't reach them.
    alias: [
      { find: /^@jpowersdev\/effect-delta$/, replacement: `${src}/index.ts` },
      { find: /^@jpowersdev\/effect-delta\/(.*)$/, replacement: `${src}/$1.ts` }
    ]
  },
  test: {
    passWithNoTests: true,
    testTimeout: 10_000,
    include: [],
    includeSource: ["src/**/*.ts", "README.md", "site/src/content/docs/**/*.{md,mdx}"],
    exclude: ["**/node_modules/**", "site/src/content/docs/api/**"]
  }
})
