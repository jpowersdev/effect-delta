import { defineConfig } from "vitest/config"

export default defineConfig({
  resolve: {
    tsconfigPaths: true
  },
  test: {
    include: ["test/**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**", "test/utils/**"],
    setupFiles: ["./vitest.setup.ts"],
    sequence: { concurrent: true }
  }
})
