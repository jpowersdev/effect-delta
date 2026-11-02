# Vendored: `@effect/jsdocs`

Copied from [Effect-TS/effect](https://github.com/Effect-TS/effect) at commit
`74d19304da`, path `packages/tools/jsdocs`. The package is private and not
published to npm. MIT licensed; see `LICENSE`.

Local changes:

- `package.json`: `effect` and `@effect/vitest` use the npm release `4.0.4`
  instead of `workspace:^`, and `vitest` is pinned to `5.0.3` to match the root
  `vitest`. With the original caret range it would resolve to a newer version.
