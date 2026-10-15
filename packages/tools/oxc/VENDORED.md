# Vendored: `@effect/oxc`

Copied from [Effect-TS/effect](https://github.com/Effect-TS/effect) at commit
`74d19304da`, path `packages/tools/oxc`. The package is private and not
published to npm. MIT licensed; see `LICENSE`.

Local changes:

- `package.json`: `@oxlint/plugins` is pinned to `1.87.0` to match the root
  `oxlint`, and `vitest` is pinned to `5.0.3` to match the root `vitest`. With
  the original caret ranges they would resolve to newer versions than the root.
