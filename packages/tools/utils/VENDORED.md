# Vendored: `@effect/utils`

Copied from [Effect-TS/effect](https://github.com/Effect-TS/effect) at commit
`74d19304da`, path `packages/tools/utils`. The package is private and not
published to npm. MIT licensed; see `LICENSE`.

Local changes:

- `package.json`: `effect` and `@effect/platform-node` use the npm release
  `4.0.4` instead of `workspace:^`.
- `tsconfig.json`: removed the project references to `../../effect` and
  `../../platform/node`, which are npm dependencies here.
