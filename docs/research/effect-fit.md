# Effect fit: tooling, conventions, reusable modules

Research notes for phase 1. Source: local clone of Effect v4 at
`/Users/jonathan/github.com/effect-ts/effect` (`packages/effect` 4.0.4, `main` at
`74d19304da`, 2026-10-13). npm `latest` is already 4.0.5. Paths below are
relative to that clone unless they are absolute.

## 1. Overview

Effect v4 is one monorepo. Most of what used to be separate packages
(`@effect/platform`, `rpc`, `cluster`, `experimental`) now lives inside the
`effect` package as subpath modules: `effect/eventlog`, `effect/rpc`,
`effect/socket`, `effect/reactivity`, `effect/persistence`. Only platform-,
provider-, framework-, or technology-specific code stays in separate
`@effect/*` packages (`MIGRATION.md`, "Package Consolidation").

There is precedent for porting an outside library into core. Atom came from
`@effect-atom/atom` (`00a9ef2095 add Atom modules (#853)`), and EventLog came
from `@effect/experimental` (`ab6086ae2c Port EventLog modules (#985)`). Both
first landed under `effect/unstable/*`. Later they moved to top-level subpaths
(`1b4461ec3a Move unstable Effect modules to top-level paths (#8354)`). The
ports left traces behind: `reactivity/Atom.ts:2552` still uses the TypeId
`"~effect-atom/atom/Atom/Serializable"`.

Three things matter most for us:

- **Imports.** Code inside `packages/effect/src` imports with relative paths
  that end in `.ts` (`../Effect.ts`). Separate packages and **all tests** import
  `effect/Effect`. An upstream move needs one find-and-replace on our library
  name plus one mechanical import rewrite (section 4).
- **Unpublished tooling.** Three of the tools we would need to copy are private
  workspace packages that are not on npm: the barrel codegen (`@effect/utils`),
  the custom oxlint rules (`@effect/oxc`), and the JSDoc checker
  (`@effect/jsdocs`).
- **Closest prior art.** The best existing foundation is `Combiner`/`Reducer`
  plus `SchemaAST` walking. `eventlog` is an op-log with server-assigned
  sequence numbers, wall-clock (UUIDv7) ordering, and per-primary-key conflict
  callbacks. It is not a CRDT and it is not peer-to-peer.

## 2. Tooling and layout to mirror

### Versions

| Tool | Version / source |
| --- | --- |
| pnpm | `"packageManager": "pnpm@11.20.0"` (root `package.json`) |
| Node | CI uses `26.4.0` (`.github/actions/setup/action.yml`). Vitest 5 needs Node `^22.12 \|\| ^24 \|\| >=26` |
| TypeScript | `"typescript": "^7.0.2"`, the native Go compiler, plus `"@effect/tsgo": "^0.51.1"`. `prepare` runs `effect-tsgo patch` |
| Type tests | `tstyche ^7.2.5`, run as `tstyche --target '>=5.9'` |
| Test / lint / format | `vitest ^5.0.3`, `vite ^8.3.4`, `oxlint ^1.87.0`, `dprint ^0.61.1`, `@changesets/cli 3.0.3`, Babel 8 |
| Other runtimes | CI also tests on Bun `1.4.0` and Deno `v2.9.4`, and runs `deno check .` |

### Root files

- **`package.json`.** The scripts to copy are `build` (`tsc -b tsconfig.packages.json && pnpm -r --filter "./packages/**/*" run build && node scripts/copy-ai-docs.mjs`), `check` (`tsc -b tsconfig.json`), `test` (`vitest`), `doctest` (`vitest --config vitest.docs.ts`), `test-types`, `lint` (`oxlint -f unix && dprint check`), `lint-fix`, `codegen`, `codemod`, `jsdocs` (`effect-jsdocs`), `circular` (madge), `changeset-version`, and `changeset-publish` (`set-strip-internal → codemod → build → changeset publish --tag staging`).
- **`pnpm-workspace.yaml`.** `packages: [ai-docs, scratchpad, scripts, packages/*, packages/ai/*, packages/atom/*, ...]`. It also sets `verifyDepsBeforeRun: error`, an `allowBuilds` deny-list, `patchedDependencies`, and `overrides`.
- **`tsconfig.json`.** This file contains only references to `tsconfig.packages.json` and `tsconfig.tests.json`.
- **`tsconfig.base.json`.** Key settings: `outDir "${configDir}/dist"`, `rootDir "${configDir}/src"`, `composite`, `incremental`, `target ES2022`, `module NodeNext`, `verbatimModuleSyntax`, `rewriteRelativeImportExtensions` (line 16, rewrites `.ts` to `.js`), `erasableSyntaxOnly` (17), `strict`, `exactOptionalPropertyTypes` (23), `noUnusedLocals/Parameters`, `noImplicitOverride`, `stripInternal: false` (29; release flips it to true), `types: []`. It also loads the `@effect/language-service` plugin with `namespaceImportPackages: ["effect", "@effect/*"]` and an override that makes `apiStabilityLeak` an error outside `internal/`, `index.ts`, and `test/` (line 60).
- **`tsconfig.packages.json`.** One reference per buildable package.
- **`tsconfig.tests.json`.** `noEmit`, and `include` covers every `packages/**/test`, `typetest`, `benchmark`, and `vitest.*.ts`. **`paths` map every package name to its `src`** (`"effect/*": ["./packages/effect/src/*.ts"]`, `"@effect/vitest": [...]`). This is how tests import by package name without a build, and how they avoid dependency cycles.
- **`vitest.config.ts`.** A `project(name, dir, include?, config?)` helper and a shared config: `include: ["test/**/*.test.{ts,tsx}"]`, `setupFiles: [vitest.setup.ts]` (calls `addEqualityTesters()`), `sequence.concurrent: true`, `resolve.tsconfigPaths: true`. Each package registers as one project. Packages have no per-package vitest configs, only an optional `vitest.setup.ts`.
- **`vitest.docs.ts`.** `plugins: [Doctest.plugin()]` (from `@effect/doctest/Plugin`), `include: []`, `includeSource: ["packages/*/src/**/*.ts", "packages/*/*/src/**/*.ts"]`. The plugin pulls every ```` ```ts import.meta.vitest ```` fence out of JSDoc and runs it as a separate module. A trailing `// => expr` asserts the value with `Equal.equals`. Effects and promises are not run automatically, so write `await Effect.runPromise(...)` (`packages/tools/doctest/README.md`).
- **`tstyche.json`.** `testFileMatch: ["packages/*/typetest/**/*.tst.*", "packages/*/*/typetest/**/*.tst.*"]`, `tsconfig: "baseline"`.
- **`.oxlintrc.json`.** Extends `packages/tools/oxc/oxlintrc.json` and loads the `jsPlugins: ["@effect/oxc/oxlint"]` plugin. Test, typetest, and scratchpad directories turn off `no-import-from-barrel-package` and `no-console`. The shared rules are `effect/no-import-from-barrel-package` (patterns `^effect$`, `^effect/(.+/)?[a-z][a-z0-9]*$`, `^@effect/[^/]+$`, plus relative `index` imports), `effect/no-js-extension-imports`, `effect/no-opaque-instance-fields`, `effect/no-unused-internal`, `effect/no-bigint-literals`, `typescript/consistent-type-imports` (`inline-type-imports`), and `typescript/array-type: generic` (write `Array<T>`, not `T[]`).
- **`dprint.json`.** `indentWidth 2`, `lineWidth 120`, `semiColons: "asi"`, `quoteStyle: "alwaysDouble"`, `trailingCommas: "never"`, `arrowFunction.useParentheses: "force"`, with the typescript 0.93.4, markdown 0.20.0, and json 0.21.1 plugins.
- **`.changeset/config.json`.** `changelog-github`, `baseBranch: main`, `updateInternalDependencies: patch`, and one large `fixed` group, so every published package shares a version. We would keep our own config and could not join that group.
- **`jsdocs.config.json`.** Points the private `effect-jsdocs` checker at `packages/**/src/**/*.ts` minus `internal/`. `pnpm jsdocs --check` runs in CI.
- **Docgen.** `@effect/docgen` reads a per-package `docgen.json`, but no package has one checked in today. Atom had one when it was added. The enforced documentation gate is now `jsdocs --check` plus doctest.
- **`deno.json`.** A Deno workspace with a list of exclusions.
- **`.agents/AGENTS.md`.** `scripts/setup-agents.mjs` symlinks it to root `AGENTS.md`. `.agents/skills/*` holds detailed guidance: `jsdocs` (tag order, section order), `package-development` (`registration.md` lists every surface a new package touches), `test-development`, and `changesets`. There is no CONTRIBUTING file.
- **`scripts/`.**
  - `codemod.mjs` runs jscodeshift `scripts/codemods/jsdoc.ts` to fix JSDoc placement before release.
  - `clean.mjs` removes build output.
  - `circular.mjs` runs madge on `packages/*/src`.
  - `set-strip-internal.mjs` flips `stripInternal`.
  - `version.mjs` writes `src/internal/version.ts`.
  - `copy-ai-docs.mjs` copies `LLMS.md` into every published package as `AGENTS.md`/`CLAUDE.md` plus `ai-docs/`, and throws if `files` omits them.
  - `check-dist-types.mjs`, `bundle-*.sh`, and `worktree-setup.sh` cover the rest.
- **Build pipeline.** Each package runs `"build": "tsc -b tsconfig.json && pnpm babel"` with `"babel": "babel dist --plugins annotate-pure-calls --out-dir dist --source-maps"`. Output is **ESM only**: `dist/**/*.js`, `.d.ts`, `.js.map`, and `.d.ts.map`. The commonjs and namespace-from Babel plugins in root devDependencies are unused.
- **Barrel codegen.** The package script is `"codegen": "effect-utils codegen"` (`packages/tools/utils/src/Codegen.ts`). It finds `// @barrel: Auto-generated exports. Do not edit manually.` in an `index.ts` and writes `export * as Module from "./Module.ts"` for every sibling `*.ts`. Each export gets a JSDoc copied from that module's header `@since` and `@stability`. It **fails** if a module lacks a header JSDoc or has anything other than exactly one `@since`.

### Unpublished tooling

`@effect/utils`, `@effect/oxc`, and `@effect/jsdocs` are `"private": true` and
return 404 on npm. `@effect/docgen`, `@effect/doctest`, `@effect/vitest`, and
`@effect/tsgo` are published. To mirror the lint and codegen setup exactly, we
would have to vendor those three packages (MIT) or use git dependencies.

### Per-package layout

Separate package (`packages/atom/react`, `packages/vitest`):

```
package.json  tsconfig.json  README.md  LICENSE  CHANGELOG.md  [vitest.setup.ts]
src/Module.ts  src/index.ts  src/internal/*.ts
test/*.test.ts
typetest/*.tst.ts
```

`package.json` (atom/react):

```json
"type": "module",
"sideEffects": [],
"exports": { "./package.json": "./package.json", ".": "./src/index.ts", "./*": "./src/*.ts",
             "./internal/*": null, "./index": null, "./*/index": null },
"files": ["src/**/*.ts", "dist/**/*.js", "dist/**/*.js.map", "dist/**/*.d.ts", "dist/**/*.d.ts.map",
          "AGENTS.md", "CLAUDE.md", "ai-docs/**/*"],
"publishConfig": { "access": "public", "provenance": true,
                   "exports": { ".": "./dist/index.js", "./*": "./dist/*.js", "./internal/*": null, ... } },
"peerDependencies": { "effect": "workspace:^" }, "devDependencies": { "effect": "workspace:^" }
```

In development the exports point at `.ts` source, and `publishConfig.exports`
swaps in `dist`. The package `tsconfig.json` is
`{ "extends": "../../../tsconfig.base.json", "include": ["src"], "references": [{ "path": "../../effect" }] }`.
Tests are not part of the package tsconfig. They belong to the root
`tsconfig.tests.json`.

Compare the subpath module `packages/effect/src/eventlog/`:

- 14 modules plus `index.ts` (barrel-generated) and `internal/`.
- Tests live in `packages/effect/test/eventlog/*.test.ts`.
- `packages/effect/package.json:41` adds `"./eventlog": "./src/eventlog/index.ts"`. Deep imports such as `effect/eventlog/EventLog` resolve through `"./*": "./src/*.ts"` (line 56).
- `./internal/*` is blocked at the root, and so are `./cli/internal/*` and `./cluster/internal/*`. **`eventlog/internal/*` is not blocked**, so an upstreamed `delta/` would need its own `null` entry.
- `effect` has no runtime `dependencies` at all.

Atom/react's `index.ts` is hand-written flat `export * from "./Hooks.ts"`, while
core subpaths use namespaced barrels. Every module in a package other than
`effect` must be `@stability unstable` (`MIGRATION.md`).

## 3. Module and doc conventions

Rules from `.agents/skills/jsdocs/*`, as applied across `src`:

- **Module header.** The first JSDoc in a file describes the module in plain
  prose, followed by required `@stability` and `@since` tags. Every module needs
  one, or barrel codegen fails.
- **Declarations.** A declaration starts with a single paragraph that opens with
  a present-tense verb ("Creates", "Returns"). Optional sections follow, at most
  once each, in the order `**When to use**` (must start "Use to/when/as/with"),
  `**Details**`, `**Gotchas**`. Then come zero or more
  `**Example** (Unique title)` blocks, each with exactly one
  ```` ```ts import.meta.vitest ```` fence. There are 3,440 such fences in `src`
  and only about a dozen plain `ts` fences. `@example` tags are not used.
- **Tag order.** `@deprecated`, `@default`, `@see`, `@stability`, `@category`,
  `@since`.
  - `@stability` is `stable`, `unstable` (may break in minor releases), or
    `experimental` (may break in patch releases).
  - Root declarations need exactly one lowercase plural `@category`, such as
    `constructors`, `models`, `guards`, `combinators`, `services`, `layers`,
    `errors`, `type IDs`, `schemas`, `combining`, or `folding`.
  - Members of a declaration may carry `@since` but not `@category`.
- **TypeId.** TypeIds are string literals with a `~` prefix, not symbols:
  `const TypeId = "~effect/Chunk"` (`Chunk.ts:35`) and
  `export const TypeId: TypeId = "~effect/eventlog/Event"` together with
  `export type TypeId = "~effect/eventlog/Event"` (`eventlog/Event.ts:26-35`).
  Core collections keep the TypeId private; unstable subpath modules export it.
  `Equal.symbol` is `"~effect/Equal"` and `Hash.symbol` is `"~effect/Hash"`.
- **String identifiers.** Service keys, Schema class identifiers, and
  annotation keys follow the module path. Examples:
  `Context.Service<...>()("effect/eventlog/EventJournal")`,
  `Schema.Class<Entry>("effect/eventlog/EventJournal/Entry")`, and
  `"~effect/encoding/SchemaBinary/fieldId"`. RPC tags look like
  `"EventLog.Changes"`.
- **Prototype objects.** Core builds a proto object holding `[TypeId]`, `pipe`
  (via `pipeArguments`), `toJSON`, `toString`, `[NodeInspectSymbol]`,
  `[Equal.symbol]`, and `[Hash.symbol]`, then calls `Object.create(Proto)`
  (`Chunk.ts:194-235`). Internal shortcuts such as `PipeInspectableProto`
  (`internal/core.ts:66`) cannot be imported from outside the package. A
  separate package must use `Pipeable.pipeArguments`, `Inspectable.BaseProto`
  or `Inspectable.Class`, and `Formatter.format`.
- **Naming.**
  - Constructors: `make`, `makeX`, `empty`, `fromX`.
  - Unsafe variants put `Unsafe` last: `makeUnsafe`, `getUnsafe`,
    `makeEntryIdUnsafe`.
  - Guards are `isX = (u: unknown): u is X => hasProperty(u, TypeId)`.
  - Layers: `layer`, `layerMemory`, `layerIndexedDb`, `layerSql`.
  - Pipeable combinators use `dual(arity, impl)` with data-last signatures
    first (`TxRef.ts:158-177`).
- **Errors.** In-process errors use `Data.TaggedError("X")<{...}>` (147 uses),
  often with a `method` and `cause` field. Errors that cross the wire use
  `Schema.TaggedError<X>()("X", fields)` (35 uses) or
  `Schema.Error<X>("effect/socket/Socket/SocketReadError")({...})` (72 uses).
- **Services.** Services are defined as
  `class X extends Context.Service<X, Shape>()("key") {}` (188 uses) or
  `const X = Context.Service<Shape>("key")` (103 uses). `ServiceMap` was the
  older name and was renamed to `Context`. Configuration with a default value
  uses `Context.Reference<A>("key", { defaultValue })`. Layers are built with
  `Layer.effect(X, make)` or `Layer.effect(X)(make)`, and `Layer.succeed(X)(v)`.
- **Internal code.** Implementation lives in `src/internal/*.ts` or
  `src/<subdir>/internal/*.ts`. Declarations there carry `/** @internal */`;
  lint enforces that they are used, and the export map blocks them.
- **Tests.** Use `it.effect` and `assert` from `@effect/vitest`, not `expect`.
  Use `TestClock` for anything time-dependent. Never call `Effect.runSync` in a
  test.

Annotated skeleton of how one of our modules would look as a separate package:

```ts
/**
 * Registers that keep the value with the greatest stamp. ...prose, no template...
 *
 * @stability unstable
 * @since 0.1.0
 */
import * as Equal from "effect/Equal"            // never "effect" (barrel lint)
import { format } from "effect/Formatter"
import { dual } from "effect/Function"
import * as Hash from "effect/Hash"
import { NodeInspectSymbol } from "effect/Inspectable"
import { type Pipeable, pipeArguments } from "effect/Pipeable"
import { hasProperty } from "effect/Predicate"
import * as internal from "./internal/register.ts"  // relative + .ts

const TypeId = "~effect-delta/Register"         // becomes "~effect/delta/Register"

/**
 * A value tagged with the stamp of the write that produced it.
 *
 * @stability unstable
 * @category models
 * @since 0.1.0
 */
export interface Register<out A> extends Pipeable, Equal.Equal {
  readonly [TypeId]: typeof TypeId
  readonly value: A
}

const Proto = {
  [TypeId]: TypeId,
  pipe() { return pipeArguments(this, arguments) },
  toJSON(this: Register<unknown>) { return { _id: "Register", value: this.value } },
  toString(this: Register<unknown>) { return format(this.toJSON()) },
  [NodeInspectSymbol](this: Register<unknown>) { return this.toJSON() },
  [Equal.symbol](this: Register<unknown>, that: unknown) { return isRegister(that) && Equal.equals(this.value, that.value) },
  [Hash.symbol](this: Register<unknown>) { return Hash.hash(this.value) }
}

/**
 * Returns `true` when a value is a `Register`.
 *
 * @stability unstable
 * @category guards
 * @since 0.1.0
 */
export const isRegister = (u: unknown): u is Register<unknown> => hasProperty(u, TypeId)

/**
 * Creates a register holding `value`.
 *
 * **Example** (Creating a register)
 *
 * ```ts import.meta.vitest
 * import { Register } from "effect-delta"
 *
 * Register.make(1).value // => 1
 * ```
 *
 * @stability unstable
 * @category constructors
 * @since 0.1.0
 */
export const make = <A>(value: A): Register<A> => Object.assign(Object.create(Proto), { value })

/**
 * Returns the register whose write wins under this module's ordering.
 *
 * @stability unstable
 * @category combining
 * @since 0.1.0
 */
export const join: {
  <A>(that: Register<A>): (self: Register<A>) => Register<A>
  <A>(self: Register<A>, that: Register<A>): Register<A>
} = dual(2, <A>(self: Register<A>, that: Register<A>): Register<A> => internal.join(self, that))
```

## 4. Import layout and upstreaming recommendation

Import styles found in the clone:

| Where | Imports core as | Imports siblings as |
| --- | --- | --- |
| `packages/effect/src/**` | `../Effect.ts`, `../../Schema.ts` | `./EventJournal.ts`, `./internal/x.ts` |
| Separate packages (`atom/react`, `sql/pg`) | `effect/Effect`, `effect/reactivity/Atom` | `./Hooks.ts` |
| **All tests**, including core's own | `effect/Effect`, `effect/eventlog/EventJournal` (via `tsconfig.tests.json` paths) | the same |
| Doc examples | `import { Effect } from "effect"`, `import { SchemaBinary } from "effect/encoding"` | the same |

Inside `src`, barrel imports are lint errors. Only `effect/<PascalModule>` or
`effect/<subdir>/<PascalModule>` is allowed. Tests and examples may import
barrels.

**Recommendation:** publish one unscoped package, `effect-delta`, that maps
one-to-one onto a future `effect/delta` subpath. The name is free on npm and
`effect/delta` does not collide with any current subpath. Concretely:

1. **Layout.** `packages/delta/src/<PascalModule>.ts`, a generated `src/index.ts`
   barrel using `// @barrel` and `export * as X`, matching core subpaths rather
   than atom/react's flat re-exports, `src/internal/`, `test/`, and `typetest/`.
2. **The library name is the only namespace token.** Pick every string so that
   the single replacement `effect-delta` → `effect/delta` produces the core form:
   - TypeIds: `"~effect-delta/Text"` → `"~effect/delta/Text"`
   - Service keys: `"effect-delta/SyncEngine"` → `"effect/delta/SyncEngine"`
   - Schema class identifiers and annotation keys follow the same pattern.
   - Imports in tests and examples: `effect-delta/Text` → `effect/delta/Text`,
     and `from "effect-delta"` → `from "effect/delta"`.

   Atom's `~effect-atom/...` leftover is exactly the failure to avoid. Do not use
   `@docustore/...`-style keys.
3. **Core imports in `src`** become a second mechanical rewrite on upstream:
   `from "effect/(X)"` → `from "../$1.ts"`. Because barrel imports are banned,
   every import already names a single module. Sibling and internal relative
   imports do not change.
4. **Public API only.** `effect/internal/*` is blocked, so we cannot reach for
   internal helpers such as `PipeInspectableProto` or the UUID helpers. After
   upstreaming we may optionally switch to them.
5. **Other steps that are not find-and-replace:**
   - `@since 0.x` → `@since 4.N.0` (one regex).
   - Add `"./delta"` and `"./delta/internal/*": null` to the effect export map.
   - Move tests to `packages/effect/test/delta/`.
   - Drop our own `package.json`, `tsconfig`, and changeset config.

Rejected alternative: shim files such as `src/Effect.ts` containing
`export * from "effect/Effect"`, so that our `src` could import `../Effect.ts`
verbatim. It removes step 3, but it adds unusual files and confuses module
augmentation. It is not worth the strangeness.

## 5. Existing modules

### Differ (`Differ.ts`, stable)

In v4 this is **only an interface** (34 lines):
`interface Differ<in out T, in out Patch> { empty; diff(old, new); combine(first, second); patch(old, patch) }`
(`Differ.ts:29-34`). The v3 combinators were merged into Schema
(`53c3e0cb4f`). The only implementation is `Schema.toDifferJsonPatch(schema)`
(`Schema.ts:16927`; `internal/schema/toDifferJsonPatch.ts`). It encodes values
to JSON, takes the patch from `JsonPatch.get`, implements `combine` as array
concatenation, and decodes after applying.

`Differ.combine` is **sequential composition**: apply first, then second. It is
not commutative. A CRDT join is commutative, so the two operations must not be
confused.

**Verdict: extend (optional).** Our types could expose a `Differ<Doc, Delta>`
for local diff-and-apply, but the merge rule should be a `Combiner` (see below).

### JsonPatch and JsonPointer (stable)

- `JsonPatch` is a deterministic subset of RFC 6902 (`add`, `remove`,
  `replace`). `get(old, new)` and `apply(patch, doc)` never mutate their input.
- **Arrays are compared by index**, with no move detection (Gotchas at
  `JsonPatch.ts:~150`). That makes it useless for concurrent text edits.
- `JsonPointer` provides `escapeToken`, `unescapeToken`, and URI-fragment
  helpers.

**Verdict: avoid for merging; reuse for interop/debug** (for example, rendering
a delta as a JSON Patch for inspection).

### eventlog/* (unstable)

| Module | Role |
| --- | --- |
| `Event` | `Event.make({ tag, primaryKey, payload, success, error })` defines an event and derives a `SchemaBinary` payload codec |
| `EventGroup` | Groups events into a union |
| `EventJournal` | Service with `entries`, `write`, `writeFromRemote`, `withRemoteUncommited`, `nextRemoteSequence`, `changes` (a `PubSub.Subscription<Entry>`), and `withLock(storeId)` |
| `EventLog` | Service `write({ schema, event, payload })` plus `Registry`, `Identity`, `group(...)` handler layers, `groupCompaction`, `groupReactivity`, and `CurrentStoreId` |
| `EventLogRemote` | `changes({ startSequence })`, which returns a `Queue` of `RemoteEntry`, plus `write` and `whenAuthenticated` |
| `EventLogMessage` | RPC protocol: `HelloRpc`, `AuthenticateRpc`, `WriteChunkedRpc`, `WriteSingleRpc`, `ChangesRpc`, grouped as `EventLogRemoteRpcs` |
| `EventLogServer*`, `Sql*` | Relay server and SQL storage, encrypted (AES-GCM keyed by identity) or unencrypted |
| `EventLogEncryption`, `EventLogSessionAuth` | WebCrypto encryption and Ed25519 challenge auth |

`EventJournal.Entry` is a `Schema.Class` with fields
`{ id: EntryId (UUIDv7 bytes), event, primaryKey, payload: Uint8Array }`
(`EventJournal.ts:298`). Ordering is by the wall clock embedded in the UUIDv7.

On import, the memory journal (`EventJournal.ts:395-540`):

- drops duplicates by id;
- runs optional compaction;
- collects local entries with the **same event tag and primaryKey** that are
  newer than the incoming entry and passes them to the handler as `conflicts`;
- re-sorts by `createdAtMillis`.

Remotes track a server-assigned `remoteSequence`. The model is therefore an
op-log relayed through a server that assigns sequence numbers. There are no
version vectors, no causal ordering, and no peer-to-peer transport. Handlers
must resolve conflicts by hand. The eventlog modules contain no doc examples.

**Verdict: avoid as the core; possibly adapt.** We could ship a thin adapter
that carries our deltas as `Event` payloads through `EventLogServer` for
server-relayed apps. Its encryption and auth are worth copying in spirit. Its
vocabulary (Entry, Remote, Identity, StoreId, conflicts, compaction) is claimed.

### Tx* and SubscriptionRef

- **`TxRef`** is STM. Reads and writes inside `Effect.tx` are recorded in
  `Effect.Transaction`'s `journal` and conflict on `version`
  (`TxRef.ts:21-70`, `Effect.ts:14804`). The family also includes `TxHashMap`,
  `TxChunk`, `TxQueue`, `TxPubSub`, and `TxSubscriptionRef`.
- **`SubscriptionRef`** has `make`, `get`, `update`, `modify`, `updateEffect`,
  and `changes: Stream<A>`, which emits the current value first and then every
  committed update, with updates serialized.

**Verdict: reuse `SubscriptionRef`** as the in-process holder of replica state.
The docustore prototype did this, using `SubscriptionRef.modify` to return
overlaps from a merge. **Avoid Tx\*** unless replicas must update atomically
together with other STM state.

### reactivity/ (unstable)

`Reactivity` is process-local invalidation by key, with `mutation`, `query`,
`stream`, and `invalidate`. EventLog uses it through `groupReactivity`.

`Atom`, `AtomRegistry`, `AtomRef`, and `AsyncResult` are the upstreamed
effect-atom. `Atom.subscriptionRef(ref)` exists, and so does `Atom.kvs`, which
is backed by `KeyValueStore`.

**Verdict: integrate, do not depend.** Expose `Stream` and `SubscriptionRef`.
An `Atom` adapter is one line, and the effect-yjs precedent of one atom per
path is a nice-to-have.

### Optic (stable)

`Iso`, `Lens`, `Prism`, `Optional`, and `Traversal`, with
`makeLens/makePrism/...`, `get/replace/modify/getAll`, `id`, `some`, and
`success`. `Schema.toIso` connects schemas to optics.

**Verdict: maybe reuse later** for path-addressed edits (`edit(doc, lens, f)`).
It does not help with merging.

### Schema: data types, tagged unions, classes, SchemaBinary

- **Tagged data and errors.** `Schema.TaggedStruct(tag, fields)`,
  `Schema.Union([...])`, `Schema.TaggedUnion({ A: {...}, B: {...} })` (with
  `.match`, `.guards`, and `.cases`), `Schema.Class<Self>(id)(fields)`,
  `Schema.TaggedClass`, and `Schema.TaggedError<Self>()(tag, fields)`. Use
  `Schema.Opaque` and `Schema.brand` for ids.
- **`encoding/SchemaBinary`** (unstable, 5,279 lines) is **not MessagePack**.
  MessagePack is gone from v4.0.4.
  - It is Effect's own compact format derived from a schema's encoded side.
    `toCodec(schema, { fingerprint? })` returns a `Codec<T, Uint8Array>`.
  - The default mode tolerates schema evolution. Fingerprint mode is positional
    with an 8-byte layout hash.
  - It provides `fieldId(n)` annotations, stream `parser` and `encoder`
    functions, and `RpcSerialization.layerSchemaBinary`.
  - Encoded bytes are views into a shared arena; call `.slice()` before keeping
    them.
- **`effect/encoding`** also has Base64, Base64Url, Hex, and Ndjson.

**Verdict: reuse.** Define every wire type (deltas, version vectors) as a
Schema and derive JSON and `SchemaBinary` codecs from it. Caveat: SchemaBinary
is unstable, so pin our wire-format guarantees to our own version tag rather
than Effect's (see the LiveStore lesson in section 7).

### SchemaAST and annotations (stable)

This is how a library reads a custom annotation and walks a schema, using the
pattern from `internal/schema/toEquivalence.ts`:

- **Declaring a key.** Write annotations with `schema.annotate({ [key]: v })`.
  For type safety, declare the key by augmenting the module, as documented at
  `Schema.ts:17134-17180`:

  ```ts
  declare module "effect/Schema" {
    namespace Annotations {
      interface Annotations { readonly x?: X | undefined }
    }
  }
  ```

  Built-in internal keys use the `~` prefix (`"~structural"`,
  `"~effect/encoding/SchemaBinary/fieldId"`).
- **Reading.** `SchemaAST.resolve(ast)` (`SchemaAST.ts:5134`) returns the
  annotations of the **last check** if any exist, otherwise the node's own
  annotations. `resolveAt(key)(ast)` reads one key, and
  `Schema.resolveAnnotations(schema)` does the same from a schema.
- **Walking.**
  - Call `SchemaAST.toType(ast)` (`:4704`, memoized) to get the type side.
  - Then switch on `ast._tag`, or use the guards `isObjects`, `isArrays`,
    `isUnion`, `isSuspend`, `isDeclaration`, and `isLiteral` (`:94-403`).
  - `Objects` has `propertySignatures: ReadonlyArray<{ name, type }>` and
    `indexSignatures: ReadonlyArray<{ parameter, type }>` (`:2652`, `:2742`,
    `:2816`). Records are `Objects` with one index signature.
    `SchemaAST.isOptional(ps.type)` gives optionality, and
    `getIndexSignatureKeys(value, parameter)` lists the keys at runtime.
  - `Union` nodes use `getCandidateIndex(types)` for dispatch.
  - `Suspend.thunk()` is resolved lazily.
  - A `Declaration` (Option, Map, Set, Graph) is identified by
    `annotations.representation.id` (`"effect/schema/Option"`, and so on) and
    `typeParameters`.
- **Override hook.** The built-in derivers first look for their own override
  annotation (`toEquivalence`, `toCodec*`, `toFormatter`, `arbitraryConstraint`).
  A merge-rule deriver should offer the same escape hatch (`toCombiner`-style).

**Verdict: reuse.** This is the foundation for deriving merge rules from an
ordinary Schema with a `text` annotation. The docustore prototype's
`internal/plan.ts:15-27` does exactly this walk: `toType` → `resolve` →
`isObjects` → struct, record, or value plan.

### Schema-derived algebra

| Function | Location | What it derives |
| --- | --- | --- |
| `Schema.toEquivalence(schema)` | `Schema.ts:16382` | Structural `Equivalence`. Override with `Schema.overrideToEquivalence` |
| `Arbitrary.schema(schema)` | `Arbitrary.ts:416` | Generator with shrinking |
| `Schema.toFormatter`, `toIso`, `toCodecJson`, `toCodecIso`, `toDifferJsonPatch`, `toRepresentation`, `toJsonSchemaDocument`, `toStandardSchemaV1` | `Schema.ts` | Formatting, optics, codecs, diffs, and schema documents |

**There is no `Schema.toOrder`, `toCombiner`, or `toReducer`.** A schema-derived
merge is therefore new ground, and a `toCombiner`-style deriver with an
override annotation is the shape most likely to be accepted upstream.

### Combiner and Reducer (stable)

`Combiner<A> = { combine(self, that) }`. The docs promise nothing about laws:
"Any purity, associativity, or mutation behavior comes from that function"
(`Combiner.ts:65`). Formally it is a magma that is intended to be a semigroup.
Its constructors are `make` (82), `flip` (114), `min`/`max(order)` (148/182),
`first`/`last` (212/242), `constant` (275), and `intercalate` (310).

**Gotcha:** `max` returns `that` when the order reports a tie (`:182`). Two
distinct values that compare equal break commutativity. A last-writer-wins
stamp order must therefore be total, for example with a replica-id tiebreak.

`Reducer<A> extends Combiner<A>` and adds `initialValue` and
`combineAll(iterable)`. That makes it a monoid, again without stated laws.
`Reducer.make(combine, initial, combineAll?)` and `Reducer.flip` complete the
module.

Helpers elsewhere:

- **Structs and tuples:** `Struct.makeCombiner(fields, { omitKeyWhen })` and
  `Struct.makeReducer` (`Struct.ts:909/967`); `Tuple.makeCombiner` and
  `Tuple.makeReducer`.
- **Records:** `Record.makeReducerUnion(combiner)` merges per key and keeps
  keys from both sides (`Record.ts:1507`). `makeReducerIntersection` folds to
  `{}` (documented gotcha).
- **Optional values:** `Option.makeReducer(c)` and `UndefinedOr.makeReducer(c)`,
  where an absent side is the identity, plus their `*FailFast` variants.
  `UndefinedOr.makeReducer` is exactly the hand-written `optional` lifter in
  docustore `plan.ts:128`.
- **Ordering and equivalence:** `Order.makeReducer` and
  `Equivalence.makeReducer`.
- **Instances:** `Number.ReducerSum`/`Max`/`Min`, `String.ReducerConcat`,
  `Boolean.ReducerAnd`/`Or`, `BigInt.CombinerMax`, `Duration.CombinerMax`,
  `Array.makeReducerConcat`, and `Newtype.makeCombiner`.

**No law-testing helpers exist.** `test/Combiner.test.ts` only checks fixed
examples, and nothing in `src` or `@effect/vitest` checks associativity,
commutativity, or idempotence.

**Verdict: reuse as the merge abstraction.** A join-semilattice is a
`Combiner` that is also commutative and idempotent, and a delta-state CRDT is a
`Combiner<Delta>` where a full state is a delta. We would write our own law
suite. A generic version of it (`Combiner` laws via `Arbitrary`) could be
upstreamed on its own.

### Property testing: Arbitrary and @effect/vitest

v4 has its **own** property-testing engine in `Arbitrary.ts` (unstable, not
fast-check). `Arbitrary.schema(S)`, `all`, `map`, `flatMap`, `filter`,
`Constant`, `checkEffect`, and `sampleEffect` cover generation and checking,
with integrated shrinking and replay tokens.

`@effect/vitest` adds `it.prop(name, arbs, fn, { arbitrary: { runs, seed } })`
and `it.effect.prop(...)`. `arbs` is an array or record of Schemas or
Arbitraries (`packages/vitest/src/index.ts:63-180`). Generators from other
property-testing libraries are not accepted.

`packages/effect/ARBITRARY.md` is a long guide. Its sections on laws
(commutativity, associativity, idempotence) and on "Comparing with a Simple
Model", which uses generated command sequences, are directly relevant to
convergence testing. `testing/TestSchema` asserts schema round-trips.

**Verdict: reuse.** Use generated concurrent edit histories, apply them in
random orders and groupings, and assert that the results match. Use Schema to
describe edit commands.

### persistence/KeyValueStore (unstable)

A service with `get`/`getUint8Array`, `set(key, string | Uint8Array)`,
`remove`, `clear`, `size`, `modify`, and `has`. It ships `prefix`,
`toSchemaStore(store, schema)`, and the layers `layerMemory`,
`layerFileSystem`, `layerStorage` (Web Storage), and `layerSql`. Browser
IndexedDB and Persistence support live in `@effect/platform-browser`.

**Verdict: reuse** as the default persistence adapter for snapshots and pending
deltas.

### socket/ and rpc/ (unstable)

- **`Socket`** has a pull-based `reader` with backpressure and a scoped
  `writer`. Every close surfaces as a `SocketError`, so reconnecting is just
  `Effect.retry`. `makeWebSocket`, `toChannel`, and `SocketServer` round it out.
- **`Rpc.make(tag, { payload, success, error, stream })`** is used with
  `RpcGroup.make(...)`, `RpcServer` and `RpcClient` protocols (HTTP, WebSocket,
  socket, worker, stdio), `RpcSerialization.layerSchemaBinary`/`layerJson`, and
  `RpcTest` for in-memory use.
- There is **no WebRTC or BroadcastChannel transport** anywhere in the repo.

**Verdict: reuse.** Define the sync protocol as an `RpcGroup`, as EventLog
does, so that any `Socket` can carry it. Peer-to-peer needs a custom `Socket`
adapter.

### Clock and TestClock

`Clock` is a `Context.Reference` with `currentTimeMillis`, `currentTimeNanos`,
and `monotonicTimeNanos`. Wall-clock and monotonic semantics were split in
`d0f1a22951`. `testing/TestClock` provides `adjust`/`setTime`. Under `it.effect`
the clock starts at 0.

**Verdict: reuse** as the physical-time source for any hybrid logical clock.
Do **not** name our logical clock `Clock`. Tests will see equal wall times, so
stamp tiebreaks matter.

### Random, PubSub, Stream, Data, Equal, Hash

- **`Random.withSeed`** gives deterministic tests, and `Random.shuffle` can
  produce delivery orders.
- **`PubSub`** (bounded, sliding, unbounded, with replay) and **`Stream`**
  (`fromPubSub`, `changes`) carry local change feeds and outbound delta streams.
- **`Data.TaggedError`** and **`Data.taggedEnum`** suit in-process errors and
  ADTs.
- **`Equal.equals`** and **`Hash`** provide structural equality and hashing for
  our values.

**Verdict: reuse all.**

### HashMap, Chunk, Trie, Graph, MutableList

- **`HashMap`** and **`Chunk`** are persistent collections with structural
  equality. They are candidates for replica state if plain objects prove too
  slow.
- **`Trie`** is a string-keyed prefix tree. It is not a sequence index.
- **`Graph`** (unstable) supports directed and undirected graphs, scoped
  mutation, `toSnapshot`/`fromSnapshot`, `Schema.Graph`, set operations, and
  traversals. It could model a causal change DAG.
- **`MutableList`** is a mutable queue that drains from the front. It is not
  suitable for text.

**Verdict: reuse HashMap/Chunk/Graph where useful.** None of them is a
sequence-CRDT tree, so we need our own text structure.

### workers/ and cluster/ (brief)

`workers/Worker` (platform-neutral workers) could move merging off the main
thread. `cluster/` is server-side sharding: Entity, Sharding, and `Snowflake`
(a sortable bigint id from timestamp, machine id, and sequence).

**Verdict: avoid for now.**

### Other relevant modules

- **`Crypto.randomUUIDv7`** is a public service. Use it for replica ids instead
  of the internal UUID helpers.
- **`HashRing`** and **`PrimaryKey`** offer consistent hashing and stable keys.
- **`Brand`** and **`Newtype`** suit ids and counters.
- **`Order`** supports total stamp orders (`Order.mapInput`, `Order.combine`).

## 6. Vocabulary already claimed by Effect

| Term | Effect meaning | Guidance |
| --- | --- | --- |
| `Differ`, `diff`, `patch`, `Patch` | `Differ<T, Patch>`: `diff`, sequential `combine`, `patch(old, p)`; `JsonPatch` | Use only in Differ's sense. Call our merge unit something else (`Delta` is free; only `ai/Response.TextDeltaPart` exists) |
| `combine`, `combineAll`, `Combiner`, `Reducer`, `initialValue` | Semigroup and monoid-shaped operations | Align: our join *is* `combine` |
| `merge`, `mergeAll` | Concurrency (`Stream.merge`, `Layer.merge`, `Effect.merge`, `Channel.merge`) | A namespaced `Text.merge` is legal but ambiguous; prefer `combine` or `join` |
| `sync` | `Effect.sync` lifts a thunk | **Avoid** `Replica.sync` and similar names |
| `replicate` | `Array/Effect/Iterable.replicate` repeat n times | Avoid as a verb. "Replica" and "replication" appear only in eventlog prose |
| `Event`, `EventGroup`, `EventLog`, `EventJournal`, `Entry`, `EntryId`, `RemoteEntry`, `RemoteId`, `Remote`, `StoreId`, `Identity`, `Registry`, `compaction`, `conflicts` | eventlog | Do not reuse for different concepts. `Identity` there means a key pair |
| `Journal`, `Transaction`, `tx`, `version`, `retry`, conflict | STM (`Effect.Transaction.journal`, `TxRef.version`) | Avoid "journal" and "transaction" for our log |
| `Ref` family | `Ref`, `SynchronizedRef`, `SubscriptionRef`, `TxRef`, `ScopedRef`, `RcRef`, `LayerRef`, `MutableRef`, `AtomRef` | A `*Ref` name implies a mutable cell; use one only if ours is one |
| `changes` | `SubscriptionRef.changes` (stream of values), `EventJournal.changes`, `Stream.changes` (drop consecutive duplicates), `ChangesRpc` | If we expose `changes`, make it a `Stream` of values, like `SubscriptionRef` |
| `Clock` | Wall and monotonic time service | Use `LogicalClock`, `HybridClock`, `Stamp`, or `VersionVector` |
| `Version` | `effect/Version` (package version), `TxRef.version` | Prefer `VersionVector` or `Frontier` |
| `Snapshot`, `snapshot` | `Graph.Snapshot`, `Metric.snapshot`, `TxHashMap.snapshot` | Acceptable inside our namespace; be aware of the overlap |
| `Document` | JsonSchema, SchemaRepresentation, MCP | Mild; `Doc` is free |
| `compact` | Filter out `None` (`Chunk`/`HashMap.compact`); eventlog compaction | Avoid for garbage-collecting tombstones |
| `Cursor` | Graph traversal | Fine for text cursors if namespaced |
| Free terms | `Delta`, `Anchor`, `Tombstone`, `Lamport`, `Peer`, `Actor`, `Site`, `Presence`, `Awareness`, `Text` (no top-level module) | — |

## 7. Prior art

### In the Effect repo

- **No mentions** of CRDT, Yjs, Automerge, collaboration, Lamport clocks, or
  local-first in code, docs, or changesets. "Replica" and "replication" appear
  only in eventlog prose (`EventLog.ts:5`, `EventLogRemote.ts:2`,
  `EventJournal.ts:2`).
- **Relevant git history:**
  - JsonPatch: `b9c866b60c add JsonPatch module (#844)`, `26ccf093a7 Refactor JsonPatch internals`.
  - Differ: `53c3e0cb4f merge Differ module into Schema (#659)`.
  - EventLog: `ab6086ae2c Port EventLog modules (#985)`, `9dd58eaa3e port sql eventlog modules`, `5c3af6d554 Eventlog unencrypted`, `52262be2ed Bind event log RPCs to authenticated identities`, `3437e21a56` and `eaa7e71b88` (memory journal conflict-scan fixes).
  - Atom and paths: `00a9ef2095 add Atom modules (#853)`, `1b4461ec3a Move unstable Effect modules to top-level paths (#8354)`.
  - Graph and Clock: `189b003a23 Add Schema.Graph and Graph.fromSnapshot`, `d0f1a22951 Split wall and monotonic clock semantics`.
  - Nothing CRDT-specific.

### On the web

- **[effect-yjs](https://github.com/hzmmohamed/effect-yjs)** (npm `effect-yjs`
  0.3.0, depends on `effect ^3.17`, `@effect-atom/atom ^0.5`, and `yjs`). It
  **wraps** Yjs rather than implementing CRDTs. It walks the Schema AST:
  `Struct`/`Record` → `Y.Map`, `Array` → `Y.Array`, and a `YText` marker →
  `Y.Text`. A lens per path exposes an `.atom()` for fine-grained reactivity.
  This confirms that the "Schema with a text marker" API shape is intuitive. It
  is Effect 3, not v4.
- Nothing else was found: `effect-crdt` does not exist on npm, and no Discord
  discussions surfaced through web search.

### LiveStore

[LiveStore](https://docs.livestore.dev/sustainable-open-source/contributing/monorepo/)
is event-sourced on SQLite.

- **Code structure.** It is a pnpm `workspace:*` monorepo.
  `@livestore/utils` re-exports and **extends** Effect modules (adds members to
  `Effect.*` and `Schema.*`) behind `@livestore/utils/effect`, so other packages
  avoid a direct `effect` dependency. It pins Effect (4.0.3), uses oxlint with
  custom plugins, and relies on `Effect`, `Layer`, `Schema`, `Stream`, and
  `Context.Reference`. Its `LiveQueryDef` and `SignalDef` implement
  `Equal`/`Hash` ([patterns](https://dev.docs.livestore.dev/patterns/effect/)).
  There is also `@effect-atom/atom-livestore`.
- **Lesson from [PR #1647](https://github.com/livestorejs/livestore/pull/1647).**
  The Effect 4.0.0 upgrade changed the Schema AST and check ids
  (`isLengthBetween` → `isBetweenLength`). That changed persisted schema
  fingerprints and event-definition hashes, and silently flipped
  `Array.partition`. **Do not derive persisted identifiers or wire hashes from
  Schema AST shape**, and do not expect `SchemaBinary` fingerprints to stay
  stable across Effect releases.
- **Not a model for us.** Re-exporting and extending Effect namespaces works
  for an app framework but conflicts with our "upstream = rename" goal.

### docustore replica prototype

`/Users/jonathan/github.com/jpowersdev/docustore/packages/replica`:

- It derives a `Plan` from the schema (`toType` → `resolve(textAnnotation)` →
  `isObjects` → struct, record, or value). Values use LWW registers via
  `Combiner.max(Order.mapInput(clock.order, ...))`, maps use
  `Struct.makeCombiner(..., { omitKeyWhen: isUndefined })` and
  `Record.makeReducerUnion`, and text uses Fugue.
- It keeps state in `SubscriptionRef` and stamps with `Clock.currentTimeMillis`.
- Its public API uses `dual` and `Pipeable`.
- It hand-rolls `UndefinedOr.makeReducer`, uses a non-Effect annotation key
  (`"@docustore/replica/text"`), and names a function `sync`, which collides
  with `Effect.sync`.
- It has no module or JSDoc tags in Effect's format.

## 8. Open questions

1. **Upstream target.** Is `effect/delta` the right subpath, or would
   maintainers want `effect/crdt` or `effect/collab`, or for this to extend
   `effect/eventlog`? The name is the token our find-and-replace depends on.
2. **Relationship to EventLog.** Do we need to interoperate with its
   server-relayed log (UUIDv7 wall-clock order, per-key conflicts), or stay
   transport-agnostic and offer an EventLog adapter later? Peer-to-peer rules
   out EventLog as the backbone.
3. **Private tooling.** Vendor `@effect/utils` (barrels), `@effect/oxc` (lint
   rules), and `@effect/jsdocs` (checker)? Use git dependencies? Or approximate
   them?
4. **Versions.** What should `@since` say before upstreaming, and do we track
   Effect's version line (4.x) or our own 0.x?
5. **Merge rule vs. patch.** Is the merge a `Combiner<Delta>` (state or delta
   join, commutative), with `Differ` only for local diff-and-apply? Or do we
   avoid `Differ` entirely to prevent confusing its sequential `combine` with a
   join?
6. **Wire format.** `SchemaBinary` (compact, unstable, fingerprint mode) or JSON
   by default? How do we version our format independently of Effect's?
7. **Law testing.** Build a reusable `Combiner` law suite on `Arbitrary` and
   `it.prop` (a candidate upstream contribution on its own), plus model-based
   convergence tests.
8. **Annotation API.** Should the text marker be an annotation key
   (`"~effect-delta/text"` declared through `effect/Schema` augmentation), a
   `Schema` combinator, or a branded schema? Do we also offer a `toCombiner`
   override annotation to match `toEquivalence`?
9. **Reactivity.** Ship a `SubscriptionRef`/`Stream` surface only, or also an
   `Atom` adapter (as effect-yjs does)?
10. **Public-API constraint.** Outside core we cannot use
    `effect/internal/*`, and `eventlog/internal/*` is reachable today, which
    looks like an oversight. Does anything we need (fast protos, UUID helpers)
    exist only internally?
11. **Clocks in tests.** `TestClock` starts at 0, so every stamp ties on wall
    time. Confirm that our stamp order is total (replica-id tiebreak) before we
    rely on `Combiner.max`.
