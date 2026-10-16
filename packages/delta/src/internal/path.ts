import * as Schema from "effect/Schema"

// Where a part of a document is: field names, record keys and list item keys
// from the root. The root is [].

/** @internal */
export type Path = ReadonlyArray<string>

/** @internal */
export const Path: Schema.Codec<Path> = Schema.Array(Schema.String)

/**
 * Frozen: every root op and replicated text shares it.
 *
 * @internal
 */
export const root: Path = Object.freeze([])

/**
 * A string that identifies a path, for use as a map key.
 *
 * @internal
 */
export const key = (path: Path): string => JSON.stringify(path)

/** @internal */
export const parent = (path: Path): Path => path.slice(0, -1)

/** @internal */
export const isRoot = (path: Path): boolean => path.length === 0

/**
 * Every proper prefix of `path`, from the root down.
 *
 * @internal
 */
export const prefixes = (path: Path): Array<Path> => Array.from({ length: path.length }, (_, i) => path.slice(0, i))

/** @internal */
export const equals = (a: Path, b: Path): boolean => a.length === b.length && a.every((segment, i) => segment === b[i])
