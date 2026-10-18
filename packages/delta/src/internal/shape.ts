import type * as Equivalence from "effect/Equivalence"
import * as Number from "effect/Number"
import * as Predicate from "effect/Predicate"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"
import * as SchemaIssue from "effect/SchemaIssue"
import * as SchemaParser from "effect/SchemaParser"
import * as TextPatch from "../TextPatch.ts"
import { compareWrites, key as changeKey } from "./change.ts"
import * as InternalDocument from "./document.ts"
import * as Path from "./path.ts"
import * as Text from "./text.ts"

// A document's merge rules, worked out from its schema: which parts are
// fields, entries, lists, texts, counters and values. With them a replica
// reads its document's state as a value, turns a new value into writes, and
// checks that a change it receives fits the schema.

/**
 * The annotation that marks a part's merge rule. Unmarked parts take theirs
 * from their shape.
 *
 * @internal
 */
export const RuleKey = "~effect-delta/ReplicatedDocument/rule"

/** @internal */
export type Rule =
  | { readonly _tag: "Text" }
  | { readonly _tag: "Counter" }
  | { readonly _tag: "List"; readonly key: string | undefined }

/** @internal */
export type Shape = Fields | Entries | List | TextShape | Counter | Value

/** @internal */
export interface Fields {
  readonly _tag: "Fields"
  readonly fields: ReadonlyArray<Field>
  /**
   * Builds the value from its fields: a class's instance, or a struct with its
   * constructor defaults filled in. Undefined for a struct without defaults.
   */
  readonly make: ((fields: object) => unknown) | undefined
  /** Where `make` is defined: whether two of its values are the same. */
  readonly equivalence: Equivalence.Equivalence<unknown> | undefined
}

/** @internal */
export interface Field {
  readonly name: string
  readonly shape: Shape
  readonly optional: boolean
  /** Whether the schema fills the field in when it's missing. */
  readonly hasDefault: boolean
}

/** @internal */
export interface Entries {
  readonly _tag: "Entries"
  readonly isKey: (key: string) => boolean
  readonly value: Shape
}

/** @internal */
export interface List {
  readonly _tag: "List"
  readonly item: Shape
  /** The item field that identifies an item; undefined for a list of values. */
  readonly key: string | undefined
}

/** @internal */
export interface TextShape {
  readonly _tag: "Text"
}

/** @internal */
export interface Counter {
  readonly _tag: "Counter"
}

/** @internal */
export interface Value {
  readonly _tag: "Value"
  readonly encode: (value: unknown) => Result.Result<Schema.Json, Schema.SchemaError>
  readonly decode: (json: Schema.Json) => Result.Result<unknown, Schema.SchemaError>
  readonly equivalence: Equivalence.Equivalence<unknown>
}

// Compiling ------------------------------------------------------------------

/**
 * Works out a document's merge rules from its schema. Throws for a schema a
 * document can't have.
 *
 * @internal
 */
export const compile = (schema: Schema.Top): Fields | Entries => {
  const ast = SchemaAST.toType(schema.ast)
  const shape = compileAst(ast, Path.root)
  if (shape._tag !== "Fields" && shape._tag !== "Entries") {
    throw new Error(
      SchemaAST.isSuspend(ast)
        ? `A document's schema must be a struct, a class or a record: ${recursive}`
        : "A document's schema must be a struct, a class or a record. For one text, use ReplicatedText"
    )
  }
  // Every receiver refuses a change that writes deeper.
  if (depthOf(shape) > InternalDocument.maxDepth) {
    refuse(
      Path.root,
      `its parts are nested more than ${InternalDocument.maxDepth} deep, deeper than a change can write`
    )
  }
  return shape
}

// The most steps a path to one of a shape's parts takes.
const depthOf = (shape: Shape): number => {
  switch (shape._tag) {
    case "Fields":
      return shape.fields.reduce((depth, field) => Math.max(depth, 1 + depthOf(field.shape)), 0)
    case "Entries":
      return 1 + depthOf(shape.value)
    case "List":
      return 1 + depthOf(shape.item)
    default:
      return 0
  }
}

const ruleOf = (ast: SchemaAST.AST): Rule | undefined => {
  const own = ast.annotations?.[RuleKey]
  if (own !== undefined) return own as Rule
  // Annotations on a schema with checks go on its last check.
  for (const check of ast.checks ?? []) {
    const found = check.annotations?.[RuleKey]
    if (found !== undefined) return found as Rule
  }
  return undefined
}

const refuse = (path: Path.Path, message: string): never => {
  throw new Error(`${path.length === 0 ? "The document" : `The part at ${Path.key(path)}`}: ${message}`)
}

const onMerged = "merging concurrent edits could break it, so check it in an authority instead"

const recursive = "a recursive schema (Schema.suspend) is a value written whole, so far"

const compileAst = (ast: SchemaAST.AST, path: Path.Path): Shape => {
  const rule = ruleOf(ast)
  switch (rule?._tag) {
    case "Text":
      if (ast.checks !== undefined) refuse(path, `a text can't have checks: ${onMerged}`)
      return { _tag: "Text" }
    case "Counter":
      // The integer check is the counter's own; a sum of integers keeps it.
      if ((ast.checks?.length ?? 0) > 1) refuse(path, `a counter can't have checks: ${onMerged}`)
      return { _tag: "Counter" }
    case "List": {
      if (!SchemaAST.isArrays(ast) || ast.elements.length > 0 || ast.rest.length !== 1) {
        return refuse(path, "a list must be an array")
      }
      if (ast.checks !== undefined) refuse(path, `a list can't have checks: ${onMerged}`)
      const item = compileAst(ast.rest[0]!, [...path, "*"])
      if (rule.key === undefined) {
        if (item._tag !== "Value") {
          refuse(path, "a list without a key holds values written whole: give a list of structs or classes a key")
        }
        return { _tag: "List", item, key: undefined }
      }
      const field = item._tag === "Fields" ? item.fields.find(({ name }) => name === rule.key) : undefined
      if (
        item._tag !== "Fields" || field === undefined || field.optional || field.shape._tag !== "Value" ||
        !holdsStrings(ast.rest[0]!, rule.key)
      ) {
        return refuse(
          path,
          SchemaAST.isSuspend(ast.rest[0]!)
            ? `a list with a key holds structs or classes, and ${recursive}`
            : `a list's items must be structs or classes with a required "${rule.key}" field that's a string`
        )
      }
      // An item's key is never made up: one without it can't be read.
      const fields = item.fields.map((other) => other === field ? { ...field, hasDefault: false } : other)
      return { _tag: "List", item: { ...item, fields }, key: rule.key }
    }
  }
  const fields = classFields(ast)
  if (fields !== undefined) {
    if (ast.checks !== undefined || fields.checks !== undefined) refuse(path, `a class can't have checks: ${onMerged}`)
    // Its fields are all it's made of.
    if (fields.indexSignatures.length > 0) refuse(path, "a class can't have a rest or entries: give them a field each")
    const schema = Schema.make<Schema.Codec<unknown>>(ast)
    return {
      _tag: "Fields",
      fields: compileFields(fields, path),
      make: SchemaParser.make(schema),
      equivalence: Schema.toEquivalence(schema)
    }
  }
  if (SchemaAST.isObjects(ast) && ast.indexSignatures.length === 0) {
    if (ast.checks !== undefined) refuse(path, `a struct can't have checks: ${onMerged}`)
    const compiled = compileFields(ast, path)
    if (!compiled.some(({ hasDefault }) => hasDefault)) {
      return { _tag: "Fields", fields: compiled, make: undefined, equivalence: undefined }
    }
    const schema = Schema.make<Schema.Codec<unknown>>(ast)
    return {
      _tag: "Fields",
      fields: compiled,
      make: SchemaParser.make(schema),
      equivalence: Schema.toEquivalence(schema)
    }
  }
  if (SchemaAST.isObjects(ast)) {
    // Schema makes a record's literal keys fields.
    if (ast.propertySignatures.length > 0) {
      refuse(path, "a struct can't have a rest, nor a record both literal keys and others: give them a field each")
    }
    if (ast.checks !== undefined) refuse(path, `a record can't have checks: ${onMerged}`)
    // Keys that fit any of several patterns, such as two template literals,
    // have a signature each, for the same entries.
    const [signature, ...others] = ast.indexSignatures
    if (others.some((other) => other.type !== signature!.type)) refuse(path, "a record's entries must have one schema")
    const parameters = ast.indexSignatures.map((each) => SchemaAST.toType(each.parameter))
    if (!parameters.every(isStringKey)) refuse(path, "a record's keys must be strings")
    const isKeys = parameters.map((parameter) => SchemaParser.is(Schema.make(parameter)))
    return {
      _tag: "Entries",
      isKey: (key) => isKeys.some((isKey) => isKey(key)),
      value: compileAst(signature!.type, [...path, "*"])
    }
  }
  if (hasRule(ast, new Set())) {
    refuse(
      path,
      SchemaAST.isSuspend(ast)
        ? `texts, counters and lists can't be inside it: ${recursive}`
        : "texts, counters and lists can't be inside a value that's written whole (a union, an array, ...)"
    )
  }
  // Written as its type's JSON, so one without any could never be written.
  if (hasNoJson(ast, new Set())) {
    refuse(
      path,
      "a value is written as its type's JSON, and this one has no JSON form: it holds a declaration without a " +
        "JSON codec, such as Schema.instanceOf makes. Give it a toCodecJson annotation, or use a schema with one"
    )
  }
  return compileValue(ast)
}

// A class is a declaration encoded as exactly its fields.
const classFields = (ast: SchemaAST.AST): SchemaAST.Objects | undefined => {
  if (!SchemaAST.isDeclaration(ast) || ast.typeParameters.length !== 1) return undefined
  const [fields] = ast.typeParameters
  const toCodec = ast.annotations?.toCodec
  if (!SchemaAST.isObjects(fields!) || !Predicate.isFunction(toCodec)) return undefined
  const link = toCodec([Schema.make(fields)]) as SchemaAST.Link | undefined
  return link?.to === fields ? fields : undefined
}

const compileFields = (ast: SchemaAST.Objects, path: Path.Path): ReadonlyArray<Field> =>
  ast.propertySignatures.map((signature) => {
    if (!Predicate.isString(signature.name)) {
      return refuse(path, "a struct's fields, and a record's literal keys, must have string names")
    }
    const optional = SchemaAST.isOptional(signature.type)
    const hasDefault = signature.type.context?.constructorDefault !== undefined
    // Filled in whenever it's absent, it could never be absent.
    if (optional && hasDefault) {
      refuse(
        [...path, signature.name],
        "an optional field can't have a constructor default: it would show while absent"
      )
    }
    const shape = compileAst(optional ? withoutUndefined(signature.type) : signature.type, [...path, signature.name])
    // Filled in whole when one value in it can't be read, it would hide the
    // rest, and an edit would write the default over them.
    if (hasDefault && shape._tag === "Fields") {
      refuse(
        [...path, signature.name],
        "a struct field can't have a constructor default: give the fields in it defaults instead"
      )
    }
    return { name: signature.name, shape, optional, hasDefault }
  })

// An optional field's `undefined` (or `void`) counts as absent, so it's no
// value the field can read, however deep in its unions, and a field that's
// only ever undefined reads nothing. The union's checks still hold: on its one
// other member, so a part with a rule refuses them, or for a suspend, which
// can't take checks, in a union of its own.
const withoutUndefined = (ast: SchemaAST.AST): SchemaAST.AST => {
  if (isUndefined(ast)) return new SchemaAST.Union([])
  if (!SchemaAST.isUnion(ast)) return ast
  const defined = ast.types.flatMap((type) => isUndefined(type) ? [] : [withoutUndefined(type)])
  if (defined.length === 1) {
    const [only] = defined
    return ast.checks === undefined
      ? only!
      : SchemaAST.isSuspend(only!)
      ? new SchemaAST.Union([only!], ast.options, ast.annotations, ast.checks)
      : Schema.make<Schema.Codec<unknown>>(only!).check(...ast.checks).ast
  }
  return defined.length === ast.types.length && defined.every((type, i) => type === ast.types[i])
    ? ast
    : new SchemaAST.Union(defined, ast.options, ast.annotations, ast.checks)
}

const isUndefined = (ast: SchemaAST.AST): boolean => SchemaAST.isUndefined(ast) || SchemaAST.isVoid(ast)

// Whether an item's field `key` holds strings, as the steps of a path are.
const holdsStrings = (item: SchemaAST.AST, key: string): boolean => {
  const fields = classFields(item) ?? (SchemaAST.isObjects(item) ? item : undefined)
  const type = fields?.propertySignatures.find(({ name }) => name === key)?.type
  return type !== undefined && isStringKey(SchemaAST.toType(type))
}

const isStringKey = (ast: SchemaAST.AST): boolean =>
  SchemaAST.isString(ast) || SchemaAST.isTemplateLiteral(ast) ||
  (SchemaAST.isLiteral(ast) && Predicate.isString(ast.literal)) ||
  (SchemaAST.isEnum(ast) && ast.enums.every(([, value]) => Predicate.isString(value))) ||
  (SchemaAST.isUnion(ast) && ast.types.every(isStringKey))

const hasRule = (ast: SchemaAST.AST, visited: Set<SchemaAST.AST>): boolean => {
  if (visited.has(ast)) return false
  visited.add(ast)
  if (ruleOf(ast) !== undefined) return true
  switch (ast._tag) {
    case "Declaration":
      return ast.typeParameters.some((parameter) => hasRule(parameter, visited))
    case "Arrays":
      return [...ast.elements, ...ast.rest].some((element) => hasRule(element, visited))
    case "Objects":
      return ast.propertySignatures.some((signature) => hasRule(signature.type, visited)) ||
        ast.indexSignatures.some((signature) => hasRule(signature.type, visited))
    case "Union":
      return ast.types.some((type) => hasRule(type, visited))
    case "Suspend":
      return hasRule(ast.thunk(), visited)
    default:
      return false
  }
}

// Whether a value holds a declaration without a JSON codec, which
// `Schema.toCodecJson` passes on as it is: an instance, which JSON can't carry.
const hasNoJson = (ast: SchemaAST.AST, visited: Set<SchemaAST.AST>): boolean => {
  if (visited.has(ast)) return false
  visited.add(ast)
  switch (ast._tag) {
    case "Declaration":
      return !Predicate.isFunction(ast.annotations?.toCodecJson ?? ast.annotations?.toCodec) ||
        ast.typeParameters.some((parameter) => hasNoJson(SchemaAST.toType(parameter), visited))
    case "Arrays":
      return [...ast.elements, ...ast.rest].some((element) => hasNoJson(element, visited))
    case "Objects":
      return ast.propertySignatures.some((signature) => hasNoJson(signature.type, visited)) ||
        ast.indexSignatures.some((signature) => hasNoJson(signature.type, visited))
    case "Union":
      return ast.types.some((type) => hasNoJson(type, visited))
    case "Suspend":
      return hasNoJson(ast.thunk(), visited)
    default:
      return false
  }
}

const asJson = Schema.decodeUnknownResult(Schema.Json)

const compileValue = (ast: SchemaAST.AST): Value => {
  const schema = Schema.make<Schema.Codec<unknown>>(ast)
  const codec = Schema.toCodecJson(schema)
  const encode = Schema.encodeUnknownResult(codec)
  const decode = Schema.decodeUnknownResult(codec)
  const equivalence = Schema.toEquivalence(schema)
  return {
    _tag: "Value",
    // As JSON sends it, so the replica that writes it shows what the rest
    // receive: some codecs, such as `Schema.Any`'s, pass on what JSON can't
    // carry, and JSON has no -0. And only if it reads back as the value: a
    // union's members can write the same JSON, such as a number's and a
    // string's "NaN", and it reads as the first.
    encode: (value) =>
      Result.flatMap(
        Result.map(Result.flatMap(encode(value), asJson), InternalDocument.asSent),
        (json) =>
          Result.flatMap(decode(json), (read) =>
            equivalence(read, value)
              ? Result.succeed(json)
              : Result.fail(
                new Schema.SchemaError(
                  new SchemaIssue.InvalidValue({ message: "its JSON reads back as another value" }, value)
                )
              ))
      ),
    decode: (json) => decode(json),
    equivalence
  }
}

/**
 * The shape at `path`, and how the part there comes and goes: always there, or
 * as an optional field, a record entry or a list item. For a path the schema
 * has no part at, why not.
 *
 * @internal
 */
export const at = (
  shape: Shape,
  path: Path.Path
): { readonly shape: Shape; readonly presence: "always" | "optional" | "entry" | "item" } | string => {
  let current = shape
  let presence: "always" | "optional" | "entry" | "item" = "always"
  for (const segment of path) {
    switch (current._tag) {
      case "Fields": {
        const field = current.fields.find(({ name }) => name === segment)
        if (field === undefined) return `which the schema doesn't have`
        current = field.shape
        presence = field.optional ? "optional" : "always"
        break
      }
      case "Entries":
        if (!current.isKey(segment)) return `whose key ${JSON.stringify(segment)} the schema doesn't take`
        current = current.value
        presence = "entry"
        break
      case "List":
        current = current.item
        presence = "item"
        break
      default:
        return `inside a ${describe(current)}`
    }
  }
  return { shape: current, presence }
}

/**
 * Whether a value read with `shape` shows the part an overlap is at: a text
 * for a text overlap, and for the others a value or a part that comes and
 * goes.
 *
 * @internal
 */
export const shows = (shape: Shape, overlap: InternalDocument.Overlap): boolean => {
  const found = at(shape, overlap.path)
  if (typeof found === "string") return false
  switch (overlap._tag) {
    case "Text":
      return found.shape._tag === "Text"
    case "Value":
      return found.shape._tag === "Value" || found.presence !== "always"
    case "Removed":
      return found.presence !== "always"
  }
}

/**
 * How a value read with `shape` reads what's written at each path, for
 * overlaps: values as the value there decodes them, and not at all where
 * there's no value or one doesn't decode; whether a part exists only where it
 * comes and goes; and positions only of list items.
 *
 * @internal
 */
export const reading = (shape: Shape): InternalDocument.Reading => {
  const valueAt = (path: Path.Path): Value | undefined => {
    const found = at(shape, path)
    return typeof found !== "string" && found.shape._tag === "Value" ? found.shape : undefined
  }
  return {
    reads: (path, json) => {
      const value = valueAt(path)
      return value !== undefined && Result.isSuccess(value.decode(json))
    },
    differ: (path, a, b) => {
      const value = valueAt(path)!
      return !value.equivalence(Result.getOrThrow(value.decode(a)), Result.getOrThrow(value.decode(b)))
    },
    comesAndGoes: (path) => {
      const found = at(shape, path)
      return typeof found !== "string" && found.presence !== "always"
    },
    moves: (path) => {
      const found = at(shape, path)
      return typeof found !== "string" && found.presence === "item"
    }
  }
}

/**
 * What in a change doesn't fit the schema, if anything: an op at a path the
 * schema doesn't have, or of a kind its rule there doesn't take, a value it
 * can't read, a record key that isn't one, an item key field that names
 * another item, or a list item added without a place in its list. Replicas
 * keep such changes and don't show them; an authority rejects them, so its
 * check reads the document as replicas show it.
 *
 * @internal
 */
export const misfit = (shape: Shape, change: InternalDocument.AnyChange): string | undefined => {
  for (const op of change.ops) {
    const path = InternalDocument.pathOf(op)
    const where = `Change ${changeKey(change)} writes to ${Path.key(path)}`
    const found = at(shape, path)
    if (typeof found === "string") return `${where}, ${found}`
    const { presence, shape: here } = found
    const fits = Text.isOp(op)
      ? here._tag === "Text"
      : op._tag === "Increment"
      ? here._tag === "Counter"
      : op._tag === "Set"
      ? here._tag === "Value"
      : op._tag === "Add"
      ? presence !== "always" && here._tag !== "Value"
      : op._tag === "Unset"
      ? presence !== "always"
      : presence === "item"
    if (!fits) return `${where} with a ${op._tag} op, where the schema has a ${describe(here)}`
    if (op._tag === "Set" && here._tag === "Value" && Result.isFailure(here.decode(op.value))) {
      return `${where} a value the schema can't read`
    }
    if (op._tag === "Set" && isItemKey(shape, path) && op.value !== path[path.length - 2]) {
      return `${where} a value other than its item's key`
    }
    // Replicas place an item as they add it. One with no place doesn't show,
    // and an authority can't tell it from a record's entry, so it would
    // refuse every place in the list after it.
    if (
      presence === "item" && (op._tag === "Add" || op._tag === "Set") &&
      !change.ops.some((other) => other._tag === "Place" && Path.equals(other.path, path))
    ) {
      return `${where}, adding a list item without placing it`
    }
  }
  return undefined
}

// Whether `path` is the key field of an item in a list with keys.
const isItemKey = (shape: Shape, path: Path.Path): boolean => {
  if (path.length < 2) return false
  const list = at(shape, path.slice(0, -2))
  return typeof list !== "string" && list.shape._tag === "List" && list.shape.key === path[path.length - 1]
}

const describe = (shape: Shape): string => {
  switch (shape._tag) {
    case "Fields":
      return "struct"
    case "Entries":
      return "record"
    case "List":
      return "list"
    case "Text":
      return "text"
    case "Counter":
      return "counter"
    case "Value":
      return "value"
  }
}

/**
 * Checks a document's initial value: it must fit its merge rules. Texts start
 * empty, records and lists start empty, and optional fields start absent;
 * everything else starts as `initial` says.
 *
 * @internal
 */
export const checkInitial = (shape: Shape, initial: unknown, path: Path.Path = Path.root): void => {
  switch (shape._tag) {
    case "Text":
      if (initial !== "") refuse(path, "a text starts empty: write starting text as a change")
      return
    case "Entries":
      if (Object.keys(initial as object).length > 0) refuse(path, "a record starts empty in the initial value")
      return
    case "List":
      if ((initial as ReadonlyArray<unknown>).length > 0) refuse(path, "a list starts empty in the initial value")
      return
    case "Fields":
      for (const field of shape.fields) {
        // Own, so a field such as `__proto__` is a field.
        const value = Object.hasOwn(initial as object, field.name)
          ? (initial as Record<string, unknown>)[field.name]
          : undefined
        if (field.optional) {
          if (value !== undefined) refuse([...path, field.name], "an optional field starts absent in the initial value")
        } else {
          checkInitial(field.shape, value, [...path, field.name])
        }
      }
      return
  }
}

// Reading --------------------------------------------------------------------

const Missing = Symbol.for("effect-delta/ReplicatedDocument/Missing")

/**
 * What a document last read, by path, so a new value shares every part that
 * didn't change, and values aren't decoded again.
 *
 * @internal
 */
export type Memo = Map<string, { readonly parts: ReadonlyArray<unknown>; readonly value: unknown }>

/**
 * Reads the document's value. Parts nobody wrote come from `initial`; parts
 * that can't be read, such as an entry missing a field, are left out. A class
 * whose constructor won't take what its fields hold, where it can't be left
 * out, reads as it was after the latest write to its values it takes.
 *
 * @internal
 */
export const read = (
  shape: Shape,
  document: InternalDocument.Document,
  initial: unknown,
  memo: Memo
): unknown => readAt(shape, documentAt(document, Path.root), { value: initial }, memo)

/**
 * Reads a document's value from the JSON a server shows it as, by the same
 * rules. JSON has only each value's latest write, so a class whose
 * constructor won't take them, where it can't be left out, can't be read as
 * replicas show it: fails with its path.
 *
 * @internal
 */
export const readJson = (
  shape: Shape,
  json: Schema.Json,
  initial: unknown,
  // Which optional fields and entries exist, by their paths in `json`, where
  // JSON alone can't say.
  exists: (path: Path.Path) => boolean
): Result.Result<unknown, Path.Path> => {
  let unbuilt: Path.Path | undefined
  const value = readAt(
    shape,
    jsonAt(json, Path.root, exists, (path) => {
      unbuilt ??= path
    }),
    { value: initial },
    new Map()
  )
  return unbuilt === undefined ? Result.succeed(value) : Result.fail(unbuilt)
}

// What reading needs from one part of a document: a replica's state, or the
// JSON a server shows it as.
interface Source {
  readonly path: Path.Path
  /** For a list item, its key, where it's known: a server's JSON lists items without theirs. */
  readonly itemKey: string | undefined
  readonly text: () => string
  readonly counter: () => number
  /** The values written here, latest first. */
  readonly values: () => Iterable<Schema.Json>
  /** Every write of a value here, earliest first, where they're known. */
  readonly writes: () => ReadonlyArray<InternalDocument.ValueWrite>
  /** Told that a class here won't build from what its fields hold. */
  readonly unbuilt: () => void
  /** Whether the optional field or record entry here exists. */
  readonly exists: () => boolean
  readonly at: (name: string) => Source
  /** The keys of the record entries here, in order. */
  readonly keys: () => ReadonlyArray<string>
  /** The list items here that show, in order, for a list whose items have the key field `key`. */
  readonly items: (key: string | undefined) => ReadonlyArray<Source>
}

const documentAt = (document: InternalDocument.Document, path: Path.Path): Source => ({
  path,
  itemKey: path[path.length - 1],
  text: () => InternalDocument.textAt(document, path),
  counter: () => InternalDocument.counterAt(document, path),
  *values() {
    const writes = InternalDocument.valuesAt(document, path)
    for (let i = writes.length - 1; i >= 0; i--) yield writes[i]!.value
  },
  writes: () => InternalDocument.valuesAt(document, path),
  // It reads as it was, which is what a replica shows.
  unbuilt: () => {},
  exists: () => InternalDocument.exists(document, path),
  at: (name) => documentAt(document, [...path, name]),
  keys: () => InternalDocument.childrenAt(document, path),
  items: () => InternalDocument.itemsAt(document, path).map((name) => documentAt(document, [...path, name]))
})

// A server's JSON has only each value's latest write, and leaves out what was
// removed, but not what was never added.
const jsonAt = (
  json: Schema.Json | undefined,
  path: Path.Path,
  exists: (path: Path.Path) => boolean,
  unbuilt: (path: Path.Path) => void
): Source => ({
  path,
  itemKey: undefined,
  text: () => Predicate.isString(json) ? json : "",
  counter: () => Predicate.isNumber(json) ? json : 0,
  values: () => json === undefined ? [] : [json],
  writes: () => [],
  unbuilt: () => unbuilt(path),
  exists: () => json !== undefined && exists(path),
  at: (name) =>
    jsonAt(isJsonObject(json) && Object.hasOwn(json, name) ? json[name] : undefined, [...path, name], exists, unbuilt),
  keys: () => isJsonObject(json) ? Object.keys(json) : [],
  // A list's items are the ones that show, so they exist. They're at paths by
  // key, as what's in them that exists is.
  items: (key) =>
    Array.isArray(json)
      ? json.map((item, i) => {
        const name = key !== undefined && isJsonObject(item) ? item[key] : undefined
        return jsonAt(item, [...path, Predicate.isString(name) ? name : String(i)], exists, unbuilt)
      })
      : []
})

const isJsonObject = (json: Schema.Json | undefined): json is Schema.JsonObject =>
  Predicate.isObject(json) && !Array.isArray(json)

const readAt = (
  shape: Shape,
  source: Source,
  initial: { readonly value: unknown } | undefined,
  memo: Memo
): unknown => {
  switch (shape._tag) {
    case "Text":
      return source.text()
    case "Counter":
      // A total past the safe integers reads as the nearest one, so it's
      // still an Int.
      return Number.clamp(((initial?.value as number | undefined) ?? 0) + source.counter(), safeIntegers)
    case "Value": {
      const k = Path.key(source.path)
      // The latest write this schema can read.
      for (const json of source.values()) {
        const previous = memo.get(k)
        if (previous !== undefined && previous.parts[0] === json) return previous.value
        const decoded = shape.decode(json)
        if (Result.isSuccess(decoded)) {
          memo.set(k, { parts: [json], value: decoded.success })
          return decoded.success
        }
      }
      return initial === undefined ? Missing : initial.value
    }
    case "Fields": {
      const parts: Array<unknown> = []
      const fields: Array<[string, unknown]> = []
      for (const field of shape.fields) {
        const part = source.at(field.name)
        if (field.optional && !part.exists()) {
          parts.push(Missing)
          continue
        }
        const fieldInitial = initial === undefined || field.optional
          ? undefined
          : { value: (initial.value as Record<string, unknown>)[field.name] }
        const value = readAt(field.shape, part, fieldInitial, memo)
        // An optional field that can't be read is left out, like an entry.
        if (value === Missing && !field.optional && !field.hasDefault) return Missing
        parts.push(value)
        if (value !== Missing && !(field.optional && value === undefined)) fields.push([field.name, value])
      }
      const built = share(memo, source.path, parts, () => build(shape, fields))
      // Only a constructor that throws leaves it so: a class's, given fields
      // a merge combined, or a constructor default's. An entry, item or
      // optional field is left out; a part always there can't be.
      if (built !== Missing || initial === undefined) return built
      source.unbuilt()
      return rollBack(shape, source, initial.value as Record<string, unknown>, fields, memo)
    }
    case "Entries": {
      const parts: Array<unknown> = []
      const entries: Array<[string, unknown]> = []
      for (const name of source.keys()) {
        const entry = source.at(name)
        if (!shape.isKey(name) || !entry.exists()) continue
        const value = readAt(shape.value, entry, undefined, memo)
        if (value === Missing) continue
        parts.push(name, value)
        entries.push([name, value])
      }
      // Defined rather than assigned, so a key such as `__proto__` is a key.
      return share(memo, source.path, parts, () => Object.fromEntries(entries))
    }
    case "List": {
      const items: Array<unknown> = []
      for (const item of source.items(shape.key)) {
        const value = readItem(shape, item, memo)
        if (value !== Missing) items.push(value)
      }
      // A copy, so a list changed in place leaves what the next read compares
      // with alone.
      return share(memo, source.path, items, () => [...items])
    }
  }
}

// The struct or class made of `fields`, or Missing if its constructor throws.
const build = (shape: Fields, fields: ReadonlyArray<readonly [string, unknown]>): unknown => {
  // Defined rather than assigned, so a field such as `__proto__` is a field.
  const value = Object.fromEntries(fields)
  if (shape.make === undefined) return value
  let made: object
  try {
    made = shape.make(value) as object
  } catch {
    return Missing
  }
  // Making copies the parts it's given. The ones read go back in, so those
  // that didn't change keep their identity.
  for (const [name, part] of fields) {
    if (Object.getOwnPropertyDescriptor(made, name)?.writable) (made as Record<string, unknown>)[name] = part
  }
  return made
}

// A class its constructor won't build from what its fields hold, always there,
// as it was after the latest write to its values that it takes: the writes are
// taken back, latest first, until it builds. Worked out from the writes
// alone, so every replica that has them shows the same. Otherwise, or where
// the writes aren't known, as `initial` has it.
const rollBack = (
  shape: Fields,
  source: Source,
  initial: Record<string, unknown>,
  fields: ReadonlyArray<readonly [string, unknown]>,
  memo: Memo
): unknown => {
  const values = shape.fields.filter(isValueField)
  const writes = values.map((field) => source.at(field.name).writes())
  // The writes, and how many of each field's still count, are what it's made
  // from.
  const k = rolledBackKey(source.path)
  const parts = [...fields.flat(), ...writes, ...writes.map(({ length }) => length)]
  const previous = memo.get(k)
  if (previous !== undefined && previous.parts.length === parts.length) {
    if (previous.parts.every((part, i) => Object.is(part, parts[i]))) return previous.value
  }
  const current = new Map(fields)
  const counted = writes.map(({ length }) => length)
  // Where each field's latest write it can read is, among those that count.
  const reading = writes.map((all, i) => latestRead(values[i]!.shape as Value, all, all.length))
  let rolled: unknown = initial
  for (;;) {
    let latest = -1
    for (let i = 0; i < values.length; i++) {
      if (
        counted[i]! > 0 &&
        (latest < 0 || compareWrites(writes[i]![counted[i]! - 1]!, writes[latest]![counted[latest]! - 1]!) > 0)
      ) {
        latest = i
      }
    }
    if (latest < 0) break
    counted[latest]!--
    // Taking back a write it couldn't read changes nothing.
    if (reading[latest]!.index < counted[latest]!) continue
    const field = values[latest]!
    reading[latest] = latestRead(field.shape as Value, writes[latest]!, counted[latest]!)
    current.set(field.name, reading[latest]!.index < 0 ? initial[field.name] : reading[latest]!.value)
    const made = build(shape, [...current])
    if (made !== Missing) {
      rolled = made
      break
    }
  }
  memo.set(k, { parts, value: rolled })
  return rolled
}

const rolledBackKey = (path: Path.Path): string => `${Path.key(path)} rolled back`

// The latest of the first `count` writes that `shape` can read, and its index,
// or -1.
const latestRead = (
  shape: Value,
  writes: ReadonlyArray<InternalDocument.ValueWrite>,
  count: number
): { readonly index: number; readonly value: unknown } => {
  for (let i = count - 1; i >= 0; i--) {
    const decoded = shape.decode(writes[i]!.value)
    if (Result.isSuccess(decoded)) return { index: i, value: decoded.success }
  }
  return { index: -1, value: undefined }
}

// A list item, or Missing if it can't be read or its key field names another
// item.
const readItem = (shape: List, item: Source, memo: Memo): unknown => {
  const value = readAt(shape.item, item, undefined, memo)
  return value === Missing || shape.key === undefined || item.itemKey === undefined ||
      (value as Record<string, unknown>)[shape.key] === item.itemKey
    ? value
    : Missing
}

const safeIntegers = { minimum: globalThis.Number.MIN_SAFE_INTEGER, maximum: globalThis.Number.MAX_SAFE_INTEGER }
const zero = BigInt(0)
const largestStep = BigInt(globalThis.Number.MAX_SAFE_INTEGER)

// The value built from `parts`: the one built before, if its parts were the
// same, NaN included.
const share = (memo: Memo, path: Path.Path, parts: ReadonlyArray<unknown>, build: () => unknown): unknown => {
  const k = Path.key(path)
  const previous = memo.get(k)
  if (
    previous !== undefined && previous.parts.length === parts.length &&
    previous.parts.every((part, i) => Object.is(part, parts[i]))
  ) return previous.value
  const value = build()
  if (value !== Missing) memo.set(k, { parts, value })
  return value
}

// Writing --------------------------------------------------------------------

/**
 * Works out the writes that turn the document into `value`, comparing it part
 * by part with what's there. Fails if a value doesn't encode.
 *
 * @internal
 */
export const plan = (
  shape: Shape,
  document: InternalDocument.Document,
  value: unknown,
  initial: unknown,
  memo: Memo
): Result.Result<ReadonlyArray<InternalDocument.Intent>, Schema.SchemaError> => {
  const intents: Array<InternalDocument.Intent> = []
  try {
    planAt(shape, document, Path.root, value, { value: initial }, false, intents, memo)
  } catch (error) {
    if (Schema.isSchemaError(error)) return Result.fail(error)
    throw error
  }
  return Result.succeed(intents)
}

const planAt = (
  shape: Shape,
  document: InternalDocument.Document,
  path: Path.Path,
  after: unknown,
  initial: { readonly value: unknown } | undefined,
  // Whether a value must be written even if it's unchanged: when it also says
  // its entry exists again.
  force: boolean,
  intents: Array<InternalDocument.Intent>,
  memo: Memo
): void => {
  switch (shape._tag) {
    case "Text": {
      const before = InternalDocument.textAt(document, path)
      if (before !== after) intents.push({ _tag: "Text", path, patch: TextPatch.get(before, after as string) })
      return
    }
    case "Counter": {
      // Left as it reads, it's left alone, though its exact total may be
      // another number. Otherwise from the exact total, in steps an op can
      // carry.
      if (readAt(shape, documentAt(document, path), initial, memo) === after) return
      const total = BigInt((initial?.value as number | undefined) ?? 0) + InternalDocument.totalAt(document, path)
      for (let by = BigInt(after as number) - total; by !== zero;) {
        const step = by > largestStep ? largestStep : by < -largestStep ? -largestStep : by
        intents.push({ _tag: "Increment", path, by: globalThis.Number(step) })
        by -= step
      }
      return
    }
    case "Value": {
      if (!force) {
        const before = readAt(shape, documentAt(document, path), initial, memo)
        if (before !== Missing && shape.equivalence(before, after)) return
      }
      intents.push({ _tag: "Set", path, value: Result.getOrThrow(shape.encode(after)) })
      return
    }
    case "Fields": {
      // What the struct shows, once a field that shows its default needs it.
      let shownValue: unknown
      // A class shown as it was, since its constructor won't take what its
      // fields hold, is left alone unless the update changes it from that.
      // Then its values are written all at once, so it shows what it's
      // given; its other parts show what they hold, and are written as they
      // change. Shown as `initial` has it, what those hold doesn't show, and
      // writing them would write over it, so it can't be changed.
      const rolledBack = initial !== undefined && shape.make !== undefined
        ? rolledBackAt(shape, document, path, initial, memo)
        : undefined
      if (rolledBack !== undefined && shape.equivalence!(rolledBack, after)) return
      if (rolledBack !== undefined && rolledBack === initial!.value) {
        throw new Schema.SchemaError(
          new SchemaIssue.InvalidValue({
            message: `The class at ${
              Path.key(path)
            } can't be changed: it shows as the initial value has it, since its constructor won't take what's in it`
          })
        )
      }
      const changesValues = rolledBack !== undefined && shape.fields.some((field) =>
        isValueField(field) &&
        !(field.shape as Value).equivalence(rolledBack[field.name], (after as Record<string, unknown>)[field.name])
      )
      for (const field of shape.fields) {
        const fieldPath = [...path, field.name]
        const fieldAfter = (after as Record<string, unknown>)[field.name]
        if (rolledBack !== undefined && isValueField(field) && !changesValues) continue
        // Inside an entry or item, a field that nothing this schema can read
        // was written to shows its default. Left as it shows, it's left
        // unwritten, so it doesn't write over what a newer schema wrote there,
        // or over a concurrent write.
        if (
          initial === undefined && field.hasDefault && field.shape._tag === "Value" &&
          readAt(field.shape, documentAt(document, fieldPath), undefined, memo) === Missing
        ) {
          shownValue ??= readAt(shape, documentAt(document, path), undefined, memo)
          if (
            shownValue !== Missing &&
            field.shape.equivalence((shownValue as Record<string, unknown>)[field.name], fieldAfter)
          ) continue
        }
        if (!field.optional) {
          const fieldInitial = initial === undefined
            ? undefined
            : { value: (initial.value as Record<string, unknown>)[field.name] }
          planAt(field.shape, document, fieldPath, fieldAfter, fieldInitial, false, intents, memo)
          continue
        }
        const present = Object.hasOwn(after as object, field.name) && fieldAfter !== undefined
        // One this schema can't read isn't in `after`, but it stays.
        if (!present && !shown(field.shape, document, fieldPath, memo)) continue
        planPresence(field.shape, document, fieldPath, present ? { value: fieldAfter } : undefined, intents, memo)
      }
      return
    }
    case "Entries": {
      const entries = after as Record<string, unknown>
      for (const name of InternalDocument.childrenAt(document, path)) {
        // Entries this schema can't read aren't in `after`, but they stay.
        if (Object.hasOwn(entries, name) || !shown(shape.value, document, [...path, name], memo, shape.isKey(name))) {
          continue
        }
        intents.push({ _tag: "Unset", path: [...path, name] })
      }
      for (const [name, entry] of Object.entries(entries)) {
        // Decoding leaves out a key the record doesn't take, so writing it
        // would write what nobody shows.
        if (!shape.isKey(name)) continue
        planPresence(shape.value, document, [...path, name], { value: entry }, intents, memo)
      }
      return
    }
    case "List":
      return shape.key === undefined
        ? planValues(shape, document, path, after as ReadonlyArray<unknown>, intents, memo)
        : planItems(shape, shape.key, document, path, after as ReadonlyArray<Record<string, unknown>>, intents, memo)
  }
}

// The class at `path`, always there, if it shows as it was because its
// constructor won't take what its fields hold.
const rolledBackAt = (
  shape: Fields,
  document: InternalDocument.Document,
  path: Path.Path,
  initial: { readonly value: unknown },
  memo: Memo
): Record<string, unknown> | undefined => {
  const shows = readAt(shape, documentAt(document, path), initial, memo)
  return shows === memo.get(rolledBackKey(path))?.value ? shows as Record<string, unknown> : undefined
}

const isValueField = (field: Field): boolean => field.shape._tag === "Value" && !field.optional

// Whether the part at `path` exists and this schema can read it.
const shown = (shape: Shape, document: InternalDocument.Document, path: Path.Path, memo: Memo, isKey = true) =>
  isKey && InternalDocument.exists(document, path) &&
  readAt(shape, documentAt(document, path), undefined, memo) !== Missing

// An optional field, record entry or list item: whether it exists, then its
// content. A removed part's content is kept, so adding it back compares with
// that.
const planPresence = (
  shape: Shape,
  document: InternalDocument.Document,
  path: Path.Path,
  after: { readonly value: unknown } | undefined,
  intents: Array<InternalDocument.Intent>,
  memo: Memo
): void => {
  const existed = InternalDocument.exists(document, path)
  if (after === undefined) {
    if (existed) intents.push({ _tag: "Unset", path })
    return
  }
  // A value says it exists by being written; anything else needs adding.
  if (!existed && shape._tag !== "Value") intents.push({ _tag: "Add", path })
  planAt(shape, document, path, after.value, undefined, !existed, intents, memo)
}

const planItems = (
  shape: List,
  key: string,
  document: InternalDocument.Document,
  path: Path.Path,
  after: ReadonlyArray<Record<string, unknown>>,
  intents: Array<InternalDocument.Intent>,
  memo: Memo
): void => {
  const keys = after.map((item) => item[key] as string)
  const wanted = new Set(keys)
  if (wanted.size < keys.length) {
    throw new Schema.SchemaError(
      new SchemaIssue.InvalidValue({ message: `Two items of the list at ${Path.key(path)} have the same ${key}` })
    )
  }
  // Items this schema can't read aren't in `after`, but they stay.
  const before = InternalDocument.itemsAt(document, path).filter((name) =>
    readItem(shape, documentAt(document, [...path, name]), memo) !== Missing
  )
  for (const name of before) if (!wanted.has(name)) intents.push({ _tag: "Unset", path: [...path, name] })
  const showing = new Set(before)
  // Items that keep their order stay put; the rest move.
  const staying = new Set(longestCommon(before.filter((name) => wanted.has(name)), keys.filter((k) => showing.has(k))))
  let previous: string | undefined
  for (const [index, name] of keys.entries()) {
    const itemPath = [...path, name]
    if (!showing.has(name)) {
      if (!InternalDocument.exists(document, itemPath)) intents.push({ _tag: "Add", path: itemPath })
      planAt(shape.item, document, itemPath, after[index], undefined, false, intents, memo)
      intents.push({ _tag: "Place", path: itemPath, after: previous })
    } else {
      planAt(shape.item, document, itemPath, after[index], undefined, false, intents, memo)
      if (!staying.has(name)) intents.push({ _tag: "Place", path: itemPath, after: previous })
    }
    previous = name
  }
}

const planValues = (
  shape: List,
  document: InternalDocument.Document,
  path: Path.Path,
  after: ReadonlyArray<unknown>,
  intents: Array<InternalDocument.Intent>,
  memo: Memo
): void => {
  const item = shape.item as Value
  const before: Array<{ readonly name: string; readonly value: unknown }> = []
  for (const name of InternalDocument.itemsAt(document, path)) {
    const value = readAt(item, documentAt(document, [...path, name]), undefined, memo)
    if (value !== Missing) before.push({ name, value })
  }
  // Values that stay are matched in order; a changed value is a removal and an
  // insert.
  const matches = longestCommonBy(before, after, (a, b) => item.equivalence(a.value, b))
  const matched = new Map(matches.map(([i, j]) => [j, before[i]!.name]))
  const kept = new Set(matches.map(([i]) => i))
  for (const [i, { name }] of before.entries()) {
    if (!kept.has(i)) intents.push({ _tag: "Unset", path: [...path, name] })
  }
  let previous: string | number | undefined
  for (const [j, value] of after.entries()) {
    const name = matched.get(j)
    if (name !== undefined) {
      previous = name
      continue
    }
    intents.push({ _tag: "Insert", path, value: Result.getOrThrow(item.encode(value)), after: previous })
    previous = intents.length - 1
  }
}

const longestCommon = (a: ReadonlyArray<string>, b: ReadonlyArray<string>): Array<string> =>
  longestCommonBy(a, b, (x, y) => x === y).map(([i]) => a[i]!)

// The longest common subsequence of `a` and `b`, as pairs of indexes. What
// they start and end with in common is matched as it is, so comparing lists
// that differ in a few places costs about their length, not its square.
const longestCommonBy = <A, B>(
  a: ReadonlyArray<A>,
  b: ReadonlyArray<B>,
  equals: (a: A, b: B) => boolean
): Array<readonly [number, number]> => {
  let start = 0
  while (start < a.length && start < b.length && equals(a[start]!, b[start]!)) start++
  let endA = a.length
  let endB = b.length
  while (endA > start && endB > start && equals(a[endA - 1]!, b[endB - 1]!)) {
    endA--
    endB--
  }
  const pairs: Array<readonly [number, number]> = []
  for (let i = 0; i < start; i++) pairs.push([i, i])
  // The rest, between them.
  const lengths = Array.from({ length: endA - start + 1 }, () => new Array<number>(endB - start + 1).fill(0))
  const length = (i: number, j: number) => lengths[i - start]![j - start]!
  for (let i = endA - 1; i >= start; i--) {
    for (let j = endB - 1; j >= start; j--) {
      lengths[i - start]![j - start] = equals(a[i]!, b[j]!)
        ? length(i + 1, j + 1) + 1
        : Math.max(length(i + 1, j), length(i, j + 1))
    }
  }
  for (let i = start, j = start; i < endA && j < endB;) {
    if (equals(a[i]!, b[j]!)) {
      pairs.push([i++, j++])
    } else if (length(i + 1, j) >= length(i, j + 1)) {
      i++
    } else {
      j++
    }
  }
  for (let i = endA, j = endB; i < a.length; i++, j++) pairs.push([i, j])
  return pairs
}
