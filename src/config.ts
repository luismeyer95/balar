import crypto from 'node:crypto';
import {
  RegistryEntry,
  BulkFn,
  Facade,
  CheckedRegistryEntry,
  BulkMethods,
  ObjectFacade,
  BalarFn,
} from './api';
import { EXECUTION } from './constants';
import { getMethodsOfClassObject } from './utils';

const wrap = { fns, object };
export { def, wrap };

/**
 * Creates a registry of bulk functions that are meant to be used within balar execution contexts (e.g. inside the `processorFn` provided to `balar.execute(inputs, processorFn)`).
 *
 * @example
 *
 * ```ts
 * async function getBooks(bookIds: number[]): Promise<Map<number, Book>> { ... }
 * const booksRepository = balar.wrap.fns({
 *   getBook: balar.def(getBooks)
 * })
 *
 * // `getBooks()` is only called once with [1, 2, 3]
 * const bookNames = await balar.execute([1, 2, 3], async (bookId: number) => {
 *   const book = await booksRepository.getBook(bookId);
 *   return book?.name;
 * })
 * ```
 *
 * @param bulkFunctions A record of bulk function configurations (created using `balar.def()`).
 *
 * @returns An object containing balar functions ready for use in balar execution contexts.
 */
function fns<
  R extends Record<string, BulkFn<any, any, any> | CheckedRegistryEntry<any, any, any>>,
>(bulkFunctions: R): Facade<R> {
  // Creates handlers from bulk functions. Those are the user-exposed functions which are
  // not aware of any execution context. They delegate execution to the context-aware handlers
  // taken from the balar execution stored in async context.
  const scalarHandlers = {} as Facade<R>;

  for (const entryName of Object.keys(bulkFunctions)) {
    const entry = bulkFunctions[entryName];

    const fn = generateUserExposedFn(
      entryName,
      'fn' in entry ? entry : def({ fn: entry }),
    );

    scalarHandlers[entryName as keyof R] = fn as Facade<R>[keyof R];
  }

  return scalarHandlers;
}

/**
 * Given an input object containing bulk methods, returns a wrapper that hooks into the balar execution context to queue and batch calls into efficient bulk operations. When a wrapper method is called inside `balar.execute()`, inputs are collected across all executions of the passed function so that a single call to the underlying bulk method is performed.
 *
 * @example
 *
 * ```ts
 * class AuthorsRepository {
 *   async getAuthorsById(authorIds: number[]): Promise<Map<number, string>> {
 *     // Async operation that fetches multiple authors at once
 *     return new Map([[1, "George Orwell"], [2, "Aldous Huxley"], [3, "Isaac Asimov"]]);
 *   }
 * }
 *
 * const wrapper = balar.wrap.object(new AuthorsRepository())
 * const authors: Map<number, string> = await balar.execute([1, 2, 3], async (authorId) => {
 *   // This wrapper method is called 3 times, but the underlying method is only called once
 *   return wrapper.getAuthorsById(authorId);
 * })
 *
 * console.log([...bookAuthors.values()]); // ["George Orwell", "Aldous Huxley", "Isaac Asimov"]
 * ```
 *
 * @param object An object containing bulk methods.
 * @param opts An options object containing `pick` and `exclude` properties to control which bulk methods of the input object should be exposed in the output object.
 * @param {string[]} opts.pick The names of bulk methods to include.
 * @param {string[]} opts.exclude The names of bulk methods to exclude.
 *
 * @returns An object containing balar functions ready for use in balar execution contexts.
 */
function object<
  O extends Record<string, any>,
  P extends BulkMethods<O> & string = BulkMethods<O> & string,
  E extends BulkMethods<O> & string = never,
>(object: O, opts: { pick?: P[]; exclude?: E[] } = {}): ObjectFacade<O, P, E> {
  const methodNames = getMethodsOfClassObject(object);

  type K = keyof O & string;
  const pickSet = new Set<K>(opts.pick ?? methodNames);
  const excludeSet = new Set<K>(opts.exclude ?? []);

  const apiMethods = methodNames.filter(
    (methodName) => pickSet.has(methodName) && !excludeSet.has(methodName),
  );

  const entries: Record<string, BalarFn<unknown, unknown, unknown[]>> = {};
  for (const methodName of apiMethods) {
    const method = object[methodName].bind(object);

    const fn = generateUserExposedFn(methodName, def(method));
    entries[methodName] = fn;
  }

  return entries as ObjectFacade<O, P, E>;
}

function def<I, O, Args extends readonly unknown[]>(
  entry:
    | RegistryEntry<Exclude<I, unknown[]>, O, Args>
    | BulkFn<Exclude<I, unknown[]>, O, Args>,
): CheckedRegistryEntry<Exclude<I, unknown[]>, O, Args> {
  const registryEntry = 'fn' in entry ? entry : { fn: entry };

  const getArgsId =
    registryEntry.getArgsId ??
    ((args) => {
      if (!args.length) {
        // Small optimization for 0 extra args
        return '';
      }
      // TODO fix: may produce different IDs on objects with different key order
      return JSON.stringify(args);
    });

  return { ...registryEntry, getArgsId, __brand: 'checked' };
}

function generateUserExposedFn(
  entryName: string,
  entry: CheckedRegistryEntry<unknown, unknown, unknown[]>,
  uniquePrefix: string = generateUniqueOperationPrefix(),
): BalarFn<unknown, unknown, unknown[]> {
  return (async (
    input: unknown | unknown[],
    ...extraArgs: unknown[]
  ): Promise<unknown | undefined | Map<unknown, unknown>> => {
    const bulkContext = EXECUTION.getStore();

    if (!bulkContext) {
      throw new Error('balar error: bulk function called outside of a balar execution');
    }

    const argsId = entry.getArgsId(extraArgs);
    const uniqueOperationId = `${uniquePrefix}-${entryName}${argsId}`;

    bulkContext.logger?.('executing user scalar fn', uniqueOperationId, 'with', input);

    const inputs = Array.isArray(input) ? input : [input];

    const allResults = await bulkContext.callBulkHandler(
      uniqueOperationId,
      entry,
      inputs,
      extraArgs,
    );

    if (!Array.isArray(input)) {
      return allResults.get(inputs[0]);
    }

    const result = new Map();
    for (const input of inputs) {
      result.set(input, allResults!.get(input));
    }

    return result;
  }) as BalarFn<unknown, unknown, unknown[]>;
}

function generateUniqueOperationPrefix(): string {
  return crypto.randomBytes(8).toString('hex').substring(0, 8);
}
