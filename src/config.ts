import crypto from 'node:crypto';
import {
  RegistryEntry,
  Facade,
  BulkMethods,
  ObjectFacade,
  BalarFn,
  AssertBulkRecord,
} from './api';
import { EXECUTION } from './constants';
import { getMethodsOfClassObject } from './utils';
import hash from 'object-hash';

/**
 * Creates a wrapper for a set of bulk functions. The wrapper can be used in balar execution contexts (e.g. inside the `processorFn` provided to `balar.execute(inputs, processorFn)`). When a wrapped function is called inside `balar.run()`, inputs are collected across all executions of the processor function so that a single call to the underlying bulk method is performed.
 *
 * @param bulkFunctions A record of bulk functions or bulk function configurations (defined with `balar.def()`).
 * @returns An object containing balar functions ready for use in balar execution contexts (`balar.run()`).
 *
 * @example
 *
 * ```ts
 * // Define a function with the required bulk signature `(inputs: I[]) => Promise<Map<I, O>>`
 * async function getBooks(bookIds: number[]): Promise<Map<number, Book>> { ... }
 *
 * // Wrap it with `balar.wrap.fns()`
 * const booksRepository = balar.wrap.fns({ getBooks });
 *
 * // You can now use these 2 overloads inside `balar.run()` to queue inputs
 * // for a call to the underlying function and get back the result once executed
 * booksRepository.getBooks(1); // Returns a Promise<Book | undefined>
 * booksRepository.getBooks([1, 2]); // Returns a Promise<Map<number, Book>>
 * ```
 */
export function fns<R extends Record<string, any>>(
  bulkFunctions: AssertBulkRecord<R>,
): Facade<R> {
  if (EXECUTION.getStore()) {
    throw new Error(
      'balar error: unexpected call to `balar.wrap.fns()` inside a balar execution context, please define your balar wrapper in advance and share it across processor function executions to produce the intended behaviour.',
    );
  }

  // Creates handlers from bulk functions. Those are the user-exposed functions which are
  // not aware of any execution context. They delegate execution to the context-aware handlers
  // taken from the balar execution stored in async context.
  const scalarHandlers: Record<string, BalarFn<unknown, unknown, unknown[]>> = {};

  for (const entryName of Object.keys(bulkFunctions)) {
    const entry = bulkFunctions[entryName];
    const fn = generateUserExposedFn(entryName, { fn: entry });

    scalarHandlers[entryName] = fn;
  }

  return scalarHandlers as Facade<R>;
}

/**
 * Creates a wrapper for an object containing bulk methods. The wrapper can be used in balar execution contexts (e.g. inside the `processorFn` provided to `balar.execute(inputs, processorFn)`). When a wrapper method is called inside `balar.run()`, inputs are collected across all executions of the processor function so that a single call to the underlying bulk method is performed.
 *
 * @param object An object containing bulk methods.
 * @param opts An options object containing `pick` and `exclude` properties to control which bulk methods of the input object should be exposed in the output object.
 * @param {string[]} opts.pick The names of bulk methods to include.
 * @param {string[]} opts.exclude The names of bulk methods to exclude.
 * @returns An object containing balar functions ready for use in balar execution contexts.
 *
 * @example
 *
 * ```ts
 * // Define an object containing methods with the required bulk signature `(inputs: I[]) => Promise<Map<I, O>>`
 * class BooksRepository {
 *   async getBooks(bookIds: number[]): Promise<Map<number, Book>> { ... }
 *   async createBooks(books: Book[]): Promise<Map<Book, boolean>> { ... }
 * }
 *
 * // Wrap it with `balar.wrap.object()`
 * const wrapper = balar.wrap.object(new BooksRepository());
 *
 * // You can also specify which methods to expose with `pick` and `exclude`
 * const wrapperWithConfig = balar.wrap.object(new BooksRepository(), {
 *   pick: ['getBooks'],
 *   exclude: ['createBooks'],
 * });
 *
 * // For each wrapped bulk method, you can now use 2 overloads inside `balar.run()`
 * // to queue inputs for a call and get back the result once it's executed
 * wrapper.getBooks(1); // Returns a Promise<Book | undefined>
 * wrapper.getBooks([1, 2]); // Returns a Promise<Map<number, Book>>
 * ```
 *
 */
export function object<
  O extends Record<string, any>,
  P extends BulkMethods<O> & string = BulkMethods<O> & string,
  E extends BulkMethods<O> & string = never,
>(object: O, opts: { pick?: P[]; exclude?: E[] } = {}): ObjectFacade<O, P, E> {
  if (EXECUTION.getStore()) {
    throw new Error(
      'balar error: unexpected call to `balar.wrap.object()` inside a balar execution context, please define your balar wrapper in advance and share it across processor function executions to produce the intended behaviour.',
    );
  }

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

    const fn = generateUserExposedFn(methodName, { fn: method });
    entries[methodName] = fn;
  }

  return entries as ObjectFacade<O, P, E>;
}

function generateUserExposedFn(
  entryName: string,
  entry: RegistryEntry<unknown, unknown, unknown[]>,
  uniquePrefix: string = generateUniqueOperationPrefix(),
): BalarFn<unknown, unknown, unknown[]> {
  return (async (
    input: unknown | unknown[],
    ...extraArgs: unknown[]
  ): Promise<unknown | Map<unknown, unknown>> => {
    const bulkContext = EXECUTION.getStore();
    const inputs = Array.isArray(input) ? input : [input];

    if (!bulkContext) {
      return entry.fn(inputs, ...extraArgs);
    }

    const argsId = extraArgs.length ? hash(extraArgs).slice(0, 6) : '';
    const uniqueOperationId = `${uniquePrefix}-${entryName}${argsId}`;

    bulkContext.logger?.(
      `queueing input ${JSON.stringify(input)} for operation ${uniqueOperationId}`,
    );

    const allResults = await bulkContext.registerCall(
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
  return crypto.randomBytes(6).toString('hex').substring(0, 6);
}
