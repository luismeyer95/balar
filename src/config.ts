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

export function facade<R extends Record<string, CheckedRegistryEntry<any, any, any>>>(
  bulkOpRegistry: R,
): Facade<R> {
  // Creates handlers from bulk functions.
  // Those are the user-exposed functions which are not aware
  // of any execution context. They delegate execution to the context-aware
  // handlers taken from the balar execution stored in async context.
  const scalarHandlers = {} as Facade<R>;

  for (const entryName of Object.keys(bulkOpRegistry)) {
    const entry = bulkOpRegistry[entryName];
    const fn = generateUserExposedFn(entryName, entry);

    scalarHandlers[entryName as keyof R] = fn as Facade<R>[keyof R];
  }

  return scalarHandlers;
}

export function object<
  O extends Record<string, any>,
  P extends BulkMethods<O> & string = BulkMethods<O> & string,
  E extends BulkMethods<O> & string = never,
>(classObject: O, opts: { pick?: P[]; exclude?: E[] } = {}): ObjectFacade<O, P, E> {
  const methodNames = getMethodsOfClassObject(classObject);

  type K = keyof O & string;
  const pickSet = new Set<K>(opts.pick ?? methodNames);
  const excludeSet = new Set<K>(opts.exclude ?? []);

  const apiMethods = methodNames.filter(
    (methodName) => pickSet.has(methodName) && !excludeSet.has(methodName),
  );

  const entries: Record<string, BalarFn<unknown, unknown, unknown[]>> = {};
  for (const methodName of apiMethods) {
    const method = classObject[methodName].bind(classObject);

    const fn = generateUserExposedFn(methodName, def(method));
    entries[methodName] = fn;
  }

  return entries as ObjectFacade<O, P, E>;
}

/**
 * Due to some TypeScript limitations, it is only possible to ensure
 * correct type-checking at the registry entry level if each entry
 * configuration is wrapped in a function like this one.
 *
 * As an added layer of type-safety, the registry's API is made to only
 * accept registry entries that have been wrapped in a call to `define()`.
 */
export function def<I, O, Args extends readonly unknown[]>(
  entry:
    | RegistryEntry<Exclude<I, unknown[]>, O, Args>
    | BulkFn<Exclude<I, unknown[]>, O, Args>,
): CheckedRegistryEntry<Exclude<I, unknown[]>, O, Args> {
  const registryEntry = 'fn' in entry ? entry : { fn: entry };

  const getArgsId =
    registryEntry.getArgsId ??
    ((args) => {
      if (!args.length) {
        // Optimization for no additional args
        return '';
      }
      // TODO fix: may produce different IDs on objects with different key order
      return JSON.stringify(args);
    });

  return { ...registryEntry, getArgsId, __brand: 'checked' };
}

function generateUniqueOperationPrefix(): string {
  return crypto.randomBytes(8).toString('hex').substring(0, 8);
}

function generateUserExposedFn(
  entryName: string,
  entry: CheckedRegistryEntry<unknown, unknown, unknown[]>,
  uniquePrefix: string = generateUniqueOperationPrefix(),
): BalarFn<unknown, unknown, unknown[]> {
  return (async (
    input: unknown | unknown[],
    ...extraArgs: unknown[]
  ): Promise<unknown | Map<unknown, unknown>> => {
    const bulkContext = EXECUTION.getStore();

    if (!bulkContext) {
      throw new Error('balar error: bulk function called outside of a balar execution');
    }

    const argsId = entry.getArgsId(extraArgs);
    const uniqueOperationId = `${uniquePrefix}-${entryName}${argsId}`;

    // console.log('executing user scalar fn', uniqueOperationId, 'with', input);

    const inputs = Array.isArray(input) ? input : [input];

    const allResults = await bulkContext.callBulkHandler(
      uniqueOperationId,
      entry,
      inputs,
      extraArgs,
    );

    if (!Array.isArray(input)) {
      return allResults.get(inputs[0])!;
    }

    const result = new Map();
    for (const input of inputs) {
      result.set(input, allResults!.get(input));
    }

    return result;
  }) as BalarFn<unknown, unknown, unknown[]>;
}
