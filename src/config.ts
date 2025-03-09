import crypto from 'node:crypto';
import {
  ScalarRegistryEntry,
  RegistryEntry,
  BulkFn,
  BulkRegistryEntry,
  BulkFacade as BulkFacade,
  ScalarFn,
  Facade,
  CheckedRegistryEntry,
  BulkMethods,
  ScalarFacade as ScalarFacade,
} from './api';
import { EXECUTION } from './constants';
import { getMethodsOfClassObject } from './utils';

export enum ApiType {
  Bulk = 'bulk',
  Scalar = 'scalar',
}

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

    const fn =
      entry.__brand === 'bulk'
        ? generateUserExposedBulkFn(entryName, entry)
        : generateUserExposedScalarFn(entryName, entry);

    scalarHandlers[entryName as keyof R] = fn as Facade<R>[keyof R];
  }

  return scalarHandlers;
}

export function object<
  O extends Record<string, any>,
  P extends BulkMethods<O> & string = BulkMethods<O> & string,
  E extends BulkMethods<O> & string = never,
  A extends ApiType = ApiType.Scalar,
>(
  classObject: O,
  opts: { api?: A; pick?: P[]; exclude?: E[] } = {},
): A extends ApiType.Scalar ? ScalarFacade<O, P, E> : BulkFacade<O, P, E> {
  opts.api ??= ApiType.Scalar as A;

  const methodNames = getMethodsOfClassObject(classObject);
  type K = keyof O & string;
  const pickSet = new Set<K>(opts.pick ?? methodNames);
  const excludeSet = new Set<K>(opts.exclude ?? []);

  const apiMethods = methodNames.filter(
    (methodName) => pickSet.has(methodName) && !excludeSet.has(methodName),
  );

  const entries: Record<string, BulkFn<any, any, any[]> | ScalarFn<any, any, any[]>> = {};
  for (const methodName of apiMethods) {
    var method = classObject[methodName].bind(classObject);

    const fn =
      opts.api === 'bulk'
        ? generateUserExposedBulkFn(methodName, bulk(method))
        : generateUserExposedScalarFn(methodName, scalar(method));

    entries[methodName] = fn;
  }

  return entries as A extends ApiType.Scalar
    ? ScalarFacade<O, P, E>
    : BulkFacade<O, P, E>;
}

/**
 * Due to some TypeScript limitations, it is only possible to ensure
 * correct type-checking at the registry entry level if each entry
 * configuration is wrapped in a function like this one.
 *
 * As an added layer of type-safety, the registry's API is made to only
 * accept registry entries that have been wrapped in a call to `define()`.
 */
export function scalar<I, O, Args extends readonly unknown[]>(
  entry: RegistryEntry<I, O, Args> | BulkFn<I, O, Args>,
): ScalarRegistryEntry<I, O, Args> {
  const registryEntry = 'fn' in entry ? entry : { fn: entry };

  registryEntry.getArgsId ??= (args) => {
    if (!args.length) {
      // Optimization for no additionnal args
      return '';
    }
    // Note: may produce different IDs on objects with different key order
    return JSON.stringify(args);
  };

  registryEntry.transformInputs ??= (inputs: I[]): Map<I, I> => {
    return new Map(inputs.map((input) => [input, input]));
  };

  return { ...registryEntry, __brand: ApiType.Scalar } as ScalarRegistryEntry<I, O, Args>;
}

export function bulk<I, O, Args extends readonly unknown[]>(
  entry: RegistryEntry<I, O, Args> | BulkFn<I, O, Args>,
): BulkRegistryEntry<I, O, Args> {
  return { ...scalar(entry), __brand: ApiType.Bulk };
}

function generateUserExposedScalarFn(
  entryName: string,
  entry: ScalarRegistryEntry<unknown, unknown, unknown[]>,
) {
  const uniquePrefix = crypto.randomBytes(8).toString('hex').substring(0, 8);

  return async (input: unknown, ...extraArgs: unknown[]): Promise<unknown> => {
    const bulkContext = EXECUTION.getStore();

    if (!bulkContext) {
      throw new Error('balar error: scalar function called outside of a balar execution');
    }

    const argsId = entry.getArgsId(extraArgs);
    const uniqueOperationId = `${uniquePrefix}-${entryName}${argsId}`;

    // console.log('executing user scalar fn', uniqueOperationId, 'with', input);

    return bulkContext.callScalarHandler(uniqueOperationId, entry, input, extraArgs);
  };
}

function generateUserExposedBulkFn(
  entryName: string,
  entry: BulkRegistryEntry<unknown, unknown, unknown[]>,
) {
  const uniquePrefix = crypto.randomBytes(8).toString('hex').substring(0, 8);

  return async (inputs: unknown[], ...extraArgs: unknown[]): Promise<unknown> => {
    const bulkContext = EXECUTION.getStore();

    if (!bulkContext) {
      throw new Error('balar error: bulk function called outside of a balar execution');
    }

    const argsId = entry.getArgsId(extraArgs);
    const uniqueOperationId = `${uniquePrefix}-${entryName}${argsId}`;

    // console.log('executing user scalar fn', uniqueOperationId, 'with', input);

    return bulkContext.callBulkHandler(uniqueOperationId, entry, inputs, extraArgs);
  };
}
