import { ApiType } from './config';
import { UnionPickAndExclude, ValueTypes } from './utils';

export type ProcessorFn<In, Out> = (request: In) => Promise<Out>;

export type BulkFn<In, Out, Args extends readonly unknown[]> = (
  request: In[],
  ...args: Args
) => Promise<Map<In, Out>>;

export type ScalarFn<In, Out, Args extends readonly unknown[]> = (
  request: In,
  ...args: Args
) => Promise<Out>;

export type RegistryEntry<In, Out, Args extends readonly unknown[]> = {
  fn: BulkFn<In, Out, Args>;
  getArgsId?: (extraArgs: Args) => string;
};

export type ScalarRegistryEntry<I, O, Args extends readonly unknown[]> = Required<
  RegistryEntry<I, O, Args>
> & { __brand: ApiType.Scalar };

export type BulkRegistryEntry<I, O, Args extends readonly unknown[]> = Required<
  RegistryEntry<I, O, Args>
> & { __brand: ApiType.Bulk };

export type CheckedRegistryEntry<I, O, Args extends readonly unknown[]> =
  | ScalarRegistryEntry<I, O, Args>
  | BulkRegistryEntry<I, O, Args>;

export type BulkOperation<In, Out, Args extends readonly unknown[]> = {
  fn: BulkFn<In, Out, Args>;
  extraArgs: Args;
  call: BulkInvocation<In, Out> | null;
};

export type BulkInvocation<In, Out> = {
  input: In[];
  resolve: (ret: Map<In, Out>) => void;
  reject: (err: Error) => void;
  cachedPromise: Promise<Map<In, Out>> | null;
};

export type ExecuteOptions = {
  concurrency?: number;
};

/**
 * Takes a bulk function and converts its signature to a scalar function.
 */
type ScalarizeFn<F> = F extends (
  input: Array<infer I>,
  ...args: infer Args
) => Promise<Map<infer I, infer O>>
  ? (input: I, ...args: Args) => Promise<O>
  : never;

export type BulkMethods<O extends Record<string, any>> = ValueTypes<{
  [K in keyof O as O[K] extends BulkFn<any, any, any> ? K : never]: K;
}>;

/**
 * Takes a class object containing bulk methods and creates a facade only
 * containing those bulk methods. Exposes pick and exclude method filters.
 */
export type BulkFacade<
  O extends Record<string, any>,
  P extends keyof O & string = BulkMethods<O> & string,
  E extends keyof O & string = never,
> = {
  [K in keyof O as O[K] extends BulkFn<any, any, any>
    ? K extends UnionPickAndExclude<keyof O, P, E>
      ? K
      : never
    : never]: O[K] extends BulkFn<infer I, infer O, infer A> ? BulkFn<I, O, A> : never;
};

/**
 * Takes a class object containing bulk methods and creates a facade only
 * containing scalar versions of these bulk methods. Exposes pick and exclude method filters.
 */
export type ScalarFacade<
  O extends Record<string, any>,
  P extends keyof O & string = BulkMethods<O> & string,
  E extends keyof O & string = never,
> = {
  [K in keyof O as O[K] extends BulkFn<any, any, any>
    ? K extends UnionPickAndExclude<keyof O, P, E>
      ? K
      : never
    : never]: O[K] extends BulkFn<infer I, infer O, infer A> ? ScalarFn<I, O, A> : never;
};

/**
 * Takes a registry and converts it to a record of scalar and bulk functions.
 */
export type Facade<R extends Record<string, RegistryEntry<any, any, any>>> = {
  [K in keyof R]: R[K] extends BulkRegistryEntry<any, any, any>
    ? R[K]['fn']
    : ScalarizeFn<R[K]['fn']>;
};
