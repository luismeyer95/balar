import { ExecutionResultsInternal } from './primitives';

export type ProcessorFn<In, Out> = (request: In) => Promise<Out>;

export type BulkFn<In, Out, Args extends readonly unknown[]> = BulkMapFn<In, Out, Args> &
  BulkArrayFn<In, Out, Args>;

export type BulkMapFn<In, Out, Args extends readonly unknown[]> = (
  request: In[],
  ...args: Args
) => Promise<Map<In, Out>>;

export type BulkArrayFn<In, Out, Args extends readonly unknown[]> = (
  request: In[],
  ...args: Args
) => Promise<Out[]>;

export type IsBulkFn<Fn> = IsBulkMapFn<Fn> extends true ? true : IsBulkArrayFn<Fn>;

export type IsBulkMapFn<Fn> = Fn extends (
  r: Array<infer In>,
  ...args: infer _Args
) => Promise<Map<infer In, infer _Out>>
  ? In extends unknown[]
    ? false
    : true
  : false;

export type IsBulkArrayFn<Fn> = Fn extends (
  r: Array<infer In>,
  ...args: infer _Args
) => Promise<Array<infer _Out>>
  ? In extends unknown[]
    ? false
    : true
  : false;

export type AssertBulkRecord<R extends Record<string, any>> = {
  [K in keyof R]: IsBulkFn<R[K]> extends true ? R[K] : never;
};

export type BulkRecord<R extends Record<string, any>> = {
  [K in keyof R as IsBulkFn<R[K]> extends true ? K : never]: R[K];
};

export type ScalarFn<In, Out, Args extends readonly unknown[], Nullable> = (
  request: In,
  ...args: Args
) => Promise<Nullable extends true ? Out | undefined : Out>;

export type BalarFn<
  In,
  Out,
  Args extends readonly unknown[],
  Nullable = true,
> = BulkMapFn<In, Out, Args> & ScalarFn<In, Out, Args, Nullable>;

export type DeferredPromise<T> = {
  resolve: (ret: T) => void;
  reject: (err: unknown) => void;
  cachedPromise: Promise<T> | null;
};

export type BulkOperation<In, Out, Args extends readonly unknown[]> = {
  input: Set<In>;
  extraArgs: Args;
  fn: BulkFn<In, Out, Args>;
  call: DeferredPromise<Map<In, Out>> | null;
};

export type ScopeOperation<In, Out> = {
  input: In[];
  fnByInput: Map<In, (request: In) => Promise<Out>>;
  call: DeferredPromise<ExecutionResultsInternal<In, Out>> | null;
};

/**
 * Options for controlling the execution behavior of Balar.
 *
 * @property [concurrency] - The maximum number of concurrent executions for the processor function given to `balar.run()`. Defaults to unlimited if not specified.
 * @property [logger] - An optional function to handle logging messages (for debugging executions only).
 */
export type ExecutionOptions = {
  concurrency?: number;
  logger?: (...args: any[]) => void;
};

/**
 * Takes a bulk map/array function and converts its signature to a hybrid scalar/bulk(map) function.
 */
type BalarizeFn<F> = F extends (
  input: Array<infer I>,
  ...args: infer Args
) => Promise<Map<infer I, infer O>>
  ? BalarFn<I, O, Args, true>
  : F extends (input: Array<infer I>, ...args: infer Args) => Promise<Array<infer O>>
    ? BalarFn<I, O, Args, false>
    : never;

export type BulkMethods<O extends Record<string, any>> = ValueTypes<{
  [K in keyof O as IsBulkFn<O[K]> extends true ? K : never]: K;
}>;

/**
 * Takes a class object containing bulk methods and creates a facade only
 * containing scalar versions of these bulk methods. Exposes pick and exclude method filters.
 */
export type ObjectFacade<
  O extends Record<string, any>,
  P extends keyof O & string = BulkMethods<O> & string,
  E extends keyof O & string = never,
> = {
  [K in keyof O as IsBulkFn<O[K]> extends true
    ? K extends UnionPickAndExclude<keyof O, P, E>
      ? K
      : never
    : never]: BalarizeFn<O[K]>;
};

/**
 * Takes a registry and converts it to a record of hybrid scalar/bulk functions.
 */
export type Facade<R extends Record<string, any>> = {
  [K in keyof R as IsBulkFn<R[K]> extends true ? K : never]: BalarizeFn<R[K]>;
};

export type UnionPickAndExclude<T, P extends T, E extends T> = Extract<Exclude<T, E>, P>;
export type ValueTypes<T> = T extends { [key: string]: infer V } ? V : never;
