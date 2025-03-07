export type BulkFn<In, Out, Args extends readonly unknown[]> = (
  request: In[],
  ...args: Args
) => Promise<Map<In, Out>>;
export type ProcessorFn<In, Out> = (request: In) => Promise<Out>;
export type TransformInputsFn<In> = (_: NoInfer<In>[]) => Map<NoInfer<In>, NoInfer<In>>;
export type ScalarFn<In, Out, Args extends readonly unknown[]> = (
  request: In,
  ...args: Args
) => Promise<Out>;

export type RegistryEntry<In, Out, Args extends readonly unknown[]> = {
  fn: BulkFn<In, Out, Args>;
  transformInputs?: TransformInputsFn<In>;
  getArgsId?: (extraArgs: Args) => string;
};

type CheckedRegistryEntry<I, O, Args extends readonly unknown[]> = Required<
  RegistryEntry<I, O, Args>
> & { __brand: 'checked' };

export type InternalRegistryEntry<In, Out, Args extends readonly unknown[]> = {
  fn: BulkFn<In, Out, Args>;
  extraArgs: Args;
  executions: Execution<In, Out>[];
  transformInputs: TransformInputsFn<In>;
};

export type Execution<In, Out> = {
  key: In;
  resolve: (ret: Out) => void;
  reject: (err: Error) => void;
};

export type InternalRegistry<T extends Record<string, RegistryEntry<any, any, any>>> = {
  [K in keyof T]: T[K] extends {
    fn: (inputs: Array<infer I>, ...args: infer Args) => Promise<Map<infer I, infer O>>;
  }
    ? InternalRegistryEntry<I, O, Args>
    : never;
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

/**
 * Takes a class containing bulk functions and converts it to a class
 * containing scalar functions.
 */
export type ScalarizeObject<O> = {
  [K in keyof O as O[K] extends BulkFn<any, any, any> ? K : never]: ScalarizeFn<O[K]>;
};

/**
 * Takes a registry and converts it to a record of scalar functions.
 */
type ScalarizeRegistry<R extends Record<string, RegistryEntry<any, any, any>>> = {
  [K in keyof R]: ScalarizeFn<R[K]['fn']>;
};
