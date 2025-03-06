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
  getCallId?: (extraArgs: Args) => string | number;
};

type CheckedRegistryEntry<I, O, Args extends readonly unknown[]> = RegistryEntry<
  I,
  O,
  Args
> & { __brand: 'checked' };

export type InternalRegistryEntry<In, Out, Args extends readonly unknown[]> = Required<
  RegistryEntry<In, Out, Args>
> & {
  scalarHandler: ScalarFn<In, Out, Args>;
  executionGroups: Map<string | number, ExecutionGroup<In, Out, Args>>;
};

export type ExecutionGroup<In, Out, Args extends readonly unknown[]> = {
  executions: Execution<In, Out>[];
  extraArgs: Args;
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
 * Takes a registry and converts it to a record of scalar functions.
 */
type ScalarizeRegistry<R extends Record<string, RegistryEntry<any, any, any>>> = {
  [K in keyof R]: ScalarizeFn<R[K]['fn']>;
};
