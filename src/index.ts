export type BulkFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
export type TransformInputsFn<In> = (_: NoInfer<In>[]) => Map<NoInfer<In>, NoInfer<In>>;
export type ScalarFn<In, Out> = (request: In) => Promise<Out>;
export type HandlerQueue<In, Out> = { key: In; resolve: (ret: Out) => void }[];

// Configuration API for a registry entry
export type RegistryEntry<In, Out> = {
  fn: BulkFn<In, Out>;
  transformInputs?: TransformInputsFn<In>;
};
type CheckedRegistryEntry<I, O> = RegistryEntry<I, O> & { __brand: 'checked' };
export type InternalRegistryEntry<In, Out> = RegistryEntry<In, Out> & {
  queue: HandlerQueue<In, Out>;
};

export type ToInternalRegistry<T extends Record<string, RegistryEntry<any, any>>> = {
  [K in keyof T]: T[K] extends {
    fn: (inputs: Array<infer I>) => Promise<Map<infer I, infer O>>;
  }
    ? InternalRegistryEntry<I, O>
    : never;
};

/**
 * Takes a bulk function and converts its signature to a scalar function.
 */
type ScalarizeFn<F> = F extends (input: Array<infer I>) => Promise<Map<infer I, infer O>>
  ? (input: I) => Promise<O>
  : never;

/**
 * Takes a registry and converts it to a record of scalar functions.
 */
type ScalarizeRegistry<R extends Record<string, RegistryEntry<any, any>>> = {
  [K in keyof R]: ScalarizeFn<R[K]['fn']>;
};

/**
 * Due to some TypeScript limitations, it is only possible to ensure
 * correct type-checking at the registry entry level if each entry
 * configuration is wrapped in an identity function like this one.
 *
 * As an added layer of type-safety, the registry's API is made to only
 * accept registry entries that have been wrapped in a call to `define()`.
 */
export function def<I, O>(
  entry: RegistryEntry<I, O> | BulkFn<I, O>,
): CheckedRegistryEntry<I, O> {
  const registryEntry = 'fn' in entry ? entry : { fn: entry };
  return registryEntry as CheckedRegistryEntry<I, O>;
}

export class BulkyPlan<
  MainIn,
  MainOut,
  R extends Record<string, CheckedRegistryEntry<any, any>>,
> {
  private processor: (request: MainIn, use: ScalarizeRegistry<R>) => Promise<MainOut>;
  private handlerRegistry: ToInternalRegistry<R>;
  private lastSeenHandler!: InternalRegistryEntry<any, any>;

  private doneCandidates = 0;
  private totalCandidates = 0;

  constructor({
    register,
    processor,
  }: {
    register: R;
    processor: (request: MainIn, use: ScalarizeRegistry<NoInfer<R>>) => Promise<MainOut>;
  }) {
    this.processor = processor;
    this.handlerRegistry = {} as ToInternalRegistry<R>;

    this.registerHandlers(register);
  }

  private registerHandlers(handlers: R) {
    // For each type of bulk operation, we need 3 things:
    // - the bulk function to execute when all candidates have reached the next checkpoint
    // - a promise queue to keep track of calls to the scalarized functions we provide to the processors
    // - a transform strategy (ex: to merge inputs that != but can still be merged)
    for (const [name, registryEntry] of Object.entries(handlers) as Array<
      [keyof R, R[keyof R]]
    >) {
      const queue: HandlerQueue<unknown, unknown> = [];

      this.handlerRegistry[name] = {
        ...registryEntry,
        queue,
      } as unknown as ToInternalRegistry<R>[keyof R]; // TODO: fix types
    }
  }

  private deduplicateInputs<In, Out>(inputs: In[]): Map<In, In> {
    return new Map(inputs.map((input) => [input, input]));
  }

  private maybeExecute<In, Out>(handler: InternalRegistryEntry<In, Out>) {
    // Checks if all candidates have reached the next checkpoint.

    // If all concurrent executions at this time have either
    // - called this handler's scalar function and awaiting execution
    // - or completed execution by returning
    // Then, we can execute the bulk function.

    if (handler.queue.length + this.doneCandidates !== this.totalCandidates) {
      // Not all candidates have reached the next checkpoint
      return;
    }

    if (handler.queue.length === 0) {
      return;
    }

    // Next checkpoint reached, run bulk operation

    // For each queued call, the input is stored as a key in order
    // to index out the result from the bulk call map. Since these
    // inputs are transformed right before execution to allow for
    // input merging, we need to apply an input key remap.
    const inputs = handler.queue.map((item) => item.key);
    const mappedInputs =
      handler.transformInputs?.(inputs) ?? this.deduplicateInputs(inputs);
    for (const queueItem of handler.queue) {
      const mappedInput = mappedInputs.get(queueItem.key);
      if (mappedInput) {
        queueItem.key = mappedInput;
      }
    }

    // In case the transform deduplicated some inputs, the resulting
    // map may contain duplicate values due to multiple original
    // inputs mapping to the same transformed input.
    const finalInputs = [...new Set(mappedInputs.values())];

    handler.fn(finalInputs).then((result) => {
      for (const item of handler.queue) {
        item.resolve(result.get(item.key)!);
      }
      handler.queue = [];
    });
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    const processorHandlers = {} as ScalarizeRegistry<R>;

    // Initialize the target total. Used to track the progress towards each
    // checkpoint during the concurrent executions.
    this.totalCandidates = requests.length;

    for (const handlerName of Object.keys(this.handlerRegistry) as Array<keyof R>) {
      const handler = this.handlerRegistry[handlerName];

      const scalarFn = async (input: unknown): Promise<unknown> => {
        // When a scalar function is called, we need to keep track of which handler it came from.
        // If an execution returns when all other concurrent executions are awaiting the next
        // checkpoint, we should execute the bulk handler for this next checkpoint, which should
        // be the one from the last registered scalar call.
        //
        // TODO: check if we should poll all handlers for execution instead.
        this.lastSeenHandler = handler;

        return new Promise((resolve) => {
          handler.queue.push({ key: input, resolve });
          this.maybeExecute(handler);
        });
      };

      processorHandlers[handlerName] = scalarFn as ScalarizeRegistry<R>[keyof R];
    }

    const executions = requests.map(async (request) => {
      const result = await this.processor(request, processorHandlers);

      if (result) {
        this.doneCandidates += 1;
        this.maybeExecute(this.lastSeenHandler);
        // for (const handler of Object.values(this.handlerRegistry)) {
        //   this.maybeExecute(handler);
        // }
      }

      return result;
    });

    const results = await Promise.all(executions);
    const resultsByKey = new Map(
      results.map((result, index) => [requests[index], result]),
    );

    return resultsByKey;
  }
}
