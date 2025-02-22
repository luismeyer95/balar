export type BulkHandlerFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
export type ScalarHandlerFn<In, Out> = (request: In) => Promise<Out>;
export type HandlerQueue<In, Out> = { key: In; resolve: (ret: Out) => void }[];

export type Handler<In, Out> = {
  fn: BulkHandlerFn<In, Out>;
  transformInputs: InputTransformerFn<In>;
  queue: HandlerQueue<In, Out>;
};

export type RegisterEntry<In, Out> =
  | BulkHandlerFn<In, Out>
  | {
      fn: BulkHandlerFn<In, Out>;
      transformInputs: InputTransformerFn<In>;
    };

export type InputTransformerFn<In> = (
  inputs: NoInfer<In>[],
) => Map<NoInfer<In>, NoInfer<In>>;

// Derives the scalar function map from the registry map
export type Scalarize<T extends Record<string, RegisterEntry<any, any>>> = {
  [K in keyof T]: T[K] extends RegisterEntry<infer In, infer Out>
    ? ScalarHandlerFn<In, Out>
    : never;
};

export type Handlerize<T extends Record<string, RegisterEntry<any, any>>> = {
  [K in keyof T]: T[K] extends RegisterEntry<infer In, infer Out>
    ? Handler<In, Out>
    : never;
};

export class BulkyPlan<
  MainIn,
  MainOut,
  T extends Record<string, RegisterEntry<any, any>>,
> {
  private processor: (request: MainIn, use: Scalarize<T>) => Promise<MainOut>;
  private handlerRegistry: Handlerize<T>;
  private lastSeenHandler!: Handler<any, any>;

  private doneCandidates = 0;
  private totalCandidates = 0;

  constructor({
    register,
    processor,
  }: {
    register: T;
    processor: (request: MainIn, use: Scalarize<NoInfer<T>>) => Promise<MainOut>;
  }) {
    this.processor = processor;
    this.handlerRegistry = {} as Handlerize<T>;

    this.registerHandlers(register);
  }

  private registerHandlers(handlers: T) {
    // For each type of bulk operation, we need 3 things:
    // - the bulk function to execute when all candidates have reached the next checkpoint
    // - a promise queue to keep track of calls to the scalarized functions we provide to the processors
    // - a merge strategy to deduplicate requests
    for (const [name, handler] of Object.entries(handlers) as Array<
      [keyof T, T[keyof T]]
    >) {
      const queue: HandlerQueue<unknown, unknown> = [];

      const { fn, transformInputs } =
        typeof handler !== 'function'
          ? handler
          : {
              fn: handler,
              transformInputs: this.deduplicateInputs,
            };

      this.handlerRegistry[name] = {
        fn,
        transformInputs,
        queue,
      } as unknown as Handlerize<T>[keyof T]; // TODO: fix types
    }
  }

  private deduplicateInputs(inputs: unknown[]): Map<unknown, unknown> {
    return new Map(inputs.map((input) => [input, input]));
  }

  private maybeExecute<In, Out>(handler: Handler<In, Out>) {
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
    const mappedInputs = handler.transformInputs(inputs);
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
    const processorHandlers = {} as Scalarize<T>;

    // Initialize the target total. Used to track the progress towards each
    // checkpoint during the concurrent executions.
    this.totalCandidates = requests.length;

    for (const handlerName of Object.keys(this.handlerRegistry) as Array<keyof T>) {
      const handler = this.handlerRegistry[handlerName];

      const scalarFn = async (input: unknown): Promise<unknown> => {
        // When a scalar function is called, we need to keep track of which handler it came from.
        // If an execution returns when all other concurrent executions are awaiting the next
        // checkpoint, we should execute the bulk handler for this next checkpoint, which should
        // be the one from the last registered scalar call.
        //
        // TODO: check if we should poll all handlers for execution instead.
        this.lastSeenHandler = handler;

        return new Promise<unknown>((resolve) => {
          handler.queue.push({ key: input, resolve });
          this.maybeExecute(handler);
        });
      };

      processorHandlers[handlerName] = scalarFn as Scalarize<T>[keyof T];
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
