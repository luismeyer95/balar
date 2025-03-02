import { AsyncLocalStorage } from 'async_hooks';

export type BulkFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
export type TransformInputsFn<In> = (_: NoInfer<In>[]) => Map<NoInfer<In>, NoInfer<In>>;
export type ScalarFn<In, Out> = (request: In) => Promise<Out>;
export type Execution<In, Out> = { key: In; resolve: (ret: Out) => void };

export type RegistryEntry<In, Out> = {
  fn: BulkFn<In, Out>;
  transformInputs?: TransformInputsFn<In>;
};
type CheckedRegistryEntry<I, O> = RegistryEntry<I, O> & { __brand: 'checked' };
export type InternalRegistryEntry<In, Out> = RegistryEntry<In, Out> & {
  scalarHandler: ScalarFn<In, Out>;
  executions: Execution<In, Out>[];
};

export type ToInternalRegistry<T extends Record<string, RegistryEntry<any, any>>> = {
  [K in keyof T]: T[K] extends {
    fn: (inputs: Array<infer I>) => Promise<Map<infer I, infer O>>;
  }
    ? InternalRegistryEntry<I, O>
    : never;
};

export class BulkRegistry<R extends Record<string, CheckedRegistryEntry<any, any>>> {
  constructor(public readonly entries: R) {}
}

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
  processor: (request: MainIn) => Promise<MainOut>;
  internalRegistry!: ToInternalRegistry<R>;
  lastSeenOp?: keyof R;

  doneCandidates = 0;
  totalCandidates = 0;

  constructor({
    registry,
    processor,
  }: {
    registry: BulkRegistry<R>;
    processor: (request: MainIn) => Promise<MainOut>;
  }) {
    this.processor = processor;
    this.registerInternal(registry.entries);
  }

  registerInternal(registry: R) {
    this.internalRegistry = {} as ToInternalRegistry<R>;
    // For each type of bulk operation, we need 3 things:
    // - the bulk function to execute when all candidates have reached the next checkpoint
    // - a promise queue to keep track of calls to the scalarized functions we provide to the processors
    // - a transform strategy (ex: to merge inputs that != but can still be merged)

    const scalarHandlers = this.createContextualScalarHandlers(registry);

    for (const [name, registryEntry] of Object.entries(registry) as Array<
      [keyof R, R[keyof R]]
    >) {
      const executions: Execution<unknown, unknown>[] = [];

      this.internalRegistry[name] = {
        ...registryEntry,
        executions: executions,
        scalarHandler: scalarHandlers[name],
      } as unknown as ToInternalRegistry<R>[keyof R]; // TODO: fix types
    }
  }

  deduplicateInputs<In, Out>(inputs: In[]): Map<In, In> {
    return new Map(inputs.map((input) => [input, input]));
  }

  maybeExecute(name: keyof R) {
    const op = this.internalRegistry[name];
    if (op.executions.length === 0) {
      return;
    }

    // Checks if all candidates have reached the next checkpoint.
    // If all concurrent executions at this time have either
    // - called this handler's scalar function and awaiting execution
    // - or completed execution by returning
    // Then, we can execute the bulk function.
    if (op.executions.length + this.doneCandidates !== this.totalCandidates) {
      // Not all candidates have reached the next checkpoint
      return;
    }

    // Next checkpoint reached, run bulk operation

    // For each queued call, the input is stored as a key in order
    // to index out the result from the bulk call map. Since these
    // inputs are transformed right before execution to allow for
    // input merging, we need to apply an input key remap.
    const inputs = op.executions.map((item) => item.key);
    const mappedInputs = op.transformInputs?.(inputs) ?? this.deduplicateInputs(inputs);
    for (const execItem of op.executions) {
      const mappedInput = mappedInputs.get(execItem.key);
      if (mappedInput) {
        execItem.key = mappedInput;
      }
    }

    // In case the transform deduplicated some inputs, the resulting
    // map may contain duplicate values due to multiple original
    // inputs mapping to the same transformed input.
    const finalInputs = [...new Set(mappedInputs.values())];

    console.log('executing bulk op', name, 'with', finalInputs);

    op.fn(finalInputs).then((result) => {
      for (const item of op.executions) {
        item.resolve(result.get(item.key)!);
      }
      op.executions = [];
    });
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    console.log('executing plan:', requests);
    // Initialize the target total. Used to track the progress towards each
    // checkpoint during the concurrent processor calls.
    this.totalCandidates = requests.length;

    const results = await STORE.run(this as any /* TODO FIX */, async () => {
      const executions = requests.map(async (request) => {
        const result = await this.processor(request);

        this.doneCandidates += 1;
        if (this.lastSeenOp) {
          this.maybeExecute(this.lastSeenOp);
        }

        return result;
      });

      return Promise.all(executions);
    });

    const resultsByKey = new Map(
      results.map((result, index) => [requests[index], result]),
    );

    return resultsByKey;
  }

  createContextualScalarHandlers(registry: R): ScalarizeRegistry<R> {
    const scalarHandlers = {} as ScalarizeRegistry<R>;

    for (const entryName of Object.keys(registry) as Array<keyof R>) {
      const scalarFn = async (input: unknown): Promise<unknown> => {
        // When a scalar function is called, we need to keep track of which handler it came from.
        // If an execution returns when all other concurrent executions are awaiting the next
        // checkpoint, we should execute the bulk handler for this next checkpoint, which should
        // be the one from the last registered scalar call.
        //
        // TODO: check if we should poll all handlers for execution instead.
        this.lastSeenOp = entryName;

        return new Promise((resolve) => {
          this.internalRegistry[entryName].executions.push({ key: input, resolve });
          this.maybeExecute(entryName);
        });
      };

      scalarHandlers[entryName] = scalarFn as ScalarizeRegistry<R>[keyof R];
    }

    return scalarHandlers;
  }
}

////////////////////////////////////////

let globalRegistryTemplate: BulkRegistry<
  Record<string, CheckedRegistryEntry<unknown, unknown>>
> | null;

const STORE = new AsyncLocalStorage<
  BulkyPlan<unknown, unknown, Record<string, CheckedRegistryEntry<unknown, unknown>>>
>();

export function scalarize<R extends Record<string, CheckedRegistryEntry<any, any>>>(
  bulkOpRegistry: R,
): ScalarizeRegistry<R> {
  // Store the registry as a global to use as template.
  // It will then be used to instantiate bulk plans lazily on execute
  // and expose them in async local storage.
  globalRegistryTemplate = new BulkRegistry(bulkOpRegistry);

  // Sets up the user scalar registry and returns it.
  // Those are the user-exposed scalar functions which are not aware
  // of any execution context. They delegate execution to the context-aware
  // scalar handlers taken from the bulk plan stored in async context.
  const scalarHandlers = {} as ScalarizeRegistry<R>;

  for (const entryName of Object.keys(bulkOpRegistry)) {
    const scalarFn = async (input: unknown): Promise<unknown> => {
      const bulkPlan = STORE.getStore();

      if (!bulkPlan) {
        throw new Error(
          'balar error: scalar function called outside of a bulk operation',
        );
      }

      if (!(entryName in bulkPlan.internalRegistry)) {
        throw new Error(`balar error: no scalar function registered for ${entryName}`);
      }

      console.log('executing user scalar fn', entryName, 'with', input);

      return bulkPlan.internalRegistry[entryName].scalarHandler(input);
    };

    scalarHandlers[entryName as keyof R] = scalarFn as ScalarizeRegistry<R>[keyof R];
  }

  return scalarHandlers;
}

const CONCURRENT_EXECS = new AsyncLocalStorage<{
  total: number;
  count: number;
  executions: Execution<unknown, unknown>[];
}>();

export async function execute<MainIn, MainOut>(
  requests: MainIn[],
  processor: (request: MainIn) => Promise<MainOut>,
): Promise<Map<MainIn, MainOut>> {
  if (!globalRegistryTemplate) {
    throw new Error('balar error: registry not created');
  }

  if (!CONCURRENT_EXECS.getStore()) {
    const plan = new BulkyPlan({
      registry: globalRegistryTemplate,
      processor,
    });

    return CONCURRENT_EXECS.run(
      { total: requests.length, count: 0, executions: [] },
      () => {
        return plan.run(requests);
      },
    );
  }

  const concurrentExecs = CONCURRENT_EXECS.getStore()!;

  // Sync all concurrent execute() calls and collect all requests
  const coalescedResults = await new Promise(async (resolve) => {
    concurrentExecs.count += 1;
    concurrentExecs.executions.push(
      ...requests.map((request) => ({ key: request, resolve })),
    );

    // Only proceed after this if we're the last concurrent execution
    if (concurrentExecs.count < concurrentExecs.total) {
      return;
    }

    // All concurrent executions have been queued => flush
    const executions = concurrentExecs.executions;
    concurrentExecs.executions = [];
    concurrentExecs.count = 0;

    const coalescedRequests = executions.map((item) => item.key);

    const plan = new BulkyPlan({
      registry: globalRegistryTemplate!,
      processor,
    });

    const coalescedResults = await CONCURRENT_EXECS.run(
      { total: coalescedRequests.length, count: 0, executions: [] },
      () => {
        return plan.run(coalescedRequests as unknown as MainIn[]);
      },
    );

    for (const execution of executions) {
      execution.resolve(coalescedResults);
    }
  });

  const requestSet = new Set(requests);
  const result = new Map<MainIn, MainOut>();

  for (const [key, value] of coalescedResults as Map<MainIn, MainOut>) {
    if (requestSet.has(key)) {
      result.set(key, value);
    }
  }

  return result;
}
