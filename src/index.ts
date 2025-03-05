import { AsyncLocalStorage } from 'async_hooks';

export type BulkFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
export type ProcessorFn<In, Out> = (request: In) => Promise<Out>;
export type TransformInputsFn<In> = (_: NoInfer<In>[]) => Map<NoInfer<In>, NoInfer<In>>;
export type ScalarFn<In, Out> = (request: In) => Promise<Out>;
export type Execution<In, Out> = {
  key: In;
  resolve: (ret: Out) => void;
  reject: (err: Error) => void;
};

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
 * configuration is wrapped in a function like this one.
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

export class BalarExecution<
  MainIn,
  MainOut,
  R extends Record<string, CheckedRegistryEntry<any, any>>,
> {
  processor: (request: MainIn) => Promise<MainOut>;
  internalRegistry!: ToInternalRegistry<R>;

  queuedExecute: ProcessorFn<unknown, unknown> | null = null;
  concurrentExecs: Execution<unknown, unknown>[] = [];
  queuedOperations: Set<keyof R> = new Set();
  awaitingProcessors: Set<number> = new Set();

  doneProcessors = 0;
  totalProcessors = 0;

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

  private registerInternal(registry: R) {
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

  private deduplicateInputs<In>(inputs: In[]): Map<In, In> {
    return new Map(inputs.map((input) => [input, input]));
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    //console.log('executing plan:', requests);
    // Initialize the target total. Used to track the progress towards each
    // checkpoint during the concurrent processor calls.
    this.totalProcessors = requests.length;

    const results = await EXECUTION.run(this as any /* TODO FIX */, async () => {
      const executions = requests.map(async (request, index) => {
        const result = await PROCESSOR_ID.run(index, () => this.processor(request));
        //console.log('yield from processor execution for request input', request);

        this.doneProcessors += 1;
        if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
          this.executeCheckpoint();
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

  private executeCheckpoint() {
    //console.log('executing checkpoint');

    if (this.queuedExecute) {
      const plan = new BalarExecution({
        registry: globalRegistryTemplate!,
        processor: this.queuedExecute,
      });

      const allRequests = this.concurrentExecs.flatMap((item) => item.key);
      plan
        .run(allRequests)
        .then((allResults) => {
          for (const execution of this.concurrentExecs) {
            execution.resolve(allResults);
          }
        })
        .catch((err) => {
          for (const execution of this.concurrentExecs) {
            execution.reject(err);
          }
        })
        .finally(() => {
          // TODO: should the clear come before the bulk response handling?
          this.concurrentExecs = [];
        });
    }

    for (const name of this.queuedOperations) {
      const op = this.internalRegistry[name];
      if (op.executions.length === 0) {
        return;
      }

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

      //console.log('executing bulk op', name, 'with', finalInputs);

      op.fn(finalInputs)
        .then((result) => {
          for (const item of op.executions) {
            item.resolve(result.get(item.key)!);
          }
        })
        .catch((err) => {
          for (const item of op.executions) {
            item.reject(err);
          }
        })
        .finally(() => {
          // TODO: should the clear come before the bulk response handling?
          op.executions = [];
        });
    }

    // Clear checkpoint buffer
    this.queuedExecute = null;
    this.queuedOperations.clear();
    this.awaitingProcessors.clear();
  }

  async nestedContextExecute<In, Out>(
    requests: In[],
    processor: (request: In) => Promise<Out>,
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    // NOTE: this impl does not allow for different execute processors on the same checkpoint!
    this.queuedExecute = processor as (request: unknown) => Promise<unknown>;
    this.awaitingProcessors.add(processorId);

    const allResults = await new Promise(async (resolve, reject) => {
      this.concurrentExecs.push({ key: requests, resolve, reject });

      //console.log(
      //   'called nested execute()',
      //   this.concurrentExecs.length,
      //   '/',
      //   this.totalProcessors,
      // );

      if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
        process.nextTick(() => this.executeCheckpoint());
      }
    });

    const requestSet = new Set(requests);
    const result = new Map<In, Out>();

    for (const [key, value] of allResults as Map<In, Out>) {
      if (requestSet.has(key)) {
        result.set(key, value);
      }
    }

    return result;
  }

  private createContextualScalarHandlers(registry: R): ScalarizeRegistry<R> {
    const scalarHandlers = {} as ScalarizeRegistry<R>;

    for (const entryName of Object.keys(registry) as Array<keyof R>) {
      const scalarFn = async (input: unknown): Promise<unknown> => {
        const processorId = PROCESSOR_ID.getStore();
        if (processorId == null) {
          throw new Error('balar error: missing processor ID');
        }

        this.awaitingProcessors.add(processorId);
        this.queuedOperations.add(entryName);

        return new Promise((resolve, reject) => {
          this.internalRegistry[entryName].executions.push({
            key: input,
            resolve,
            reject,
          });

          // Checks if all candidates have reached the next checkpoint.
          // If all concurrent processor executions at this time have either
          // - called 1+ scalar functions and awaiting execution
          // - or completed execution by returning
          // Then, we can execute the bulk functions.
          if (
            this.awaitingProcessors.size + this.doneProcessors ===
            this.totalProcessors
          ) {
            process.nextTick(() => this.executeCheckpoint());
          }
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

const EXECUTION = new AsyncLocalStorage<
  BalarExecution<unknown, unknown, Record<string, CheckedRegistryEntry<unknown, unknown>>>
>();

const PROCESSOR_ID = new AsyncLocalStorage<number>();

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
      const bulkContext = EXECUTION.getStore();

      if (!bulkContext) {
        throw new Error(
          'balar error: scalar function called outside of a bulk operation',
        );
      }

      if (!(entryName in bulkContext.internalRegistry)) {
        throw new Error(`balar error: no scalar function registered for ${entryName}`);
      }

      //console.log('executing user scalar fn', entryName, 'with', input);

      // TODO: encapsulation
      return bulkContext.internalRegistry[entryName].scalarHandler(input);
    };

    scalarHandlers[entryName as keyof R] = scalarFn as ScalarizeRegistry<R>[keyof R];
  }

  return scalarHandlers;
}

export async function execute<In, Out>(
  requests: In[],
  processor: (request: In) => Promise<Out>,
): Promise<Map<In, Out>> {
  if (!globalRegistryTemplate) {
    throw new Error('balar error: registry not created');
  }

  const execution = EXECUTION.getStore();

  if (!execution) {
    const execution = new BalarExecution({
      registry: globalRegistryTemplate,
      processor,
    });

    return execution.run(requests);
  }

  // Sync all concurrent execute() calls, then execute and get back ALL the results
  return execution.nestedContextExecute(requests, processor);
}
