import { AsyncLocalStorage } from 'async_hooks';
import {
  CheckedRegistryEntry,
  RegistryEntry,
  BulkFn,
  InternalRegistry,
  ProcessorFn,
  Execution,
  ScalarizeRegistry,
} from './types';

export class BulkRegistry<R extends Record<string, CheckedRegistryEntry<any, any, any>>> {
  constructor(public readonly entries: R) {}
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
  entry: RegistryEntry<I, O, Args> | BulkFn<I, O, Args>,
): CheckedRegistryEntry<I, O, Args> {
  const registryEntry = 'fn' in entry ? entry : { fn: entry };

  registryEntry.getCallId ??= (args) => {
    if (!args.length) {
      // Optimization for no additionnal args
      return 0;
    }
    // Note: may produce different IDs on objects with different key order
    return JSON.stringify(args);
  };

  registryEntry.transformInputs ??= (inputs: I[]): Map<I, I> => {
    return new Map(inputs.map((input) => [input, input]));
  };

  return registryEntry as CheckedRegistryEntry<I, O, Args>;
}

export class BalarExecution<
  MainIn,
  MainOut,
  R extends Record<string, CheckedRegistryEntry<any, any, any>>,
> {
  processor: (request: MainIn) => Promise<MainOut>;
  internalRegistry!: InternalRegistry<R>;

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
    this.internalRegistry = {} as InternalRegistry<R>;
    // For each type of bulk operation, we need 3 things:
    // - the bulk function to execute when all candidates have reached the next checkpoint
    // - a promise queue to keep track of calls to the scalarized functions we provide to the processors
    // - a transform strategy (ex: to merge inputs that != but can still be merged)

    const scalarHandlers = this.createContextualScalarHandlers(registry);

    for (const [name, registryEntry] of Object.entries(registry) as Array<
      [keyof R, R[keyof R]]
    >) {
      this.internalRegistry[name] = {
        ...registryEntry,
        executionGroups: new Map(),
        scalarHandler: scalarHandlers[name],
      } as unknown as InternalRegistry<R>[keyof R]; // TODO: fix types
    }
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    //console.log('executing plan:', requests);

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
        .then((allResults) =>
          this.concurrentExecs.forEach((exec) => exec.resolve(allResults)),
        )
        .catch((err) => this.concurrentExecs.forEach((exec) => exec.reject(err)))
        .finally(() => (this.concurrentExecs = []));
    }

    for (const name of this.queuedOperations) {
      const op = this.internalRegistry[name];
      if (op.executionGroups.size === 0) {
        return;
      }

      for (const call of op.executionGroups.values()) {
        // For each queued call, the input is stored as a key in order
        // to index out the result from the bulk call map. Since these
        // inputs are transformed right before execution to allow for
        // input merging, we need to apply an input key remap.
        const inputs = call.executions.map((item) => item.key);
        const mappedInputs = op.transformInputs(inputs);
        for (const execItem of call.executions) {
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

        op.fn(finalInputs, ...call.extraArgs)
          .then((result) =>
            call.executions.forEach((item) => item.resolve(result.get(item.key))),
          )
          .catch((err) => call.executions.forEach((item) => item.reject(err)))
          .finally(() => (call.executions = []));
      }

      op.executionGroups.clear();
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
      const scalarFn = async (input: unknown, args: unknown[]): Promise<unknown> => {
        const processorId = PROCESSOR_ID.getStore();
        if (processorId == null) {
          throw new Error('balar error: missing processor ID');
        }

        this.awaitingProcessors.add(processorId);
        this.queuedOperations.add(entryName);

        const registryEntry = this.internalRegistry[entryName];
        const callId = registryEntry.getCallId(args);

        return new Promise((resolve, reject) => {
          const execGroup = registryEntry.executionGroups.get(callId) ?? {
            executions: [],
            extraArgs: args,
          };

          execGroup.executions.push({
            key: input,
            resolve,
            reject,
          });

          registryEntry.executionGroups.set(callId, execGroup);

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

  callScalarHandler(entryName: string, input: unknown, ...args: unknown[]) {
    if (!(entryName in this.internalRegistry)) {
      throw new Error(`balar error: no scalar function registered for ${entryName}`);
    }

    return this.internalRegistry[entryName].scalarHandler(input, ...args);
  }
}

////////////////////////////////////////

let globalRegistryTemplate: BulkRegistry<
  Record<string, CheckedRegistryEntry<unknown, unknown, unknown[]>>
> | null;

const EXECUTION = new AsyncLocalStorage<
  BalarExecution<
    unknown,
    unknown,
    Record<string, CheckedRegistryEntry<unknown, unknown, unknown[]>>
  >
>();

const PROCESSOR_ID = new AsyncLocalStorage<number>();

export function scalarize<R extends Record<string, CheckedRegistryEntry<any, any, any>>>(
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
    const scalarFn = async (
      input: unknown,
      ...extraArgs: unknown[]
    ): Promise<unknown> => {
      const bulkContext = EXECUTION.getStore();

      if (!bulkContext) {
        throw new Error(
          'balar error: scalar function called outside of a bulk operation',
        );
      }

      //console.log('executing user scalar fn', entryName, 'with', input);

      return bulkContext.callScalarHandler(entryName, input, extraArgs);
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

  return execution.nestedContextExecute(requests, processor);
}
