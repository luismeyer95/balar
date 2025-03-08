import { AsyncLocalStorage } from 'async_hooks';
import crypto from 'node:crypto';
import {
  CheckedRegistryEntry,
  RegistryEntry,
  BulkFn,
  ProcessorFn,
  Execution,
  ScalarizeRegistry,
  InternalRegistryEntry,
  ExecuteOptions,
} from './api';
import { DEFAULT_MAX_CONCURRENT_EXECUTIONS } from './constants';
import { chunk } from './utils';

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

  return registryEntry as CheckedRegistryEntry<I, O, Args>;
}

// export function bulk<I, O, Args extends readonly unknown[]>(
//   entry: RegistryEntry<I, O, Args> | BulkFn<I, O, Args>,
// ): CheckedBulkRegistryEntry<I, O, Args> {
//   return def(entry) as CheckedBulkRegistryEntry<I, O, Args>;
// }

export class BalarExecution<MainIn, MainOut> {
  internalRegistry: Map<string, InternalRegistryEntry<unknown, unknown, unknown[]>> =
    new Map();

  queuedExecute: ProcessorFn<unknown, unknown> | null = null;
  concurrentExecs: Execution<unknown, unknown>[] = [];

  queuedOperations: Set<string> = new Set();
  awaitingProcessors: Set<number> = new Set();
  doneProcessors = 0;
  totalProcessors = 0;

  constructor(
    private readonly processor: (request: MainIn) => Promise<MainOut>,
    private readonly opts: Required<ExecuteOptions>,
  ) {
    this.processor = processor;
    this.opts = opts;
  }

  reset() {
    this.internalRegistry = new Map();
    this.queuedExecute = null;
    this.concurrentExecs = [];
    this.queuedOperations = new Set();
    this.awaitingProcessors = new Set();
    this.doneProcessors = 0;
    this.totalProcessors = 0;
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    // console.log('executing plan:', requests);
    const resultByRequest = new Map<MainIn, MainOut>();

    const results = await EXECUTION.run(this as any /* TODO FIX */, async () => {
      for (const requestsBatch of chunk(requests, this.opts.concurrency)) {
        const batchResults = await Promise.all(this.runBatch(requestsBatch));
        for (let i = 0; i < batchResults.length; i += 1) {
          resultByRequest.set(requestsBatch[i], batchResults[i]);
        }
      }
    });

    return resultByRequest;
  }

  private runBatch(requestBatch: MainIn[]): Promise<MainOut>[] {
    this.reset();
    this.totalProcessors = requestBatch.length;

    return requestBatch.map(async (request, index) => {
      const result = await PROCESSOR_ID.run(index, () => this.processor(request));
      // console.log('yield from processor execution for request input', request);

      this.doneProcessors += 1;
      if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
        this.executeCheckpoint();
      }

      return result;
    });
  }

  private executeCheckpoint() {
    // console.log('executing checkpoint');

    if (this.queuedExecute) {
      const plan = new BalarExecution(this.queuedExecute, this.opts);

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
      const op = this.internalRegistry.get(name)!;
      if (op.executions.length === 0) {
        return;
      }

      // For each queued call, the input is stored as a key in order
      // to index out the result from the bulk call map. Since these
      // inputs are transformed right before execution to allow for
      // input merging, we need to apply an input key remap.
      const inputs = op.executions.map((item) => item.key);
      const mappedInputs = op.transformInputs(inputs);
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

      // console.log('executing bulk op', name, 'with', finalInputs, ...op.extraArgs);

      op.fn(finalInputs, ...op.extraArgs)
        .then((result) =>
          op.executions.forEach((item) => item.resolve(result.get(item.key))),
        )
        .catch((err) => op.executions.forEach((item) => item.reject(err)))
        .finally(() => (op.executions = []));
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

      // console.log(
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

  callScalarHandler(
    operationId: string,
    config: CheckedRegistryEntry<unknown, unknown, unknown[]>,
    input: unknown,
    args: unknown[],
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    if (!this.internalRegistry.has(operationId)) {
      this.internalRegistry.set(operationId, {
        ...config,
        extraArgs: args,
        executions: [],
      });
    }

    this.awaitingProcessors.add(processorId);
    this.queuedOperations.add(operationId);

    const registryEntry = this.internalRegistry.get(operationId)!;

    return new Promise((resolve, reject) => {
      registryEntry.executions.push({
        key: input,
        resolve,
        reject,
      });
      // Checks if all candidates have reached the next checkpoint.
      // If all concurrent processor executions at this time have either
      // - called 1+ scalar functions and awaiting execution
      // - or completed execution by returning
      // Then, we can execute the bulk functions.
      if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
        process.nextTick(() => this.executeCheckpoint());
      }
    });
  }
}

////////////////////////////////////////

const EXECUTION = new AsyncLocalStorage<BalarExecution<unknown, unknown>>();
const PROCESSOR_ID = new AsyncLocalStorage<number>();

export function scalarize<R extends Record<string, CheckedRegistryEntry<any, any, any>>>(
  bulkOpRegistry: R,
): ScalarizeRegistry<R> {
  // Creates scalar handlers from bulk functions.
  // Those are the user-exposed scalar functions which are not aware
  // of any execution context. They delegate execution to the context-aware
  // scalar handlers taken from the bulk plan stored in async context.
  const scalarHandlers = {} as ScalarizeRegistry<R>;

  for (const entryName of Object.keys(bulkOpRegistry)) {
    const uniquePrefix = crypto.randomBytes(8).toString('hex').substring(0, 8);

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

      const argsId = bulkOpRegistry[entryName].getArgsId(extraArgs);
      const uniqueOperationId = `${uniquePrefix}-${entryName}${argsId}`;

      // console.log('executing user scalar fn', uniqueOperationId, 'with', input);

      return bulkContext.callScalarHandler(
        uniqueOperationId,
        bulkOpRegistry[entryName],
        input,
        extraArgs,
      );
    };

    scalarHandlers[entryName as keyof R] = scalarFn as ScalarizeRegistry<R>[keyof R];
  }

  return scalarHandlers;
}

export async function execute<In, Out>(
  requests: In[],
  processor: (request: In) => Promise<Out>,
  opts: ExecuteOptions = {},
): Promise<Map<In, Out>> {
  const execution = EXECUTION.getStore();

  if (!execution) {
    return new BalarExecution(processor, {
      concurrency: DEFAULT_MAX_CONCURRENT_EXECUTIONS,
      ...opts,
    }).run(requests);
  }

  return execution.nestedContextExecute(requests, processor);
}

export default {
  execute,
  scalarize,
  def,
};
