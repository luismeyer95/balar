import {
  ExecutionOptions,
  BulkOperation,
  ScopeOperation,
  DeferredPromise,
  BulkFn,
} from './api';
import { EXECUTION, PROCESSOR_ID } from './constants';
import { BalarError, BalarStopError, Result } from './primitives';
import crypto from 'node:crypto';
import { chunk } from './utils';

/**
 * Processes a batch of inputs using the provided processor function and returns the results keyed by input. When a wrapped bulk function (obtained via `balar.wrap.fns()` or `balar.wrap.object()`) is called inside `balar.run()`, inputs are collected across all executions of the processor function so that only a single call to the underlying bulk function is performed.
 *
 * @param inputs - The array of inputs to process.
 * @param processor - An asynchronous function that processes each individual input.
 * @param opts - Optional execution options (e.g., concurrency control, logger).
 * @returns A `Promise` resolving to a `Map` that associates each input request with its corresponding output.
 *
 * @example
 *
 * ```ts
 * import { balar } from 'balar';
 *
 * // Suppose we have a remote API for managing a greenhouse that we interact with
 * // through this service
 * class GreenhouseService {
 *   async getPlants(plantIds: number[]): Promise<Map<number, Plant>> { ... }
 *   async waterPlants(plants: Plant[]): Promise<Map<Plant, Date>> { ... }
 * }
 *
 * // Wrap the service object with Balar
 * const wrapper = balar.wrap.object(new GreenhouseService());
 *
 * // You can also wrap standalone functions like this
 * // const wrapper = balar.wrap.fns({ getPlants, waterPlants });
 *
 * // Let's water multiple plants at once
 * const plantIds = [1, 2, 3]; // 🌿 🌵 🌱
 *
 * // This code reads like plants are being watered in sequence, but...
 * const results = await balar.run(plantIds, async function waterPlant(plantId) {
 *   // Balar queues all calls to `wrapper.getPlants(plantId)` and invokes
 *   // the real `getPlants([1, 2, 3])` exactly once under the hood
 *   const plant = await wrapper.getPlants(plantId);
 *
 *   // ... Do other sync/async operations, return error, anything goes ...
 *
 *   // Similarly, the real `waterPlants([plant, ...])` is called exactly once
 *   const wateredAt = await wrapper.waterPlants(plant!);
 *
 *   return { name: plant!.name, wateredAt };
 * });
 *
 * // Total number of requests to our remote API: 2! ✔️
 *
 * // Map { 1 => { name: "Fern", wateredAt: ... }, 2 => { name: "Cactus", wateredAt: ... }, ... }
 * console.log(results);
 * ```
 */
export async function run<In, Out>(
  inputs: In[],
  processor: (request: In) => Promise<Out>,
  opts: ExecutionOptions = {},
): Promise<Result<In, Out>> {
  const execution = EXECUTION.getStore();

  if (!execution) {
    return new BalarExecution(processor, opts).run(inputs);
  }

  return execution.runScope(inputs, processor);
}

export class BalarExecution<MainIn, MainOut> {
  checkpointCache: Map<string, BulkOperation<unknown, unknown, unknown[]>> = new Map();

  scopeSyncCache: Map<string, ScopeOperation<unknown, unknown>> = new Map();
  // Tracks the number of scope registration calls made by each processor
  // since the last checkpoint. Used to determine call order and assign
  // the correct partition key when registering scopes
  nextScopeOrderKey: Map<number, number> = new Map();

  awaitingProcessors: Set<number> = new Set();
  doneProcessors = 0;
  totalProcessors = 0;

  scopeId: string;
  maxConcurrency = Infinity;
  logger?: (...msgs: any[]) => void;

  constructor(
    private readonly processor:
      | ((request: MainIn) => Promise<MainOut>)
      | Map<MainIn, (request: MainIn) => Promise<MainOut>>,
    opts: ExecutionOptions,
  ) {
    this.processor = processor;
    this.scopeId = crypto.randomBytes(6).toString('hex').substring(0, 4);

    if (opts.concurrency) {
      this.maxConcurrency = opts.concurrency;
    }
    if (opts.logger) {
      this.logger = (...args) => opts.logger?.(`[${this.scopeId}] `, ...args);
    }
  }

  clearState() {
    this.checkpointCache = new Map();
    this.awaitingProcessors = new Set();
    this.doneProcessors = 0;
    this.totalProcessors = 0;
  }

  initScopeSyncPartition() {
    const scopeSyncMetadata: ScopeOperation<unknown, unknown> = {
      input: [],
      fnByInput: new Map(),
      call: this.initDeferredPromise(),
    };

    return scopeSyncMetadata;
  }

  initDeferredPromise<T>(): DeferredPromise<T> {
    const call: DeferredPromise<T> = {
      resolve: () => {},
      reject: () => {},
      cachedPromise: null,
    };

    call.cachedPromise = new Promise((resolve, reject) => {
      call.resolve = resolve;
      call.reject = reject;
    });

    return call;
  }

  async run(requests: MainIn[]): Promise<Result<MainIn, MainOut>> {
    this.logger?.('starting execution for requests: ', requests);

    const successes = new Map<MainIn, MainOut>();
    const errors = new Map<MainIn, unknown>();

    // Deduplicate inputs
    var requestSet = new Set(requests);

    try {
      await EXECUTION.run(this as BalarExecution<unknown, unknown>, async () => {
        for (const requestsBatch of chunk(requestSet, this.maxConcurrency)) {
          this.logger?.('starting execution for request batch: ', requestsBatch);
          const batchResults = await Promise.allSettled(this.runBatch(requestsBatch));

          for (let i = 0; i < batchResults.length; i += 1) {
            const batchResult = batchResults[i];
            if (batchResult.status === 'fulfilled') {
              successes.set(requestsBatch[i], batchResult.value);
            } else {
              if (batchResult.reason instanceof BalarError) {
                // Only balar errors are expected to bubble up
                throw batchResult.reason;
              }
              errors.set(requestsBatch[i], batchResult.reason);
            }
          }
        }
      });
      return { successes, errors };
    } catch (err: unknown) {
      if (err instanceof BalarStopError) {
        return {
          successes: new Map(),
          errors: new Map(requests.map((req) => [req, err])),
        };
      }
      throw err;
    }
  }

  private runBatch(requestBatch: MainIn[]): Promise<MainOut>[] {
    this.clearState();
    this.totalProcessors = requestBatch.length;

    return requestBatch.map(async (request, index) => {
      let error: unknown = null;

      try {
        const result = await PROCESSOR_ID.run(index, () => {
          if (this.processor instanceof Map) {
            return this.processor.get(request)!(request);
          }
          return this.processor(request);
        });

        this.logger?.('returned from processor execution for request input', request);
        return result;
      } catch (err: unknown) {
        error = err;
        this.logger?.('throwed from processor execution for request input', request, err);
        throw err;
      } finally {
        if (!(error instanceof BalarError)) {
          this.doneProcessors += 1;
          if (
            this.awaitingProcessors.size + this.doneProcessors ===
            this.totalProcessors
          ) {
            this.executeCheckpoint();
          }
        } else {
          this.forceFailCheckpoint(error);
        }
      }
    });
  }

  private forceFailCheckpoint(error: BalarStopError) {
    if (this.checkpointCache.size > 0) {
      for (const opName of this.checkpointCache.keys()) {
        const bulkOp = this.checkpointCache.get(opName);
        bulkOp?.call?.reject(error);
      }
      this.checkpointCache.clear();
    }

    if (this.scopeSyncCache.size > 0) {
      for (const scopeSyncPartition of this.scopeSyncCache.values()) {
        scopeSyncPartition.call?.reject(error);
      }

      // Prevent next scope calls in same stack frame to schedule redundant checkpoint
      this.scopeSyncCache.clear();
      this.nextScopeOrderKey.clear();
    }

    this.awaitingProcessors.clear();
  }

  private executeCheckpoint() {
    if (this.checkpointCache.size > 0) {
      this.logger?.('executing checkpoint');

      for (const name of this.checkpointCache.keys()) {
        this.executeCheckpointBulkOperation(name);
      }

      // Prevent next balar fn calls in same stack frame to schedule redundant checkpoint
      this.checkpointCache.clear();
    }

    if (this.scopeSyncCache.size > 0) {
      for (const scopeSyncPartition of this.scopeSyncCache.values()) {
        this.logger?.('executing scope checkpoint');

        const plan = new BalarExecution(scopeSyncPartition.fnByInput, {
          concurrency: this.maxConcurrency,
          logger: this.logger,
        });

        plan
          .run(scopeSyncPartition!.input)
          .then((allResults) => scopeSyncPartition.call?.resolve(allResults));
        // Scope calls are not expected to throw (errors reported in the result)
      }

      // Prevent next scope calls in same stack frame to schedule redundant checkpoint
      this.scopeSyncCache.clear();
      this.nextScopeOrderKey.clear();
    }

    this.awaitingProcessors.clear();
  }

  private executeCheckpointBulkOperation(opName: string) {
    const bulkOp = this.checkpointCache.get(opName);
    if (!bulkOp?.call) {
      return;
    }

    this.logger?.(
      'executing underlying bulk operation',
      opName,
      'with args',
      bulkOp.input,
      ...bulkOp.extraArgs,
    );

    const inputArray = [...bulkOp.input];

    bulkOp
      .fn(inputArray, ...bulkOp.extraArgs)
      .then((result) => {
        if (!Array.isArray(result)) {
          bulkOp.call!.resolve(result);
          return;
        }

        if (result.length !== bulkOp.input.size) {
          throw new BalarError(
            'result array length does not match input array length for operation ' +
              opName,
          );
        }

        bulkOp.call!.resolve(new Map(result.map((res, i) => [inputArray[i], res])));
      })
      .catch(bulkOp.call.reject);
  }

  async runScope<In, Out>(
    requests: In[],
    processor: (request: In) => Promise<Out>,
    partitionKey?: number,
  ): Promise<Result<In, Out>> {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new BalarError('balar error: missing processor ID');
    }

    const orderKey = this.nextScopeOrderKey.get(processorId) ?? 0;
    this.nextScopeOrderKey.set(processorId, orderKey + 1);
    // Same branches within same scope calls (if/switch) across executions will have the
    // same branch key, we use it to distinguish between concurrent if/switch calls and
    // correctly group executions
    const branchKey = `$${orderKey}/${partitionKey ?? 0}`;

    const scopeSyncPartition =
      this.scopeSyncCache.get(branchKey) ?? this.initScopeSyncPartition();
    this.scopeSyncCache.set(branchKey, scopeSyncPartition);

    scopeSyncPartition.input.push(...requests);

    for (const request of requests) {
      scopeSyncPartition.fnByInput.set(
        request,
        processor as (req: unknown) => Promise<unknown>,
      );
    }
    this.awaitingProcessors.add(processorId);

    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    const allResults = (await scopeSyncPartition.call?.cachedPromise) as Result<In, Out>;

    const result = {
      successes: new Map<In, Out>(),
      errors: new Map<In, unknown>(),
    };

    for (const req of requests) {
      if (allResults.successes.has(req)) {
        result.successes.set(req, allResults.successes.get(req)!);
      }
      if (allResults.errors.has(req)) {
        result.errors.set(req, allResults.errors.get(req)!);
      }
    }

    return result;
  }

  registerCall(
    operationId: string,
    fn: BulkFn<unknown, unknown, unknown[]>,
    inputs: unknown[],
    extraArgs: unknown[],
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new BalarError('balar error: missing processor ID');
    }

    const bulkOp = this.checkpointCache.get(operationId) ?? {
      fn,
      input: new Set(),
      extraArgs,
      call: null,
    };
    this.checkpointCache.set(operationId, bulkOp);

    if (!bulkOp.call) {
      bulkOp.call = this.initDeferredPromise();
    }
    inputs.forEach((input) => bulkOp.input.add(input));

    this.awaitingProcessors.add(processorId);
    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    // Returns the whole result set => consumers should filter
    return bulkOp.call!.cachedPromise!;
  }
}
