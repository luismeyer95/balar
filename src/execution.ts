import {
  ExecutionOptions,
  BatchOperation,
  ScopeOperation,
  DeferredPromise,
  BatchFn,
} from './api';
import { EXECUTION, PROCESSOR_ID } from './constants';
import {
  BalarError,
  BalarStopError,
  ExecutionResults,
  ExecutionResultsInternal,
} from './primitives';
import crypto from 'node:crypto';
import { chunk } from './utils';

/**
 * Processes a batch of inputs using the provided processor function and returns the results keyed by input. When a wrapped batch function (obtained via `balar.wrap.fns()` or `balar.wrap.object()`) is called inside `balar.run()`, inputs are collected across all executions of the processor function so that only a single call to the underlying batch function is performed.
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
): Promise<ExecutionResults<In, Out>> {
  const execution = EXECUTION.getStore();

  const results = execution
    ? await execution.runScope(inputs, processor)
    : await new BalarExecution(processor, opts).run(inputs);

  const successes = Array.from(results.successes.entries()).map(([input, result]) => ({
    input,
    result,
  }));
  const errors = Array.from(results.errors.entries()).map(([input, err]) => ({
    input,
    err,
  }));

  return [successes, errors];
}

export class BalarExecution<MainIn, MainOut> {
  checkpointCache: Map<string, BatchOperation<unknown, unknown, unknown[]>> = new Map();

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

  async run(requests: MainIn[]): Promise<ExecutionResultsInternal<MainIn, MainOut>> {
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
        const batchOp = this.checkpointCache.get(opName);
        batchOp?.call?.reject(error);
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
        this.executeCheckpointBatchOperation(name);
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

  private executeCheckpointBatchOperation(opName: string) {
    const batchOp = this.checkpointCache.get(opName);
    if (!batchOp?.call) {
      return;
    }

    this.logger?.(
      'executing underlying batch operation',
      opName,
      'with args',
      batchOp.input,
      ...batchOp.extraArgs,
    );

    const inputArray = [...batchOp.input];

    batchOp
      .fn(inputArray, ...batchOp.extraArgs)
      .then((result) => {
        if (!Array.isArray(result)) {
          batchOp.call!.resolve(result);
          return;
        }

        if (result.length !== batchOp.input.size) {
          throw new BalarError(
            'result array length does not match input array length for operation ' +
              opName,
          );
        }

        batchOp.call!.resolve(new Map(result.map((res, i) => [inputArray[i], res])));
      })
      .catch(batchOp.call.reject);
  }

  /**
   * `runScope` is the core abstraction that enables:
   * - Creating top-level and nested balar contexts for processor executions to interact with,
   *   allowing for synchronization of API calls across them
   * - Having branches within a balar context (if/switch) that execute concurrently, automatically
   *   partitioning the dataset by branch and synchronizing API calls within
   *
   * If ran at top-level (`balar.run()`), the provided processor function is applied to each
   * input in `inputs`, and the execution finishes when all of them are settled
   *
   * If ran within an existing balar context (nested `balar.run()`, `balar.if()`, `balar.switch()`),
   * then `runScope` is being ran concurrently for all inputs in this context. In this case, the
   * execution finishes when all the (different) processor fn calls are settled for all inputs
   * across executions.
   *
   *   In the case of nested `balar.run()`, the engine waits for all executions to have provided
   *   its inputs + handler. Then, all handlers are executed concurrently as part of a new balar
   *   context stacked on top of the 1st.
   *
   *   In the particular case of if/switch, multiple balar contexts must be created and executed
   *   independently for each branch (2+). To allow for this, `runScope` exposes a `partitionKey`
   *   arg that is used by the engine to understand which inputs + handler is part of which partition.
   *   All inputs are still awaited before executing handlers, but once done, each partition will be
   *   executed as a separate balar context.
   */
  async runScope<In, Out>(
    requests: In[],
    processor: (request: In) => Promise<Out>,
    partitionKey?: number,
  ): Promise<ExecutionResultsInternal<In, Out>> {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new BalarError('balar error: missing processor ID');
    }

    const orderKey = this.nextScopeOrderKey.get(processorId) ?? 0;
    this.nextScopeOrderKey.set(processorId, orderKey + 1);
    // Same-position branches across concurrent scope calls (if/switch) within the same execution
    // will have colliding partition keys. To handle this, we use the order key to distinguish
    // scope calls and partition key for the branch within.
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

    const allResults = (await scopeSyncPartition.call
      ?.cachedPromise) as ExecutionResultsInternal<In, Out>;

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
    fn: BatchFn<unknown, unknown, unknown[]>,
    inputs: unknown[],
    extraArgs: unknown[],
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new BalarError('balar error: missing processor ID');
    }

    const batchOp = this.checkpointCache.get(operationId) ?? {
      fn,
      input: new Set(),
      extraArgs,
      call: null,
    };
    this.checkpointCache.set(operationId, batchOp);

    if (!batchOp.call) {
      batchOp.call = this.initDeferredPromise();
    }
    inputs.forEach((input) => batchOp.input.add(input));

    this.awaitingProcessors.add(processorId);
    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    // Returns the whole result set => consumers should filter
    return batchOp.call!.cachedPromise!;
  }
}
