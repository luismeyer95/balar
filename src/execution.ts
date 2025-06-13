import {
  ExecutionOptions,
  BulkOperation,
  RegistryEntry,
  ScopeProcessors,
  DeferredScopeExecutionCache,
} from './api';
import { DEFAULT_AWAITER, EXECUTION, PROCESSOR_ID } from './constants';
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
): Promise<Map<In, Out>> {
  const execution = EXECUTION.getStore();

  if (!execution) {
    return new BalarExecution(processor, opts).run(inputs);
  }

  return execution.runNested(inputs, processor);
}

export class BalarExecution<MainIn, MainOut> {
  checkpointCache: Map<string, BulkOperation<unknown, unknown, unknown[]>> = new Map();

  queuedScopeProcessors: ScopeProcessors<unknown, unknown> = new Map();
  scopeSyncMetadata: DeferredScopeExecutionCache<unknown, unknown> = {
    input: [],
    resolve: () => {},
    reject: () => {},
    cachedPromise: null,
    cachedResolutionAwaiter: DEFAULT_AWAITER,
  };

  awaitingProcessors: Set<number> = new Set();
  doneProcessors = 0;
  totalProcessors = 0;

  maxConcurrency = Infinity;
  logger?: (...msgs: any[]) => void;

  constructor(
    private readonly processor:
      | ((request: MainIn) => Promise<MainOut>)
      | ScopeProcessors<MainIn, MainOut>,
    opts: ExecutionOptions,
  ) {
    this.processor = processor;
    this.clearScopeSyncMetadata();

    if (opts.concurrency) {
      this.maxConcurrency = opts.concurrency;
    }
    if (opts.logger) {
      this.logger = opts.logger;
    }
  }

  clearState() {
    this.checkpointCache = new Map();
    this.queuedScopeProcessors = new Map();
    this.awaitingProcessors = new Set();
    this.doneProcessors = 0;
    this.totalProcessors = 0;
  }

  clearScopeSyncMetadata() {
    this.queuedScopeProcessors.clear();

    const scopeSyncMetadata: DeferredScopeExecutionCache<unknown, unknown> = {
      input: [],
      resolve: () => {},
      reject: () => {},
      cachedPromise: null,
      cachedResolutionAwaiter: DEFAULT_AWAITER,
    };

    scopeSyncMetadata.cachedPromise = new Promise((resolve, reject) => {
      scopeSyncMetadata.resolve = resolve;
      scopeSyncMetadata.reject = reject;
    });

    // Whatever the outcome of the nested scope, it should not fail awaiters
    scopeSyncMetadata.cachedResolutionAwaiter = scopeSyncMetadata.cachedPromise
      .then(() => undefined)
      .catch(() => undefined);

    this.scopeSyncMetadata = scopeSyncMetadata;
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    this.logger?.('starting execution for requests: ', requests);

    const resultByRequest = new Map<MainIn, MainOut>();

    await EXECUTION.run(this as BalarExecution<unknown, unknown>, async () => {
      for (const requestsBatch of chunk(requests, this.maxConcurrency)) {
        this.logger?.('starting execution for request batch: ', requestsBatch);

        const batchResults = await Promise.all(this.runBatch(requestsBatch));
        for (let i = 0; i < batchResults.length; i += 1) {
          resultByRequest.set(requestsBatch[i], batchResults[i]);
        }
      }
    });

    return resultByRequest;
  }

  private runBatch(requestBatch: MainIn[]): Promise<MainOut>[] {
    this.clearState();
    this.totalProcessors = requestBatch.length;

    return requestBatch.map(async (request, index) => {
      const result = await PROCESSOR_ID.run(index, () => {
        if (this.processor instanceof Map) {
          return this.processor.get(request)!(request);
        }
        return this.processor(request);
      });

      this.logger?.('returned from processor execution for request input', request);

      this.doneProcessors += 1;
      if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
        this.executeCheckpoint();
      }

      return result;
    });
  }

  private executeCheckpoint() {
    if (this.checkpointCache.size > 0) {
      this.logger?.('executing checkpoint');

      for (const name of this.checkpointCache.keys()) {
        this.executeCheckpointBulkOperation(name);
      }

      // Prevent next balar fn calls in same stack frame to schedule redundant checkpoint
      this.awaitingProcessors.clear();
    }

    if (this.queuedScopeProcessors.size > 0) {
      this.logger?.('executing scope checkpoint');

      const plan = new BalarExecution(this.queuedScopeProcessors, {
        concurrency: this.maxConcurrency,
        logger: this.logger,
      });

      plan
        .run(this.scopeSyncMetadata!.input)
        .then((allResults) => this.scopeSyncMetadata?.resolve(allResults))
        .catch((err) => this.scopeSyncMetadata?.reject(err))
        .finally(() => this.clearScopeSyncMetadata());
      // Prevent next balar fn calls in same stack frame to schedule redundant checkpoint
      this.queuedScopeProcessors.clear();
    }
  }

  private executeCheckpointBulkOperation(opName: string) {
    const bulkOp = this.checkpointCache.get(opName);
    const call = bulkOp?.call;
    if (!call) {
      return;
    }

    this.checkpointCache.delete(opName);

    this.logger?.(
      'executing underlying bulk operation',
      opName,
      'with args',
      call.input,
      ...bulkOp.extraArgs,
    );

    bulkOp
      .fn(call.input, ...bulkOp.extraArgs)
      .then(call.resolve)
      .catch(call.reject);
  }

  async runNested<In, Out>(requests: In[], processor: (request: In) => Promise<Out>) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    this.scopeSyncMetadata.input.push(...requests);

    for (const request of requests) {
      this.queuedScopeProcessors.set(
        request,
        processor as (req: unknown) => Promise<unknown>,
      );
    }
    this.awaitingProcessors.add(processorId);

    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    const allResults = (await this.scopeSyncMetadata.cachedPromise) as Map<In, Out>;

    const result = new Map<In, Out>();
    for (const req of requests) {
      result.set(req, allResults.get(req)!);
    }

    return result;
  }

  registerCall(
    operationId: string,
    config: RegistryEntry<unknown, unknown, unknown[]>,
    inputs: unknown[],
    extraArgs: unknown[],
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    const registryEntry = this.checkpointCache.get(operationId) ?? {
      ...config,
      extraArgs,
      call: null,
    };

    if (!registryEntry.call) {
      registryEntry.call = {
        input: [],
        resolve: () => {},
        reject: () => {},
        cachedPromise: null,
      };

      registryEntry.call.cachedPromise = new Promise((resolve, reject) => {
        registryEntry.call!.resolve = resolve;
        registryEntry.call!.reject = reject;
      });

      this.checkpointCache.set(operationId, registryEntry);
    }

    registryEntry.call!.input.push(...inputs);

    this.awaitingProcessors.add(processorId);

    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      // Only the last call to the processor should trigger the checkpoint,
      // but multiple calls in that processor may schedule the checkpoint
      process.nextTick(() => this.executeCheckpoint());
    }

    // Returns the whole result set => consumers should filter
    return registryEntry.call!.cachedPromise!;
  }

  async awaitNextScopeResolution() {
    return this.scopeSyncMetadata.cachedResolutionAwaiter!;
  }
}
