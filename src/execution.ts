import {
  ProcessorFn,
  ExecutionOptions,
  BulkOperation,
  CheckedRegistryEntry,
  BulkInvocation,
} from './api';
import { EXECUTION, PROCESSOR_ID } from './constants';
import { chunk } from './utils';

export async function run<In, Out>(
  requests: In[],
  processor: (request: In) => Promise<Out>,
  opts: ExecutionOptions = {},
): Promise<Map<In, Out>> {
  const execution = EXECUTION.getStore();

  if (!execution) {
    return new BalarExecution(processor, opts).run(requests);
  }

  return execution.runNested(requests, processor);
}

export class BalarExecution<MainIn, MainOut> {
  internalRegistry: Map<string, BulkOperation<unknown, unknown, unknown[]>> = new Map();

  queuedExecute: ProcessorFn<unknown, unknown> | null = null;
  concurrentExecs: BulkInvocation<unknown, unknown> | null = null;

  queuedOperations: Set<string> = new Set();
  awaitingProcessors: Set<number> = new Set();
  doneProcessors = 0;
  totalProcessors = 0;

  maxConcurrency = Infinity;
  logger?: (...msgs: any[]) => void;

  constructor(
    private readonly processor: (request: MainIn) => Promise<MainOut>,
    opts: ExecutionOptions,
  ) {
    this.processor = processor;

    if (opts.concurrency) {
      this.maxConcurrency = opts.concurrency;
    }
    if (opts.logger) {
      this.logger = opts.logger;
    }
  }

  reset() {
    this.internalRegistry = new Map();
    this.queuedExecute = null;
    this.concurrentExecs = null;
    this.queuedOperations = new Set();
    this.awaitingProcessors = new Set();
    this.doneProcessors = 0;
    this.totalProcessors = 0;
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
    this.reset();
    this.totalProcessors = requestBatch.length;

    return requestBatch.map(async (request, index) => {
      const result = await PROCESSOR_ID.run(index, () => this.processor(request));
      this.logger?.('returned from processor execution for request input', request);

      this.doneProcessors += 1;
      if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
        this.executeCheckpoint();
      }

      return result;
    });
  }

  private executeCheckpoint() {
    this.logger?.('executing checkpoint');

    if (this.queuedExecute) {
      const plan = new BalarExecution(this.queuedExecute, {
        concurrency: this.maxConcurrency,
        logger: this.logger,
      });

      plan
        .run(this.concurrentExecs!.input)
        .then((allResults) => this.concurrentExecs?.resolve(allResults))
        .catch((err) => this.concurrentExecs?.reject(err))
        .finally(() => (this.concurrentExecs = null));
    }

    for (const name of this.queuedOperations) {
      const op = this.internalRegistry.get(name)!;
      this.executeCheckpointBulkOperation(name, op);
    }

    // Clear checkpoint buffer
    this.queuedExecute = null;
    this.queuedOperations.clear();
    this.awaitingProcessors.clear();
  }

  private executeCheckpointBulkOperation(
    opName: string,
    bulkOp: BulkOperation<unknown, unknown, unknown[]>,
  ) {
    const call = bulkOp.call;
    if (!call) {
      return;
    }

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
      .catch(call.reject)
      .finally(() => (bulkOp.call = null));
  }

  async runNested<In, Out>(requests: In[], processor: (request: In) => Promise<Out>) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    const concurrentExecs = this.concurrentExecs ?? {
      input: [],
      resolve: () => {},
      reject: () => {},
      cachedPromise: null,
    };

    if (!concurrentExecs.cachedPromise) {
      concurrentExecs.cachedPromise = new Promise((resolve, reject) => {
        concurrentExecs.resolve = resolve;
        concurrentExecs.reject = reject;
      });
    }

    concurrentExecs.input.push(...requests);
    this.concurrentExecs = concurrentExecs;

    this.queuedExecute = processor as (request: unknown) => Promise<unknown>;
    this.awaitingProcessors.add(processorId);

    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    const allResults = (await concurrentExecs.cachedPromise) as Map<In, Out>;

    const result = new Map<In, Out>();
    for (const req of requests) {
      result.set(req, allResults.get(req)!);
    }

    return result;
  }

  callBulkHandler(
    operationId: string,
    config: CheckedRegistryEntry<unknown, unknown, unknown[]>,
    inputs: unknown[],
    extraArgs: unknown[],
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    const registryEntry = this.internalRegistry.get(operationId) ?? {
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

      this.internalRegistry.set(operationId, registryEntry);
    }

    registryEntry.call!.input.push(...inputs);

    this.awaitingProcessors.add(processorId);
    this.queuedOperations.add(operationId);

    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    // Returns the whole result set => consumers should filter
    return registryEntry.call!.cachedPromise!;
  }
}
