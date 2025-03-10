import {
  ProcessorFn,
  ExecuteOptions,
  BulkOperation,
  CheckedRegistryEntry,
  BulkInvocation,
} from './api';
import { DEFAULT_MAX_CONCURRENT_EXECUTIONS, EXECUTION, PROCESSOR_ID } from './constants';
import { chunk } from './utils';

export class BalarExecution<MainIn, MainOut> {
  internalRegistry: Map<string, BulkOperation<unknown, unknown, unknown[]>> = new Map();

  queuedExecute: ProcessorFn<unknown, unknown> | null = null;
  concurrentExecs: BulkInvocation<unknown, unknown> | null = null;

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
    this.concurrentExecs = null;
    this.queuedOperations = new Set();
    this.awaitingProcessors = new Set();
    this.doneProcessors = 0;
    this.totalProcessors = 0;
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    // console.log('executing plan:', requests);
    const resultByRequest = new Map<MainIn, MainOut>();

    await EXECUTION.run(this as any /* TODO FIX */, async () => {
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

    // console.log(
    //   'executing underlying op',
    //   opName,
    //   'with',
    //   finalInputs,
    //   ...bulkOp.extraArgs,
    // );

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

////////////////////////////////////////

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

  return execution.runNested(requests, processor);
}

export { def as scalar, object, facade } from './config';
