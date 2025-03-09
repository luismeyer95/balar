import {
  ScalarRegistryEntry,
  ProcessorFn,
  ScalarOperation,
  ExecuteOptions,
  BulkRegistryEntry,
  Operation,
  BulkOperation,
  Invocation,
  BulkInvocation,
} from './api';
import { DEFAULT_MAX_CONCURRENT_EXECUTIONS, EXECUTION, PROCESSOR_ID } from './constants';
import { chunk } from './utils';
import { bulk, scalar, object, facade } from './config';

export class BalarExecution<MainIn, MainOut> {
  internalRegistry: Map<string, Operation<unknown, unknown, unknown[]>> = new Map();

  queuedExecute: ProcessorFn<unknown, unknown> | null = null;
  concurrentExecs: Invocation<unknown, unknown>[] = [];

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

      const allRequests = this.concurrentExecs.flatMap((item) => item.input);
      plan
        .run(allRequests)
        .then((allResults) => {
          for (const exec of this.concurrentExecs) {
            exec.resolve(allResults);
          }
        })
        .catch((err) => this.concurrentExecs.forEach((exec) => exec.reject(err)))
        .finally(() => (this.concurrentExecs = []));
    }

    for (const name of this.queuedOperations) {
      const op = this.internalRegistry.get(name)!;

      if ('call' in op) {
        this.executeCheckpointBulkOperation(name, op);
      } else {
        this.executeCheckpointScalarOperation(name, op);
      }
    }

    // Clear checkpoint buffer
    this.queuedExecute = null;
    this.queuedOperations.clear();
    this.awaitingProcessors.clear();
  }

  private executeCheckpointScalarOperation(
    opName: string,
    scalarOp: ScalarOperation<unknown, unknown, unknown[]>,
  ) {
    if (scalarOp.calls.length === 0) {
      return;
    }

    // For each queued call, the input is stored as a key in order
    // to index out the result from the bulk call map. Since these
    // inputs are transformed right before execution to allow for
    // input merging, we need to apply an input key remap.
    const inputs = scalarOp.calls.map((item) => item.input);
    const mappedInputs = scalarOp.transformInputs(inputs);
    for (const execItem of scalarOp.calls) {
      const mappedInput = mappedInputs.get(execItem.input);
      if (mappedInput) {
        execItem.input = mappedInput;
      }
    }

    // In case the transform deduplicated some inputs, the resulting
    // map may contain duplicate values due to multiple original
    // inputs mapping to the same transformed input.
    const finalInputs = [...new Set(mappedInputs.values())];

    // console.log(
    //   'executing underlying op',
    //   opName,
    //   'with',
    //   finalInputs,
    //   ...scalarOp.extraArgs,
    // );

    scalarOp
      .fn(finalInputs, ...scalarOp.extraArgs)
      .then((result) => {
        for (const item of scalarOp.calls) {
          item.resolve(result.get(item.input));
        }
      })
      .catch((err) => scalarOp.calls.forEach((item) => item.reject(err)))
      .finally(() => (scalarOp.calls = []));
  }

  private executeCheckpointBulkOperation(
    opName: string,
    bulkOp: BulkOperation<unknown, unknown, unknown[]>,
  ) {
    const call = bulkOp.call;
    if (!call) {
      return;
    }

    const finalInputs = call.input.flat();
    // console.log(
    //   'executing underlying op',
    //   opName,
    //   'with',
    //   finalInputs,
    //   ...bulkOp.extraArgs,
    // );

    bulkOp
      .fn(finalInputs, ...bulkOp.extraArgs)
      .then(call.resolve)
      .catch(call.reject)
      .finally(() => (bulkOp.call = null));
  }

  async runNested<In, Out>(requests: In[], processor: (request: In) => Promise<Out>) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    // NOTE: this impl does not allow for different execute processors on the same checkpoint!
    this.queuedExecute = processor as (request: unknown) => Promise<unknown>;
    this.awaitingProcessors.add(processorId);

    const allResults = await new Promise(async (resolve, reject) => {
      this.concurrentExecs.push({ input: requests, resolve, reject });

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
    config: ScalarRegistryEntry<unknown, unknown, unknown[]>,
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
        calls: [],
      });
    }

    this.awaitingProcessors.add(processorId);
    this.queuedOperations.add(operationId);

    type ScalarOp = ScalarOperation<unknown, unknown, unknown[]>;
    const registryEntry = this.internalRegistry.get(operationId) as ScalarOp;

    return new Promise((resolve, reject) => {
      registryEntry.calls.push({
        input: input,
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

  callBulkHandler(
    operationId: string,
    config: BulkRegistryEntry<unknown, unknown, unknown[]>,
    inputs: unknown[],
    extraArgs: unknown[],
  ) {
    const processorId = PROCESSOR_ID.getStore();
    if (processorId == null) {
      throw new Error('balar error: missing processor ID');
    }

    type BulkOp = BulkOperation<unknown, unknown, unknown[]>;

    if (!this.internalRegistry.has(operationId)) {
      let call: BulkInvocation<unknown, unknown> | null = null;
      const promise = new Promise<Map<unknown, unknown>>((resolve, reject) => {
        call = {
          input: [],
          resolve,
          reject,
        };
      });
      // Promise constructor executes synchronously
      call!.cachedPromise = promise;

      const registryEntry: BulkOp = {
        ...config,
        extraArgs,
        call,
      };

      this.internalRegistry.set(operationId, registryEntry);
    }

    const registryEntry = this.internalRegistry.get(operationId)! as BulkOp;
    registryEntry.call!.input.push(inputs);

    this.awaitingProcessors.add(processorId);
    this.queuedOperations.add(operationId);

    if (this.awaitingProcessors.size + this.doneProcessors === this.totalProcessors) {
      process.nextTick(() => this.executeCheckpoint());
    }

    return registryEntry.call!.cachedPromise;
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

export default {
  execute,
  scalar,
  bulk,
  object,
  facade,
};

export { scalar, bulk, object, facade } from './config';
