import { EventEmitter } from 'node:events';

interface UpdateRequest {
  id: number;
  budget: number;
}

class UpdateIssues {
  errors: string[];

  constructor() {
    this.errors = [];
  }
}

// How to handle the API of operations
// - either leave untouched, still with bulk semantincs. each request processor must retrieve its own data from the result.
// - or transform into scalar APIs. lib is tasked to retrieve the queried data from the result.
// For the latter, the scalar result must be resolvable.
// - either the result is keyed by input, or by an id in the input
//

export type ExpandRecursively<T> = T extends object
  ? T extends infer O
    ? { [K in keyof O]: ExpandRecursively<O[K]> }
    : never
  : T;

export type BulkHandlerFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
export type ScalarHandlerFn<In, Out> = (request: In) => Promise<Out>;
export type HandlerQueue<In, Out> = { key: In; resolve: (ret: Out) => void }[];

export type Handler<In, Out> = {
  fn: BulkHandlerFn<In, Out>;
  queue: HandlerQueue<In, Out>;
};

export type Scalarize<T extends Record<string, BulkHandlerFn<any, any>>> = {
  [K in keyof T]: T[K] extends BulkHandlerFn<infer In, infer Out>
    ? ScalarHandlerFn<In, Out>
    : never;
};

export type Handlerize<T extends Record<string, BulkHandlerFn<any, any>>> = {
  [K in keyof T]: T[K] extends BulkHandlerFn<infer In, infer Out>
    ? Handler<In, Out>
    : never;
};

export class BulkyPlan<
  MainIn,
  MainOut,
  T extends Record<string, BulkHandlerFn<any, any>>,
> {
  processor: (request: MainIn, use: Scalarize<T>) => Promise<MainOut>;

  handlerRegistry: Handlerize<T>;
  lastSeenHandler!: Handler<any, any>;

  doneCandidates = 0;
  totalCandidates = 0;

  constructor({
    register,
    processor,
  }: {
    register: T;
    processor: (request: MainIn, use: Scalarize<NoInfer<T>>) => Promise<MainOut>;
  }) {
    this.processor = processor;
    this.handlerRegistry = {} as Handlerize<T>;

    this.registerHandlers(register);
  }

  private registerHandlers(handlers: T) {
    // For each type of bulk operation, we need 2 things:
    // - the bulk function to execute when all candidates have reached the next checkpoint
    // - a promise queue to keep track of calls to the scalarized functions we provide to the processors
    for (const [name, handler] of Object.entries(handlers) as Array<
      [keyof T, T[keyof T]]
    >) {
      this.handlerRegistry[name] = {
        fn: handler,
        queue: [],
      } as unknown as Handlerize<T>[keyof T];
    }
  }

  private maybeExecute<In, Out>(handler: Handler<In, Out>) {
    // Checks if all candidates have reached the next checkpoint.

    // If all concurrent executions at this time have either
    // - called this handler's scalar function and awaiting execution
    // - or completed execution by returning
    // Then, we can execute the bulk function.

    if (handler.queue.length + this.doneCandidates !== this.totalCandidates) {
      // Not all candidates have reached the next checkpoint
      return;
    }

    if (handler.queue.length === 0) {
      return;
    }

    // Next checkpoint reached, run bulk operation
    const args = this.deduplicate(handler.queue.map((item) => item.key));
    handler.fn(args).then((result) => {
      for (const item of handler.queue) {
        item.resolve(result.get(item.key)!);
      }
      handler.queue = [];
    });
  }

  private deduplicate<In>(inputs: In[]): In[] {
    return Array.from(new Set(inputs));
  }

  async run(requests: MainIn[]): Promise<Map<MainIn, MainOut>> {
    const processorHandlers = {} as Scalarize<T>;

    // Initialize the target total. Used to track the progress towards each
    // checkpoint during the concurrent executions.
    this.totalCandidates = requests.length;

    for (const handlerName of Object.keys(this.handlerRegistry) as Array<keyof T>) {
      const handler = this.handlerRegistry[handlerName];

      const scalarFn = async (input: unknown): Promise<unknown> => {
        // When a scalar function is called, we need to keep track of which handler it came from.
        // If an execution returns when all other concurrent executions are awaiting the next
        // checkpoint, we should execute the bulk handler for this next checkpoint, which should
        // be the one from the last registered scalar call.
        //
        // TODO: check if we should poll all handlers for execution instead.
        this.lastSeenHandler = handler;

        return new Promise<unknown>((resolve) => {
          handler.queue.push({ key: input, resolve });
          this.maybeExecute(handler);
        });
      };

      processorHandlers[handlerName] = scalarFn as Scalarize<T>[keyof T];
    }

    const executions = requests.map(async (request) => {
      const result = await this.processor(request, processorHandlers);

      if (result) {
        this.doneCandidates += 1;
        this.maybeExecute(this.lastSeenHandler);
        // for (const handler of Object.values(this.handlerRegistry)) {
        //   this.maybeExecute(handler);
        // }
      }

      return result;
    });

    const results = await Promise.all(executions);
    const resultsByKey = new Map(
      results.map((result, index) => [requests[index], result]),
    );

    return resultsByKey;
  }
}
