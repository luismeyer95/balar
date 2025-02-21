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

type Handler<In, Out> = {
  fn: BulkHandlerFn<In, Out>;
  queue: HandlerQueue<In, Out>;
};

type BulkHandlerFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;
type ScalarHandlerFn<In, Out> = (request: In) => Promise<Out>;

type HandlerQueue<In, Out> = { key: In; resolve: (ret: Out) => void }[];

type Scalarize<T extends Record<string, BulkHandlerFn<any, any>>> = {
  [K in keyof T]: T[K] extends BulkHandlerFn<infer In, infer Out>
    ? ScalarHandlerFn<In, Out>
    : never;
};

type Handlerize<T extends Record<string, BulkHandlerFn<any, any>>> = {
  [K in keyof T]: T[K] extends BulkHandlerFn<infer In, infer Out>
    ? Handler<In, Out>
    : never;
};

export class BulkyPlan<T extends Record<string, BulkHandlerFn<any, any>>> {
  lastSeenHandler!: Handler<any, any>;
  handlerRegistry: Handlerize<T>;

  filteredCandidates: number = 0;
  totalCandidates: number = 0;

  constructor(
    private processor: (
      request: UpdateRequest,
      use: Scalarize<T>,
    ) => Promise<UpdateIssues | null>,

    registry: T,
  ) {
    this.handlerRegistry = {} as Handlerize<T>;
    this.registerHandlers(registry);
  }

  private registerHandlers(handlers: T) {
    for (const [name, handler] of Object.entries(handlers) as Array<
      [keyof T, T[keyof T]]
    >) {
      this.handlerRegistry[name] = {
        fn: handler,
        queue: [],
      } as any; // TODO: fix this
    }
  }

  maybeExecute<In, Out>(handler: Handler<In, Out>) {
    if (handler.queue.length + this.filteredCandidates !== this.totalCandidates) {
      // not all candidates have reached the next checkpoint
      return;
    }

    // next checkpoint reached, run bulk operation
    const args = handler.queue.map((item) => item.key);
    handler.fn(args).then((result) => {
      for (const item of handler.queue) {
        item.resolve(result.get(item.key)!);
      }
      handler.queue = [];
    });
  }

  async run(requests: UpdateRequest[]): Promise<Map<UpdateRequest, UpdateIssues>> {
    this.totalCandidates = requests.length;

    const processorHandlers = {} as Scalarize<T>;

    for (const handlerName of Object.keys(this.handlerRegistry) as Array<keyof T>) {
      const handler = this.handlerRegistry[handlerName];

      processorHandlers[handlerName] = (async (input: any): Promise<any> => {
        this.lastSeenHandler = handler;

        return new Promise<any>((resolve) => {
          handler.queue.push({ key: input, resolve });
          this.maybeExecute(handler);
        });
      }) as any; // TODO: fix this
    }

    const handlers = requests.map(async (request) => {
      const result = await this.processor(request, processorHandlers);

      if (result) {
        this.filteredCandidates += 1;
        this.maybeExecute(this.lastSeenHandler);
      }

      return result;
    });

    const results = await Promise.all(handlers);
    const resultsByKey = new Map(
      results
        .map((result, index) => [requests[index], result] as const)
        .filter((pair): pair is [UpdateRequest, UpdateIssues] => !!pair[1]),
    );

    return resultsByKey;
  }
}
