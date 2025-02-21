import { EventEmitter } from "node:events";

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
  fn: (request: In[]) => Promise<Map<number, Out>>;
  queue: { key: In; resolve: (ret: Out) => void }[];
  keySelector: (request: In) => number;
};

export class BulkyPlan {
  getCurrentBudgets: Handler<number, number>;
  updateBudgets: Handler<UpdateRequest, boolean>;

  lastSeenHandler!: Handler<any, any>;

  maybeExecute<In, Out>(handler: Handler<In, Out>) {
    if (
      handler.queue.length + this.filteredCandidates !==
      this.totalCandidates
    ) {
      // not all candidates have reached the next checkpoint
      return;
    }

    // next checkpoint reached, run bulk operation
    const args = handler.queue.map((item) => item.key);
    handler.fn(args).then((result) => {
      for (const item of handler.queue) {
        const key = handler.keySelector(item.key);
        item.resolve(result.get(key)!);
      }
      handler.queue = [];
    });
  }

  constructor(
    public processor: (
      request: UpdateRequest,
      use: {
        getCurrentBudgets: (id: number) => Promise<number>;
        updateBudgets: (request: UpdateRequest) => Promise<boolean>;
      },
    ) => Promise<UpdateIssues | null>,

    registry: {
      getCurrentBudgets: (requestIds: number[]) => Promise<Map<number, number>>;
      updateBudgets: {
        fn: (requests: UpdateRequest[]) => Promise<Map<number, boolean>>;
        keySelector: (request: UpdateRequest) => number;
      };
    },
  ) {
    this.getCurrentBudgets = {
      fn: registry.getCurrentBudgets,
      keySelector: (id) => id,
      queue: [],
    };
    this.updateBudgets = { ...registry.updateBudgets, queue: [] };
  }

  events = new EventEmitter();
  filteredCandidates: number = 0;
  totalCandidates: number = 0;

  private getGetCurrentBudgets() {
    return async (id: number): Promise<number> => {
      this.lastSeenHandler = this.getCurrentBudgets;

      return new Promise<number>((resolve) => {
        this.getCurrentBudgets.queue.push({ key: id, resolve });
        this.maybeExecute(this.getCurrentBudgets);
      });
    };
  }

  private getUpdateBudgets() {
    return async (req: UpdateRequest): Promise<boolean> => {
      this.lastSeenHandler = this.updateBudgets;

      return new Promise<boolean>((resolve) => {
        this.updateBudgets.queue.push({ key: req, resolve });
        this.maybeExecute(this.updateBudgets);
      });
    };
  }

  async run(
    requests: UpdateRequest[],
    keySelector: (r: UpdateRequest) => number,
  ): Promise<Map<number, UpdateIssues>> {
    this.totalCandidates = requests.length;

    const getCurrentBudgets = this.getGetCurrentBudgets();
    const updateBudgets = this.getUpdateBudgets();

    const handlers = requests.map(async (request) => {
      const result = await this.processor(request, {
        getCurrentBudgets,
        updateBudgets,
      });

      if (result) {
        this.filteredCandidates += 1;
        this.maybeExecute(this.lastSeenHandler);
      }

      return result;
    });

    const results = await Promise.all(handlers);
    const resultsByKey = new Map(
      results
        .map((result, index) => [keySelector(requests[index]), result] as const)
        .filter((pair): pair is [number, UpdateIssues] => !!pair[1]),
    );

    return resultsByKey;
  }
}
