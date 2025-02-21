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

export async function processBulkRequest(
  requests: UpdateRequest[],
): Promise<Map<number, UpdateIssues>> {
  const issues: Map<number, UpdateIssues> = new Map();

  // validate budget not 0
  for (const request of requests) {
    if (request.budget === 0) {
      if (!issues.has(request.id)) {
        issues.set(request.id, new UpdateIssues());
      }
      issues.get(request.id)!.errors.push("budget should be greater than 0");
    }
  }

  requests = requests.filter((request) => !issues.has(request.id));

  // fetch current budgets and map them into new model for processing
  const currentBudgetById = await getCurrentBudgets(
    requests.map((request) => request.id),
  );
  const oldAndNewBudgetsById = new Map(
    requests.map((request) => [
      request.id,
      [currentBudgetById.get(request.id)!, request.budget],
    ]),
  );

  // validate budget not lowered
  for (const [requestId, [oldBudget, newBudget]] of oldAndNewBudgetsById) {
    if (oldBudget > newBudget) {
      if (!issues.has(requestId)) {
        issues.set(requestId, new UpdateIssues());
      }
      issues.get(requestId)!.errors.push("budget must not be lowered");
    }
  }

  requests = requests.filter((request) => !issues.has(request.id));

  // run the update
  const updatedBudgets = await updateBudgets(requests);

  for (const request of requests) {
    if (!updatedBudgets.get(request.id)) {
      if (!issues.has(request.id)) {
        issues.set(request.id, new UpdateIssues());
      }
      issues.get(request.id)!.errors.push("budget update failed");
    }
  }

  return issues;
}

export async function processor(
  request: UpdateRequest,
  use: {
    getCurrentBudgets: (id: number) => Promise<number>;
    updateBudgets: (request: UpdateRequest) => Promise<boolean>;
  },
): Promise<UpdateIssues | null> {
  const issues = new UpdateIssues();

  if (request.budget === 0) {
    issues.errors.push("budget should be greater than 0");
    return issues;
  }

  const currentBudget = await use.getCurrentBudgets(request.id);
  if (request.budget < currentBudget) {
    issues.errors.push("budget must not be lowered");
    return issues;
  }

  const updatedBudget = await use.updateBudgets(request);
  if (!updatedBudget) {
    issues.errors.push("budget update failed");
    return issues;
  }

  const newBudget = await use.getCurrentBudgets(request.id);

  return null;
}

// How to handle the API of operations
// - either leave untouched, still with bulk semantincs. each request processor must retrieve its own data from the result.
// - or transform into scalar APIs. lib is tasked to retrieve the queried data from the result.
// For the latter, the scalar result must be resolvable.
// - either the result is keyed by input, or by an id in the input

export async function processBulkRequest2(
  requests: UpdateRequest[],
): Promise<Map<number, UpdateIssues>> {
  const plan = new BulkyPlan(processor, {
    getCurrentBudgets,
    updateBudgets: { fn: updateBudgets, keySelector: (r) => r.id },
  });

  const result: Map<number, UpdateIssues> = await plan.run(
    requests,
    (r) => r.id,
  );

  return result;
}

type Handler<In, Out> = {
  fn: (request: In[]) => Promise<Map<number, Out>>;
  queue: { key: In; resolve: (ret: Out) => void }[];
  keySelector: (request: In) => number;
};

class BulkyPlan {
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

// External APIs

async function getCurrentBudgets(
  requestIds: number[],
): Promise<Map<number, number>> {
  console.log("getCurrentBudgets", requestIds);
  return new Map(requestIds.map((requestId) => [requestId, requestId * 500]));
}

async function updateBudgets(
  requests: UpdateRequest[],
): Promise<Map<number, boolean>> {
  console.log("updateBudgets", requests);
  return new Map(requests.map((request) => [request.id, request.id % 4 !== 0]));
}
