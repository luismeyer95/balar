import { BulkyPlan } from "../src/index";

test("it works", async () => {
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

  // Processing function

  async function processor(
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

    return null;
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
    return new Map(
      requests.map((request) => [request.id, request.id % 4 !== 0]),
    );
  }

  const requests = [
    { id: 1, budget: 1000 }, // success
    { id: 2, budget: 0 }, // fail: can't have 0
    { id: 3, budget: 1 }, // fail: can't lower
    { id: 4, budget: 3000 }, // fail: divisible by 4
  ];

  const expected = new Map([
    [2, { errors: ["budget should be greater than 0"] }],
    [3, { errors: ["budget must not be lowered"] }],
    [4, { errors: ["budget update failed"] }],
  ]);

  const plan = new BulkyPlan(processor, {
    getCurrentBudgets,
    updateBudgets: { fn: updateBudgets, keySelector: (r) => r.id },
  });

  const issues: Map<number, UpdateIssues> = await plan.run(
    requests,
    (r) => r.id,
  );
  expect(issues).toEqual(expected);
});
