import { BulkyPlan } from '../src/index';

describe('budget tests', () => {
  interface UpdateRequest {
    id: number;
    budget: number;
  }

  class UpdateIssues {
    errors: string[];

    constructor(...errors: string[]) {
      this.errors = errors;
    }
  }

  async function getCurrentBudgets(requestIds: number[]): Promise<Map<number, number>> {
    console.log('getCurrentBudgets', requestIds);
    return new Map(requestIds.map((requestId) => [requestId, requestId * 500]));
  }

  async function getBudgetLimits(requestIds: number[]): Promise<Map<number, number>> {
    console.log('getBudgetLimits', requestIds);
    return new Map(
      requestIds.map((requestId) => [
        requestId,
        requestId % 2 === 0 ? 0 : requestId * 1000,
      ]),
    );
  }

  async function updateBudgets(
    requests: UpdateRequest[],
  ): Promise<Map<UpdateRequest, boolean>> {
    console.log('updateBudgets', requests);
    return new Map(requests.map((request) => [request, request.id % 4 !== 0]));
  }

  const spyGetCurrentBudgets = jest.fn(getCurrentBudgets);
  const spyGetBudgetLimits = jest.fn(getBudgetLimits);
  const spyUpdateBudgets = jest.fn(updateBudgets);

  beforeEach(() => {
    spyGetCurrentBudgets.mockClear();
    spyGetBudgetLimits.mockClear();
    spyUpdateBudgets.mockClear();
  });

  test('it works', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets: spyGetCurrentBudgets as typeof getCurrentBudgets,
        updateBudgets: spyUpdateBudgets as typeof updateBudgets,
      },

      // Define your scalar processor
      processor: async function (
        request: UpdateRequest,
        use,
      ): Promise<UpdateIssues | null> {
        const issues = new UpdateIssues();

        if (request.budget === 0) {
          issues.errors.push('budget should be greater than 0');
          return issues;
        }

        const currentBudget = await use.getCurrentBudgets(request.id);
        if (request.budget < currentBudget) {
          issues.errors.push('budget must not be lowered');
          return issues;
        }

        const updatedBudget = await use.updateBudgets(request);
        if (!updatedBudget) {
          issues.errors.push('budget update failed');
          return issues;
        }

        return null;
      },
    });

    // Act
    const requests: UpdateRequest[] = [
      { id: 1, budget: 1000 }, // success
      { id: 2, budget: 0 }, // fail: can't have 0
      { id: 3, budget: 1 }, // fail: can't lower
      { id: 4, budget: 3000 }, // fail: divisible by 4
    ];
    const issues = await plan.run(requests);

    // Assert
    const expected = new Map([
      [{ id: 1, budget: 1000 }, null],
      [{ id: 2, budget: 0 }, new UpdateIssues('budget should be greater than 0')],
      [{ id: 3, budget: 1 }, new UpdateIssues('budget must not be lowered')],
      [{ id: 4, budget: 3000 }, new UpdateIssues('budget update failed')],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyUpdateBudgets).toHaveBeenCalledTimes(1);
  });

  test.skip('concurrent checkpoints should work', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets: spyGetCurrentBudgets as typeof getCurrentBudgets,
        getBudgetLimits: spyGetBudgetLimits as typeof getBudgetLimits,
      },

      // Define your scalar processor
      processor: async function (id: number, use): Promise<UpdateIssues | null> {
        const issues = new UpdateIssues();

        const [currentBudget, budgetLimit] = await Promise.all([
          use.getCurrentBudgets(id),
          use.getBudgetLimits(id),
        ]);

        if (currentBudget > budgetLimit) {
          console.log(currentBudget, budgetLimit);
          issues.errors.push('current budget is above limit');
          return issues;
        }

        return null;
      },
    });

    // Act
    const requestIds = [0, 1, 2]; // even numbers should fail
    const issues = await plan.run(requestIds);

    // Assert
    const expected = new Map([
      [0, new UpdateIssues('current budget is above limit')],
      [1, null],
      [2, new UpdateIssues('current budget is above limit')],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyGetBudgetLimits).toHaveBeenCalledTimes(1);
  });
});
