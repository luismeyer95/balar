import { BulkHandlerFn, BulkyPlan } from '../src/index';
import {
  Account,
  AccountsRepository,
  BudgetsRepository,
  UpdateIssues as Issues,
} from './fakes/budget.fake';

describe('budget tests', () => {
  const accountsRepo = new AccountsRepository();
  const budgetsRepo = new BudgetsRepository();

  const ACCOUNT: Account = { name: 'Default Account' };

  async function setupAccountsAndBudgets() {
    const accountId = await accountsRepo.createAccount(ACCOUNT);
    for (const id of [1, 2, 3, 4]) {
      await budgetsRepo.createBudget({
        accountId,
        amount: id * 500,
      });
    }
  }

  const spyGetCurrentBudgets = jest.fn(budgetsRepo.getCurrentBudgets.bind(budgetsRepo));
  const spyGetBudgetSpends = jest.fn(budgetsRepo.getBudgetSpends.bind(budgetsRepo));
  const spyUpdateBudgets = jest.fn(budgetsRepo.updateBudgets.bind(budgetsRepo));
  const spyGetAccountsById = jest.fn(accountsRepo.getAccountsById.bind(accountsRepo));
  const spyLinkBudgetsToAccount = jest.fn(
    accountsRepo.linkAccountToBudgets.bind(accountsRepo),
  );

  beforeEach(async () => {
    spyGetCurrentBudgets.mockClear();
    spyGetBudgetSpends.mockClear();
    spyUpdateBudgets.mockClear();
    spyGetAccountsById.mockClear();
    spyLinkBudgetsToAccount.mockClear();

    accountsRepo.reset();
    budgetsRepo.reset();
    await setupAccountsAndBudgets();
  });

  test('simple workflow with 1 checkpoint', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets: spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      },

      // Define your scalar processor
      async processor(id: number, use): Promise<number> {
        const currentBudget = await use.getCurrentBudgets(id);
        return currentBudget.amount;
      },
    });

    // Act
    const issues = await plan.run([1]);

    // Assert
    const expected = new Map([[1, 500]]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
  });

  test('standard bulk update workflow with sequential checkpoints', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets: spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
        updateBudgets: spyUpdateBudgets as typeof budgetsRepo.updateBudgets,
      },

      // Define your scalar processor
      async processor(
        request: { id: number; amount: number },
        use,
      ): Promise<Issues | null> {
        const issues = new Issues();
        const requestBudget = request.amount;

        if (requestBudget === 0) {
          issues.errors.push('budget should be greater than 0');
          return issues;
        }

        const currentBudget = await use.getCurrentBudgets(request.id);
        if (requestBudget < currentBudget.amount) {
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

    budgetsRepo.failUpdateForBudget(4);

    // Act
    const requests = [
      { id: 1, amount: 1000 }, // success (from 500 to 1000)
      { id: 2, amount: 0 }, // fail: can't have 0
      { id: 3, amount: 1 }, // fail: can't lower (from 1500 to 1)
      { id: 4, amount: 3000 }, // fail (forced failure)
    ];
    const issues = await plan.run(requests);

    // Assert
    const expected = new Map([
      [{ id: 1, amount: 1000 }, null],
      [{ id: 2, amount: 0 }, new Issues('budget should be greater than 0')],
      [{ id: 3, amount: 1 }, new Issues('budget must not be lowered')],
      [{ id: 4, amount: 3000 }, new Issues('budget update failed')],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyUpdateBudgets).toHaveBeenCalledTimes(1);
  });

  test('concurrent checkpoints with Promise.all()', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets: spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
        getBudgetSpends: spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends,
      },

      // Define your scalar processor
      processor: async function isBudgetSpendBelowLimit(
        id: number,
        use,
      ): Promise<Issues | true> {
        const issues = new Issues();

        const [currentBudget, budgetSpend] = await Promise.all([
          use.getCurrentBudgets(id),
          use.getBudgetSpends(id),
        ]);

        if (budgetSpend > currentBudget.amount) {
          issues.errors.push(
            `current spend is above limit: ${budgetSpend} > ${currentBudget.amount}`,
          );
          return issues;
        }

        return true;
      },
    });

    // Odd numbers should fail (way over budget)
    budgetsRepo.spendOnBudget(1, 1000);
    budgetsRepo.spendOnBudget(3, 5000);

    // Act
    const requestIds = [1, 2, 3, 4];
    const issues = await plan.run(requestIds);

    // Assert
    const expected = new Map<number, Issues | true>([
      [1, new Issues('current spend is above limit: 1000 > 500')],
      [2, true],
      [3, new Issues('current spend is above limit: 5000 > 1500')],
      [4, true],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
  });

  test('concurrent reads of the same account should coalesce into 1 shared bulk input/output', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getAccountsById: spyGetAccountsById as typeof accountsRepo.getAccountsById,
        getCurrentBudgets: spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      },

      // Define your scalar processor
      async processor(budgetId: number, use): Promise<Account> {
        const budget = await use.getCurrentBudgets(budgetId);

        return await use.getAccountsById(budget.accountId);
      },
    });

    // Act
    const requestIds = [1, 2, 3]; // all budgets are under the same account
    const issues = await plan.run(requestIds);

    // Assert
    const expected = new Map([
      [1, ACCOUNT],
      [2, ACCOUNT],
      [3, ACCOUNT],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyGetCurrentBudgets).toHaveBeenCalledWith(requestIds);
    expect(spyGetAccountsById).toHaveBeenCalledWith([1]);
    expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
  });

  test('concurrent requests that patch the same account should coalesce into 1 shared bulk input/output', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets: spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
        linkAccountToBudgets: {
          fn: spyLinkBudgetsToAccount as typeof accountsRepo.linkAccountToBudgets,
          transformInputs: (
            reqs: { budgetIds: number[]; accountId: number }[],
          ): Map<
            { accountId: number; budgetIds: number[] },
            { accountId: number; budgetIds: number[] }
          > => {
            const requestMapping: Map<
              { accountId: number; budgetIds: number[] },
              { accountId: number; budgetIds: number[] }
            > = new Map();
            const newRequestByAccount: Map<
              number,
              { accountId: number; budgetIds: number[] }
            > = new Map();

            for (const req of reqs) {
              const newRequest = newRequestByAccount.get(req.accountId) ?? {
                accountId: req.accountId,
                budgetIds: [],
              };
              newRequest.budgetIds.push(...req.budgetIds);
              newRequestByAccount.set(req.accountId, newRequest);
              requestMapping.set(req, newRequest);
            }

            return requestMapping;
          },
        },
      },

      // Define your scalar processor
      processor: async function linkAccountToBudget(
        budgetId: number,
        use,
      ): Promise<boolean> {
        const budget = await use.getCurrentBudgets(budgetId);

        const result = await use.linkAccountToBudgets({
          accountId: budget.accountId,
          budgetIds: [budgetId],
        });

        return result;
      },
    });

    // Act
    const requestIds = [1, 2, 3]; // all budgets are under the same account
    const issues = await plan.run(requestIds);

    // Assert
    const expected = new Map([
      [1, true],
      [2, true],
      [3, true],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyGetCurrentBudgets).toHaveBeenCalledWith(requestIds);

    expect(spyLinkBudgetsToAccount).toHaveBeenCalledTimes(1);
    expect(spyLinkBudgetsToAccount).toHaveBeenCalledWith([
      { accountId: 1, budgetIds: [1, 2, 3] },
    ]);
  });
});
