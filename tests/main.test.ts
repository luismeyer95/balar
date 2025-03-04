import { def } from '../src/index';
import * as balar from '../src/index';
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
    accountsRepo.linkAccountToBudgets([{ accountId: 1, budgetIds: [1, 2, 3, 4] }]);

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

  const registry = balar.scalarize({
    // Register your bulk API dependencies
    getAccountsById: def(spyGetAccountsById as typeof accountsRepo.getAccountsById),
    getCurrentBudget: def(spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets),
    updateBudget: def(spyUpdateBudgets as typeof budgetsRepo.updateBudgets),
    getBudgetSpends: def(spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends),
    linkAccountToBudgets: def({
      fn: spyLinkBudgetsToAccount as typeof accountsRepo.linkAccountToBudgets,
      transformInputs: (reqs) => {
        type Req = (typeof reqs)[0];
        const requestMapping: Map<Req, Req> = new Map();
        const newRequestByAccount: Map<number, Req> = new Map();

        for (const req of reqs) {
          const newRequest = newRequestByAccount.get(req.accountId) ?? {
            accountId: req.accountId,
            budgetIds: [],
          };
          newRequestByAccount.set(req.accountId, newRequest);

          newRequest.budgetIds.push(...req.budgetIds);
          requestMapping.set(req, newRequest);
        }

        return requestMapping;
      },
    }),
  });

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

  test('executing scalar function outside bulk exec should fail', async () => {
    await expect(registry.getCurrentBudget(1)).rejects.toThrowError();
  });

  test('no op processor', async () => {
    // Act
    const result = await balar.execute([1, 2], async function () {});

    // Assert
    expect(result).toEqual(
      new Map([
        [1, undefined],
        [2, undefined],
      ]),
    );
  });

  test('empty inputs', async () => {
    // Act
    const result = await balar.execute(
      [],
      async function getBudgetAmount(budgetId: number): Promise<number> {
        const currentBudget = await registry.getCurrentBudget(budgetId);
        return currentBudget.amount;
      },
    );

    // Assert
    expect(result).toEqual(new Map());
  });

  test('simple workflow with 1 checkpoint', async () => {
    // Act
    const result = await balar.execute([1], async function (id: number): Promise<number> {
      const currentBudget = await registry.getCurrentBudget(id);
      return currentBudget.amount;
    });

    // Assert
    const expected = new Map([[1, 500]]);
    expect(result).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
  });

  test('standard bulk update workflow with sequential checkpoints', async () => {
    // Arrange
    budgetsRepo.failUpdateForBudget(4);

    // Act
    const requests = [
      { id: 1, amount: 1000 }, // success (from 500 to 1000)
      { id: 2, amount: 0 }, // fail: can't have 0
      { id: 3, amount: 1 }, // fail: can't lower (from 1500 to 1)
      { id: 4, amount: 3000 }, // fail (forced failure)
    ];
    const issues = await balar.execute(
      requests,
      async function updateBudgetWithValidation(request: {
        id: number;
        amount: number;
      }): Promise<Issues | null> {
        const issues = new Issues();
        const requestBudget = request.amount;

        if (requestBudget === 0) {
          issues.errors.push('budget should be greater than 0');
          return issues;
        }

        const currentBudget = await registry.getCurrentBudget(request.id);
        if (requestBudget < currentBudget.amount) {
          issues.errors.push('budget must not be lowered');
          return issues;
        }

        const updatedBudget = await registry.updateBudget(request);
        if (!updatedBudget) {
          issues.errors.push('budget update failed');
          return issues;
        }

        return null;
      },
    );

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

  test('simple concurrent checkpoints with Promise.all()', async () => {
    // Arrange
    // Odd numbers should fail (way over budget)
    budgetsRepo.spendOnBudget(1, 1000);
    budgetsRepo.spendOnBudget(3, 5000);

    // Act
    const budgetIds = [1, 2, 3, 4];
    const issues = await balar.execute(
      budgetIds,
      async function (budgetId: number): Promise<Issues | true> {
        const issues = new Issues();

        const [currentBudget, budgetSpend] = await Promise.all([
          registry.getCurrentBudget(budgetId),
          registry.getBudgetSpends(budgetId),
        ]);

        if (budgetSpend > currentBudget.amount) {
          issues.errors.push(
            `current spend is above limit: ${budgetSpend} > ${currentBudget.amount}`,
          );
          return issues;
        }

        return true;
      },
    );

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

  test('concurrent bulk execs using the same scalar fns should be isolated', async () => {
    // Arrange
    async function processor(budgetId: number): Promise<number> {
      const currentBudget = await registry.getCurrentBudget(budgetId);
      return currentBudget.amount;
    }

    // Act
    const [result1, result2] = await Promise.all([
      balar.execute([1, 2], processor),
      balar.execute([3, 4], processor),
    ]);

    // Assert
    const expected1 = new Map([
      [1, 500],
      [2, 1000],
    ]);
    expect(result1).toEqual(expected1);
    const expected2 = new Map([
      [3, 1500],
      [4, 2000],
    ]);
    expect(result2).toEqual(expected2);
    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(2);
  });

  test('mixing in yields before concurrent checkpoints', async () => {
    // Arrange
    // Odd numbers should fail (way over budget)
    budgetsRepo.spendOnBudget(1, 1000);
    budgetsRepo.spendOnBudget(3, 5000);

    // Act
    const budgetIds = [1, 2, 3, 4];
    const results = await balar.execute(
      budgetIds,
      async function getRemainingBudget(budgetId: number) {
        // Arbitrary return for ID 4
        if (budgetId === 4) {
          return 'budget does not exist';
        }

        const [currentBudget, budgetSpend] = await Promise.all([
          registry.getCurrentBudget(budgetId),
          registry.getBudgetSpends(budgetId),
        ]);

        return currentBudget.amount - (budgetSpend || 0);
      },
    );

    // Assert
    const expected = new Map<number, number | string>([
      [1, -500],
      [2, 1000],
      [3, -3500],
      [4, 'budget does not exist'],
    ]);
    expect(results).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyGetCurrentBudgets).toHaveBeenCalledWith([1, 2, 3]);

    expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
    expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3]);
  });

  test('reads of the same account should coalesce into 1 shared bulk input/output', async () => {
    // Act
    const requestIds = [1, 2, 3]; // all budgets are under the same account
    const issues = await balar.execute(
      requestIds,
      async function processor(budgetId: number): Promise<Account> {
        const budget = await registry.getCurrentBudget(budgetId);

        return await registry.getAccountsById(budget.accountId);
      },
    );

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

  test('requests that patch the same account should coalesce into 1 shared bulk input/output', async () => {
    // Act
    const requestIds = [1, 2, 3]; // all budgets are under the same account
    const issues = await balar.execute(
      requestIds,
      async function linkAccountToBudget(budgetId: number): Promise<boolean> {
        const budget = await registry.getCurrentBudget(budgetId);

        const result = await registry.linkAccountToBudgets({
          accountId: budget.accountId,
          budgetIds: [budgetId],
        });

        return result;
      },
    );

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

  test('simple workflow with exception thrown inline', async () => {
    // Act
    const executeCall = () =>
      balar.execute(
        [1, 2, 777],
        async function getBudgetOrThrowIfNotExist(id: number): Promise<number> {
          const currentBudget = await registry.getCurrentBudget(id);
          if (currentBudget === undefined) {
            throw new Error('budget does not exist');
          }

          return currentBudget.amount;
        },
      );

    // Assert
    await expect(executeCall).rejects.toThrow('budget does not exist');
    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
  });
});
