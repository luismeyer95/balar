import { AsyncLocalStorage } from 'async_hooks';
import { BulkRegistry, BulkyPlan, def } from '../src/index';
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
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
    });

    // Act/Assert
    await expect(registry.getCurrentBudgets(1)).rejects.toThrowError();
  });

  test('concurrent bulk execs using the same scalar fns should be isolated', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
    });

    async function processor(budgetId: number): Promise<number> {
      const currentBudget = await registry.getCurrentBudgets(budgetId);
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

  // TODO: fix and enable
  test('nested bulk execs should re-use the same context', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudget: def(spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets),
      getAccountById: def(spyGetAccountsById as typeof accountsRepo.getAccountsById),
      getBudgetSpend: def(spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends),
    });

    accountsRepo.reset();
    budgetsRepo.reset();

    const accountId1 = await accountsRepo.createAccount({ name: 'Account 1' });
    accountsRepo.linkAccountToBudgets([{ accountId: accountId1, budgetIds: [1, 2] }]);
    for (const id of [1, 2]) {
      await budgetsRepo.createBudget({
        accountId: accountId1,
        amount: id * 500,
      });
    }

    const accountId2 = await accountsRepo.createAccount({ name: 'Account 2' });
    accountsRepo.linkAccountToBudgets([{ accountId: accountId2, budgetIds: [3, 4] }]);
    for (const id of [3, 4]) {
      await budgetsRepo.createBudget({
        accountId: accountId2,
        amount: id * 500,
      });
    }

    budgetsRepo.spendOnBudget(1, 1000);
    budgetsRepo.spendOnBudget(1, 1000);
    budgetsRepo.spendOnBudget(2, 1);
    budgetsRepo.spendOnBudget(3, 4);
    budgetsRepo.spendOnBudget(3, 6);

    // Act/Assert
    const result = await balar.execute(
      [accountId1, accountId2],
      async function getSpendOfAccount(accountId: number) {
        const account = await registry.getAccountById(accountId);
        if (!account?.budgetIds) {
          return null;
        }

        const spends = await balar.execute(account.budgetIds, (budgetId) =>
          registry.getBudgetSpend(budgetId),
        );
        // const spends = await balar.execute(account.budgetIds, (budgetId) =>
        //   balar
        //     .execute([budgetId], (budgetId) => registry.getBudgetSpend(budgetId))
        //     .then((spends) => spends.get(budgetId)!),
        // );

        return [...spends.values()].reduce((acc, spend) => acc + (spend || 0), 0);
      },
    );

    expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
    expect(spyGetAccountsById).toHaveBeenCalledWith([accountId1, accountId2]);

    expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
    expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3, 4]);

    const expected = new Map([
      [accountId1, 2001],
      [accountId2, 10],
    ]);
    expect(result).toEqual(expected);
  });

  test('simple workflow with 1 checkpoint', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
    });

    // Act
    const result = await balar.execute([1], async function (id: number): Promise<number> {
      const currentBudget = await registry.getCurrentBudgets(id);
      return currentBudget.amount;
    });

    // Assert
    const expected = new Map([[1, 500]]);
    expect(result).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
  });

  test('standard bulk update workflow with sequential checkpoints', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudget: def(spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets),
      updateBudget: def(spyUpdateBudgets as typeof budgetsRepo.updateBudgets),
    });

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

  test('concurrent checkpoints with Promise.all()', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
      getBudgetSpends: def(spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends),
    });

    // Odd numbers should fail (way over budget)
    budgetsRepo.spendOnBudget(1, 1000);
    budgetsRepo.spendOnBudget(3, 5000);

    // Act
    const requestIds = [1, 2, 3, 4];
    const issues = await balar.execute(requestIds, async function (id: number): Promise<
      Issues | true
    > {
      const issues = new Issues();

      const [currentBudget, budgetSpend] = await Promise.all([
        registry.getCurrentBudgets(id),
        registry.getBudgetSpends(id),
      ]);

      if (budgetSpend > currentBudget.amount) {
        issues.errors.push(
          `current spend is above limit: ${budgetSpend} > ${currentBudget.amount}`,
        );
        return issues;
      }

      return true;
    });

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
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getAccountsById: def(spyGetAccountsById as typeof accountsRepo.getAccountsById),
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
    });

    // Act
    const requestIds = [1, 2, 3]; // all budgets are under the same account
    const issues = await balar.execute(
      requestIds,
      async function processor(budgetId: number): Promise<Account> {
        const budget = await registry.getCurrentBudgets(budgetId);

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

  test('concurrent requests that patch the same account should coalesce into 1 shared bulk input/output', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
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

    // Act
    const requestIds = [1, 2, 3]; // all budgets are under the same account
    const issues = await balar.execute(
      requestIds,
      async function linkAccountToBudget(budgetId: number): Promise<boolean> {
        const budget = await registry.getCurrentBudgets(budgetId);

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
});
