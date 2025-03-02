import { def } from '../src/index';
import * as balar from '../src/index';
import {
  Account,
  AccountsRepository,
  BudgetsRepository,
  UpdateIssues as Issues,
} from './fakes/budget.fake';

describe('nested contexts tests', () => {
  const accountsRepo = new AccountsRepository();
  const budgetsRepo = new BudgetsRepository();

  const ACCOUNT_1: Account = { name: 'Account 1' };
  const ACCOUNT_2: Account = { name: 'Account 2' };
  let accountId1 = 0;
  let accountId2 = 0;

  async function setupAccountsAndBudgets() {
    accountId1 = await accountsRepo.createAccount(ACCOUNT_1);
    accountsRepo.linkAccountToBudgets([{ accountId: accountId1, budgetIds: [1, 2] }]);
    for (const id of [1, 2]) {
      await budgetsRepo.createBudget({
        accountId: accountId1,
        amount: id * 500,
      });
    }

    accountId2 = await accountsRepo.createAccount({ name: 'Account 2' });
    accountsRepo.linkAccountToBudgets([{ accountId: accountId2, budgetIds: [3, 4] }]);
    for (const id of [3, 4]) {
      await budgetsRepo.createBudget({
        accountId: accountId2,
        amount: id * 500,
      });
    }

    budgetsRepo.spendOnBudget(1, 100);
    budgetsRepo.spendOnBudget(1, 100);
    budgetsRepo.spendOnBudget(2, 1);
    budgetsRepo.spendOnBudget(3, 4);
    budgetsRepo.spendOnBudget(3, 6);
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

  test('1-level nested execute() context should sync with concurrent executions at 0th level', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudget: def(spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets),
      getAccountById: def(spyGetAccountsById as typeof accountsRepo.getAccountsById),
      getBudgetSpend: def(spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends),
    });

    // Act/Assert
    const result = await balar.execute(
      [accountId1, accountId2],
      async function getSpendOfAccount(accountId) {
        const account = await registry.getAccountById(accountId);
        if (!account?.budgetIds) {
          return null;
        }

        const spends = await balar.execute(account.budgetIds, (budgetId) =>
          registry.getBudgetSpend(budgetId),
        );

        return [...spends.values()].reduce((acc, spend) => acc + (spend || 0), 0);
      },
    );

    expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
    expect(spyGetAccountsById).toHaveBeenCalledWith([accountId1, accountId2]);

    expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
    expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3, 4]);

    const expected = new Map([
      [accountId1, 201],
      [accountId2, 10],
    ]);
    expect(result).toEqual(expected);
  });

  test('return from 1-level nested execute() context should restore 0th level context syncing', async () => {
    // Arrange
    const registry = balar.scalarize({
      // Register your bulk API dependencies
      getCurrentBudget: def(spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets),
      getAccountById: def(spyGetAccountsById as typeof accountsRepo.getAccountsById),
      getBudgetSpend: def(spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends),
    });

    // Act/Assert
    const result = await balar.execute(
      [accountId1, accountId2],
      async function getSpendOfAccount(accountId) {
        const account = await registry.getAccountById(accountId);
        if (!account?.budgetIds) {
          return null;
        }

        // Run nested execute() context
        const spends = await balar.execute(account.budgetIds, (budgetId) =>
          registry.getBudgetSpend(budgetId),
        );

        let maxSpendingBudgetId = 0;
        for (const [budgetId, amount] of spends) {
          if (!maxSpendingBudgetId || amount > spends.get(maxSpendingBudgetId)!) {
            maxSpendingBudgetId = budgetId;
          }
        }

        // Continue main context after popping the nested context
        return registry.getCurrentBudget(maxSpendingBudgetId);
      },
    );

    expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
    expect(spyGetAccountsById).toHaveBeenCalledWith([accountId1, accountId2]);

    expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
    expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3, 4]);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyGetCurrentBudgets).toHaveBeenCalledWith([1, 3]);

    const expected = new Map([
      [accountId1, { accountId: accountId1, amount: 500 }], // budget 1
      [accountId2, { accountId: accountId2, amount: 1500 }], // budget 3
    ]);
    expect(result).toEqual(expected);
  });
});
