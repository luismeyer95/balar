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

  test('nested execute() context should sync with concurrent executions', async () => {
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
      [accountId1, 2001],
      [accountId2, 10],
    ]);
    expect(result).toEqual(expected);
  });
});
