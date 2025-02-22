import { BulkyPlan } from '../src/index';
import {
  Account,
  AccountsRepository,
  BudgetsRepository,
  UpdateIssues,
} from './fakes/budget.fake';

describe('budget tests', () => {
  const accountsRepository = new AccountsRepository();
  const budgetsRepository = new BudgetsRepository();

  const ACCOUNT = { name: 'Default Account', exceededBudgetsCount: 0 };

  async function setupAccountsAndBudgets() {
    const accountId = await accountsRepository.createAccount(ACCOUNT);
    for (const id of [1, 2, 3, 4]) {
      await budgetsRepository.createBudget({
        accountId,
        amount: id * 500,
      });
    }
  }

  const spyGetCurrentBudgets = jest.fn(
    budgetsRepository.getCurrentBudgets.bind(budgetsRepository),
  );
  const spyGetBudgetSpends = jest.fn(
    budgetsRepository.getBudgetSpends.bind(budgetsRepository),
  );
  const spyUpdateBudgets = jest.fn(
    budgetsRepository.updateBudgets.bind(budgetsRepository),
  );
  const spyGetAccountsById = jest.fn(
    accountsRepository.getAccountsById.bind(accountsRepository),
  );

  beforeEach(async () => {
    spyGetCurrentBudgets.mockClear();
    spyGetBudgetSpends.mockClear();
    spyUpdateBudgets.mockClear();
    spyGetAccountsById.mockClear();

    accountsRepository.reset();
    budgetsRepository.reset();
    await setupAccountsAndBudgets();
  });

  test('standard workflow with sequential checkpoints should work', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets:
          spyGetCurrentBudgets as typeof budgetsRepository.getCurrentBudgets,
        updateBudgets: spyUpdateBudgets as typeof budgetsRepository.updateBudgets,
      },

      // Define your scalar processor
      async processor(
        request: { id: number; amount: number },
        use,
      ): Promise<UpdateIssues | null> {
        const issues = new UpdateIssues();
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

    budgetsRepository.failUpdateForBudget(4);

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
      [{ id: 2, amount: 0 }, new UpdateIssues('budget should be greater than 0')],
      [{ id: 3, amount: 1 }, new UpdateIssues('budget must not be lowered')],
      [{ id: 4, amount: 3000 }, new UpdateIssues('budget update failed')],
    ]);
    expect(issues).toEqual(expected);

    expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    expect(spyUpdateBudgets).toHaveBeenCalledTimes(1);
  });

  test('concurrent checkpoints with Promise.all() should work', async () => {
    // Arrange
    const plan = new BulkyPlan({
      // Register your bulk API dependencies
      register: {
        getCurrentBudgets:
          spyGetCurrentBudgets as typeof budgetsRepository.getCurrentBudgets,
        getBudgetSpends: spyGetBudgetSpends as typeof budgetsRepository.getBudgetSpends,
      },

      // Define your scalar processor
      async processor(id: number, use): Promise<UpdateIssues | null> {
        const issues = new UpdateIssues();

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

        return null;
      },
    });

    // Odd numbers should fail (way over budget)
    budgetsRepository.spendOnBudget(1, 1000);
    budgetsRepository.spendOnBudget(3, 5000);

    // Act
    const requestIds = [1, 2, 3, 4];
    const issues = await plan.run(requestIds);

    // Assert
    const expected = new Map([
      [1, new UpdateIssues('current spend is above limit: 1000 > 500')],
      [2, null],
      [3, new UpdateIssues('current spend is above limit: 5000 > 1500')],
      [4, null],
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
        getAccountsById: spyGetAccountsById as typeof accountsRepository.getAccountsById,
        getCurrentBudgets:
          spyGetCurrentBudgets as typeof budgetsRepository.getCurrentBudgets,
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

  test('concurrent requests that patch the same account should coalesce into 1 shared bulk input/output', async () => {});
});
