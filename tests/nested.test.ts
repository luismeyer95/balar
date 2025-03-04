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

  test('multiple nesting levels with processing before and after nestings', async () => {
    // Arrange
    const noop2 = jest.fn(async (arr: number[]) => new Map(arr.map((id) => [id, id])));
    const registry = balar.scalarize({
      noop1: def(async (arr: number[]) => new Map(arr.map((id) => [id, id]))),
      noop2: def(noop2),
    });

    // Act
    const result = await balar.execute([10, 15, 30], async function (a: number) {
      await registry.noop1(a);

      const result = await balar.execute([a + 1, a + 11], async (b) => {
        if (b % 2 === 0) {
          return -1;
        }

        await Promise.all([registry.noop1(b), registry.noop2(b)]);

        const result = await balar.execute([b + 1, b + 11], async (c: number) => {
          const [one, two] = [registry.noop1(c), registry.noop2(c)];
          await two;
          await one;

          return c;
        });

        return [...result.values()];
      });

      await registry.noop2(a);
      return [...result.values()];
    });

    // Assert
    expect(noop2).toHaveBeenCalledTimes(3);
    expect(noop2).toHaveBeenNthCalledWith(1, [11, 21, 31, 41]);
    expect(noop2).toHaveBeenNthCalledWith(2, [12, 22, 32, 42, 52]); // deduplication!
    expect(noop2).toHaveBeenNthCalledWith(3, [10, 15, 30]);
    expect(result).toEqual(
      new Map<number, (number[] | -1)[]>([
        [
          10,
          [
            [12, 22],
            [22, 32],
          ],
        ],
        [15, [-1, -1]],
        [
          30,
          [
            [32, 42],
            [42, 52],
          ],
        ],
      ]),
    );
  });

  test('return from level-1 nested execute() context should restore level-0 context syncing', async () => {
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

  test('branching - simple branching', async () => {
    // Arrange
    async function noop(arr: number[]) {
      return new Map(arr.map((id) => [id, id]));
    }

    const noopEven = jest.fn(noop);
    const noopOdd = jest.fn(noop);

    const registry = balar.scalarize({
      noopEven: def(noopEven),
      noopOdd: def(noopOdd),
    });

    // Act
    const result = await balar.execute(
      [1, 2, 3, 4],
      async function (id: number): Promise<number> {
        // .. arbitrary code, that could take different amount of time to execute across calls

        // no work
        // if we resolve when max(exec.count
        const result = await (async () => {
          if (id % 2 === 0) {
            return registry.noopEven(id);
          } else {
            return registry.noopOdd(id);
          }
        })();

        return result;
      },
    );

    // Assert
    expect(noopEven).toHaveBeenNthCalledWith(1, [2, 4]);
    expect(noopOdd).toHaveBeenNthCalledWith(1, [1, 3]);
    expect(result).toEqual(
      new Map([
        [1, 1],
        [2, 2],
        [3, 3],
        [4, 4],
      ]),
    );
  });

  test('branching - binary search', async () => {
    // Arrange
    async function lt(arr: number[], num: number) {
      return new Map(arr.map((id) => [id, id < num]));
    }
    async function gt(arr: number[], num: number) {
      return new Map(arr.map((id) => [id, id > num]));
    }

    const lt5 = jest.fn((arr) => lt(arr, 5));
    const gt1 = jest.fn((arr) => gt(arr, 1));
    const lt3 = jest.fn((arr) => lt(arr, 3));
    const lt9 = jest.fn((arr) => lt(arr, 9));
    const gt7 = jest.fn((arr) => gt(arr, 7));

    const registry = balar.scalarize({
      lt5: def(lt5),

      gt1: def(gt1),
      lt3: def(lt3),

      lt9: def(lt9),
      gt7: def(gt7),
    });

    // Act
    const result = await balar.execute(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      async function search(id: number) {
        if (await registry.lt5(id)) {
          if (await registry.gt1(id)) {
            if (await registry.lt3(id)) {
              return 2;
            }
          }
        } else {
          if (await registry.lt9(id)) {
            if (await registry.gt7(id)) {
              return 8;
            }
          }
        }

        return null;
      },
    );

    // Assert
    expect(lt5).toHaveBeenCalledWith([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    expect(gt1).toHaveBeenCalledWith([0, 1, 2, 3, 4]);
    expect(lt3).toHaveBeenCalledWith([2, 3, 4]);

    expect(lt9).toHaveBeenCalledWith([5, 6, 7, 8, 9, 10]);
    expect(gt7).toHaveBeenCalledWith([5, 6, 7, 8]);

    for (const op of [lt5, gt1, lt3, lt9, gt7]) {
      expect(op).toHaveBeenCalledTimes(1);
    }

    expect(result).toEqual(
      new Map([
        [0, null],
        [1, null],
        [2, 2],
        [3, null],
        [4, null],
        [5, null],
        [6, null],
        [7, null],
        [8, 8],
        [9, null],
        [10, null],
      ]),
    );
  });
});
