import { def } from '../src/index';
import * as balar from '../src/index';
import {
  Account,
  AccountsRepository,
  BudgetsRepository,
  UpdateIssues as Issues,
} from './fakes/budget.fake';

describe('tests', () => {
  const accountsRepo = new AccountsRepository();
  const budgetsRepo = new BudgetsRepository();

  const spyGetCurrentBudgets = jest.fn(budgetsRepo.getCurrentBudgets.bind(budgetsRepo));
  const spyGetBudgetSpends = jest.fn(budgetsRepo.getBudgetSpends.bind(budgetsRepo));
  const spyUpdateBudgets = jest.fn(budgetsRepo.updateBudgets.bind(budgetsRepo));
  const spyGetAccountsById = jest.fn(accountsRepo.getAccountsById.bind(accountsRepo));
  const spyLinkBudgetsToAccount = jest.fn(
    accountsRepo.linkAccountToBudgets.bind(accountsRepo),
  );

  let registry = createDefaultRegistry();

  function createDefaultRegistry() {
    return balar.scalarize({
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
  }

  let account1: Account;
  let account2: Account;
  let accountId1 = 0;
  let accountId2 = 0;

  async function setupAccountsAndBudgets() {
    account1 = { name: 'Account 1', budgetIds: [1, 2, 3, 4] };
    accountId1 = await accountsRepo.createAccount(account1);
    for (const id of account1.budgetIds ?? []) {
      await budgetsRepo.createBudget({
        accountId: accountId1,
        amount: id * 500,
      });
    }

    account2 = { name: 'Account 2', budgetIds: [5, 6] };
    accountId2 = await accountsRepo.createAccount(account2);
    for (const id of account2.budgetIds ?? []) {
      await budgetsRepo.createBudget({
        accountId: accountId2,
        amount: id * 500,
      });
    }
  }

  beforeEach(async () => {
    jest.clearAllMocks();
    accountsRepo.reset();
    budgetsRepo.reset();

    registry = createDefaultRegistry();
    await setupAccountsAndBudgets();
  });

  describe('basic tests', () => {
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
      const result = await balar.execute(
        [1],
        async function (id: number): Promise<number> {
          const currentBudget = await registry.getCurrentBudget(id);
          return currentBudget.amount;
        },
      );

      // Assert
      const expected = new Map([[1, 500]]);
      expect(result).toEqual(expected);

      expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    });

    test('standard bulk workflow with sequential checkpoints', async () => {
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
  });

  describe('concurrent checkpoints', () => {
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
  });

  describe('input/output mapping', () => {
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
        [1, account1],
        [2, account1],
        [3, account1],
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
  });

  describe('exceptions', () => {
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

  describe('branching', () => {
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
      const result = await balar.execute([1, 2, 3, 4], async function (id: number) {
        const result = await (() => {
          if (id % 2 === 0) {
            return registry.noopEven(id);
          } else {
            return registry.noopOdd(id);
          }
        })();

        return result;
      });

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

    test('branching - scalar vs execute', async () => {
      // Arrange
      async function noop(arr: number[]) {
        return new Map(arr.map((id) => [id, id]));
      }

      const noopMock = jest.fn(noop);

      const registry = balar.scalarize({
        noop: def(noopMock),
      });

      // Act
      const result = await balar.execute([1, 2, 3, 4], async function (id: number) {
        const result = await (() => {
          if (id % 2 === 0) {
            return registry.noop(id);
          } else {
            return balar.execute([id * 10, id * 100], (id) => registry.noop(id));
          }
        })();

        return typeof result === 'number' ? result : [...result.values()];
      });

      // Assert
      expect(noopMock).toHaveBeenNthCalledWith(1, [2, 4]);
      expect(noopMock).toHaveBeenNthCalledWith(2, [10, 100, 30, 300]);
      expect(noopMock).toHaveBeenCalledTimes(2);

      expect(result).toEqual(
        new Map<number, number | number[]>([
          [1, [10, 100]],
          [2, 2],
          [3, [30, 300]],
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

  describe('nested contexts', () => {
    beforeEach(() => {
      budgetsRepo.spendOnBudget(1, 100);
      budgetsRepo.spendOnBudget(1, 100);
      budgetsRepo.spendOnBudget(2, 1);
      budgetsRepo.spendOnBudget(5, 4);
      budgetsRepo.spendOnBudget(5, 6);
    });

    test('1-level nested execute() context should sync with concurrent executions at 0th level', async () => {
      // Act/Assert
      const result = await balar.execute(
        [accountId1, accountId2],
        async function getSpendOfAccount(accountId) {
          const account = await registry.getAccountsById(accountId);
          if (!account?.budgetIds) {
            return null;
          }

          const spends = await balar.execute(account.budgetIds, (budgetId) =>
            registry.getBudgetSpends(budgetId),
          );

          return [...spends.values()].reduce((acc, spend) => acc + (spend || 0), 0);
        },
      );

      expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
      expect(spyGetAccountsById).toHaveBeenCalledWith([accountId1, accountId2]);

      expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
      expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3, 4, 5, 6]);

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
      // Act/Assert
      const result = await balar.execute(
        [accountId1, accountId2],
        async function getSpendOfAccount(accountId) {
          const account = await registry.getAccountsById(accountId);
          if (!account?.budgetIds) {
            return null;
          }

          // Run nested execute() context
          const spends = await balar.execute(account.budgetIds, (budgetId) =>
            registry.getBudgetSpends(budgetId),
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
      expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3, 4, 5, 6]);

      expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
      expect(spyGetCurrentBudgets).toHaveBeenCalledWith([1, 5]);

      const expected = new Map([
        [accountId1, { accountId: accountId1, amount: 500 }], // budget 1
        [accountId2, { accountId: accountId2, amount: 2500 }], // budget 5
      ]);
      expect(result).toEqual(expected);
    });
  });
});
