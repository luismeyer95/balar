import { balar } from '../src';
import { def } from '../src/config';
import {
  Account,
  AccountsRepository,
  BudgetsRepository,
  UpdateIssues as Issues,
} from './fakes/budget.fake';

describe('tests', () => {
  const accountsRepo = new AccountsRepository();
  const budgetsRepo = new BudgetsRepository();

  const spyInspect = jest.fn(async () => new Map());
  const spyGetCurrentBudgets = jest.fn(budgetsRepo.getCurrentBudgets.bind(budgetsRepo));
  const spyGetBudgetSpends = jest.fn(budgetsRepo.getBudgetSpends.bind(budgetsRepo));
  const spyUpdateBudgets = jest.fn(budgetsRepo.updateBudgets.bind(budgetsRepo));
  const spyGetAccountsById = jest.fn(accountsRepo.getAccountsById.bind(accountsRepo));
  const spyLinkBudgetsToAccount = jest.fn(
    accountsRepo.linkAccountToBudgets.bind(accountsRepo),
  );

  let registry = createDefaultRegistry();

  function createDefaultRegistry() {
    return balar.wrap.fns({
      // Register your bulk API dependencies
      getAccountsById: def(spyGetAccountsById as typeof accountsRepo.getAccountsById),
      getCurrentBudgets: def(
        spyGetCurrentBudgets as typeof budgetsRepo.getCurrentBudgets,
      ),
      updateBudgets: def(spyUpdateBudgets as typeof budgetsRepo.updateBudgets),
      getBudgetSpends: def(spyGetBudgetSpends as typeof budgetsRepo.getBudgetSpends),
      linkAccountToBudgets: def({
        fn: spyLinkBudgetsToAccount as typeof accountsRepo.linkAccountToBudgets,
      }),
      inspect: def(spyInspect as (...args: any[]) => Promise<Map<any, void>>),
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

  describe('facade', () => {
    test('array as scalar not supported', async () => {
      const mock = jest.fn(async (_: number[][]) => new Map<number[], boolean>());
      const registry = balar.wrap.fns({
        // @ts-expect-error
        takesArrayScalar: def(mock),
      });

      await balar.execute([1, 2], async function (args: number) {
        return await registry.takesArrayScalar([args]);
      });

      // Result is unexpectedly a 1-level deep array as the runtime resolved
      // signature was bulk (can't easily disambiguate between scalar and bulk
      // calls for array types).
      expect(mock).toHaveBeenCalledWith([1, 2]);
    });

    test('array as scalar not supported', async () => {
      const mock = jest.fn(async (_: number[][]) => new Map<number[], boolean>());
      const registry = balar.wrap.fns({
        // @ts-expect-error
        takesArrayScalar: def(mock),
      });

      await balar.execute([1, 2], async function (args: number) {
        return await registry.takesArrayScalar([args]);
      });

      // Result is unexpectedly a 1-level deep array as the runtime resolved
      // signature was bulk (can't easily disambiguate between scalar and bulk
      // calls for array types).
      expect(mock).toHaveBeenCalledWith([1, 2]);
    });

    describe('object', () => {
      // Different ways to produce the same facade type
      const testCases = [
        {
          test: 'default object facade',
          registry: balar.wrap.object(budgetsRepo),
        },
        {
          test: 'pick facade',
          picks: ['getCurrentBudgets', 'getBudgetSpends'],
          registry: balar.wrap.object(budgetsRepo, {
            pick: ['getCurrentBudgets', 'getBudgetSpends'],
          }),
        },
        {
          test: 'exclude facade',
          excludes: ['updateBudgets'],
          registry: balar.wrap.object(budgetsRepo, {
            exclude: ['updateBudgets'],
          }),
        },
      ];

      test.each(testCases)('$test', async ({ registry, picks, excludes }) => {
        budgetsRepo.spendOnBudget(1, 200);
        budgetsRepo.spendOnBudget(2, 450);

        // Act
        const resultWithScalarApi = await balar.execute(
          [1, 2],
          async function getRemainingAmount(budgetId: number): Promise<number> {
            const currentBudget = await registry.getCurrentBudgets(budgetId);
            const budgetSpent = await registry.getBudgetSpends(budgetId);

            return currentBudget!.amount - (budgetSpent || 0);
          },
        );

        // Act
        const resultWithBulkApi = await balar.execute(
          [1, 2],
          async function getRemainingAmount(budgetId: number): Promise<number> {
            const currentBudget = (await registry.getCurrentBudgets([budgetId])).get(
              budgetId,
            )!;
            const budgetSpent = (await registry.getBudgetSpends([budgetId])).get(
              budgetId,
            )!;

            return currentBudget.amount - (budgetSpent || 0);
          },
        );

        // Assert
        const effectiveApi = (picks ?? Object.keys(registry)).filter(
          (key) => !(excludes ?? []).includes(key),
        );
        expect(Object.keys(registry)).toEqual(effectiveApi);
        expect(resultWithScalarApi).toEqual(resultWithBulkApi);
        expect(resultWithBulkApi).toEqual(
          new Map([
            [1, 300],
            [2, 550],
          ]),
        );
      });
    });
  });

  describe('basic tests', () => {
    test('executing scalar function outside bulk exec should fail', async () => {
      await expect(registry.getCurrentBudgets(1)).rejects.toThrowError();
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
          const currentBudget = await registry.getCurrentBudgets(budgetId);
          return currentBudget!.amount;
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
          const currentBudget = await registry.getCurrentBudgets(id);
          return currentBudget!.amount;
        },
      );

      // Assert
      const expected = new Map([[1, 500]]);
      expect(result).toEqual(expected);

      expect(spyGetCurrentBudgets).toHaveBeenCalledTimes(1);
    });

    test('standard bulk workflow with sequential checkpoints (scalar api)', async () => {
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

          const currentBudget = await registry.getCurrentBudgets(request.id);
          if (requestBudget < currentBudget!.amount) {
            issues.errors.push('budget must not be lowered');
            return issues;
          }

          const updatedBudget = await registry.updateBudgets(request);
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

    test('standard bulk workflow with sequential checkpoints (bulk api)', async () => {
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

          const currentBudget = (await registry.getCurrentBudgets([request.id])).get(
            request.id,
          )!;

          if (requestBudget < currentBudget.amount) {
            issues.errors.push('budget must not be lowered');
            return issues;
          }

          const updatedBudget = (await registry.updateBudgets([request])).get(request)!;

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
      expect(spyGetCurrentBudgets).toHaveBeenCalledWith([1, 3, 4]);
      expect(spyUpdateBudgets).toHaveBeenCalledTimes(1);
      expect(spyUpdateBudgets).toHaveBeenCalledWith([
        { id: 1, amount: 1000 },
        { id: 4, amount: 3000 },
      ]);
    });

    test('bulk fn with extended arglist', async () => {
      async function bulkMul(arr: number[], rhs: number) {
        return new Map(arr.map((id) => [id, id * rhs]));
      }

      const spyBulkMul = jest.fn(bulkMul);
      const registry = balar.wrap.fns({
        mul: def(spyBulkMul),
      });

      // Act
      const budgetIds = [1, 2, 3, 4];
      const issues = await balar.execute(budgetIds, async (n) => {
        return n % 2 === 0 ? registry.mul(n, 2) : registry.mul(n, 3);
      });

      // Assert
      const expected = new Map([
        [1, 3],
        [2, 4],
        [3, 9],
        [4, 8],
      ]);
      expect(issues).toEqual(expected);
      expect(spyBulkMul).toHaveBeenCalledTimes(2);
      expect(spyBulkMul).toHaveBeenCalledWith([1, 3], 3);
      expect(spyBulkMul).toHaveBeenCalledWith([2, 4], 2);
    });
  });

  describe('bulk api', () => {
    test('results of processor bulk calls should only include their own data', async () => {
      // Arrange
      budgetsRepo.spendOnBudget(1, 100);
      budgetsRepo.spendOnBudget(1, 100);
      budgetsRepo.spendOnBudget(2, 1);
      budgetsRepo.spendOnBudget(5, 4);
      budgetsRepo.spendOnBudget(5, 6);

      // Act/Assert
      const result = await balar.execute(
        [accountId1, accountId2],
        async function getSpendOfAccount(accountId) {
          const account = await registry.getAccountsById(accountId);
          if (!account?.budgetIds) {
            return null;
          }

          const spends = await registry.getBudgetSpends(account.budgetIds);
          await registry.inspect([...spends.values()]);

          return [...spends.values()].reduce((acc, spend) => acc + (spend || 0), 0);
        },
      );

      expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
      expect(spyGetAccountsById).toHaveBeenCalledWith([accountId1, accountId2]);

      expect(spyGetBudgetSpends).toHaveBeenCalledTimes(1);
      expect(spyGetBudgetSpends).toHaveBeenCalledWith([1, 2, 3, 4, 5, 6]);

      expect(spyInspect).toHaveBeenCalledTimes(1);
      expect(spyInspect).toHaveBeenCalledWith([
        200,
        1,
        undefined,
        undefined,
        10,
        undefined,
      ]);

      const expected = new Map([
        [accountId1, 201],
        [accountId2, 10],
      ]);
      expect(result).toEqual(expected);
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
            registry.getCurrentBudgets(budgetId),
            registry.getBudgetSpends(budgetId),
          ]);

          if (budgetSpend! > currentBudget!.amount) {
            issues.errors.push(
              `current spend is above limit: ${budgetSpend} > ${currentBudget!.amount}`,
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
        const currentBudget = await registry.getCurrentBudgets(budgetId);
        return currentBudget!.amount;
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

    test('processor should trigger checkpoint by returning', async () => {
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
            registry.getCurrentBudgets(budgetId),
            registry.getBudgetSpends(budgetId),
          ]);

          return currentBudget!.amount - (budgetSpend || 0);
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

    test('checkpoint-triggering processor calls multiple scalar fns', async () => {
      async function noop(arr: number[]) {
        return new Map(arr.map((id) => [id, id]));
      }
      const noopMock1 = jest.fn(noop);
      const noopMock2 = jest.fn(noop);
      const registry = balar.wrap.fns({
        noop1: def(noopMock1),
        noop2: def(noopMock2),
      });

      // Act
      const budgetIds = [1, 2, 3, 4];
      const issues = await balar.execute(budgetIds, async (n) => {
        let p1 = registry.noop1(n);
        let p2: Promise<number | undefined> = Promise.resolve(0);

        if (n >= 3) {
          p2 = registry.noop2(n);
        }

        const [one, two] = await Promise.all([p1, p2]);
        return one! + two!;
      });

      // Assert
      const expected = new Map([
        [1, 1],
        [2, 2],
        [3, 6],
        [4, 8],
      ]);
      expect(issues).toEqual(expected);

      expect(noopMock1).toHaveBeenCalledTimes(1);
      expect(noopMock1).toHaveBeenCalledWith([1, 2, 3, 4]);
      expect(noopMock2).toHaveBeenCalledTimes(1);
      expect(noopMock2).toHaveBeenCalledWith([3, 4]);
    });
  });

  describe('input/output mapping', () => {
    test('should not deduplicate equal inputs to bulk fn (not in scope)', async () => {
      // Act
      const requestIds = [1, 2, 3]; // all budgets are under the same account
      const issues = await balar.execute(
        requestIds,
        async function processor(budgetId: number) {
          const budget = await registry.getCurrentBudgets(budgetId);

          return registry.getAccountsById(budget!.accountId);
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
      expect(spyGetAccountsById).toHaveBeenCalledWith([1, 1, 1]);
      expect(spyGetAccountsById).toHaveBeenCalledTimes(1);
    });
  });

  describe('exceptions', () => {
    test('should bubble up exception thrown directly inside processor', async () => {
      // Act
      const executeCall = () =>
        balar.execute(
          [1, 2, 777],
          async function getBudgetOrThrowIfNotExist(id: number): Promise<number> {
            const currentBudget = await registry.getCurrentBudgets(id);
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

    test('should bubble up exception thrown inside bulk fn', async () => {
      const mayThrow = jest.fn(async (arr: number[]) => {
        return new Map(
          arr.map((id) => {
            if (id === 777) {
              throw new Error('budget does not exist');
            }
            return [id, id];
          }),
        );
      });

      const registry = balar.wrap.fns({
        mayThrow: def(mayThrow),
      });

      // Act
      const executeCall = () =>
        balar.execute([1, 2, 777], async function (id: number) {
          return registry.mayThrow(id);
        });

      // Assert
      await expect(executeCall).rejects.toThrow('budget does not exist');
      expect(mayThrow).toHaveBeenCalledTimes(1);
      expect(mayThrow).toHaveBeenCalledWith([1, 2, 777]);
    });
  });

  describe('branching', () => {
    test('simple branching', async () => {
      // Arrange
      async function noop(arr: number[]) {
        return new Map(arr.map((id) => [id, id]));
      }

      const noopEven = jest.fn(noop);
      const noopOdd = jest.fn(noop);

      const registry = balar.wrap.fns({
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

    test('scalar vs execute', async () => {
      // Arrange
      async function noop(arr: number[]) {
        return new Map(arr.map((id) => [id, id]));
      }

      const noopMock = jest.fn(noop);

      const registry = balar.wrap.fns({
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

        return typeof result === 'number' ? result : [...result!.values()];
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

    test('binary search', async () => {
      // Arrange
      async function lt(arr: number[], num: number) {
        return new Map(arr.map((id) => [id, id < num]));
      }
      async function gt(arr: number[], num: number) {
        return new Map(arr.map((id) => [id, id > num]));
      }

      const spyLt = jest.fn(lt);
      const spyGt = jest.fn(gt);

      const registry = balar.wrap.fns({
        lt: def(spyLt),
        gt: def(spyGt),
      });

      // Act
      const result = await balar.execute(
        [0, 1, 2, 3, 4, 5, 6, 7, 8],
        async function search(id: number) {
          if (await registry.lt(id, 5)) {
            if (await registry.gt(id, 2)) {
              return id;
            } else if (await registry.lt(id, 3)) {
              return id;
            }
          } else if (await registry.lt(id, 9)) {
            if (await registry.gt(id, 6)) {
              return id;
            } else if (await registry.lt(id, 7)) {
              return id;
            }
          }
          return id;
        },
      );

      // Assert
      expect(spyLt).toHaveBeenCalledWith([0, 1, 2, 3, 4, 5, 6, 7, 8], 5);

      expect(spyGt).toHaveBeenCalledWith([0, 1, 2, 3, 4], 2);
      expect(spyLt).toHaveBeenCalledWith([0, 1, 2], 3);

      expect(spyLt).toHaveBeenCalledWith([5, 6, 7, 8], 9);

      expect(spyGt).toHaveBeenCalledWith([5, 6, 7, 8], 6);
      expect(spyLt).toHaveBeenCalledWith([5, 6], 7);

      expect(spyLt).toHaveBeenCalledTimes(4);
      expect(spyGt).toHaveBeenCalledTimes(2);

      expect(result).toEqual(
        new Map(
          Array(9)
            .fill(0)
            .map((_, i) => [i, i]),
        ),
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
            registry.getBudgetSpends(budgetId).then((s) => s!),
          );

          return [...spends!.values()].reduce((acc, spend) => acc + (spend || 0), 0);
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
      const registry = balar.wrap.fns({
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
      expect(noop2).toHaveBeenNthCalledWith(2, [12, 22, 22, 32, 32, 42, 42, 52]);
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
            if (!maxSpendingBudgetId || amount! > spends.get(maxSpendingBudgetId)!) {
              maxSpendingBudgetId = budgetId;
            }
          }

          // Continue main context after popping the nested context
          return registry.getCurrentBudgets(maxSpendingBudgetId);
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

  describe('extra arguments', () => {
    async function bulkMul(arr: number[], rhs: number) {
      return new Map(arr.map((id) => [id, id * rhs]));
    }
    async function bulkMulObj(arr: number[], { rhs }: { rhs: number }) {
      return new Map(arr.map((id) => [id, id * rhs]));
    }
    async function bulkAdd3(arr: number[], two: number, three: number) {
      return new Map(arr.map((id) => [id, id + two + three]));
    }

    const spyBulkMul = jest.fn(bulkMul);
    const spyBulkMulObj = jest.fn(bulkMulObj);
    const spyBulkAdd3 = jest.fn(bulkAdd3);

    const registry = balar.wrap.fns({
      mul: def(spyBulkMul),
      mulObj: def(spyBulkMulObj),
      add3: def({
        fn: spyBulkAdd3,
        getArgsId: ([two, three]) => (two + three).toString(),
      }),
    });

    beforeEach(() => {
      jest.clearAllMocks();
    });

    test('bulk fn with extended arglist', async () => {
      // Act
      const budgetIds = [1, 2, 3, 4];
      const issues = await balar.execute(budgetIds, async (n) => {
        return n % 2 === 0 ? registry.mul(n, 2) : registry.mul(n, 3);
      });

      // Assert
      const expected = new Map([
        [1, 3],
        [2, 4],
        [3, 9],
        [4, 8],
      ]);
      expect(issues).toEqual(expected);
      expect(spyBulkMul).toHaveBeenCalledTimes(2);
      expect(spyBulkMul).toHaveBeenCalledWith([1, 3], 3);
      expect(spyBulkMul).toHaveBeenCalledWith([2, 4], 2);
    });

    test('extra object arg with default args stringify resolver', async () => {
      // Act
      const budgetIds = [1, 2, 3, 4];
      const issues = await balar.execute(budgetIds, async (n) => {
        return n % 2 === 0
          ? registry.mulObj(n, { rhs: 2 })
          : registry.mulObj(n, { rhs: 3 });
      });

      // Assert
      const expected = new Map([
        [1, 3],
        [2, 4],
        [3, 9],
        [4, 8],
      ]);
      expect(issues).toEqual(expected);
      expect(spyBulkMulObj).toHaveBeenCalledTimes(2);
      expect(spyBulkMulObj).toHaveBeenCalledWith([1, 3], { rhs: 3 });
      expect(spyBulkMulObj).toHaveBeenCalledWith([2, 4], { rhs: 2 });
    });

    test('extra object arg with custom args id resolver', async () => {
      function isEven(n: number) {
        return n % 2 === 0;
      }

      // Act
      const budgetIds = [1, 2, 3];
      const issues = await balar.execute(budgetIds, async (n) => {
        const extra = [isEven(n) ? 2 : 3, isEven(n) ? 3 : 2] as const;
        return registry.add3(n, ...extra);
      });

      // Assert
      const expected = new Map([
        [1, 6],
        [2, 7],
        [3, 8],
      ]);
      expect(issues).toEqual(expected);
      expect(spyBulkAdd3).toHaveBeenCalledTimes(1);
      expect(spyBulkAdd3).toHaveBeenCalledWith([1, 2, 3], 3, 2);
    });
  });

  describe('concurrency', () => {
    test('configure max concurrency', async () => {
      // Arrange
      const mock = jest.fn(async (n: number[]) => new Map(n.map((id) => [id, id])));
      const reg = balar.wrap.fns({
        noop: def(mock),
      });

      // Act
      await balar.execute(
        [1, 2, 3, 4],
        async (n) => {
          await reg.noop(n);
        },
        { concurrency: 2 },
      );

      // Assert
      expect(mock).toHaveBeenCalledTimes(2);
      expect(mock).toHaveBeenNthCalledWith(1, [1, 2]);
      expect(mock).toHaveBeenNthCalledWith(2, [3, 4]);
    });
  });
});
