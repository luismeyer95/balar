export interface UpdateRequest<T> {
  id: number;
  entity: T;
}

export class UpdateIssues {
  errors: string[];

  constructor(...errors: string[]) {
    this.errors = errors;
  }
}

export interface Account {
  name: string;
  budgetIds?: number[];
}

export interface Budget {
  accountId: number;
  amount: number;
}

export class AccountsRepository {
  nextId = 1;
  accounts = new Map<number, Account>();

  reset() {
    this.nextId = 1;
    this.accounts = new Map();
  }

  async createAccount(account: Account) {
    const id = this.nextId++;
    this.accounts.set(id, account);
    return id;
  }

  async getAccountsById(accountIds: number[]): Promise<Map<number, Account>> {
    return new Map(
      accountIds
        .map((id) => [id, this.accounts.get(id)] as const)
        .filter((kv): kv is [number, Account] => !!kv[1]),
    );
  }

  async linkAccountToBudgets(
    requests: { budgetIds: number[]; accountId: number }[],
  ): Promise<Map<{ budgetIds: number[]; accountId: number }, boolean>> {
    const results = new Map<{ budgetIds: number[]; accountId: number }, boolean>();

    for (const req of requests) {
      if (!this.accounts.has(req.accountId)) {
        results.set(req, false);
      } else {
        results.set(req, true);
        const account = this.accounts.get(req.accountId)!;
        account.budgetIds ??= [];
        account.budgetIds.push(...req.budgetIds);
      }
    }

    return results;
  }
}

export class BudgetsRepository {
  nextId = 1;

  budgets = new Map<number, Budget>();
  spends = new Map<number, number>();
  budgetUpdatesShouldFail = new Set<number>();

  // Fake manipulations

  reset() {
    this.nextId = 1;
    this.budgets = new Map();
    this.spends = new Map();
    this.budgetUpdatesShouldFail = new Set();
  }

  failUpdateForBudget(id: number) {
    this.budgetUpdatesShouldFail.add(id);
  }

  // Budgets

  async createBudget(budget: Budget): Promise<number> {
    const id = this.nextId++;
    this.budgets.set(id, budget);
    return id;
  }

  async updateBudgets(
    updates: { id: number; amount: number }[],
  ): Promise<Map<{ id: number; amount: number }, boolean>> {
    const results = new Map<{ id: number; amount: number }, boolean>();

    for (const upd of updates) {
      if (!this.budgets.has(upd.id) || this.budgetUpdatesShouldFail.has(upd.id)) {
        results.set(upd, false);
      } else {
        results.set(upd, true);
        this.budgets.set(upd.id, { ...this.budgets.get(upd.id)!, amount: upd.amount });
      }
    }

    return results;
  }

  async getCurrentBudgets(budgetIds: number[]): Promise<Map<number, Budget>> {
    return new Map(
      budgetIds
        .map((id) => [id, this.budgets.get(id) ?? null] as const)
        .filter((kv): kv is [number, Budget] => !!kv[1]),
    );
  }

  // Spends

  async spendOnBudget(id: number, amount: number) {
    this.spends.set(id, (this.spends.get(id) ?? 0) + amount);
  }

  async getBudgetSpends(budgetIds: number[]): Promise<Map<number, number>> {
    return new Map(
      budgetIds
        .map((id) => [id, this.spends.get(id) ?? null] as const)
        .filter((kv): kv is [number, number] => !!kv[1]),
    );
  }
}
