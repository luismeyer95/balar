# Balar

**Efficient bulk processing with scalar simplicity for Typescript + Node.js.**

---

## Installation

```bash
npm install balar
```

---

## Introduction

Balar allows writing single-item logic that runs as efficient bulk code.

Say you have an API endpoint to allow users to update the budget they can spend on your service. It has some validation checks like below. (For the sake of simplicity, we use `string` for errors and `true`  for success).

```ts
type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

class BudgetsRepository {
  async getBudget(id: number): Promise<Budget> { ... }
  async updateBudget(id: number): Promise<boolean> { ... }
}

const repository = new BudgetsRepository();

async function updateBudgetWithValidation(
  updateBudget: BudgetUpdateRequest
): Promise<string | true> {
  if (updateBudget.amount === 0) {
    return 'budget should be greater than 0';
  }

  const currentBudget = await repository.getBudget(request.id);
  if (updateBudget.amount < currentBudget!.amount) {
    return 'budget must not be lowered';
  }

  const success = await repository.updateBudget(request);
  if (!success) {
    return 'budget update failed';
  }

  return true;
}
```

Now let’s say your product offering evolved, and users can have multiple budgets to allocate to different services which they will want to update in real-time with low latency. Surely with these requirements, we don’t want to just run this code for each budget in sequence but instead batch reads and updates to minimize network latency. 

### **Why do we need bulk endpoints?**

Networking, or more specifically, the *number* of calls we need to make, is often the bottleneck in modern applications. Adoption of public clouds such as AWS has made it easy to scale up the processing power, RAM, or storage of our applications, but each networking call still needs to negotiate a complicated and unreliable global network of computers, routers, switches, and protocols, such as TCP, [adding a lot of overhead](https://www2.cs.duke.edu/ari/trapeze/freenix/node11.html) for each call. Therefore, it's usually better to make fewer requests with more data as opposed to making more requests with less data in each request.

Alright, let’s just create a bulk update endpoint that can process a list of budget updates.

```ts
type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

// Notice that we adapted the methods to handle multiple items at once
class BudgetsRepository {
  async getBudgets(id: number[]): Promise<Map<number, Budget>> { ... }
  async updateBudgets(id: number[]): Promise<Map<number, boolean>>  { ... }
}

const repository = new BudgetsRepository();

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[]
): Promise<Map<Budget, string | null>> {
  const resultByRequest = new Map<BudgetUpdateRequest, string | true>();

  const nonZeroBudgetUpdateRequests: BudgetUpdateRequest[] = []
  for (const updateBudget of requests) {
    if (updateBudget.amount <= 0) {
      resultByRequest.set(request, 'budget should be greater than 0');
    } else {
      nonZeroBudgetUpdateRequests.push(request);
    }
  }
  
  const currentBudgets = await this.budgetService.getBudgets(
    nonZeroBudgetUpdateRequests.map((r) => r.id),
  );

  const validBudgetUpdateRequests: BudgetUpdateRequest[] = [];
  for (const request of nonZeroBudgetUpdateRequests) {
    const currentBudget = currentBudgets.get(request.id)!;
    if (request.amount < currentBudget.amount) {
      resultByRequest.set(request, 'budget must not be lowered');
    } else {
      validBudgetUpdateRequests.push(request);
    }
  }

  const updateResult = await this.budgetService.updateBudgets(validBudgetUpdateRequests);
  
  for (const [request, success] of updateResult) {
    resultByRequest.set(request, success || 'budget update failed');
  }
  
  return resultByRequest;
}
```

As you may know, efficient bulk processing adds a lot of complexity and can leak everywhere into your application code. What if you could have the efficiency of bulk processing and the simplicity of single-item (scalar) processing logic? Balar allows you to have both. This code using balar may look like it runs 2 network calls per request, but it only runs 2 network calls in total regardless of the number of requests.

```ts
import { balar } from 'balar';

type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

// Notice that we adapted the methods to handle multiple items at once
class BudgetsRepository {
  async getBudgets(id: number[]): Promise<Map<number, Budget>> { ... }
  async updateBudgets(id: number[]): Promise<Map<number, boolean>>  { ... }
}

const repository = balar.wrap.object(new BudgetsRepository());

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[],
): Promise<Map<Budget, string | null>> {
  return balar.run(requests, (updateBudget: BudgetUpdateRequest) => {
    if (updateBudget.amount === 0) {
      return 'budget should be greater than 0';
    }

    const currentBudget = await repository.getBudgets(request.id);
    if (updateBudget.amount < currentBudget!.amount) {
      return 'budget must not be lowered';
    }

    const success = await repository.updateBudgets(request);
    if (!success) {
      return 'budget update failed';
    }

    return true;
  });
}
```

In short, Balar provides a clean scalar-like API to queue inputs to bulk functions of your choice and executes them as batches. The handler provided to `balar.run()` is immediately invoked for each request, but these executions are kept in sync at “checkpoints” to collect inputs for bulk calls. Once all inputs have been collected for a particular bulk operation, the underlying operation is called and the output is dispatched to each execution so that they can continue to progress until the next checkpoint (or return statement). No manual batching, no handling of partial success with input filtering; just clean, focused scalar logic with bulk efficiency.

---

## Core features

- Flexible type API: provide any operation that match Balar’s bulk function contract and get back the fine-grained types that match your needs.
- Bulk processing: describe how you would handle a single item and Balar takes care of executing your plan with bulk processing efficiency.
- Tracing logs: transparency is key, plug the logger of your choice to debug or observe balar executions.

### Non-goals: everything else

By design, balar concerns itself with only 3 things: gathering inputs for bulk operations, executing them, and being transparent about how it does it. With how “intrusive” the abstractions of this library are, I would not want to require you to trust it with more than strictly necessary. Any other feature (caching, timeouts, retries…) are out of scope and can be handled by other means in your application code instead.
