# Balar

Describe a plan to process one item, let it automatically scale to handle more. Efficient bulk async processing made simpler for Typescript + Node.js.

---

## Installation

```bash
npm install balar       # npm
deno add npm:balar      # deno
yarn add balar          # yarn
bun add balar           # bun
pnpm add balar          # pnpm
```

---


## ⚡ Quick Start

```ts
import { balar } from 'balar';

// Suppose we have a remote API for managing a greenhouse,
// it exposes bulk operations to get or water plants
class GreenhouseService {
  async getPlants(plantIds: number[]): Promise<Map<number, Plant>> { ... }
  async waterPlants(plants: Plant[]): Promise<Map<Plant, Date>> { ... }
}

// Let's get these plants and water them while minimizing 
// the number of requests to our API
const plantIds = [1, 2, 3]; // 🌿 🌵 🌱

const wrapper = balar.wrap.object(new GreenhouseService());

const results = await balar.run(plantIds, async function waterPlant(plantId) {
  // Balar collects all inputs given to `wrapper.getPlants(plantId)` 
  // and invokes the real `getPlants([1, 2, 3])` exactly once under the hood
  const plant = await wrapper.getPlants(plantId);

  // ... Do other sync/async operations, return error, anything goes ...

  // Similarly, the real `waterPlants([plant, ...])` is called exactly once
  const wateredAt = await wrapper.waterPlants(plant!);

  return { name: plant!.name, wateredAt };
});

// We described how to handle 1 plant, Balar scaled it to 3 plants efficiently
// Total number of requests to our remote API: 2 ✅

console.log(results);
// Map { 
//   1 => { name: "Fern", wateredAt: ... },
//   2 => { name: "Cactus", wateredAt: ... },
//   3 => { name: "Tulip", wateredAt: ... },
// }
```

### Core Features

- **Automatic batching**: Write async logic to process a single item and let Balar scale it efficiently to handle more without increasing the number of outbound requests.
- **Flexibility**: Put any asynchronous operation behind your bulk functions, be it API calls, database queries, etc.
- **Transparency**: Plug the logger of your choice to debug or observe Balar executions.

### Runnable Demos

- [Balar for NestJS](https://replit.com/@luismeyer95/BalarGreenhouse-NestJS#src/app.controller.ts)
- [Balar for ExpressJS](https://replit.com/@luismeyer95/BalarGreenhouse-ExpressJS#index.ts)

---


## 🌀 Introduction

When it comes to asynchronous bulk processing, Balar gives you the best of both worlds: the simplicity of single-item processing logic with the performance of bulk operations.

Networking is often a bottleneck in modern web applications. Cloud technology has made it easy to scale up processing power, RAM, or storage, but each networking call still needs to negotiate a complicated and unreliable global network of computers, routers, switches, and protocols adding a lot of overhead. Therefore to minimize time spent in code that processes items in bulk, it's usually better to make fewer requests with more data as opposed to making more requests with less data.

However, some simple logic to process one item can become quite complex when scaled to multiple items in a way that batches outbound requests to minimize network calls. You suddenly have to handle "diverging states" at each step of your processing (e.g. some items may pass a validation check, but others may not and should be filtered out for the next step). The core logic can easily get buried under batching concerns, reducing the expressiveness of your code.

Balar allows you to write asynchronous bulk processing code that <em>looks</em> like it handles one item at a time in complete isolation, but without compromising on the efficiency of outbound asynchronous requests. Effectively, you describe how to handle one item, and Balar ensures that the underlying execution is as efficient as hand-written bulk processing code.

<summary><h2 style="display: inline-block;">Full Example</h2></summary>

Say you have an API endpoint to allow users to update the budget they can spend on your service. It has some validation checks like below. For the sake of simplicity, we use `string` for errors and `true` for success.

```ts
type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

class BudgetsRepository {
  async getBudget(id: number): Promise<Budget> { ... }
  async updateBudget(id: BudgetUpdateRequest): Promise<boolean> { ... }
}

const repository = new BudgetsRepository();

async function updateBudgetWithValidation(
  updateBudget: BudgetUpdateRequest,
): Promise<string | true> {
  if (updateBudget.amount === 0) {
    return 'budget should be greater than 0';
  }

  const currentBudget = await repository.getBudget(updateBudget.id);
  if (updateBudget.amount < currentBudget!.amount) {
    return 'budget must not be lowered';
  }

  const success = await repository.updateBudget(updateBudget);
  if (!success) {
    return 'budget update failed';
  }

  return true;
}
```

Now let’s say your product offering evolved, and users can have multiple budgets to allocate to different services which they will want to update in real-time with low latency. Surely with these requirements, we don’t want to just run this code for each budget in sequence but instead batch reads and updates to minimize network latency.

Alright, let’s create a bulk endpoint that can process a list of budget updates.

```ts
type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

// Notice that we adapted the methods to handle multiple items at once
class BudgetsRepository {
  async getBudgets(id: number[]): Promise<Map<number, Budget>> { ... }
  async updateBudgets(
    requests: BudgetUpdateRequest[],
  ): Promise<Map<BudgetUpdateRequest, boolean>> { ... }
}

const repository = new BudgetsRepository();

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[],
): Promise<Map<Budget, string | true>> {
  const resultByRequest = new Map<BudgetUpdateRequest, string | true>();

  const positiveBudgetUpdateRequests: BudgetUpdateRequest[] = [];
  for (const request of requests) {
    if (request.amount <= 0) {
      resultByRequest.set(request, 'budget should be greater than 0');
    } else {
      positiveBudgetUpdateRequests.push(request);
    }
  }

  const currentBudgets = await repository.getBudgets(
    positiveBudgetUpdateRequests.map((r) => r.id),
  );

  const validBudgetUpdateRequests: BudgetUpdateRequest[] = [];
  for (const request of positiveBudgetUpdateRequests) {
    const currentBudget = currentBudgets.get(request.id)!;
    if (request.amount < currentBudget.amount) {
      resultByRequest.set(request, 'budget must not be lowered');
    } else {
      validBudgetUpdateRequests.push(request);
    }
  }

  const updateResult = await repository.updateBudgets(validBudgetUpdateRequests);

  for (const [request, success] of updateResult) {
    resultByRequest.set(request, success || 'budget update failed');
  }

  return resultByRequest;
}
```

Okay, this works but we definitely see how bulk processing can obscure a bit the original logic. What if you could have the efficiency of bulk processing and the simplicity of single-item (scalar) processing logic? Balar allows you to have both.

```ts
import { balar } from 'balar';

type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

// Notice that we adapted the methods to handle multiple items at once
class BudgetsRepository {
  async getBudgets(id: number[]): Promise<Map<number, Budget>> { ... }
  async updateBudgets(
    requests: BudgetUpdateRequest[],
  ): Promise<Map<BudgetUpdateRequest, boolean>> { ... }
}

// The repository is wrapped in a Balar object. The Balar object only exposes the
// methods that match the above "bulk signature", adding an overload for each of
// the methods so they can also be called with a single item. Precisely, for every
// `(i: I[]) => Map<I, O>` method in the object, a `(i: I) => O` overload is added.
const repository = balar.wrap.object(new BudgetsRepository());

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[],
): Promise<Map<BudgetUpdateRequest, string | true>> {
  return balar.run(requests, async (request) => {
    if (request.amount === 0) {
      return 'budget should be greater than 0';
    }

    const currentBudget = await repository.getBudgets(request.id);
    if (request.amount < currentBudget!.amount) {
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

This code is equivalent to the previous example doing manual batching. It may look like it runs 2 network calls per request, but it only runs 2 network calls in total regardless of the number of requests.

Essentially, Balar provides a clean API to queue inputs to bulk functions of your choice and execute them in one batch. No manual batching, no managing parallel states; just clean, focused scalar logic with bulk efficiency!

## ⚙️ How it works

In short, the processor function is executed concurrently for all inputs, but all executions "join" at synchronization checkpoints (Balar-wrapped function call sites) to allow the aggregation of inputs into batches before execution. Internally, the context tracking and synchronization is done by leveraging `AsyncLocalStorage` and deferred promises. 

When you call `balar.run(inputs, inputProcessorFn)`, the processor function is called for each input immediately. Balar then tracks and controls the progress of each call. The concurrent execution of these calls is divided into steps, each new step being the result of a “synchronization event”.

Whenever any given execution of the processor function hits a call to a Balar-wrapped function, the provided input(s) are stored internally and the execution is put on hold until a "sync event" happens. The sync event happens once all the other executions have either:

- Called a Balar-wrapped function themselves
- Or returned from the processor function

Once this happens, Balar executes all bulk operations that were buffered during this step using the inputs gathered from all executions. Results are then dispatched to the processor function executions which can continue to progress towards the next checkpoint. Rinse and repeat until all executions have returned their result.

See the budget update example annotated with checkpoint information below.

```ts 
const requests = [
  { id: 1, amount: 1000 }, // success (from 500 to 1000)
  { id: 2, amount: 0 },    // fail: can't have 0
  { id: 3, amount: 1 },    // fail: can't lower (from 1500 to 1)
  { id: 4, amount: 3000 }, // fail (arbitrary update failure)
];

// Total number of checkpoints: 3

const results = await balar.run(requests, async (request) => {
  if (request.amount === 0) {
    return 'budget should be greater than 0';              // ]-- #2 returns 
  }                                                        //              |
  const currentBudget = await repo.getBudgets(request.id); // ]-- getBudgets([1,3,4])

  if (request.amount < currentBudget!.amount) {
    return 'budget must not be lowered';                   // ]-- #3 returns
  }                                                        //              |
  const success = await repo.updateBudgets(request);       // ]-- updateBudgets([1,4])

  if (!success) {
    return 'budget update failed';                         // ]-- #4 returns
  }                                                        //              |
  return true;                                             // ]-- #1 returns
});

expect(results).toEqual(
  new Map([
    [{ id: 1, amount: 1000 }, true],
    [{ id: 2, amount: 0 }, 'budget should be greater than 0'],
    [{ id: 3, amount: 1 }, 'budget must not be lowered'],
    [{ id: 4, amount: 3000 }, 'budget update failed'],
  ])
)

```

---

## ❓ FAQ

### Which signatures are accepted for bulk functions?

At this time, you can only provide the following signature:

```ts
type BulkFn<In, Out, Args extends readonly unknown[]> = (
  request: In[],
  ...args: Args
) => Promise<Map<In, Out>>;
```

### How to handle errors?

Balar never catches any errors thrown from within a `balar.run()` processor function, meaning the whole operation fails if any execution throws. The recommended way to handle errors is to have them represented in the return type of the processor function.

### How does it differ from GraphQL's DataLoader?

DataLoader is the primary source of inspiration for Balar. It allows you to batch requests to the same source within the same event loop tick. Balar takes the same concept but with a different implementation, batching requests to the same source within the explicit scope you provide (e.g. across the executions of a processor function for a given set of inputs). This approach guarantees consistent batching behaviour even when executing workflows that include calls to "non-batch" async functions (see https://github.com/graphql/dataloader/issues/285). Balar also provides some utilities to simplify usage at scale within API development projects (object wrappers).
