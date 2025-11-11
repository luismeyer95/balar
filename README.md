# Balar

Describe a plan to process one item, let it automatically scale to handle more. Efficient batch async processing made simpler for Typescript + Node.js.

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
// it exposes batch operations to get or water plants
class GreenhouseService {
  async getPlants(plantIds: number[]): Promise<Plant[]> { ... }
  async waterPlants(plants: Plant[]): Promise<Date[]> { ... }
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
  const wateredAt = await wrapper.waterPlants(plant);

  return { name: plant.name, wateredAt };
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
- **Flexibility**: Put any asynchronous operation behind your batch functions, be it API calls, database queries, etc.
- **Transparency**: Plug the logger of your choice to debug or observe Balar executions.

### Runnable Demos

- [Balar for NestJS](https://replit.com/@luismeyer95/BalarGreenhouse-NestJS#src/app.controller.ts)
- [Balar for ExpressJS](https://replit.com/@luismeyer95/BalarGreenhouse-ExpressJS#index.ts)

---


## 🌀 Introduction

When it comes to asynchronous batch processing, Balar gives you the best of both worlds: the simplicity of single-item processing logic with the performance of batch operations.

Networking is often a bottleneck in modern web applications. Cloud technology has made it easy to scale up processing power, RAM, or storage, but each networking call still needs to negotiate a complicated and unreliable global network of computers, routers, switches, and protocols adding a lot of overhead. Therefore to minimize time spent in code that processes items in batch, it's usually better to make fewer requests with more data as opposed to making more requests with less data.

However, some simple logic to process one item can become quite complex when scaled to multiple items in a way that batches outbound requests to minimize network calls. You suddenly have to handle "diverging states" at each step of your processing (e.g. some items may pass a validation check, but others may not and should be filtered out for the next step). The core logic can easily get buried under batching concerns, reducing the expressiveness of your code.

Balar allows you to write asynchronous batch processing code that <em>looks</em> like it handles one item at a time in complete isolation, but without compromising on the efficiency of outbound asynchronous requests. Effectively, you describe how to handle one item, and Balar ensures that the underlying execution is as efficient as hand-written batch processing code.

<summary><h2 style="display: inline-block;">Full Example</h2></summary>

Say you have an API endpoint to allow users to update the budget they can spend on your service. It has some validation checks like below.

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
): Promise<string> {
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

  return 'success!';
}
```

Now let’s say your product offering evolved, and users can have multiple budgets to allocate to different services which they will want to update in real-time with low latency. Surely with these requirements, we don’t want to just run this code for each budget in sequence but instead batch reads and updates to minimize network latency.

Alright, let’s create a batch endpoint that can process a list of budget updates.

```ts
type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

// Notice that we adapted the methods to handle multiple items at once
class BudgetsRepository {
  async getBudgets(id: number[]): Promise<Budget[]> { ... }
  async updateBudgets(
    requests: BudgetUpdateRequest[],
  ): Promise<boolean[]> { ... }
}

const repository = new BudgetsRepository();

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[],
): Promise<string[]> {
  const resultByRequest = new Map<BudgetUpdateRequest, string>();

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
    if (success) {
      resultByRequest.set(request, 'success!');
    } else {
      resultByRequest.set(request, 'budget update failed');
    }
  }

  const results = requests.map((req) => resultByRequest.get(req)!);
  return results;
}
```

Okay, this works but we definitely see how batch processing can obscure a bit the original logic. What if you could have the efficiency of batch processing and the simplicity of single-item (scalar) processing logic? Balar allows you to have both.

```ts
import { balar } from 'balar';

type Budget = { id: number; amount: number };
type BudgetUpdateRequest = Budget;

class BudgetsRepository {
  async getBudgets(id: number[]): Promise<Budget[]> { ... }
  async updateBudgets(
    requests: BudgetUpdateRequest[],
  ): Promise<boolean[]> { ... }
}

// The repository is now wrapped in a balar object. This object is a proxy 
// to the original repository, enabling it for use with balar.
const repository = balar.wrap.object(new BudgetsRepository());

async function updateBudgetsWithValidation(
  requests: BudgetUpdateRequest[],
): Promise<Result<BudgetUpdateRequest, string | true>> {
  return balar.run(requests, async (request) => {
    if (request.amount === 0) {
      return 'budget should be greater than 0';
    }

    const currentBudget = await repository.getBudgets(request.id);
    if (request.amount < currentBudget.amount) {
      return 'budget must not be lowered';
    }

    const success = await repository.updateBudgets(request);
    if (!success) {
      return 'budget update failed';
    }

    return 'success!';
  });
}
```

This code is equivalent to the previous example doing manual batching. It may look like it runs 2 network calls per request, but it only runs 2 network calls in total regardless of the number of requests.

Essentially, Balar provides a clean API to queue inputs to batch functions of your choice and execute them in one go. No manual batching, no managing parallel states; just clean, focused single-item logic with batch efficiency!

## ⚙️ How it works

In short, the processor function is executed concurrently for all inputs, but all executions "join" at synchronization checkpoints (Balar-wrapped function call sites) to allow the aggregation of inputs into batches before execution. Internally, the context tracking and synchronization is done by leveraging `AsyncLocalStorage` and deferred promises. 

When you call `balar.run(inputs, inputProcessorFn)`, the processor function is called for each input immediately. Balar then tracks and controls the progress of each call. The concurrent execution of these calls is divided into steps, each new step being the result of a “synchronization event”.

Whenever any given execution of the processor function hits a call to a Balar-wrapped function, the provided input(s) are stored internally and the execution is put on hold until a "sync event" happens. The sync event happens once all the other executions have either:

- Called a Balar-wrapped function themselves
- Or returned from the processor function

Once this happens, Balar executes all batch operations that were buffered during this step using the inputs gathered from all executions. Results are then dispatched to the processor function executions which can continue to progress towards the next checkpoint. Rinse and repeat until all executions have returned their result.

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

  if (request.amount < currentBudget.amount) {
    return 'budget must not be lowered';                   // ]-- #3 returns
  }                                                        //              |
  const success = await repo.updateBudgets(request);       // ]-- updateBudgets([1,4])

  if (!success) {
    return 'budget update failed';                         // ]-- #4 returns
  }                                                        //              |
  return 'success!';                                       // ]-- #1 returns
});

expect(results).toEqual(
  new Map([
    [{ id: 1, amount: 1000 }, 'success!'],
    [{ id: 2, amount: 0 }, 'budget should be greater than 0'],
    [{ id: 3, amount: 1 }, 'budget must not be lowered'],
    [{ id: 4, amount: 3000 }, 'budget update failed'],
  ])
)

```

## API overview

### `balar.run()`

The entrypoint function for your batch workflows. 
Think of it as a variant of `Promise.all()` that automatically batches calls to the same source inside executions of the function you provide it.

```ts
const service = balar.wrap.object(new MyService());

const results = await balar.run(
  [1, 2, 3],
  async (id) => {
    const item = await service.getItems(id);  // batched
    return service.processItems(item);        // batched
  }
);
```

**Nested execution:**
You can call `balar.run()` inside another `balar.run()` to create nested execution contexts. This is particularly useful for hierarchical data structures where you would typically run into the [N+1 query problem](https://stackoverflow.com/a/97253).

```ts
class Repository {
  async getUsers(ids: number[]): Promise<User[]> { ... }
  async getPosts(ids: number[]): Promise<Post[]> { ... }
  async getComments(ids: number[]): Promise<Comment[]> { ... }
}

const repo = balar.wrap.object(new Repository());

// Fetch users, their posts, and comments for each post
const [usersOk] = await balar.run([1, 2, 3], async (userId) => {
  const user = await repo.getUsers(userId);

  const [postsOk] = await balar.run(user.postIds, async (postId) => {
    const post = await repo.getPosts(postId);
    const comments = await repo.getComments(post.commentIds);

    return { post, comments };
  });

  return { user, posts: postsOk.map(p => p.result) };
});

// Regardless of the input size, 3 API calls total: 1 for users, 1 for all posts, 1 for all comments
```

---

### `balar.wrap.fns()`

Wraps standalone batch functions into `balar`-compatible functions that can be called with either single inputs or arrays, automatically batching when inside `balar.run()`.

```ts
// Define your batch functions
async function getBooks(ids: number[]): Promise<Book[]> {
  const response = await api.post('/books/search', { ids });
  return response.data; // returns Book[]
}

async function getAuthors(ids: number[]): Promise<Author[]> {
  const response = await api.post('/authors/search', { ids });
  return response.data; // returns Author[]
}

// Wrap them with balar
const library = balar.wrap.fns({ getBooks, getAuthors });

// Use them inside balar.run()
const bookIds = [1, 2, 3];
const results = await balar.run(bookIds, async (bookId) => {
  const book = await library.getBooks(bookId);     // single call, returns Book
  const author = await library.getAuthors(book.authorId);
  return { book, author };
});
```

---

### `balar.wrap.object()`

Wraps an object or class instance containing batch methods, exposing only the compatible batch methods + added overloads to support calling them with single inputs.

```ts
class UserRepository {
  async getUsers(ids: number[]): Promise<User[]> { ... }
  async getPermissions(ids: number[]): Promise<Permission[][]> { ... }
  async updateUsers(users: User[]): Promise<boolean[]> { ... }

  // Non-batch method (won't be wrapped)
  async healthCheck(): Promise<boolean> { ... }
}

// Wrap the entire repository
const repo = balar.wrap.object(new UserRepository());

// Only wrap specific methods
const readOnlyRepo = balar.wrap.object(new UserRepository(), {
  pick: ['getUsers', 'getPermissions']
});

// Wrap all except specific methods
const safeRepo = balar.wrap.object(new UserRepository(), {
  exclude: ['updateUsers']
});

// Use inside balar.run()
const userIds = [1, 2, 3, 4];
const results = await balar.run(userIds, async (userId) => {
  const [user, perms] = await Promise.all([
    repo.getUsers(userId),       // batched
    repo.getPermissions(userId)  // batched
  ]);

  return { user, perms };
});
```

---

### Control flow operators (`balar.if()`, `balar.switch()`)

In order to enable efficient batching in more complex workflows, `balar` needs hints to understand how your processing logic partitions the input dataset. This is done by using 
special control flow operators: `balar.if()` and `balar.switch()`.

**When to use:** when your batch items have different processing logic that lead to different data-fetching requirements. If you find yourself conditionally calling a wrapped batch function, you should be using `balar.if()`/`balar.switch()` instead to ensure efficient batching.

---

#### `balar.if()`

```ts
class ShippingService {
  async getDomesticRates(ids: number[]): Promise<Rate[]> { ... }
  async getInternationalRates(ids: number[]): Promise<Rate[]> { ... }
}

const shipping = balar.wrap.object(new ShippingService());

type Order = { id: number; country: string; };
const orders: Order[] = [
  { id: 1, country: 'FR' },
  { id: 2, country: 'UK' },
  { id: 3, country: 'FR' },
  { id: 4, country: 'JP' },
];

const results = await balar.run(orders, async (order) => {
  const isDomestic = order.country === 'FR';

  // Automatically partitions domestic vs international orders
  const rate = await balar.if(
    isDomestic,
    () => shipping.getDomesticRates(order.id),      // batched: orders 1, 3
    () => shipping.getInternationalRates(order.id)  // batched: orders 2, 4
  );

  return { order, rate, isDomestic };
});

// Total API calls: 2 (1 domestic + 1 international)
```

---

#### `balar.switch()` (value-based)

```ts
class PaymentService {
  async processCreditCard(ids: number[]): Promise<Receipt[]> { ... }
  async processPayPal(ids: number[]): Promise<Receipt[]> { ... }
  async processBankTransfer(ids: number[]): Promise<Receipt[]> { ... }
}

const payments = balar.wrap.object(new PaymentService());

type Payment = { id: number; method: 'card' | 'paypal' | 'bank'; amount: number };
const paymentQueue: Payment[] = [
  { id: 1, method: 'card', amount: 100 },
  { id: 2, method: 'paypal', amount: 50 },
  { id: 3, method: 'card', amount: 200 },
  { id: 4, method: 'bank', amount: 1000 },
];

const results = await balar.run(paymentQueue, async (payment) => {
  // Route to the appropriate payment processor
  const receipt = await balar.switch(payment.method, [
    ['card', () => payments.processCreditCard(payment.id)],
    ['paypal', () => payments.processPayPal(payment.id)],
    ['bank', () => payments.processBankTransfer(payment.id)],
  ]);

  return { payment, receipt };
});

// Payments automatically grouped by method and batched
// Total API calls: 3 (one per payment method)
```

---

#### `balar.switch()` (first-match)

```ts
class DiscountService {
  async getNoDiscount(ids: number[]): Promise<number[]> { ... }
  async getStandardDiscount(ids: number[]): Promise<number[]> { ... }
  async getPremiumDiscount(ids: number[]): Promise<number[]> { ... }
  async getVIPDiscount(ids: number[]): Promise<number[]> { ... }
}

const discounts = balar.wrap.object(new DiscountService());

type Customer = { id: number; totalSpent: number };
const customers: Customer[] = [
  { id: 1, totalSpent: 50 },
  { id: 2, totalSpent: 500 },
  { id: 3, totalSpent: 5000 },
  { id: 4, totalSpent: 50000 },
];

const results = await balar.run(customers, async (customer) => {
  // Route based on spending tiers
  const discount = await balar.switch(
    [customer.totalSpent < 100, () => discounts.getNoDiscount(customer.id)],
    [customer.totalSpent < 1000, () => discounts.getStandardDiscount(customer.id)],
    [customer.totalSpent < 10000, () => discounts.getPremiumDiscount(customer.id)],
    () => discounts.getVIPDiscount(customer.id) // default for totalSpent >= 10000
  );

  return { customer, discount };
});

// Customers automatically grouped by tier and batched
```

## ❓ FAQ

### How does it differ from GraphQL's DataLoader?

DataLoader is a primary source of inspiration for `balar`. It allows you to batch requests to the same source within the same event loop tick. This library takes the same concept but with a different implementation, batching requests to the same source within the explicit scope you provide (e.g. across the executions of a processor function for a given set of inputs). With the addition of control flow operators, this approach guarantees consistent batching behaviour even when executing workflows that include conditional data fetching or calls to "non-batch" async functions (see https://github.com/graphql/dataloader/issues/285). `balar` also provides some utilities to facilitate and customize the batching behaviour (convenient proxy wrappers, concurrency control, error propagation strategies, etc).

