# `balar`

![Tests](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/luismeyer95/f53bbd1f6bf5d5d0a7469dda9b493095/raw/balar.json)
 ![Code](https://img.shields.io/badge/code-100%25%20artisanal%2C%20made%20in%20🇫🇷%20with%20❤-0055A4)

A TypeScript/Node.js library that allows developers to build network-efficient batch processing APIs with simpler code. Write logic that processes 1 item, and let `balar` scale it to handle more without multiplying the number of outbound requests.

---

## Installation

```bash
npm install balar
```

---


## Quick start

```ts
import { balar } from 'balar';

// suppose we have an API for updating budgets with validation rules
type Budget = { id: number; amount: number };

class BudgetsRepository {
  async getBudgets(ids: number[]): Promise<Budget[]> { ... }
  async updateBudgets(budgets: Budget[]): Promise<boolean[]> { ... }
}

const repo = balar.wrap.object(new BudgetsRepository());

// we have a list of budget updates to process
const requests = [
  { id: 1, amount: 1000 }, // success
  { id: 2, amount: 0 },    // should fail: can't be 0
  { id: 3, amount: 1 },    // should fail: can't decrease the budget
];

const [successes] = await balar.run(requests, async (request) => {
  if (request.amount === 0) {
    return 'budget should be greater than 0';
  }

  // balar collects all `request.id`s and executes 
  // `getBudgets([1, 2, 3])` exactly once
  const currentBudget = await repo.getBudgets(request.id);

  if (request.amount < currentBudget.amount) {
    return 'budget must not be lowered';
  }

  // similarly, `updateBudgets([...])` is called exactly once
  // regardless of the number of requests
  const success = await repo.updateBudgets(request);

  if (!success) {
    return 'budget update failed';
  }

  return 'success!';
});

// we describe how to handle 1 update, balar takes care of 
// scaling it to multiple updates without increasing the number
// of outbound requests to our API

// total API calls: 2 ✨ (1 getBudgets + 1 updateBudgets) 

console.log(successes);
// [
//   { input: { id: 1, amount: 1000 }, result: 'success!' },
//   { input: { id: 2, amount: 0 }, result: 'budget should be greater than 0' },
//   { input: { id: 3, amount: 1 }, result: 'budget must not be lowered' }
// ]
```

### Core features

- **Automatic batching**: Write async logic to process a single item and let `balar` scale it efficiently to handle more without increasing the number of outbound requests.
- **Flexibility**: Put any asynchronous operation behind your batch functions, be it API calls, database queries, etc.
- **Transparency**: Plug the logger of your choice to debug or observe `balar` executions.


---


## Introduction

When it comes to asynchronous batch processing, `balar` gives you the best of both worlds: the simplicity of single-item processing logic with the performance of batch operations.

Networking is often a bottleneck in modern web applications. Cloud technology has made it easy to scale up processing power, RAM, or storage, but each networking call still needs to negotiate a complicated and unreliable global network of computers, routers, switches, and protocols adding a lot of overhead. Therefore to minimize time spent in code that processes items in batch, it's usually better to make fewer requests with more data as opposed to making more requests with less data.

However, some simple logic to process one item can become quite complex when scaled to multiple items in a way that batches outbound requests to minimize network calls. You suddenly have to handle "diverging states" at each step of your processing (e.g. some items may pass a validation check, but others may not and should be filtered out for the next step). The core logic can easily get buried under batching concerns, reducing the expressiveness of your code.

`balar` allows you to write asynchronous batch processing code that <em>looks</em> like it handles one item at a time in complete isolation, but without compromising on the efficiency of outbound asynchronous requests. Effectively, you describe how to handle one item, and `balar` ensures that the underlying execution is as network-efficient as hand-written batch processing code.

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

Let’s create a batch endpoint that can process a list of budget updates.

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

This works but we definitely see how batch processing can obscure the original logic. What if you could have the efficiency of batch processing and the simplicity of single-item (scalar) processing logic? `balar` allows you to have both.

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

Essentially, `balar` provides a clean API to queue inputs to batch functions of your choice and execute them in one go. No manual batching, no managing parallel states; just clean, focused single-item logic with batch efficiency!

<details>
<summary><h2 style="display: inline-block;">⚙️ How it works</h2></summary>

In short, the processor function is executed concurrently for all inputs, but all executions "join" at synchronization checkpoints (balar-wrapped function call sites) to allow the aggregation of inputs into batches before execution. Internally, the context tracking and synchronization is done by leveraging `AsyncLocalStorage` and deferred promises.

When you call `balar.run(inputs, inputProcessorFn)`, the processor function is called for each input immediately. `balar` then tracks and controls the progress of each call. The concurrent execution of these calls is divided into steps, with balar-wrapped function calls acting as boundaries between them.

Whenever any given execution of the processor function hits a call to a balar-wrapped function, the provided input(s) are stored internally and the execution is put on hold. The actual batch function call happens once all the other executions have either:
- Called a balar-wrapped function themselves
- Returned from the processor function
- Thrown an error

Once this happens, `balar` executes all batch operations that were buffered during this step using the inputs gathered from all executions. Results are then dispatched to the processor function executions which can continue to progress towards the next checkpoint. Rinse and repeat until all executions have returned their result.

See the budget update example annotated with checkpoint information below.

```ts
const requests = [
  { id: 1, amount: 1000 }, // success (from 500 to 1000)
  { id: 2, amount: 0 },    // fail: can't have 0
  { id: 3, amount: 1 },    // fail: can't lower (from 1500 to 1)
  { id: 4, amount: 3000 }, // fail (arbitrary update failure)
];

// Total number of checkpoints: 3

const [successes] = await balar.run(requests, async (request) => {
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

// Output:

// [1, 'success!']
// [2, 'budget should be greater than 0']
// [3, 'budget must not be lowered']
// [4, 'budget update failed']

```

</details>

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

**Error handling**

`balar.run()` returns a tuple: 
- the first element contains all successful execution results
- the second contains all errors

```ts
const [successes, errors] = await balar.run(inputs, processor);

// successes: Array<{ input: In, result: Out }>
// errors: Array<{ input: In, err: unknown }>
```

When a processor throws an error, that specific processor stops execution. Other processors continue unaffected. The error is collected in the `errors` array.

```ts
const [successes, errors] = await balar.run([1, 2, 3, 4], async (id) => {
  if (id % 2 === 0) {
    throw new Error('even numbers not allowed');
  }
  return service.processItem(id);
});

// successes: [{ input: 1, result: ... }, { input: 3, result: ... }]
// errors: [{ input: 2, err: Error(...) }, { input: 4, err: Error(...) }]
```

> ⓘ  If an error is thrown inside a balar-wrapped batch function, all processors that depend on that batch call will fail with that error. Processors that don't depend on the failed batch call continue normally.

**Critical errors**

To stop all processors when a critical error occurs, throw a `BalarStopError`. This will force-fail all processors, prevent the execution of the next batch function and settle the returned promise as soon as possible.

```ts
import { balar, BalarStopError } from 'balar';

const [successes, errors] = await balar.run(requests, async (request) => {
  const isValid = await service.validateRequest(request);

  if (!isValid && request.critical) {
    // stop everything, this is a critical failure
    throw new BalarStopError('Critical validation failure');
  }

  return await service.processRequest(request);
});

// successes: []
// errors: [{ input: req1, err: BalarStopError(...) }, { input: req2, err: BalarStopError(...) }, ...]
```

**Nested execution**

You can call `balar.run()` inside another `balar.run()` to create nested execution contexts. This is particularly useful for hierarchical data structures where you would typically run into the [N+1 query problem](https://stackoverflow.com/a/97253).

```ts
class Repository {
  async getUsers(ids: number[]): Promise<User[]> { ... }
  async getPosts(ids: number[]): Promise<Post[]> { ... }
  async getComments(ids: number[]): Promise<Comment[]> { ... }
}

const repo = balar.wrap.object(new Repository());

// fetch users, their posts, and comments for each post
const [usersOk] = await balar.run([1, 2, 3], async (userId) => {
  const user = await repo.getUsers(userId);

  const [postsOk] = await balar.run(user.postIds, async (postId) => {
    const post = await repo.getPosts(postId);
    const comments = await repo.getComments(post.commentIds);

    return { post, comments };
  });

  return { user, posts: postsOk.map(p => p.result) };
});

// regardless of the input size, 3 API calls total: 1 for users, 1 for all posts, 1 for all comments
```

---

### `balar.wrap.fns()`

Wraps standalone batch functions into `balar`-compatible functions that can be called with either single inputs or arrays, automatically batching when inside `balar.run()`.

```ts
// define your batch functions
async function getBooks(ids: number[]): Promise<Book[]> {
  const response = await api.post('/books/search', { ids });
  return response.data; // returns Book[]
}

async function getAuthors(ids: number[]): Promise<Author[]> {
  const response = await api.post('/authors/search', { ids });
  return response.data; // returns Author[]
}

// wrap them with balar
const library = balar.wrap.fns({ getBooks, getAuthors });

// use them inside balar.run()
const bookIds = [1, 2, 3];
const results = await balar.run(bookIds, async (bookId) => {
  const book = await library.getBooks(bookId); // batched
  const author = await library.getAuthors(book.authorId); // batched
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

  // non-batch method (won't be available on the wrapper)
  async healthCheck(): Promise<boolean> { ... }
}

// wrap the entire repository
const repo = balar.wrap.object(new UserRepository());

// only expose specific methods
const readOnlyRepo = balar.wrap.object(new UserRepository(), {
  pick: ['getUsers', 'getPermissions']
});

// expose all except specific methods
const safeRepo = balar.wrap.object(new UserRepository(), {
  exclude: ['updateUsers']
});

// use inside balar.run()
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

In order to enable improved batching behavior in more complex workflows, `balar` needs hints to understand how your processing logic partitions the input dataset. This is done by using 
special control flow operators: `balar.if()` and `balar.switch()`.

> ⓘ  Using these is **optional**. Without control flow operators, batch items that go down the same logic paths will always have their calls batched together. Control flow operators allow `balar` to see **more** opportunities to consolidate batches (e.g. when two executions take diverging paths leading to different sequences of batch calls, but "rejoin" at a later point) and more opportunities to parallelize workflows across branches. 

Use these when your batch items have different processing logic that lead to different data-fetching requirements, and there is a strong need to minimize the number of network calls (maximizing batch size)

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

  // automatically partitions domestic vs international orders
  const rate = await balar.if(
    isDomestic,
    () => shipping.getDomesticRates(order.id),      // batched: orders 1, 3
    () => shipping.getInternationalRates(order.id)  // batched: orders 2, 4
  );

  return { order, rate, isDomestic };
});

// total API calls: 2 (1 domestic + 1 international)
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
  // route to the appropriate payment processor
  const receipt = await balar.switch(payment.method, [
    ['card', () => payments.processCreditCard(payment.id)],
    ['paypal', () => payments.processPayPal(payment.id)],
    ['bank', () => payments.processBankTransfer(payment.id)],
  ]);

  return { payment, receipt };
});

// payments automatically grouped by method and batched
// total API calls: 3 (one per payment method)
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
  // route based on spending tiers
  const discount = await balar.switch(
    [customer.totalSpent < 100, () => discounts.getNoDiscount(customer.id)],
    [customer.totalSpent < 1000, () => discounts.getStandardDiscount(customer.id)],
    [customer.totalSpent < 10000, () => discounts.getPremiumDiscount(customer.id)],
    () => discounts.getVIPDiscount(customer.id) // default for totalSpent >= 10000
  );

  return { customer, discount };
});

// customers automatically grouped by tier and batched
```

## ❓ FAQ

### How does it differ from GraphQL's DataLoader?

DataLoader is a primary source of inspiration for `balar`. It allows you to batch requests to the same source within the same event loop tick. This library takes the same concept but with a different implementation, batching requests to the same source within the explicit scope you provide (e.g. across the executions of a processor function for a given set of inputs). With the addition of control flow operators, this approach guarantees consistent batching behaviour even when executing workflows that include conditional data fetching or calls to "non-batch" async functions (see https://github.com/graphql/dataloader/issues/285). `balar` also provides some utilities to facilitate and customize the batching behaviour (convenient proxy wrappers, concurrency control, error propagation strategies, etc).

