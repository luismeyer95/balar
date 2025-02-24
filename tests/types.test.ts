import { AccountsRepository, BudgetsRepository } from './fakes/budget.fake';
import { ZipToEntries, ZipToRecord } from '../src/types';

test('lol', () => {
  expect(1).toBe(1);

  // Goal
  // User provides a record of { fn, transformInputs }
  // Each entry can have a different type for In (funcyou has number, lolo has string)
  // The In type of each entry MUST be inferred from the `fn` implementation
  // The In type of each entry's transformInputs MUST match with its `fn` In type
  type AcceptedInput = [
    { fn: (i: string[]) => null[]; transformInputs: (i: string) => string },
    { fn: (i: number[]) => undefined[]; transformInputs: (i: number) => number },
  ];
  type AcceptedInput2 = Array<
    | { fn: (i: string[]) => null[]; transformInputs: (i: string) => string }
    | { fn: (i: number[]) => undefined[]; transformInputs: (i: number) => number }
  >;
  type WrongInput = [
    { fn: (i: string[]) => null[]; transformInputs: (i: number) => number },
    {
      fn: (i: number[]) => undefined[];
      transformInputs: (i: string) => /** wrong! **/ number;
    },
  ];

  // We define the type of each entry in the record
  // type RegisterEntry<I, O> = {
  //   fn: (i: I[]) => O[];
  //   transformInputs: (i: NoInfer<I>) => NoInfer<I>;
  // };

  // With this, we can create a type [RegisterEntry<In, Out>, ...] for a given any[]
  // type Input<T extends any[]> = [
  //   ...{
  //     [I in keyof T]: T[I] extends RegisterEntry<infer In, infer Out>
  //       ? RegisterEntry<In, Out>
  //       : never;
  //   },
  // ];
  // type Test2 = Input<AcceptedInput>;
  // Test 3 fails! How to treat input as a tuple record and not array of unions?
  // type Test3 = Input<AcceptedInput2>;

  // TS will narrow the input to a tuple instead
  // of a union array IF the input is a tuple literal.
  // So why does Test2 work and not the below?
  // Is the input not actually resolved as a tuple literal?

  // This does NOT work
  // function f<T extends any[]>(series: Input<T>) {}
  // f([
  //   // @ts-expect-error
  //   {
  //     fn: (_: string[]): null[] => [],
  //     transformInputs: (_: string) => 's',
  //   },
  //   // @ts-expect-error
  //   {
  //     fn: (_: string[]): number[] => [],
  //     transformInputs: (_: string) => 's',
  //   },
  // ]);

  // Trying solution from https://stackoverflow.com/questions/74883663/how-to-infer-typescript-variadic-tuple-types-with-multiple-generics
  // This works!
  type RegisterEntry<I, O> =
    | {
        fn: (i: I[]) => Promise<Map<I, O>>;
        transformInputs: (i: NoInfer<I>) => NoInfer<I>;
      }
    | ((i: I[]) => Promise<Map<I, O>>);

  type Idx<T, K> = K extends keyof T ? T[K] : never;
  type RegistryInput<In extends any[], Out extends any[]> = {
    [I in keyof In]: RegisterEntry<In[I], Idx<Out, I>>;
  } & {
    [I in keyof Out]: RegisterEntry<Idx<In, I>, Out[I]>;
  };

  const f2 = <In extends any[], Out extends any[]>(...args: RegistryInput<In, Out>) => {
    for (const arg of args) {
      if (typeof arg === 'function') {
        console.log(arg.name); // arg.name;
      } else {
        console.log(arg.fn.name);
      }
    }
  };

  f2(
    {
      fn: async (nums: number[]) => new Map(nums.map((n) => [n, n])),
      transformInputs: () => 0,
    },
    {
      fn: new AccountsRepository().getAccountsById,
      transformInputs: () => 0,
    },
    new AccountsRepository().linkAccountToBudgets,
    {
      fn: new BudgetsRepository().updateBudgets,
      transformInputs: () => ({ id: 0, amount: 0 }),
    },
    {
      fn: async (nums: string[]) => new Map(nums.map((n) => [n, n])),
      transformInputs: () => '',
    },
  );

  async function localFunc(nums: number[]) {
    return new Map(nums.map((n) => [n, n]));
  }

  f2(
    new AccountsRepository().linkAccountToBudgets,
    new BudgetsRepository().updateBudgets,
    localFunc,
    {
      fn: localFunc,
      transformInputs: (i) => i,
    },
    new BudgetsRepository().updateBudgets,
    // TODO: investigate type issue
    //
    // For some reason, the below signatures do not seem to work
    // in certain cases. In/Out resolve to unknown for both, why?
    // Also, this happens only if the bulk function is
    // - a regular, non-arrow fn
    // - passed as function literal
    //
    // async function helloMyWorld(items: string[]): Promise<Map<string, boolean[]>> {
    //   return new Map(items.map((i) => [i, []]));
    // },
    // {
    //   fn: async function wtf(nums: number[]) {
    //     return new Map(nums.map((n) => [n, n]));
    //   },
    //   transformInputs: (i) => i,
    // },
  );

  // Now let's make this input a record instead
  // We have a [RegisterEntry<In, Out>, ...]
  // We want instead a Record<name, RegisterEntry<In, Out>>]

  function takeRegister<T extends any[], U extends RegisterEntry<any, any>[]>(
    series: [...{ [I in keyof T]: U }],
  ) {}

  takeRegister([
    {
      fn: async (nums: number[]) => new Map(nums.map((n) => [n, n])),
      transformInputs: () => 0,
    },
    {
      fn: new AccountsRepository().getAccountsById,
      transformInputs: () => 0,
    },
  ]);
});
