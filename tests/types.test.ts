import { AccountsRepository } from './fakes/budget.fake';

test('type testing - registry API', () => {
  type BulkFn<In, Out> = (request: In[]) => Promise<Map<In, Out>>;

  type RegisterEntry<I, O> = {
    fn: (i: I[]) => Promise<Map<I, O>>;
    transformInputs?: (i: NoInfer<I>) => NoInfer<I>;
  };
  type CheckedRegisterEntry<I, O> = RegisterEntry<I, O> & { __brand: 'checked' };

  /**
   * Takes a bulk function and converts its signature to a scalar function.
   */
  type ScalarizeFn<F> = F extends (
    input: Array<infer I>,
  ) => Promise<Map<infer I, infer O>>
    ? (input: I) => Promise<O>
    : never;

  /**
   * Takes a registry and converts it to a record of scalar functions.
   */
  type ScalarizeRegistry<R extends Record<string, RegisterEntry<any, any>>> = {
    [K in keyof R]: ScalarizeFn<R[K]['fn']>;
  };

  function createRegistry<R extends Record<string, CheckedRegisterEntry<any, any>>>(
    registry: R,
  ): ScalarizeRegistry<R> {
    return {} as ScalarizeRegistry<R>;
  }

  /**
   * Due to some TypeScript limitations, it is only possible to ensure
   * correct type-checking at the registry entry level if each entry
   * configuration is wrapped in an identity function like this one.
   *
   * As an added layer of type-safety, the registry's API is made to only
   * accept register entries that have been wrapped in a call to `define()`.
   */
  function define<I, O>(
    obj: RegisterEntry<I, O> | BulkFn<I, O>,
  ): CheckedRegisterEntry<I, O> {
    return {} as CheckedRegisterEntry<I, O>;
  }

  const varWrappper = define(new AccountsRepository().linkAccountToBudgets);

  const result = createRegistry({
    create: define({
      fn: async (_: string[]): Promise<Map<string, number>> => new Map(),
    }),
    update: define({
      fn: async (_: string[]): Promise<Map<string, number>> => new Map(),
      transformInputs: () => '',
    }),
    linkAccountToBudget: define({ fn: new AccountsRepository().linkAccountToBudgets }),
    wtf: define({
      fn: async function (_: number[]): Promise<Map<number, null>> {
        return new Map();
      },
      transformInputs: () => 0,
    }),
    onlyFn: define(new AccountsRepository().linkAccountToBudgets),
    onlyFnInline: define(async (_: number[]): Promise<Map<number, null>> => new Map()),
    noWrapper: define({
      fn: async (_: string[]): Promise<Map<string, number>> => new Map(),
      transformInputs: (_: string): string => '',
    }),
    varWrappper,
    test: define(async (_: number[]): Promise<Map<number, number>> => new Map()),
  });

  // TODO: why is uncommenting failing TS compile when LSP/other tests don't complain?
  //
  // result.create('1');
  // // @ts-expect-error
  // result.create(1);
  //
  // result.update('1'); // (property) update: (request: string[]) => Promise<Map<string, number>>
  //
  // result.linkAccountToBudget({ budgetIds: [1], accountId: 1 });
  // // @ts-expect-error
  // result.linkAccountToBudget({ accountId: 1 });
  //
  // result.wtf(1);
  // result.onlyFn({ budgetIds: [1], accountId: 1 });
  // result.onlyFnInline(5);
  // result.noWrapper('1');
});
