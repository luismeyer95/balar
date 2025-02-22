import {
  ExpandRecursively,
  Handlerize,
  BulkHandlerFn,
  HandlerQueue,
  Handler,
} from '../src';

test('type experiments', () => {
  interface UpdateRequest {
    id: number;
    budget: number;
  }

  let getCurrentBudgets: (requestIds: number[]) => Promise<Map<number, number>>;
  let updateBudgets: (requests: UpdateRequest[]) => Promise<Map<UpdateRequest, boolean>>;
  const register = {
    // @ts-expect-error
    getCurrentBudgets,
    // @ts-expect-error
    updateBudgets,
  };

  // 1. Type '{ fn: T[keyof T]; queue: any; }' is not assignable to type 'T[keyof T] extends BulkHandlerFn<infer In, infer Out> ? Handler<In, Out> : never'. [2322]
  type Register = typeof register;
  type Test = ExpandRecursively<Handlerize<Register>>;
  type ProjectBulkFnUnionToHandlerUnion<T> =
    T extends BulkHandlerFn<infer In, infer Out>
      ? { fn: BulkHandlerFn<In, Out>; queue: HandlerQueue<In, Out> }
      : never;
  type Test8 = ProjectBulkFnUnionToHandlerUnion<Register[keyof Register]>;
  type Test1 = { fn: Register[keyof Register]; queue: any };
  type Test3 = ExpandRecursively<
    {
      [K in keyof Register]: Register[K] extends BulkHandlerFn<infer In, infer Out>
        ? Handler<In, Out>
        : never;
    }[keyof Register]
  >;
});
