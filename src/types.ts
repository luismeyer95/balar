export type Normalize<T> = T extends (...args: infer A) => infer R
  ? (...args: Normalize<A>) => Normalize<R>
  : T extends any
    ? { [K in keyof T]: Normalize<T[K]> }
    : never;
