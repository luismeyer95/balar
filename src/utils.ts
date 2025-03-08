export type Normalize<T> = T extends (...args: infer A) => infer R
  ? (...args: Normalize<A>) => Normalize<R>
  : T extends any
    ? { [K in keyof T]: Normalize<T[K]> }
    : never;

/**
 * Type to convert an array of tuples to a record.
 * Produces { A: X, B: Y } from param [[A, X], [B, Y], ..] (formatted as entries)
 *
 * ```ts
 * type Tup = [['hey', string], ['hi', number], ['hello', boolean]];
 * type Result = EntriesToRecord<Tup>; // { hey: string, hi: number, hello: boolean }
 * ```
 */
export type EntriesToRecord<T extends any[]> = {
  [K in T[number] as K[0]]: K[1];
};

type Idx<T, K> = K extends keyof T ? T[K] : never;

/**
 * Zips a tuple of arrays into an array of tuples.
 * Useful to make entries out of some generic type arguments.
 * Produces [[A, X], [B, Y], [C, Z] ..] from params [A, B, C], [X, Y, Z].
 */
export type ZipToEntries<K extends readonly string[], V extends readonly any[]> = [
  ...{
    [I in keyof K]: [Idx<K, I>, Idx<V, I>];
  },
];

/**
 * Zips a tuple of arrays into a record.
 * Useful to make a record out of some generic type arguments.
 * Produces { A: X, B: Y, C: Z } from params [A, B, C], [X, Y, Z].
 */
export type ZipToRecord<
  K extends readonly string[],
  V extends readonly any[],
> = EntriesToRecord<ZipToEntries<K, V>>;

export function* chunk<T>(iterable: Iterable<T>, size: number): Iterable<T[]> {
  let chunk: T[] = [];
  let i = 0;

  for (const item of iterable) {
    chunk[i] = item;
    i += 1;

    if (i >= size) {
      yield chunk;
      chunk = [];
      i = 0;
    }
  }
  if (i > 0) {
    yield chunk.filter((item) => item !== undefined);
  }
}
