import { EXECUTION, NO_OP_PROCESSOR, PROCESSOR_ID } from './constants';
import { BalarError } from './primitives';

export function _if<T, U = undefined>(
  condition: boolean,
  thenFn: () => Promise<T>,
  elseFn?: () => Promise<U>,
): Promise<T | U> {
  const execution = EXECUTION.getStore();
  const id = PROCESSOR_ID.getStore();
  if (id == null || !execution) {
    throw new Error(
      "balar error: calling control flow operator 'if' outside balar context",
    );
  }

  const key = `p${id}-${condition}`;
  const handler = condition ? thenFn : elseFn ?? (NO_OP_PROCESSOR as () => Promise<U>);

  const ifResult = execution
    .runScope<string, T | U>([key], handler, condition ? 1 : 0)
    .then((res) => {
      if (res.errors.has(key)) {
        // Rethrow to bubble up the scope stack
        throw res.errors.get(key);
      }
      return res.successes.get(key)!;
    });

  return ifResult;
}

type Case<T, R> = [T, () => Promise<R>];
type DefaultHandler<R> = () => Promise<R>;

export function _switch<T, R extends readonly unknown[], D>(
  val: T extends readonly unknown[] ? never : T,
  cases: [...{ [K in keyof R]: Case<T, R[K]> }, DefaultHandler<D>],
): Promise<R[number] | D>;
export function _switch<T, R extends readonly unknown[]>(
  val: T extends readonly unknown[] ? never : T,
  cases: { [K in keyof R]: Case<T, R[K]> },
): Promise<R[number] | undefined>;
export function _switch<R extends readonly unknown[], D>(
  ...cases: [...{ [K in keyof R]: Case<boolean, R[K]> }, DefaultHandler<D>]
): Promise<R[number] | D>;
export function _switch<R extends readonly unknown[]>(
  ...cases: { [K in keyof R]: Case<boolean, R[K]> }
): Promise<R[number] | undefined>;
export function _switch(...args: unknown[]): Promise<unknown> {
  const [value, cases] = (() => {
    if (args.length === 2 && !Array.isArray(args[0])) {
      return [args[0] as unknown, args[1] as unknown[]];
    }
    return [true, args];
  })();

  if (typeof cases.at(-1) === 'function') {
    const defaultCase = cases.pop();
    cases.push([value, defaultCase]);
  } else {
    cases.push([value, NO_OP_PROCESSOR]);
  }
  return switchCase(value, cases as Case<unknown, unknown>[]);
}

export function switchCase<T, R extends readonly unknown[]>(
  val: T,
  cases: { [K in keyof R]: Case<T, R[K]> },
): Promise<R[number]> {
  const execution = EXECUTION.getStore();
  const id = PROCESSOR_ID.getStore();
  if (id == null || !execution) {
    throw new Error(
      "balar error: calling control flow operator 'switch' outside balar context",
    );
  }

  const idLabel = `p${id}`;

  for (let i = 0; i < cases.length; i += 1) {
    const _case = cases[i];
    const casePartitionKey = i;

    const [caseValue, caseFn] = _case;
    if (caseValue !== val) {
      continue;
    }

    const caseInputKey = idLabel + '-' + caseValue;

    return execution.runScope([caseInputKey], caseFn, casePartitionKey).then((res) => {
      if (res.errors.has(caseInputKey)) {
        // Rethrow to bubble up the scope stack
        throw res.errors.get(caseInputKey);
      }
      return res.successes.get(caseInputKey);
    });
  }

  // We guarantee that the cases are exhaustive, so we should never reach this point
  throw new BalarError('unexpected: could not find a matching case');
}
