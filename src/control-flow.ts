import { EXECUTION, NO_OP_PROCESSOR, PROCESSOR_ID } from './constants';

export function _if<T>(
  condition: boolean,
  processorFn: () => Promise<T>,
): Promise<T | undefined> {
  const execution = EXECUTION.getStore();
  const id = PROCESSOR_ID.getStore();
  if (id == null || !execution) {
    throw new Error(
      "balar error: calling control flow operator 'if' outside balar context",
    );
  }

  const key = `p${id}-${condition}`;

  const ifResult = execution
    .runScope([key], condition ? processorFn : NO_OP_PROCESSOR, condition ? 1 : 0)
    .then((res) => {
      if (res.errors.has(key)) {
        // Rethrow to bubble up the scope stack
        throw res.errors.get(key);
      }
      return res.successes.get(key);
    });

  return ifResult;
}

type CaseFn = () => Promise<unknown>;
type SwitchCases<T> = Array<[T, CaseFn] | CaseFn>;

export function _switch<T>(value: T, cases: SwitchCases<T>): Promise<unknown>;
export function _switch<T>(
  condition: boolean,
  trueCase: CaseFn,
  falseCase: CaseFn,
): Promise<unknown>;
export function _switch<T>(
  value: T | boolean,
  _arg1: SwitchCases<T> | CaseFn,
  _arg2?: CaseFn,
): Promise<unknown> {
  if (typeof value === 'boolean' && _arg2) {
    return switchCase(value, [
      [true, _arg1 as CaseFn],
      [false, _arg2 as CaseFn],
    ]);
  }

  return switchCase(value, _arg1 as SwitchCases<T>);
}

async function switchCase<T>(value: T, cases: SwitchCases<T>): Promise<unknown> {
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

    if (typeof _case === 'function') {
      const defaultCaseKey = Symbol('default');
      // Default case
      return execution.runScope([defaultCaseKey], _case, i).then((res) => {
        if (res.errors.has(defaultCaseKey)) {
          // Rethrow to bubble up the scope stack
          throw res.errors.get(defaultCaseKey);
        }
        return res.successes.get(defaultCaseKey);
      });
    }

    const [caseValue, caseFn] = _case;
    if (caseValue !== value) {
      continue;
    }

    const caseKey = idLabel + '-' + caseValue;

    return execution.runScope([caseKey], caseFn, i).then((res) => {
      if (res.errors.has(caseKey)) {
        // Rethrow to bubble up the scope stack
        throw res.errors.get(caseKey);
      }
      return res.successes.get(caseKey);
    });
  }
}
