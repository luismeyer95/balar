import { EXECUTION, NO_OP_PROCESSOR, PROCESSOR_ID } from './constants';

type IfThenable<T> = {
  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2>;

  catch<TResult = never>(
    onrejected?: ((reason: unknown) => TResult | PromiseLike<TResult>) | null,
  ): Promise<T | TResult>;
  else: <U>(elseProcessorFn: () => Promise<U>) => Promise<T | U>;
};

export function _if<T>(
  condition: boolean,
  processorFn: () => Promise<T>,
): IfThenable<T | undefined> {
  const execution = EXECUTION.getStore();
  const id = PROCESSOR_ID.getStore();
  if (id == null || !execution) {
    throw new Error(
      "balar error: calling control flow operator 'if' outside balar context",
    );
  }

  const idLabel = `p${id}`;
  const idTrue = idLabel + '-true';
  const idFalse = idLabel + '-false';

  let elseCalled = false;
  process.nextTick(() => {
    if (!elseCalled) {
      // Add no-op processors to fill the count and trigger checkpoint
      execution.runScope([idFalse], NO_OP_PROCESSOR);
    }
  });

  const ifResult = condition
    ? execution.runScope([idTrue], processorFn).then((res) => {
        if (res.errors.has(idTrue)) {
          // Rethrow to bubble up the scope stack
          throw res.errors.get(idTrue);
        }
        return res.successes.get(idTrue);
      })
    : execution.awaitScope();

  return {
    then(...args) {
      return ifResult.then(...args);
    },
    catch(...args) {
      return ifResult.catch(...args);
    },
    else<U>(elseProcessorFn: () => Promise<U>) {
      elseCalled = true;

      if (condition) {
        return ifResult;
      }

      const elseResult = execution.runScope([idFalse], elseProcessorFn).then((res) => {
        if (res.errors.has(idFalse)) {
          // Rethrow to bubble up the scope stack
          throw res.errors.get(idFalse);
        }
        return res.successes.get(idFalse);
      });
      return elseResult;
    },
  };
}
