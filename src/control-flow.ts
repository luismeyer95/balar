import { EXECUTION, NO_OP_PROCESSOR, PROCESSOR_ID } from './constants';
import { run } from './execution';

type IfThenable<T> = PromiseLike<T> & {
  then: typeof Promise.prototype.then;
  catch: typeof Promise.prototype.catch;
  else: <U>(elseProcessorFn: () => Promise<U>) => Promise<T | U>;
};

export function _if<T>(
  condition: boolean,
  processorFn: () => Promise<T>,
): IfThenable<T | undefined> {
  const execution = EXECUTION.getStore();
  if (!execution) {
    throw new Error(
      "balar error: calling control flow operator 'if' outside balar context",
    );
  }

  const id = PROCESSOR_ID.getStore();
  if (id == null) {
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
      execution.runNested([idFalse], NO_OP_PROCESSOR).catch(() => {
        // Silence on throw for true branch processors as they will share
        // the same cached promise in the checkpoint
        // TODO: this can be removed once scope partitioning is implemented
      });
    }
  });

  const trueResult = condition
    ? execution.runNested([idTrue], processorFn).then((res) => res.get(idTrue))
    : execution.awaitNextScopeResolution();

  return {
    then(...args) {
      return trueResult.then(...args);
    },
    catch(...args) {
      return trueResult.catch(...args);
    },
    else<U>(elseProcessorFn: () => Promise<U>) {
      elseCalled = true;

      if (condition) {
        return trueResult;
      }

      const falseResult = execution
        .runNested([idFalse], elseProcessorFn)
        .then((res) => res.get(idFalse));
      return falseResult;
    },
  };
}
