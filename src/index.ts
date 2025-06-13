import { run } from './execution';
import { fns, object } from './config';
import { _if } from './control-flow';

export { ExecutionOptions, BalarFn } from './api';

export const balar = {
  wrap: {
    fns,
    object,
  },
  ['if']: _if,
  run,
};
