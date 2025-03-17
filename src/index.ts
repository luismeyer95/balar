import { run } from './execution';
import { fns, object } from './config';

export { ExecutionOptions, BalarFn } from './api';

export const balar = {
  wrap: {
    fns,
    object,
  },
  run,
};
