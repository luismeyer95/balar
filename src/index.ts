import { run } from './execution';
import { fns, object } from './config';
import { _if, _switch } from './control-flow';

export { ExecutionOptions, BalarFn } from './api';
export {
  BalarStopError,
  ExecutionResults as Result,
  ExecutionSuccess,
  ExecutionFailure,
} from './primitives';

export const balar = {
  wrap: {
    fns,
    object,
  },
  ['if']: _if,
  ['switch']: _switch,
  run,
};
