import { run } from './execution';
import { def, wrap } from './config';

export { ExecutionOptions, BulkFn, RegistryEntry } from './api';

export const balar = {
  def,
  wrap,
  run,
};
