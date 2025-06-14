import { AsyncLocalStorage } from 'async_hooks';
import { BalarExecution } from './execution';

export const EXECUTION = new AsyncLocalStorage<BalarExecution<unknown, unknown>>();
export const PROCESSOR_ID = new AsyncLocalStorage<number>();

export const NO_OP_PROCESSOR = async (_: string): Promise<any> => {};
export const DEFAULT_AWAITER = Promise.resolve(undefined);

export const DEFAULT_SCOPE_PARTITION_KEY = -1;
