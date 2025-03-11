import { AsyncLocalStorage } from 'async_hooks';
import { BalarExecution } from './execution';

export const EXECUTION = new AsyncLocalStorage<BalarExecution<unknown, unknown>>();
export const PROCESSOR_ID = new AsyncLocalStorage<number>();
