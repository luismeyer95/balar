import { BalarExecution } from '.';
import { AsyncLocalStorage } from 'async_hooks';

export const DEFAULT_MAX_CONCURRENT_EXECUTIONS = 500;

export const EXECUTION = new AsyncLocalStorage<BalarExecution<unknown, unknown>>();
export const PROCESSOR_ID = new AsyncLocalStorage<number>();
