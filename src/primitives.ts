export type ExecutionSuccess<In, Out> = {
  input: In;
  result: Out;
};

export type ExecutionFailure<In> = {
  input: In;
  err: unknown;
};

export type ExecutionResultsInternal<In, Out> = {
  successes: Map<In, Out>;
  errors: Map<In, unknown>;
};

export type ExecutionResults<In, Out> = [
  Array<ExecutionSuccess<In, Out>>,
  Array<ExecutionFailure<In>>,
];

export class BalarError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BalarError';
  }
}

export class BalarStopError extends BalarError {
  constructor(message: string) {
    super(message);
    this.name = 'BalarStopError';
  }
}
