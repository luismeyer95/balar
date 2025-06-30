export type Result<K, T> = {
  errors: Map<K, unknown>;
  successes: Map<K, T>;
};

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
