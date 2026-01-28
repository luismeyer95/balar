export class DeferredPromise<T> {
  resolve: (ret: T) => void;
  reject: (err: unknown) => void;
  cachedPromise: Promise<T>;

  constructor() {
    const { promise, resolve, reject } = Promise.withResolvers<T>();

    this.cachedPromise = promise;
    this.resolve = resolve;
    this.reject = reject;
  }
}
