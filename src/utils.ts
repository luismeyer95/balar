export function* chunk<T>(iterable: Iterable<T>, size: number): Iterable<T[]> {
  let chunk: T[] = [];
  let i = 0;

  for (const item of iterable) {
    chunk[i] = item;
    i += 1;

    if (i >= size) {
      yield chunk;
      chunk = [];
      i = 0;
    }
  }
  if (i > 0) {
    yield chunk.filter((item) => item !== undefined);
  }
}

export function getMethodsOfClassObject(classObject: object, depth = Infinity): string[] {
  let obj = classObject as object;
  const methods = new Set<string>();

  while (depth-- && obj) {
    for (const key of Object.getOwnPropertyNames(obj)) {
      if (typeof (obj as any)[key] !== 'function') {
        continue;
      }
      methods.add(key);
    }
    obj = Object.getPrototypeOf(obj)!;
    if (obj === Object.prototype) {
      break;
    }
  }

  methods.delete('constructor');
  return [...methods];
}
