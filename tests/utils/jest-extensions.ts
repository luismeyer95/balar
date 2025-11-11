import { ExecutionFailure, ExecutionSuccess } from '../../src';

export function expectToHaveBeenCalledWithUnordered<Params extends any[], Out>(
  mockFn: jest.Mock<Out, Params>,
  ...expectedArgs: any[]
) {
  // Assuming single call
  expect(mockFn).toHaveBeenCalledTimes(1);

  const call = mockFn.mock.calls[0];
  for (let i = 0; i < call.length; i += 1) {
    const actualArg = call[i];
    const expectedArg = expectedArgs[i];
    if (Array.isArray(actualArg)) {
      expect(actualArg.slice().sort()).toEqual(expectedArg.slice().sort());
    } else {
      expect(actualArg).toEqual(expectedArg);
    }
  }
}

export function expectSuccessesToMatch<In, Out>(
  successes: Array<ExecutionSuccess<In, Out>>,
  expectedMap: Map<In, Out>,
) {
  const actualMap = new Map(successes.map((item) => [item.input, item.result]));
  expect(actualMap).toEqual(expectedMap);
}

export function expectErrorsToMatch<In>(
  array: Array<ExecutionFailure<In>>,
  expectedMap: Map<In, unknown>,
) {
  const actualMap = new Map(array.map((item) => [item.input, item.err]));
  expect(actualMap).toEqual(expectedMap);
}
