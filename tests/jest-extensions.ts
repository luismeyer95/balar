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
    console.log({ actualArg, expectedArg });
    if (Array.isArray(actualArg)) {
      expect(actualArg.slice().sort()).toEqual(expectedArg.slice().sort());
    } else {
      expect(actualArg).toEqual(expectedArg);
    }
  }
}
