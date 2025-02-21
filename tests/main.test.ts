import { processBulkRequest, processBulkRequest2 } from "../src/index";

test("it works", async () => {
  const requests = [
    { id: 1, budget: 1000 },
    { id: 2, budget: 0 },
    { id: 3, budget: 1 },
    { id: 4, budget: 3000 },
  ];

  const expected = new Map([
    [2, { errors: ["budget should be greater than 0"] }],
    [3, { errors: ["budget must not be lowered"] }],
    [4, { errors: ["budget update failed"] }],
  ]);

  const issues = await processBulkRequest2(requests);
  expect(issues).toEqual(expected);
});
