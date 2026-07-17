// The export-side completion channel: `test_await_completion` is a JSPI
// import (listed in -sJSPI_IMPORTS) suspending its caller until
// `test_complete` delivers that id's result from a hosted drive.
addToLibrary({
  $testWaiters: {},
  $testResults: {},
  test_await_completion__deps: ['$testWaiters', '$testResults'],
  test_await_completion: (id) =>
    new Promise((resolve) => {
      if (id in testResults) return resolve();
      testWaiters[id] = resolve;
    }),
  test_complete__deps: ['$testWaiters', '$testResults'],
  test_complete: (id, val) => {
    testResults[id] = val;
    const w = testWaiters[id];
    if (w) {
      delete testWaiters[id];
      w();
    }
  },
  test_take_result__deps: ['$testResults'],
  test_take_result: (id) => {
    const val = testResults[id];
    delete testResults[id];
    return val;
  },
});
