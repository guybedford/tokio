// `--pre-js` for the emscripten test lanes. A test binary whose runtime never
// exits (a JSPI park with no wake source suspends `main` forever; an
// event-loop root that stalls leaves nothing armed) is drained by Node, which
// exits 0, which cargo would take as success.
//
// A binary that sets `Module.tokioExpectDone` must also set `Module.tokioDone`
// before the runtime exits: an event loop that stops holding the runtime too
// early exits cleanly with its roots unfinished.

// Immediates scheduled since load, for tests that bound them.
Module.tokioImmediates = 0;
if (globalThis.setImmediate) {
  const setImmediateHost = globalThis.setImmediate;
  globalThis.setImmediate = (...args) => {
    Module.tokioImmediates++;
    return setImmediateHost(...args);
  };
}
process.on('exit', (code) => {
  // A runtime exit sets `process.exitCode`; a drained loop leaves it unset.
  if (code === 0 && process.exitCode === undefined) {
    console.error('rt_emscripten_pre.js: event loop drained before the runtime exited');
    process.exitCode = 1;
  }
  if (code === 0 && Module.tokioExpectDone && !Module.tokioDone) {
    console.error('rt_emscripten_pre.js: runtime exited before the roots completed');
    process.exitCode = 1;
  }
  if (code === 0 && Module.tokioDeadline && Date.now() > Module.tokioDeadline) {
    console.error('rt_emscripten_pre.js: the runtime outlived its work');
    process.exitCode = 1;
  }
});
// `eai-again.tokio.test` fails its lookup with EAI_AGAIN, as a DNS server
// failure would, for tests of the resolver's transient-failure path.
{
  const dns = globalThis.process?.getBuiltinModule?.('dns');
  if (dns) {
    const lookupHost = dns.lookup;
    dns.lookup = function (name, ...args) {
      if (name !== 'eai-again.tokio.test') return lookupHost.call(this, name, ...args);
      const err = Object.assign(new Error(`getaddrinfo EAI_AGAIN ${name}`), { code: 'EAI_AGAIN' });
      process.nextTick(args[args.length - 1], err);
    };
  }
}
