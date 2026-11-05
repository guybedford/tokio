// `--pre-js` for the emscripten test lanes.

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
