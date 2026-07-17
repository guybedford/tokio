// Call ex_slow (suspends on a 30 ms root), then ex_fast (5 ms root) while
// ex_slow is still suspended. Two suspended export activations over one
// ambient runtime; the fast one must resume first (non-LIFO), the slow one
// must not lose its wake.
import assert from 'node:assert/strict';
import initModule from './module.mjs';

const m = await initModule();

const slow = m._ex_slow();
const fast = m._ex_fast();
assert.ok(slow instanceof Promise, 'JSPI export must return a Promise');
assert.ok(fast instanceof Promise, 'reentrant call while suspended must not throw');

const order = [];
const [slowVal, fastVal] = await Promise.all([
  slow.then((v) => (order.push('slow'), v)),
  fast.then((v) => (order.push('fast'), v)),
]);

assert.equal(fastVal, 7);
assert.equal(slowVal, 42);
assert.deepEqual(order, ['fast', 'slow'], 'resumes must be completion-ordered, not call-ordered');
console.log('ok: two suspended export activations, one ambient runtime, non-LIFO resume');
