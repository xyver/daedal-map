import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = {
  MessagePack: {},
  setTimeout,
  clearTimeout
};
globalThis.document = { body: { classList: { contains: () => false } } };

const { ChatManager } = await import('../static/modules/chat-panel.js');
const { resolveDefaultLoadAction } = await import('../static/modules/overlay-default-loads.js');

test('the 10-year disaster preset has a bounded seven-layer load', () => {
  const action = resolveDefaultLoadAction({ presetId: 'explore:disasters_2020_2025' });
  assert.equal(action.type, 'multi_default_load');
  assert.equal(action.actions.length, 7);
  assert.equal(action.timeoutMs, 90_000);
});

test('a stalled multi-load aborts its requests and reports the failure', async () => {
  const messages = [];
  let aborted = 0;
  const context = {
    mode: 'explore',
    addMessage(message) { messages.push(message); },
    executeDefaultLoadAction(action, options) {
      if (action.type === 'stalled_load') {
        return new Promise((resolve) => {
          options.signal.addEventListener('abort', () => {
            aborted += 1;
            resolve(false);
          }, { once: true });
        });
      }
      return ChatManager.executeDefaultLoadAction.call(this, action, options);
    }
  };

  const result = await context.executeDefaultLoadAction({
    type: 'multi_default_load',
    timeoutMs: 20,
    actions: Array.from({ length: 3 }, () => ({ type: 'stalled_load' }))
  }, {});

  assert.equal(result, false);
  assert.equal(aborted, 3);
  assert.match(messages[0], /timed out/i);
});
