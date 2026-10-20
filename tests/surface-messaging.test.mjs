import assert from 'node:assert/strict';
import test from 'node:test';

import { buildOverlayStatusMessage } from '../static/modules/shared/surface-messaging.js';

test('an unknown overlay count is not announced as zero loaded events', () => {
  for (const loadedCount of [undefined, null, '']) {
    const message = buildOverlayStatusMessage('earthquakes', true, {
      mode: 'explore',
      loadedCount,
    });
    assert.doesNotMatch(message, /Loaded 0 earthquakes/);
  }

  assert.match(
    buildOverlayStatusMessage('earthquakes', true, { mode: 'explore', loadedCount: 0 }),
    /Loaded 0 earthquakes/,
  );
  assert.match(
    buildOverlayStatusMessage('earthquakes', true, { mode: 'explore', loadedCount: 12 }),
    /Loaded 12 earthquakes/,
  );
});
