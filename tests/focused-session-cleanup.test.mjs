import assert from 'node:assert/strict';
import test from 'node:test';

import { EventAnimator, setDependencies } from '../static/modules/event-animator.js';

test('event animation exit restores speed before the focused scale is removed', () => {
  const calls = [];
  const slider = {
    scales: [{ id: 'primary' }, { id: 'focused' }],
    exitEventAnimation() { calls.push('restore-speed'); },
    removeChangeListener() { calls.push('remove-listener'); },
    removeScale() { calls.push('remove-scale'); },
    setActiveScale() { calls.push('restore-scale'); },
  };
  setDependencies({ TimeSlider: slider });
  globalThis.document = { getElementById: () => null };
  const animator = {
    ...EventAnimator,
    isActive: true,
    scaleId: 'focused',
    _timeChangeHandler: () => {},
    _stopSmoothWaveLoop() {},
    _stopEarthquakeAnimation() {},
    _removeEarthquakeLayers() {},
    _stopTornadoAnimation() {},
    _removeTornadoLayers() {},
    renderer: null,
    onExitCallback: null,
  };

  animator.stop();

  assert.deepEqual(calls, [
    'restore-speed',
    'remove-listener',
    'remove-scale',
    'restore-scale',
  ]);
  assert.equal(animator.isActive, false);
});
