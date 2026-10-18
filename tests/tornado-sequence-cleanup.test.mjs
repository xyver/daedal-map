import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { MessagePack: {} };
const { PointRadiusModel, setDependencies } = await import('../static/modules/models/model-point-radius.js');

test('exiting a tornado sequence removes its focused timeline state', () => {
  const calls = [];
  const handler = () => {};
  const timeSlider = {
    removeChangeListener(value) { calls.push(['listener', value]); },
    exitEventAnimation() { calls.push(['speed']); },
    removeScale(value) { calls.push(['scale', value]); },
  };
  const map = {
    getLayer() { return false; },
    getSource() { return false; },
  };
  setDependencies({ TimeSlider: timeSlider, MapAdapter: { map } });
  PointRadiusModel._tornadoSeqState = {
    scaleId: 'tornado-seq-test',
    timeChangeHandler: handler,
  };

  PointRadiusModel._clearTornadoSequence();

  assert.deepEqual(calls, [
    ['listener', handler],
    ['speed'],
    ['scale', 'tornado-seq-test'],
  ]);
  assert.equal(PointRadiusModel._tornadoSeqState, null);
});
