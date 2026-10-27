import assert from 'node:assert/strict';
import test from 'node:test';

import { TimeSlider } from '../static/modules/time-slider.js';
import { ChoroplethModel } from '../static/modules/models/model-choropleth.js';

test('timestamp snapshots hold each location until its next authored value', () => {
  const first = Date.parse('2024-06-01T00:00:00Z');
  const second = Date.parse('2024-07-01T00:00:00Z');
  const third = Date.parse('2024-08-01T00:00:00Z');
  const slider = {
    ...TimeSlider,
    useTimestamps: true,
    sortedTimes: [first, second, third],
  };

  try {
    ChoroplethModel.baseGeojson = {
      type: 'FeatureCollection',
      features: [
        { type: 'Feature', properties: { loc_id: 'A' }, geometry: null },
        { type: 'Feature', properties: { loc_id: 'B' }, geometry: null },
      ],
    };
    ChoroplethModel.timeData = {
      [first]: { A: { value: 1 } },
      [second]: { B: { value: 2 } },
      [third]: { A: { value: 3 } },
    };
    ChoroplethModel.timeDataFilled = ChoroplethModel.buildFilledTimeData(slider);

    assert.equal(slider.getDataLookupKey(first - 1), null);
    assert.equal(slider.getDataLookupKey(second - 1), first);
    assert.equal(slider.getDataLookupKey(second), second);
    assert.equal(slider.getDataLookupKey(third + 1), third);

    const before = ChoroplethModel.buildTimeGeojson(slider, first - 1);
    assert.equal(before.features[0].properties.value, undefined);
    const held = ChoroplethModel.buildTimeGeojson(slider, second + 1);
    assert.equal(held.features[0].properties.value, 1);
    assert.equal(held.features[0].properties.data_time, first);
    assert.equal(held.features[1].properties.value, 2);
    const updated = ChoroplethModel.buildTimeGeojson(slider, third);
    assert.equal(updated.features[0].properties.value, 3);
    assert.equal(updated.features[1].properties.value, 2);
    assert.equal(updated.features[1].properties.data_time, second);
  } finally {
    ChoroplethModel.reset();
  }
});

test('focused playback restores the prior slider speed on exit', () => {
  const speedSlider = { value: 0.41 };
  const slider = {
    ...TimeSlider,
    speedSlider,
    speedLabel: { textContent: '' },
    speedSliderValue: 0.41,
    _inEventMode: false,
    _previousSpeedSlider: null,
  };
  slider.setSpeedFromSlider(0.41);
  const originalSteps = slider.stepsPerFrame;
  const start = Date.parse('2026-05-01T00:00:00Z');
  slider.enterEventAnimation(start, start + 60 * 60 * 1000);
  assert.notEqual(slider.speedSliderValue, 0.41);
  slider.enterEventAnimation(start, start + 2 * 60 * 60 * 1000);
  slider.exitEventAnimation();
  assert.equal(slider.speedSliderValue, 0.41);
  assert.equal(slider.stepsPerFrame, originalSteps);
  assert.equal(speedSlider.value, 0.41);
  assert.equal(slider._inEventMode, false);
});
