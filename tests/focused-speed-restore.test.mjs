import assert from 'node:assert/strict';
import test from 'node:test';

import { TimeSlider } from '../static/modules/time-slider.js';

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
