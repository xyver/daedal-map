import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { MessagePack: {} };
globalThis.document = { body: { classList: { contains: () => false } } };
const { applyOverlayCatalogResponse, OverlaySelector } = await import('../static/modules/overlay-selector.js');

test('Explore disaster category uses the same bounded preset as the welcome button', () => {
  applyOverlayCatalogResponse({ overlay_tree: {} });
  const events = [];
  OverlaySelector.currentLaneMode = 'explore';
  OverlaySelector.activeOverlays = new Set();
  OverlaySelector.listeners = [(overlayId, active, options) => {
    events.push({ overlayId, active, options });
  }];
  OverlaySelector.list = { querySelector: () => null };
  OverlaySelector._watchForOpsFocusData = () => {};

  OverlaySelector._toggleCategory('disasters', true);

  assert.equal(events.length, 1);
  assert.equal(events[0].overlayId, 'disasters');
  assert.equal(events[0].options.categoryPreset, 'explore:disasters_2020_2025');
  assert.equal(OverlaySelector.activeOverlays.size, 7);
});
