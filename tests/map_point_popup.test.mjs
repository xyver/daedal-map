import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { MessagePack: {}, location: { hostname: 'localhost' } };
const { MapAdapter } = await import('../static/modules/map-adapter.js');

test('point popup calls shallow then deep MCP tools', async () => {
  const calls = [];
  let html = '';
  MapAdapter.showPopup = (_point, value) => { html = value; };
  MapAdapter.setupPopupTabHandlers = () => {};
  globalThis.fetch = async (_url, options) => {
    const request = JSON.parse(options.body);
    calls.push(request);
    const payload = request.params.name === 'resolve_point'
      ? { matched: { loc_id: 'USA-TX-201', admin_level: 2 },
          stack: [{ loc_id: 'USA', admin_level: 0 },
            { loc_id: 'USA-TX', baseline_loc_id: 'USA-GB-TX', admin_level: 1 },
            { loc_id: 'USA-TX-201', admin_level: 2 }] }
      : { matched: { loc_id: 'USA-TX-201-123', admin_level: 4 }, stack: [] };
    return { ok: true, json: async () => ({ result: { structuredContent: payload } }) };
  };
  await MapAdapter.resolvePointPopupLookup({ dataset: { lng: '-95.37', lat: '29.76' } });
  assert.equal(calls[0].params.name, 'resolve_point');
  assert.deepEqual(calls[0].params.arguments, {
    lon: -95.37, lat: 29.76, scope_id: 'GLOBAL', family: 'administrative'
  });
  assert.match(html, /Shallow match: admin_2/);
  assert.match(html, /data-shallow-loc-id="USA-TX"/);
  await MapAdapter.resolveDeepPointPopupLookup({ dataset: {
    lng: '-95.37', lat: '29.76', shallowLocId: 'USA-TX'
  } });
  assert.equal(calls[1].params.name, 'resolve_deep_point');
  assert.equal(calls[1].params.arguments.shallow_loc_id, 'USA-TX');
  assert.match(html, /Deep match: admin_4/);
});

test('ocean point gives an empty land result without asking for a Marine family', async () => {
  let html = '';
  MapAdapter.showPopup = (_point, value) => { html = value; };
  globalThis.fetch = async () => ({ ok: true, json: async () => ({
    result: { structuredContent: { error: { code: 'no_point_overlap', message: 'No match' } } }
  }) });
  await MapAdapter.resolvePointPopupLookup({ dataset: {
    lng: '-132.592879', lat: '9.739425'
  } });
  assert.match(html, /No land administrative loc_id contains/);
  assert.doesNotMatch(html, /Select one Marine family/);
  assert.doesNotMatch(html, /resolve-deep-point-loc-id/);
});

test('world basemap repair fills the ocean only at low zoom', () => {
  const calls = [];
  const previousMap = MapAdapter.map;
  const previousBase = MapAdapter.activeBaseStyle;
  const previousSatellite = MapAdapter.satelliteMode;
  MapAdapter.map = {
    getLayer: (id) => id === 'background',
    getSource: () => null,
    setPaintProperty: (...args) => calls.push(['paint', ...args]),
    addSource: (...args) => calls.push(['source', ...args]),
    getStyle: () => ({ layers: [{ id: 'background' }, { id: 'water' }] }),
    addLayer: (...args) => calls.push(['layer', ...args])
  };
  MapAdapter.activeBaseStyle = MapAdapter.STYLES.dark;
  MapAdapter.satelliteMode = false;
  try {
    MapAdapter.repairLowZoomOcean();
    assert.deepEqual(calls[0], ['paint', 'background', 'background-color',
      ['step', ['zoom'], '#2C353C', 6, '#0e0e0e']]);
    assert.equal(calls[1][1], 'daedal-world-land');
    assert.equal(calls[2][2], 'water');
    assert.equal(calls[2][1].maxzoom, 6);
    calls.length = 0;
    MapAdapter.satelliteMode = true;
    MapAdapter.repairLowZoomOcean();
    assert.equal(calls.length, 0);
  } finally {
    MapAdapter.map = previousMap;
    MapAdapter.activeBaseStyle = previousBase;
    MapAdapter.satelliteMode = previousSatellite;
  }
});
