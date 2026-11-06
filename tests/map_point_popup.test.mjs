import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { MessagePack: {}, location: { hostname: 'localhost' } };
const { MapAdapter } = await import('../static/modules/map-adapter.js');

test('cached administrative redraw keeps stable hover IDs and a visible hover color', () => {
  const previous = {
    map: MapAdapter.map,
    currentRegionGeojson: MapAdapter.currentRegionGeojson,
    focalPrefixes: MapAdapter.focalPrefixes,
    updateFocusedParent: MapAdapter.updateFocusedParent,
    updateFocalColors: MapAdapter.updateFocalColors,
    hoveredFeatureId: MapAdapter.hoveredFeatureId
  };
  let swaps = 0;
  const source = { setData: () => { swaps += 1; } };
  const feature = { type: 'Feature', properties: { loc_id: 'USA-KS-045' }, geometry: null };
  const collection = () => ({ type: 'FeatureCollection', features: [feature] });
  MapAdapter.map = {
    getLayer: () => ({}),
    getSource: () => source
  };
  MapAdapter.updateFocusedParent = () => {};
  MapAdapter.updateFocalColors = () => {};
  MapAdapter.currentRegionGeojson = null;
  MapAdapter.focalPrefixes = ['USA', 'USA-KS', 'USA-KS-045', 'USA-KS-045-001', 'USA-KS-045-001-1'];
  const originalDocument = globalThis.document;
  globalThis.document = { getElementById: () => ({ set textContent(_value) {} }) };
  try {
    MapAdapter.loadGeoJSONWithFade(collection());
    MapAdapter.loadGeoJSONWithFade(collection());
    assert.equal(feature.id, 'USA-KS-045');
    assert.equal(swaps, 1, 'same cached shapes should not reset hover feature-state');
    const fillExpression = MapAdapter.getFocalFillColorExpression();
    const deepest = fillExpression[2];
    assert.notEqual(deepest[2], deepest[3], 'deepest feature hover must differ from base color');
  } finally {
    Object.assign(MapAdapter, previous);
    globalThis.document = originalDocument;
  }
});

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
  assert.match(html, /Match: admin_2/);
  assert.match(html, /Check deep loc_id/);
  assert.match(html, /data-shallow-loc-id="USA-TX"/);
  await MapAdapter.resolveDeepPointPopupLookup({ dataset: {
    lng: '-95.37', lat: '29.76', shallowLocId: 'USA-TX'
  } });
  assert.equal(calls[1].params.name, 'resolve_deep_point');
  assert.equal(calls[1].params.arguments.shallow_loc_id, 'USA-TX');
  assert.match(html, /Deep match: admin_4/);
});

test('ocean point offers a named Marine lookup after the land miss', async () => {
  let html = '';
  const calls = [];
  MapAdapter.showPopup = (_point, value) => { html = value; };
  globalThis.fetch = async (_url, options) => {
    const request = JSON.parse(options.body);
    calls.push(request);
    const payload = calls.length === 1
      ? { error: { code: 'no_point_overlap', message: 'No match' } }
      : { family_result: { status: 'matched', matches: [
        { loc_id: 'MARINE-PACIFIC', name: 'Pacific Ocean', family: 'water_body' }
      ] } };
    return { ok: true, json: async () => ({ result: { structuredContent: payload } }) };
  };
  await MapAdapter.resolvePointPopupLookup({ dataset: {
    lng: '-132.592879', lat: '9.739425'
  } });
  assert.match(html, /Outside land mass/);
  assert.match(html, /Get marine loc_id/);
  assert.doesNotMatch(html, /resolve-deep-point-loc-id/);
  await MapAdapter.resolveMarinePointPopupLookup({ dataset: {
    lng: '-132.592879', lat: '9.739425'
  } });
  assert.deepEqual(calls[1].params.arguments, {
    lon: -132.592879, lat: 9.739425, scope_id: 'MARINE',
    family: 'water_body', water_kind: 'named'
  });
  assert.match(html, /Pacific Ocean/);
});

test('world basemap repair fills the ocean only at low zoom', () => {
  const calls = [];
  const previousMap = MapAdapter.map;
  const previousBase = MapAdapter.activeBaseStyle;
  const previousSatellite = MapAdapter.satelliteMode;
  MapAdapter.map = {
    getLayer: (id) => id === 'background' || id === 'park_nature_reserve',
    getSource: () => null,
    setPaintProperty: (...args) => calls.push(['paint', ...args]),
    setLayerZoomRange: (...args) => calls.push(['zoom', ...args]),
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
    assert.deepEqual(calls[1], ['zoom', 'park_nature_reserve', 6, 24]);
    assert.equal(calls[2][1], 'daedal-world-land');
    assert.equal(calls[3][2], 'water');
    assert.equal(calls[3][1].maxzoom, 6);
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
