import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = {
  MessagePack: {
    encode: () => new Uint8Array(0),
    decode: (bytes) => {
      if (bytes[0] === 1) return { rows: [{ loc_id: 'USA-KS-045-001202' }], count: 1 };
      if (bytes[0] === 2) return { features: [newTract] };
      return { rows: [], count: 0 };
    }
  },
  OverlaySelector: { getActiveOverlays: () => ['admin_layers'] }
};

const newTract = {
  type: 'Feature',
  properties: {
    loc_id: 'USA-KS-045-001202', admin_level: 3,
    bbox_min_lon: -95.05, bbox_min_lat: 38.7,
    bbox_max_lon: -94.85, bbox_max_lat: 39.0
  },
  geometry: { type: 'Polygon', coordinates: [[[-95.05, 38.7], [-94.85, 38.7], [-94.85, 39], [-95.05, 38.7]]] }
};

const { GeometryCache } = await import('../static/modules/cache.js');
const { ViewportLoader, setDependencies } = await import('../static/modules/viewport-loader.js');

test('zoom holds Admin 3 when Admin 4 has no published rows, including cached revisits', async () => {
  const bbox = { west: -95.3, south: 38.8, east: -95.2, north: 38.9 };
  const bounds = {
    getWest: () => bbox.west, getSouth: () => bbox.south,
    getEast: () => bbox.east, getNorth: () => bbox.north
  };
  const rendered = [];
  const displayedLevels = [];
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  globalThis.document = {
    getElementById: (id) => id === 'totalAreas' ? { set textContent(value) {} } : null,
    querySelector: () => null
  };
  const fetchedUrls = [];
  globalThis.fetch = async (url) => {
    fetchedUrls.push(url);
    const responseKind = url.includes('admin_level=3') ? 1
      : url.includes('/geometry/display-features') ? 2 : 0;
    return { ok: true, arrayBuffer: async () => new Uint8Array([responseKind]).buffer };
  };
  setDependencies({
    MapAdapter: { map: { getBounds: () => bounds }, loadGeoJSONWithFade: (data) => rendered.push(data.features) },
    NavigationManager: { updateLevelDisplay: (level) => displayedLevels.push(level) }
  });

  try {
    GeometryCache.clear();
    GeometryCache.add([{
      type: 'Feature',
      properties: {
        loc_id: 'USA-KS-045-001201', admin_level: 3,
        bbox_min_lon: -95.4, bbox_min_lat: 38.7,
        bbox_max_lon: -95.1, bbox_max_lat: 39.0
      },
      geometry: { type: 'Polygon', coordinates: [[[-95.4, 38.7], [-95.1, 38.7], [-95.1, 39], [-95.4, 38.7]]] }
    }]);
    ViewportLoader.currentAdminLevel = 4;
    ViewportLoader.lastRenderedLevel = 3;

    await ViewportLoader.doLoad(4);
    assert.equal(GeometryCache.isCovered(4, bbox), true);
    assert.deepEqual(rendered.at(-1).map((feature) => feature.properties.admin_level), [3]);

    await ViewportLoader.doLoad(4);
    assert.deepEqual(rendered.at(-1).map((feature) => feature.properties.admin_level), [3]);
    assert.deepEqual(displayedLevels, [3, 3]);

    // A pan at the same zoom leaves the cached tract behind. Fetch Admin 3
    // for the new view instead of clearing the map or holding stale shapes.
    bbox.west = -95.0;
    bbox.east = -94.9;
    await ViewportLoader.doLoad(4);
    assert.equal(rendered.at(-1)[0].properties.loc_id, 'USA-KS-045-001202');
    assert.equal(fetchedUrls.some((url) => url.includes('admin_level=3')), true);
    assert.equal(displayedLevels.at(-1), 3);

    ViewportLoader.currentAdminLevel = 5;
    await ViewportLoader.doLoad(5);
    assert.equal(rendered.at(-1)[0].properties.loc_id, 'USA-KS-045-001202');
    assert.equal(displayedLevels.at(-1), 3);
  } finally {
    GeometryCache.clear();
    ViewportLoader.abortController?.abort();
    ViewportLoader.abortController = null;
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
  }
});

test('a cancelled same-level request does not strand the replacement as in flight', async () => {
  const bbox = { west: 18, south: 4, east: 24, north: 9 };
  const bounds = {
    getWest: () => bbox.west, getSouth: () => bbox.south,
    getEast: () => bbox.east, getNorth: () => bbox.north
  };
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  let fetchCount = 0;
  const rendered = [];
  globalThis.document = {
    getElementById: (id) => id === 'totalAreas' ? { set textContent(value) {} } : null,
    querySelector: () => null
  };
  globalThis.fetch = (_url, { signal }) => {
    fetchCount += 1;
    if (fetchCount === 1) {
      return new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => {
          const error = new Error('cancelled');
          error.name = 'AbortError';
          reject(error);
        }, { once: true });
      });
    }
    return Promise.resolve({ ok: true, arrayBuffer: async () => new Uint8Array([0]).buffer });
  };
  setDependencies({
    MapAdapter: { map: { getBounds: () => bounds }, loadGeoJSONWithFade: (data) => rendered.push(data.features) },
    NavigationManager: { updateLevelDisplay: () => {} }
  });
  try {
    GeometryCache.clear();
    GeometryCache.add([{
      type: 'Feature',
      properties: {
        loc_id: 'CAF', admin_level: 0,
        bbox_min_lon: 14, bbox_min_lat: 2, bbox_max_lon: 28, bbox_max_lat: 11
      },
      geometry: { type: 'Polygon', coordinates: [[[14, 2], [28, 2], [28, 11], [14, 2]]] }
    }]);
    ViewportLoader.currentAdminLevel = 1;
    ViewportLoader.lastRenderedLevel = 0;
    const first = ViewportLoader.doLoad(1);
    const replacement = ViewportLoader.doLoad(1);
    await Promise.all([first, replacement]);
    assert.equal(fetchCount, 2);
    assert.equal(GeometryCache.isCovered(1, bbox), true);
    assert.equal(GeometryCache.inFlightByLevel.has(1), false);
    assert.equal(rendered.at(-1)[0].properties.loc_id, 'CAF');
  } finally {
    ViewportLoader.cancelActiveRequest();
    GeometryCache.clear();
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
  }
});
