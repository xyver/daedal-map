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
    assert.ok(displayedLevels.length >= 2);
    assert.ok(displayedLevels.every((level) => level === 3));

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

test('same-level zooms keep the active request and load only the latest viewport afterward', async () => {
  const bbox = { west: 18, south: 4, east: 24, north: 9 };
  const bounds = {
    getWest: () => bbox.west, getSouth: () => bbox.south,
    getEast: () => bbox.east, getNorth: () => bbox.north
  };
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  const originalLoad = ViewportLoader.load;
  let fetchCount = 0;
  let abortCount = 0;
  let releaseFirst;
  let followUp;
  const rendered = [];
  globalThis.document = {
    getElementById: (id) => id === 'totalAreas' ? { set textContent(value) {} } : null,
    querySelector: () => null
  };
  globalThis.fetch = (_url, { signal }) => {
    fetchCount += 1;
    if (fetchCount === 1) {
      return new Promise((resolve) => {
        releaseFirst = () => resolve({ ok: true, arrayBuffer: async () => new Uint8Array([0]).buffer });
        signal.addEventListener('abort', () => { abortCount += 1; }, { once: true });
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
        loc_id: 'CAF-G1', admin_level: 1,
        bbox_min_lon: 14, bbox_min_lat: 2, bbox_max_lon: 28, bbox_max_lat: 11
      },
      geometry: { type: 'Polygon', coordinates: [[[14, 2], [28, 2], [28, 11], [14, 2]]] }
    }]);
    ViewportLoader.currentAdminLevel = 1;
    ViewportLoader.lastRenderedLevel = 1;
    ViewportLoader.load = (level) => { followUp = ViewportLoader.doLoad(level); return followUp; };
    const first = ViewportLoader.doLoad(1);
    bbox.west = 35;
    bbox.east = 41;
    await ViewportLoader.doLoad(1);
    await ViewportLoader.doLoad(1);
    assert.equal(fetchCount, 1);
    assert.equal(abortCount, 0);
    releaseFirst();
    await first;
    await followUp;
    assert.equal(fetchCount, 2);
    assert.equal(GeometryCache.isCovered(1, bbox), true);
    assert.equal(GeometryCache.inFlightByLevel.has(1), false);
    assert.equal(abortCount, 0);
    assert.equal(rendered.at(-1).length, 0);
  } finally {
    ViewportLoader.cancelActiveRequest();
    ViewportLoader.load = originalLoad;
    GeometryCache.clear();
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
  }
});

test('crossing an admin threshold still cancels the previous request', async () => {
  const bbox = { west: 18, south: 4, east: 24, north: 9 };
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  let abortCount = 0;
  globalThis.document = {
    getElementById: (id) => id === 'totalAreas' ? { set textContent(_value) {} } : null,
    querySelector: () => null
  };
  globalThis.fetch = (_url, { signal }) => {
    if (!abortCount && !signal.aborted) {
      return new Promise((_resolve, reject) => {
        signal.addEventListener('abort', () => {
          abortCount += 1;
          const error = new Error('cancelled');
          error.name = 'AbortError';
          reject(error);
        }, { once: true });
      });
    }
    return Promise.resolve({ ok: true, arrayBuffer: async () => new Uint8Array([0]).buffer });
  };
  setDependencies({
    MapAdapter: { map: { getBounds: () => ({
      getWest: () => bbox.west, getSouth: () => bbox.south,
      getEast: () => bbox.east, getNorth: () => bbox.north
    }) }, loadGeoJSONWithFade: () => {} },
    NavigationManager: { updateLevelDisplay: () => {} }
  });
  try {
    GeometryCache.clear();
    ViewportLoader.currentAdminLevel = 1;
    ViewportLoader.lastRenderedLevel = 2;
    const first = ViewportLoader.doLoad(1);
    ViewportLoader.currentAdminLevel = 2;
    const second = ViewportLoader.doLoad(2);
    await Promise.all([first, second]);
    assert.equal(abortCount, 1);
    assert.equal(GeometryCache.inFlightByLevel.has(1), false);
  } finally {
    ViewportLoader.cancelActiveRequest();
    GeometryCache.clear();
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
  }
});

test('Africa cache cannot cover Germany and a partial country response is retried', async () => {
  const bbox = { west: 5, south: 47, east: 15, north: 55 };
  const bounds = {
    getWest: () => bbox.west, getSouth: () => bbox.south,
    getEast: () => bbox.east, getNorth: () => bbox.north
  };
  const feature = (locId, west) => ({
    type: 'Feature',
    properties: { loc_id: locId, admin_level: 1,
      bbox_min_lon: west, bbox_min_lat: 48, bbox_max_lon: west + 1, bbox_max_lat: 50 },
    geometry: { type: 'Polygon', coordinates: [[[west, 48], [west + 1, 48], [west + 1, 50], [west, 48]]] }
  });
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  const originalDecode = globalThis.window.MessagePack.decode;
  const rendered = [];
  let shapeRequests = 0;
  globalThis.document = {
    getElementById: (id) => id === 'totalAreas' ? { set textContent(_value) {} } : null,
    querySelector: () => null
  };
  globalThis.window.MessagePack.decode = (bytes) => {
    if (bytes[0] === 1) return { rows: [{ loc_id: 'DEU-BY' }, { loc_id: 'DEU-BW' }] };
    return { features: [shapeRequests === 1 ? feature('DEU-BY', 10) : feature('DEU-BW', 8)] };
  };
  globalThis.fetch = async (url) => {
    if (url.includes('/geometry/display-features')) shapeRequests += 1;
    return { ok: true, arrayBuffer: async () => new Uint8Array([url.includes('/geometry/index') ? 1 : 2]).buffer };
  };
  setDependencies({
    MapAdapter: { map: { getBounds: () => bounds }, loadGeoJSONWithFade: (data) => rendered.push(data.features) },
    NavigationManager: { updateLevelDisplay: () => {} }
  });
  try {
    GeometryCache.clear();
    GeometryCache.add([{
      type: 'Feature', properties: { loc_id: 'CAF-G1', admin_level: 1 },
      geometry: { type: 'Polygon', coordinates: [[[18, 4], [24, 4], [24, 9], [18, 4]]] }
    }]);
    GeometryCache.markCoverage(1, { west: 17, south: 3, east: 25, north: 10 });
    ViewportLoader.currentAdminLevel = 1;
    ViewportLoader.lastRenderedLevel = 1;

    await ViewportLoader.doLoad(1);
    assert.deepEqual(rendered[0], []);
    assert.equal(GeometryCache.isCovered(1, bbox), false);
    assert.equal(shapeRequests, 1);

    await ViewportLoader.doLoad(1);
    assert.equal(shapeRequests, 2);
    assert.equal(GeometryCache.isCovered(1, bbox), true);
    assert.deepEqual(rendered.at(-1).map((item) => item.properties.loc_id).sort(), ['DEU-BW', 'DEU-BY']);
  } finally {
    ViewportLoader.cancelActiveRequest();
    GeometryCache.clear();
    globalThis.fetch = originalFetch;
    globalThis.document = originalDocument;
    globalThis.window.MessagePack.decode = originalDecode;
  }
});

test('a same-level zoom refreshes the new viewport bbox', () => {
  const bbox = { west: 5, south: 47, east: 15, north: 55 };
  let zoom = 5;
  const requested = [];
  const originalLoad = ViewportLoader.load;
  setDependencies({
    MapAdapter: { map: {
      getBounds: () => ({
        getWest: () => bbox.west, getSouth: () => bbox.south,
        getEast: () => bbox.east, getNorth: () => bbox.north
      }),
      getZoom: () => zoom
    } },
    NavigationManager: { updateLevelDisplay: () => {} }
  });
  try {
    ViewportLoader.enabled = true;
    ViewportLoader.orderMode = false;
    ViewportLoader.levelLocked = false;
    ViewportLoader.currentAdminLevel = 2;
    ViewportLoader.lastZoom = 5;
    ViewportLoader.lastRequestedLevel = 2;
    ViewportLoader.lastRequestedBbox = { ...bbox };
    ViewportLoader.load = (level) => requested.push({ level, ...bbox });
    bbox.west = 6;
    bbox.east = 16;
    ViewportLoader.onMoveEnd();
    assert.deepEqual(requested, [], 'a tenth-screen pan should reuse current coverage');
    // Both views are 80 square degrees, so the admin level stays at 2.
    bbox.west = 18;
    bbox.east = 28;
    zoom = 6;
    ViewportLoader.onMoveEnd();
    assert.deepEqual(requested, [{ level: 2, west: 18, south: 47, east: 28, north: 55 }]);
  } finally {
    ViewportLoader.load = originalLoad;
    ViewportLoader.lastZoom = null;
    ViewportLoader.lastRequestedBbox = null;
  }
});
