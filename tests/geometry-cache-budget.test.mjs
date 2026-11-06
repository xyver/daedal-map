import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { MessagePack: {} };

const {
  GeometryCache,
  countGeometryPositions,
  estimateGeometryFeatureBytes
} = await import('../static/modules/cache.js');

function pointFeature(locId, longitude = 0) {
  return {
    type: 'Feature',
    properties: { loc_id: locId, admin_level: 2 },
    geometry: { type: 'Point', coordinates: [longitude, 0] }
  };
}

test('counts positions across nested polygon coordinates', () => {
  const coordinates = [[
    [0, 0], [1, 0], [1, 1], [0, 0]
  ]];
  assert.equal(countGeometryPositions(coordinates), 4);
  assert.ok(estimateGeometryFeatureBytes({ geometry: { coordinates }, properties: {} }) > 1024);
});

test('geometry cache evicts least-recently-used entries by estimated bytes', () => {
  const previous = GeometryCache.getStats();
  GeometryCache.clear();
  GeometryCache.configureLimits({ maxFeatures: 10, maxEstimatedBytes: 1500 });
  GeometryCache.coverageByLevel.set(2, [{ west: -1, south: -1, east: 2, north: 1, lastSeen: Date.now() }]);

  GeometryCache.add([pointFeature('USA-AA-001'), pointFeature('USA-AA-003', 1)]);

  assert.equal(GeometryCache.features.size, 1);
  assert.equal(GeometryCache.hasLocId('USA-AA-001'), false);
  assert.equal(GeometryCache.hasLocId('USA-AA-003'), true);
  assert.ok(GeometryCache.estimatedBytes <= GeometryCache.maxEstimatedBytes);
  assert.equal(GeometryCache.coverageByLevel.has(2), false);

  GeometryCache.clear();
  GeometryCache.configureLimits(previous);
});

test('cached shapes without bbox properties stay in their own viewport', () => {
  GeometryCache.clear();
  GeometryCache.add([{
    type: 'Feature',
    properties: { loc_id: 'CAF-G1', admin_level: 1 },
    geometry: { type: 'Polygon', coordinates: [[[18, 4], [24, 4], [24, 9], [18, 4]]] }
  }]);

  assert.equal(GeometryCache.getForViewport(1, { west: 17, south: 3, east: 25, north: 10 }).length, 1);
  assert.equal(GeometryCache.getForViewport(1, { west: 5, south: 47, east: 15, north: 55 }).length, 0);
  GeometryCache.clear();
});
