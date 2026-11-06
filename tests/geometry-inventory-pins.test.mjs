import test from 'node:test';
import assert from 'node:assert/strict';
import { GeometryPopup, setDependencies } from '../static/modules/geometry-popup.js';

test('small-country markers toggle immediately using catalog points', () => {
  const sources = new Map();
  const layers = new Map();
  const map = {
    getSource: (id) => sources.get(id),
    addSource: (id, source) => sources.set(id, { ...source, setData(data) { this.data = data; } }),
    getLayer: (id) => layers.get(id),
    addLayer: (layer) => layers.set(layer.id, { ...layer, layout: { ...layer.layout } }),
    setLayoutProperty: (id, key, value) => { layers.get(id).layout[key] = value; },
    on: () => {}
  };
  setDependencies({ MapAdapter: { map } });
  const inventory = { features: [
    { properties: { loc_id: 'ABW', is_small: true, centroid_lon: -69.97,
      centroid_lat: 12.51, depth_color: '#5b8db8' } },
    { properties: { loc_id: 'BRA', is_small: false, centroid_lon: -53,
      centroid_lat: -10 } }
  ] };

  assert.equal(GeometryPopup.setPinsVisible(true, inventory), true);
  assert.equal(sources.get('geometry-inventory-small-country-pins').data.features.length, 1);
  assert.equal(layers.get('geometry-inventory-small-country-symbols').type, 'circle');
  assert.equal(GeometryPopup.setPinsVisible(false), false);
  assert.equal(layers.get('geometry-inventory-small-country-symbols').layout.visibility, 'none');
  assert.equal(GeometryPopup.setPinsVisible(true, inventory), true);
  assert.equal(layers.get('geometry-inventory-small-country-symbols').layout.visibility, 'visible');
});
