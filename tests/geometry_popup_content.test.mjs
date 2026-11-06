import assert from 'node:assert/strict';
import test from 'node:test';

globalThis.window = { MessagePack: {} };
const { PopupBuilder } = await import('../static/modules/popup-builder.js');

test('admin hover shows one current loc_id and useful place facts', () => {
  const properties = {
    name: 'Delta', loc_id: 'CAN-BC-5915', admin_level: 2,
    admin_0_loc_id: 'CAN', admin_1_loc_id: 'CAN-BC',
    admin_2_loc_id: 'CAN-BC-5915', valid_from: '2021-01-01',
    valid_from_precision: 'year', source_release_id: 'can_statcan_2021_admin_3',
    temporal_scope: 'source_release', geometry_source: 'Statistics Canada'
  };
  const info = {
    memberships: ['Part of: British Columbia, Canada'],
    land_area: 12_500_000, geometry_source: 'Statistics Canada',
    children_count: 42, descendants_count: 42
  };
  const html = PopupBuilder.build(properties, null, info, { mode: 'hover' });
  assert.match(html, /<strong>Delta<\/strong>/);
  assert.match(html, /Part of: British Columbia, Canada/);
  assert.match(html, /Year: 2021/);
  assert.match(html, /Land area: 13/);
  assert.match(html, /loc_id: CAN-BC-5915/);
  assert.match(html, /Source: Statistics Canada/);
  assert.doesNotMatch(html, /Valid From|Precision|Release Id|Temporal Scope|Admin 0 Loc Id|CAN-BC<br>/);
  assert.doesNotMatch(html, /subdivisions|districts|datasets/);
});

test('click adds available country facts and counts', () => {
  const html = PopupBuilder.build(
    { name: 'Canada', loc_id: 'CAN', source_vintage: '2021' }, null,
    { admin_level: 0, capital_name: 'Ottawa', area_total_sq_km: 9_984_670,
      coastline_km: 202_080, dataset_counts: { country: 12 },
      memberships: ['G7', 'G20'] }
  );
  assert.match(html, /Year: 2021/);
  assert.match(html, /loc_id: CAN/);
  assert.match(html, /Capital: Ottawa/);
  assert.match(html, /Coastline:/);
  assert.match(html, /12 datasets/);
  assert.match(html, /G7, G20/);
});
