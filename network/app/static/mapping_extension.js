/** Network expansion: validated graph packs on Mapping's existing global map. */
import { MapAdapter } from '/static/modules/map-adapter.js';

const API = '/api/local/network';
const COLORS = { road: '#f7a928', rail: '#b58cff', power: '#f06380', river: '#36c2dc' };
const loaded = new Map();
let panel;
let status;
let details;
let refreshTimer;

async function readJson(url) {
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.detail || `Network request failed (${response.status})`);
  return payload;
}

function ids(packId) {
  return { source: `network-pack-${packId}`, line: `network-line-${packId}`, point: `network-point-${packId}` };
}

function showStatus(message) { status.textContent = message; }

function removePack(packId) {
  const entry = loaded.get(packId);
  loaded.delete(packId);
  const map = MapAdapter.map;
  const key = ids(packId);
  if (entry?.onLineClick) map.off('click', key.line, entry.onLineClick);
  if (entry?.onNodeClick) map.off('click', key.point, entry.onNodeClick);
  if (map.getLayer(key.line)) map.removeLayer(key.line);
  if (map.getLayer(key.point)) map.removeLayer(key.point);
  if (map.getSource(key.source)) map.removeSource(key.source);
}

function paintPack(packId) {
  const entry = loaded.get(packId);
  const map = MapAdapter.map;
  if (!entry || !entry.data || !map?.isStyleLoaded()) return;
  const key = ids(packId);
  if (map.getSource(key.source)) {
    map.getSource(key.source).setData(entry.data);
    return;
  }
  map.addSource(key.source, { type: 'geojson', data: entry.data });
  map.addLayer({ id: key.line, type: 'line', source: key.source,
    filter: ['any', ['==', ['geometry-type'], 'LineString'], ['==', ['geometry-type'], 'MultiLineString']],
    paint: { 'line-color': COLORS[entry.pack.network_type] || '#ffffff', 'line-width': 2.5, 'line-opacity': 0.9 } });
  map.addLayer({ id: key.point, type: 'circle', source: key.source,
    filter: ['==', ['geometry-type'], 'Point'],
    paint: { 'circle-color': COLORS[entry.pack.network_type] || '#ffffff', 'circle-radius': 4 } });
  if (!entry.onLineClick) {
    entry.onLineClick = async (event) => {
      const featureId = event.features?.[0]?.properties?.feature_id;
      if (!featureId) return;
      try {
        const result = await readJson(`${API}/packs/${encodeURIComponent(packId)}/feature?feature_id=${encodeURIComponent(featureId)}`);
        details.textContent = JSON.stringify({ pack: packId, type: entry.pack.network_type,
          properties: result.feature.properties, links: result.links }, null, 2);
      } catch (error) { details.textContent = error.message; }
    };
    entry.onNodeClick = async (event) => {
      const nodeId = event.features?.[0]?.properties?.node_id;
      if (!nodeId) return;
      try {
        const result = await readJson(`${API}/packs/${encodeURIComponent(packId)}/node?node_id=${encodeURIComponent(nodeId)}`);
        details.textContent = JSON.stringify({ pack: packId, type: entry.pack.network_type, ...result }, null, 2);
      } catch (error) { details.textContent = error.message; }
    };
    map.on('click', key.line, entry.onLineClick);
    map.on('click', key.point, entry.onNodeClick);
  }
}

async function refreshPack(packId) {
  const entry = loaded.get(packId);
  if (!entry) return;
  const bounds = MapAdapter.map.getBounds();
  // A wrapped or world-wide viewport is simplest as the full longitude span.
  const west = bounds.getWest();
  const east = bounds.getEast();
  const fullWorld = west < -180 || east > 180 || east - west >= 360;
  const bbox = [fullWorld ? -180 : west, Math.max(-90, bounds.getSouth()),
    fullWorld ? 180 : east, Math.min(90, bounds.getNorth())].join(',');
  const request = ++entry.request;
  try {
    const collection = await readJson(`${API}/packs/${encodeURIComponent(packId)}/features?bbox=${encodeURIComponent(bbox)}`);
    if (!loaded.has(packId) || request !== entry.request) return;
    entry.data = collection;
    paintPack(packId);
    showStatus(`${loaded.size} network layer(s) visible; ${collection.features.length} features in ${packId}.`);
  } catch (error) {
    if (loaded.has(packId) && request === entry.request) showStatus(error.message);
  }
}

function refreshVisible() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => { for (const packId of loaded.keys()) refreshPack(packId); }, 150);
}

function createPanel(packs) {
  panel = document.createElement('details');
  panel.id = 'networkExpansion';
  panel.style.cssText = 'position:absolute;top:150px;left:10px;z-index:1100;width:min(300px,42vw);max-height:55vh;overflow:auto;background:#101d2bf2;color:#eaf4ff;border:1px solid #57728b;border-radius:6px;padding:8px;font:12px system-ui;';
  const summary = document.createElement('summary');
  summary.textContent = 'Network layers';
  summary.style.cssText = 'cursor:pointer;font-weight:700;font-size:14px;';
  panel.append(summary);
  const list = document.createElement('div');
  list.style.cssText = 'display:grid;gap:6px;margin-top:8px;';
  for (const pack of packs) {
    const label = document.createElement('label');
    label.style.cssText = 'display:flex;align-items:center;gap:7px;cursor:pointer;';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    const text = document.createElement('span');
    text.textContent = `${pack.network_type} · ${pack.pack_id}`;
    label.append(checkbox, text);
    list.append(label);
    checkbox.addEventListener('change', () => {
      if (!checkbox.checked) { removePack(pack.pack_id); showStatus(`${loaded.size} network layer(s) visible.`); return; }
      loaded.set(pack.pack_id, { pack, data: null, request: 0 });
      const bbox = pack.bbox;
      if (Array.isArray(bbox) && bbox.length === 4) {
        MapAdapter.map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 35, maxZoom: 11 });
      }
      refreshPack(pack.pack_id);
    });
  }
  status = document.createElement('p');
  status.setAttribute('role', 'status');
  status.textContent = `${packs.length} installed network pack(s). Select one to display it.`;
  details = document.createElement('pre');
  details.style.cssText = 'max-height:150px;overflow:auto;white-space:pre-wrap;word-break:break-word;';
  details.textContent = 'Click a network feature to inspect its graph links.';
  panel.append(list, status, details);
  document.getElementById('mapContainer')?.append(panel);
}

export async function initNetworkExpansion() {
  if (document.body?.dataset.localWrapper !== 'true' || !MapAdapter.map) return;
  const { packs } = await readJson(`${API}/packs`);
  createPanel(packs);
  MapAdapter.map.on('moveend', refreshVisible);
  MapAdapter.map.on('style.load', () => { for (const packId of loaded.keys()) paintPack(packId); });
}
