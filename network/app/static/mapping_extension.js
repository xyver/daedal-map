/** Network expansion: validated graph packs on Mapping's existing global map. */
import { MapAdapter } from '/static/modules/map-adapter.js';

const API = '/api/local/network';
const COLORS = { road: '#f7a928', rail: '#b58cff', power: '#f06380', river: '#36c2dc' };
const loaded = new Map();
let panel;
let status;
let refreshTimer;
let active = false;
let networkPopup;
let selectionToken = 0;

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

function popupContent(title, rows, links = []) {
  const content = document.createElement('div');
  content.className = 'network-feature-content';
  const heading = document.createElement('h3');
  heading.textContent = title;
  content.append(heading);
  for (const [label, value] of rows) {
    if (value === undefined || value === null || value === '') continue;
    const row = document.createElement('div');
    row.className = 'network-feature-row';
    const name = document.createElement('span');
    name.textContent = label;
    const detail = document.createElement('span');
    detail.textContent = String(value);
    row.append(name, detail);
    content.append(row);
  }
  if (links.length) {
    const disclosure = document.createElement('details');
    const summary = document.createElement('summary');
    summary.textContent = `${links.length} graph link${links.length === 1 ? '' : 's'}`;
    disclosure.append(summary);
    for (const link of links.slice(0, 12)) {
      const item = document.createElement('div');
      item.className = 'network-feature-link';
      item.textContent = `${link.from} → ${link.to}${link.distance_km != null ? ` · ${Number(link.distance_km).toFixed(2)} km` : ''}`;
      disclosure.append(item);
    }
    if (links.length > 12) {
      const more = document.createElement('div');
      more.textContent = `And ${links.length - 12} more links`;
      disclosure.append(more);
    }
    content.append(disclosure);
  }
  return content;
}

function openPopup(coordinate, content) {
  networkPopup?.remove();
  const token = ++selectionToken;
  const popup = new maplibregl.Popup({ closeButton: true, closeOnClick: false,
    maxWidth: '340px', className: 'network-feature-popup' });
  networkPopup = popup;
  popup.on('close', () => {
    if (networkPopup === popup) {
      networkPopup = null;
      selectionToken++;
    }
  });
  popup.setLngLat(coordinate).setDOMContent(content).addTo(MapAdapter.map);
  return token;
}

function updatePopup(token, content) {
  if (token === selectionToken && networkPopup) networkPopup.setDOMContent(content);
}

function closePopup() {
  networkPopup?.remove();
  networkPopup = null;
  selectionToken++;
}

function removePack(packId) {
  closePopup();
  const entry = loaded.get(packId);
  loaded.delete(packId);
  const map = MapAdapter.map;
  const key = ids(packId);
  MapAdapter.priorityClickLayerIds.delete(key.line);
  MapAdapter.priorityClickLayerIds.delete(key.point);
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
  MapAdapter.priorityClickLayerIds.add(key.line);
  MapAdapter.priorityClickLayerIds.add(key.point);
  if (!entry.onLineClick) {
    entry.onLineClick = async (event) => {
      const featureId = event.features?.[0]?.properties?.feature_id;
      if (!featureId) return;
      const token = openPopup(event.lngLat, popupContent(`${entry.pack.network_type} feature`,
        [['Feature ID', featureId], ['Status', 'Loading details…']]));
      try {
        const result = await readJson(`${API}/packs/${encodeURIComponent(packId)}/feature?feature_id=${encodeURIComponent(featureId)}`);
        const rows = Object.entries(result.feature.properties || {}).map(([key, value]) =>
          [key.replaceAll('_', ' '), value]);
        updatePopup(token, popupContent(`${entry.pack.network_type} feature`, rows, result.links));
      } catch (error) { updatePopup(token, popupContent('Network feature', [['Error', error.message]])); }
    };
    entry.onNodeClick = async (event) => {
      const nodeId = event.features?.[0]?.properties?.node_id;
      if (!nodeId) return;
      const coordinate = event.features[0].geometry?.coordinates || event.lngLat;
      const token = openPopup(coordinate, popupContent(`${entry.pack.network_type} node`,
        [['Node ID', nodeId], ['Status', 'Loading connections…']]));
      try {
        const result = await readJson(`${API}/packs/${encodeURIComponent(packId)}/node?node_id=${encodeURIComponent(nodeId)}`);
        updatePopup(token, popupContent(`${entry.pack.network_type} node`, [
          ['Node ID', result.node_id],
          ['Coordinates', result.coordinate.map((value) => Number(value).toFixed(5)).join(', ')],
          ['Pack', packId]
        ], result.links));
      } catch (error) { updatePopup(token, popupContent('Network node', [['Error', error.message]])); }
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
    if (!active || loaded.get(packId) !== entry || request !== entry.request) return;
    entry.data = collection;
    paintPack(packId);
    showStatus(`${loaded.size} network layer(s) visible; ${collection.features.length} features in ${packId}.`);
  } catch (error) {
    if (active && loaded.get(packId) === entry && request === entry.request) showStatus(error.message);
  }
}

function refreshVisible() {
  if (!active) return;
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => { for (const packId of loaded.keys()) refreshPack(packId); }, 150);
}

function repaintVisible() {
  if (!active) return;
  for (const packId of loaded.keys()) paintPack(packId);
}

function setLayersVisible(visible) {
  const map = MapAdapter.map;
  if (!map?.isStyleLoaded()) return;
  for (const packId of loaded.keys()) {
    const key = ids(packId);
    if (visible) paintPack(packId);
    for (const layerId of [key.line, key.point]) {
      if (map.getLayer(layerId)) map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
    }
  }
}

function createPanel(packs) {
  panel = document.createElement('details');
  panel.id = 'networkExpansion';
  panel.open = true;
  panel.style.cssText = 'position:absolute;bottom:110px;right:14px;z-index:1100;width:min(300px,calc(100vw - 28px));max-height:55vh;overflow:auto;background:#101d2bf2;color:#eaf4ff;border:1px solid #57728b;border-radius:6px;padding:8px;font:12px system-ui;';
  const summary = document.createElement('summary');
  summary.textContent = 'Network layers';
  summary.style.cssText = 'cursor:pointer;font-weight:700;font-size:14px;';
  panel.append(summary);
  const list = document.createElement('div');
  list.style.cssText = 'display:grid;gap:6px;margin-top:8px;';
  const defaultPackId = (packs.find((pack) => pack.network_type === 'rail') || packs[0])?.pack_id;
  let defaultCheckbox;
  for (const pack of packs) {
    const label = document.createElement('label');
    label.style.cssText = 'display:flex;align-items:center;gap:7px;cursor:pointer;';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    if (pack.pack_id === defaultPackId) defaultCheckbox = checkbox;
    const text = document.createElement('span');
    text.textContent = `${pack.network_type} | ${pack.pack_id}`;
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
  panel.append(list, status);
  document.getElementById('mapContainer')?.append(panel);
  defaultCheckbox?.click();
}

export async function initNetworkExpansion() {
  if (active || document.body?.dataset.localWrapper !== 'true' || !MapAdapter.map) return;
  if (panel) {
    panel.hidden = false;
    active = true;
    setLayersVisible(true);
    refreshVisible();
    return;
  }
  const { packs } = await readJson(`${API}/packs`);
  createPanel(packs);
  MapAdapter.map.on('moveend', refreshVisible);
  MapAdapter.map.on('style.load', repaintVisible);
  active = true;
}

export function closeNetworkExpansion() {
  if (!active) return;
  clearTimeout(refreshTimer);
  active = false;
  for (const entry of loaded.values()) entry.request++;
  closePopup();
  panel.hidden = true;
  setLayersVisible(false);
}
