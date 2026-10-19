const scenarioId = "default";
let lakeCountyView = { center: [39.08, -122.76], zoom: 10 };
const map = L.map("map", { preferCanvas: true }).setView(lakeCountyView.center, lakeCountyView.zoom);
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png", {
  maxZoom: 19,
  attribution: "© OpenStreetMap contributors © CARTO",
}).addTo(map);
const stateColors = { open: "#19b84b", forward_only: "#1687e8", reverse_only: "#8f42df", closed: "#ef3434" };
let roads;
let roadHitAreas;
let shelters;
let liveShelters;
let addedShelters;
let startMarker;
let startPoint;
let routeOverlay;
let selectedRouteOverlay;
let addedShelterCount = 0;
let scenarioShelterChanges = { added: [], removed_ids: [] };
let studyFog;
let countyOutlines;
let loadTimer;
const scenarioMarkers = new Map();
const routeShelters = new Map();
const shelterMarkers = new Map();
const shelterFeatures = new Map();

function selectedRoadTypes() {
  const types = new Set();
  document.querySelectorAll("#road-type-filters input:checked").forEach(input => {
    input.value.split(",").forEach(type => types.add(type));
  });
  return types;
}
function clearScenarioMarkers() {
  scenarioMarkers.forEach(marker => map.removeLayer(marker));
  scenarioMarkers.clear();
}
function routePointFromFeature(feature) {
  const [longitude, latitude] = feature.geometry.coordinates;
  return { id: String(feature.properties.id || ""), name: feature.properties.name || "Unnamed shelter", longitude, latitude };
}
function rememberRouteShelter(feature) {
  const point = routePointFromFeature(feature);
  routeShelters.set(point.id || `${point.longitude},${point.latitude}`, point);
}

const centerControl = L.control({ position: "topleft" });
centerControl.onAdd = () => {
  const button = L.DomUtil.create("button", "lake-center-control");
  button.type = "button";
  button.title = "Center map on Lake County";
  button.setAttribute("aria-label", "Center map on Lake County");
  button.textContent = "Center Lake County";
  L.DomEvent.disableClickPropagation(button);
  L.DomEvent.on(button, "click", () => map.setView(lakeCountyView.center, lakeCountyView.zoom));
  return button;
};
centerControl.addTo(map);

function style(feature) {
  const state = feature.properties.scenario_state || "open";
  return { color: stateColors[state], weight: state === "open" ? 2.7 : 4.2, opacity: state === "open" ? .86 : 1 };
}
function iconSize() {
  return Math.max(18, Math.min(38, 18 + (map.getZoom() - 9) * 3.5));
}
function segmentMidpoint(feature) {
  const coordinates = feature.geometry.coordinates;
  return coordinates[Math.floor(coordinates.length / 2)];
}
function segmentAngle(feature, state) {
  const coordinates = feature.geometry.coordinates;
  const start = coordinates[0];
  const end = coordinates[coordinates.length - 1];
  let angle = -Math.atan2(end[1] - start[1], end[0] - start[0]) * 180 / Math.PI;
  if (state === "reverse_only") angle += 180;
  return angle;
}
function scenarioIcon(feature, state) {
  const size = iconSize();
  const closed = state === "closed";
  const symbol = closed ? "×" : "➜";
  const rotation = closed ? 0 : segmentAngle(feature, state);
  const className = `road-scenario-icon is-${state}`;
  return L.divIcon({
    className: "scenario-icon-wrapper",
    html: `<span class="${className}" style="width:${size}px;height:${size}px;font-size:${size}px;transform:rotate(${rotation}deg)">${symbol}</span>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}
function updateScenarioMarker(feature) {
  const id = feature.properties.segment_id;
  const state = feature.properties.scenario_state || "open";
  const existing = scenarioMarkers.get(id);
  if (state === "open") {
    if (existing) {
      map.removeLayer(existing);
      scenarioMarkers.delete(id);
    }
    return;
  }
  const midpoint = segmentMidpoint(feature);
  if (existing) {
    existing.setLatLng([midpoint[1], midpoint[0]]);
    existing.setIcon(scenarioIcon(feature, state));
    return;
  }
  const marker = L.marker([midpoint[1], midpoint[0]], {
    icon: scenarioIcon(feature, state),
    interactive: true,
    keyboard: false,
    zIndexOffset: 1000,
  }).addTo(map);
  marker.on("click", event => {
    L.DomEvent.stopPropagation(event.originalEvent);
    if (marker.__scenarioFeature && marker.__scenarioRoadLayer) {
      cycle(marker.__scenarioFeature, marker.__scenarioRoadLayer).catch(error => alert(error.message));
    }
  });
  scenarioMarkers.set(id, marker);
}
function resizeScenarioMarkers() {
  scenarioMarkers.forEach((marker, id) => {
    const feature = marker.__scenarioFeature;
    if (feature) marker.setIcon(scenarioIcon(feature, feature.properties.scenario_state));
  });
}
function selectedText(properties) {
  return `${properties.name} · ${properties.highway}<br><strong>${properties.scenario_state || "open"}</strong><br><small>${properties.segment_id}</small>`;
}
async function cycle(feature, layer) {
  const id = encodeURIComponent(feature.properties.segment_id);
  const response = await fetch(`/api/scenarios/${scenarioId}/segments/${id}/cycle`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not change segment state");
  const result = await response.json();
  feature.properties.scenario_state = result.state;
  layer.setStyle(style(feature));
  updateScenarioMarker(feature);
  const marker = scenarioMarkers.get(feature.properties.segment_id);
  if (marker) {
    marker.__scenarioFeature = feature;
    marker.__scenarioRoadLayer = layer;
  }
  document.querySelector("#selection").innerHTML = `Selected: <strong>${feature.properties.name}</strong><br>State: ${result.state}`;
}
async function clearRoadChanges() {
  if (!window.confirm("Reset every edited road in this scenario to its normal baseline state?")) return;
  const response = await fetch(`/api/scenarios/${scenarioId}/clear`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not clear road changes.");
  await loadRoads();
  document.querySelector("#selection").textContent = "All road changes cleared; every segment is back to its normal baseline state.";
}
async function loadRoads() {
  const bounds = map.getBounds();
  const bbox = [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()].join(",");
  const response = await fetch(`/api/network?scenario_id=${scenarioId}&zoom=${map.getZoom()}&bbox=${bbox}`);
  if (!response.ok) throw new Error((await response.json()).detail || "Could not load roads for this map area.");
  const network = await response.json();
  const types = selectedRoadTypes();
  network.features = network.features.filter(feature => types.has(feature.properties.highway));
  if (roads) map.removeLayer(roads);
  if (roadHitAreas) map.removeLayer(roadHitAreas);
  clearScenarioMarkers();
  const visibleLayers = new Map();
  roads = L.geoJSON(network, { style, onEachFeature(feature, layer) {
    visibleLayers.set(feature.properties.segment_id, layer);
    updateScenarioMarker(feature);
    const marker = scenarioMarkers.get(feature.properties.segment_id);
    if (marker) {
      marker.__scenarioFeature = feature;
      marker.__scenarioRoadLayer = layer;
    }
  }}).addTo(map);
  roadHitAreas = L.geoJSON(network, {
    style: { color: "#000000", weight: 14, opacity: .001, interactive: true },
    onEachFeature(feature, layer) {
      layer.on("click", async event => {
        L.DomEvent.stopPropagation(event.originalEvent);
        try { await cycle(feature, visibleLayers.get(feature.properties.segment_id)); } catch (error) { alert(error.message); }
      });
    },
  }).addTo(map);
  const detail = map.getZoom() < 11 ? "major-road overview — zoom in for local streets" : "local editable road segments";
  document.querySelector("#status").textContent = `Loaded ${network.features.length.toLocaleString()} ${detail}.`;
}
function shelterIcon(feature) {
  const state = feature.properties.is_added ? "added" : (feature.properties.is_currently_open ? "open" : "closed");
  const closedMark = feature.properties.is_removed ? '<span class="shelter-closed-x">×</span>' : "";
  return L.divIcon({
    className: "shelter-pin-wrapper",
    html: `<span class="shelter-pin is-${state} ${feature.properties.is_removed ? "is-removed" : ""}"></span>${closedMark}`,
    iconSize: [16, 16],
    iconAnchor: [8, 16],
  });
}
function shelterTooltip(feature) {
  const properties = feature.properties;
  const location = [properties.address, properties.city, properties.state].filter(Boolean).join(", ");
  return [properties.name, properties.status, location, properties.capacity ? `Capacity: ${properties.capacity}` : ""].filter(Boolean).join("<br>");
}
function shelterPopup(feature) {
  const properties = feature.properties;
  const content = L.DomUtil.create("div", "shelter-info-popup");
  const title = L.DomUtil.create("strong", "", content);
  title.textContent = properties.name || "Unnamed shelter";
  const lines = [
    ["Status", properties.is_removed ? "Closed in this scenario" : properties.status],
    ["Address", [properties.address, properties.city, properties.state].filter(Boolean).join(", ")],
    ["Capacity", properties.capacity ? `${properties.capacity} people` : ""],
    ["Type", properties.is_added ? "User-added scenario shelter" : "FEMA NSS shelter snapshot"],
  ];
  lines.filter(([, value]) => value).forEach(([label, value]) => {
    const line = L.DomUtil.create("div", "", content);
    line.textContent = `${label}: ${value}`;
  });
  const toggleButton = L.DomUtil.create("button", "remove-shelter", content);
  toggleButton.type = "button";
  toggleButton.textContent = properties.is_removed ? "Reopen shelter" : "Close shelter in scenario";
  L.DomEvent.disableClickPropagation(content);
  L.DomEvent.on(toggleButton, "click", () => toggleShelter(feature.properties.id, properties.is_removed).catch(error => alert(error.message)));
  if (properties.is_added) {
    const deleteButton = L.DomUtil.create("button", "delete-shelter", content);
    deleteButton.type = "button";
    deleteButton.textContent = "Delete custom shelter";
    L.DomEvent.on(deleteButton, "click", () => deleteShelter(feature.properties.id).catch(error => alert(error.message)));
  }
  return content;
}
function scenarioShelterFeature(shelter) {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [shelter.longitude, shelter.latitude] },
    properties: { id: shelter.id, name: shelter.name, status: "Scenario shelter", is_added: true },
  };
}
function isRemovedShelter(feature) {
  return scenarioShelterChanges.removed_ids.includes(String(feature.properties.id));
}
function registerShelterMarker(feature, marker, cluster) {
  const id = String(feature.properties.id);
  shelterMarkers.set(id, marker);
  shelterFeatures.set(id, feature);
  marker.__shelterCluster = cluster;
}
function updateShelterChangesSummary() {
  const added = scenarioShelterChanges.added.length;
  const removed = scenarioShelterChanges.removed_ids.length;
  const box = document.querySelector("#shelter-changes");
  if (!added && !removed) {
    box.textContent = "No shelter changes. Using the baseline dataset.";
  } else {
    box.textContent = `Scenario changes: ${added} added, ${removed} removed from the baseline.`;
  }
  const list = document.querySelector("#shelter-changes-list");
  list.replaceChildren();
  const changed = [
    ...scenarioShelterChanges.added.map(item => ({ id: String(item.id), name: item.name, state: isRemovedShelter(scenarioShelterFeature(item)) ? "closed added shelter" : "added shelter" })),
    ...scenarioShelterChanges.removed_ids
      .filter(id => !scenarioShelterChanges.added.some(item => String(item.id) === String(id)))
      .map(id => ({ id: String(id), name: shelterFeatures.get(String(id))?.properties.name || `Shelter ${id}`, state: "closed baseline shelter" })),
  ];
  changed.forEach(change => {
    const button = L.DomUtil.create("button", "shelter-change-item", list);
    button.type = "button";
    button.textContent = `${change.name} — ${change.state}`;
    button.title = "Show this shelter on the map";
    L.DomEvent.on(button, "click", () => focusShelterChange(change.id));
  });
}
async function loadScenarioShelterChanges() {
  const response = await fetch(`/api/scenarios/${scenarioId}/shelters`);
  if (!response.ok) throw new Error((await response.json()).detail || "Could not load shelter changes.");
  scenarioShelterChanges = await response.json();
  const existingNumbers = scenarioShelterChanges.added.map(item => Number((item.name || "").match(/^New Shelter (\d+)$/)?.[1] || 0));
  addedShelterCount = Math.max(0, ...existingNumbers);
}
async function loadShelters() {
  const payload = await (await fetch("/api/shelters")).json();
  if (shelters) map.removeLayer(shelters);
  shelters = L.markerClusterGroup({ disableClusteringAtZoom: 14, maxClusterRadius: 42, showCoverageOnHover: false });
  routeShelters.clear();
  shelterMarkers.clear();
  shelterFeatures.clear();
  payload.features.forEach(feature => {
    feature.properties.is_removed = isRemovedShelter(feature);
    const [lon, lat] = feature.geometry.coordinates;
    const marker = L.marker([lat, lon], { icon: shelterIcon(feature), keyboard: false });
    marker.bindTooltip(shelterTooltip(feature), { sticky: true });
    marker.bindPopup(shelterPopup(feature));
    shelters.addLayer(marker);
    registerShelterMarker(feature, marker, shelters);
    if (!feature.properties.is_removed) rememberRouteShelter(feature);
  });
  shelters.addTo(map);
}
async function loadAddedShelters() {
  if (addedShelters) map.removeLayer(addedShelters);
  addedShelters = L.markerClusterGroup({ disableClusteringAtZoom: 14, maxClusterRadius: 42, showCoverageOnHover: false });
  scenarioShelterChanges.added.forEach(shelter => {
    const feature = scenarioShelterFeature(shelter);
    feature.properties.is_removed = isRemovedShelter(feature);
    const [lon, lat] = feature.geometry.coordinates;
    const marker = L.marker([lat, lon], { icon: shelterIcon(feature), keyboard: false, zIndexOffset: 150 });
    marker.bindTooltip(shelterTooltip(feature), { sticky: true });
    marker.bindPopup(shelterPopup(feature));
    addedShelters.addLayer(marker);
    registerShelterMarker(feature, marker, addedShelters);
    if (!feature.properties.is_removed) rememberRouteShelter(feature);
  });
  addedShelters.addTo(map);
  updateShelterChangesSummary();
}
async function loadLiveShelters() {
  const response = await fetch("/api/shelters/live");
  if (!response.ok) throw new Error((await response.json()).detail || "Could not load live FEMA shelter updates.");
  const payload = await response.json();
  if (liveShelters) map.removeLayer(liveShelters);
  liveShelters = L.markerClusterGroup({ disableClusteringAtZoom: 14, maxClusterRadius: 42, showCoverageOnHover: false });
  payload.features.forEach(feature => {
    feature.properties.is_removed = isRemovedShelter(feature);
    const [lon, lat] = feature.geometry.coordinates;
    const marker = L.marker([lat, lon], { icon: shelterIcon(feature), keyboard: false, zIndexOffset: 100 });
    marker.bindTooltip(shelterTooltip(feature), { sticky: true });
    marker.bindPopup(shelterPopup(feature));
    liveShelters.addLayer(marker);
    registerShelterMarker(feature, marker, liveShelters);
  });
  liveShelters.addTo(map);
  const checkedAt = new Date(payload.metadata.fetched_at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  document.querySelector("#live-shelter-status").textContent = `FEMA live: ${payload.features.length} open in road scope (checked ${checkedAt}).`;
}
async function loadStudyArea() {
  const payload = await (await fetch("/api/study-area")).json();
  if (studyFog) map.removeLayer(studyFog);
  if (countyOutlines) map.removeLayer(countyOutlines);
  studyFog = L.geoJSON(payload.fog, {
    style: { color: "transparent", weight: 0, fillColor: "#687583", fillOpacity: .24, interactive: false },
  }).addTo(map);
  countyOutlines = L.geoJSON(payload.counties, {
    style: { color: "#657281", weight: 1.2, opacity: .75, fillOpacity: 0, interactive: false },
  }).addTo(map);
}
function homeIcon() {
  return L.divIcon({
    className: "home-marker-wrapper",
    html: '<span class="home-marker" aria-label="Start point">🏠</span>',
    iconSize: [36, 36],
    iconAnchor: [18, 18],
  });
}
function removeStartPoint() {
  if (startMarker) map.removeLayer(startMarker);
  startMarker = undefined;
  startPoint = undefined;
  map.closePopup();
  document.querySelector("#route-results").className = "route-results warning";
  document.querySelector("#route-results").textContent = "Start point removed. Set a new one on a road before calculating routes.";
}
function setStartPoint(snap) {
  const latlng = L.latLng(snap.latitude, snap.longitude);
  startPoint = { longitude: snap.longitude, latitude: snap.latitude };
  if (startMarker) map.removeLayer(startMarker);
  startMarker = L.marker(latlng, { icon: homeIcon(), zIndexOffset: 500, keyboard: false }).addTo(map);
  startMarker.bindTooltip("Start point", { direction: "top" });
  const content = L.DomUtil.create("div", "start-info-popup");
  const snapDescription = snap.restored ? "Restored from saved scenario." : `Snapped ${snap.distance_meters} m to the nearest road.`;
  content.innerHTML = `<strong>Start point</strong><div>${snapDescription}</div>`;
  const removeButton = L.DomUtil.create("button", "remove-start-point", content);
  removeButton.type = "button";
  removeButton.textContent = "Remove start point";
  L.DomEvent.disableClickPropagation(content);
  L.DomEvent.on(removeButton, "click", removeStartPoint);
  startMarker.bindPopup(content);
  map.closePopup();
  document.querySelector("#route-results").className = "route-results";
  document.querySelector("#route-results").textContent = "Start point set. Choose a travel-time limit and calculate routes.";
}
async function addShelter(snap, name) {
  const latlng = L.latLng(snap.latitude, snap.longitude);
  addedShelterCount += 1;
  const id = `added-${Date.now()}`;
  const defaultName = `New Shelter ${addedShelterCount}`;
  const feature = {
    type: "Feature",
    geometry: { type: "Point", coordinates: [latlng.lng, latlng.lat] },
    properties: { id, name: name || defaultName, status: "Scenario shelter", is_added: true },
  };
  const response = await fetch(`/api/scenarios/${scenarioId}/shelters`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(routePointFromFeature(feature)),
  });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not add shelter.");
  await reloadScenarioShelters();
  map.closePopup();
  document.querySelector("#route-results").className = "route-results";
  document.querySelector("#route-results").textContent = `${feature.properties.name} added. It will be included in route calculations.`;
}
async function removeShelter(shelterId) {
  const response = await fetch(`/api/scenarios/${scenarioId}/shelters/${encodeURIComponent(shelterId)}/remove`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not remove shelter.");
  await reloadScenarioShelters();
  map.closePopup();
}
async function reopenShelter(shelterId) {
  const response = await fetch(`/api/scenarios/${scenarioId}/shelters/${encodeURIComponent(shelterId)}/reopen`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not reopen shelter.");
  await reloadScenarioShelters();
  map.closePopup();
}
async function toggleShelter(shelterId, isRemoved) {
  return isRemoved ? reopenShelter(shelterId) : removeShelter(shelterId);
}
async function deleteShelter(shelterId) {
  if (!window.confirm("Delete this custom shelter permanently from the scenario?")) return;
  const response = await fetch(`/api/scenarios/${scenarioId}/shelters/${encodeURIComponent(shelterId)}/delete`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not delete custom shelter.");
  await reloadScenarioShelters();
  map.closePopup();
}
function focusShelterChange(shelterId) {
  const feature = shelterFeatures.get(String(shelterId));
  const marker = shelterMarkers.get(String(shelterId));
  if (!feature || !marker) return;
  const [longitude, latitude] = feature.geometry.coordinates;
  map.setView([latitude, longitude], Math.max(map.getZoom(), 15));
  marker.__shelterCluster.zoomToShowLayer(marker, () => marker.openPopup());
}
async function resetShelters() {
  if (!window.confirm("Remove all added shelters and restore every baseline shelter?")) return;
  const response = await fetch(`/api/scenarios/${scenarioId}/shelters/reset`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not reset shelters.");
  await reloadScenarioShelters();
}
async function loadSavedScenarios() {
  const response = await fetch("/api/saved-scenarios");
  if (!response.ok) throw new Error((await response.json()).detail || "Could not load saved scenarios.");
  const payload = await response.json();
  const select = document.querySelector("#saved-scenarios");
  const selected = select.value;
  select.replaceChildren();
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Load saved scenario…";
  select.append(placeholder);
  payload.scenarios.forEach(scenario => {
    const option = document.createElement("option");
    option.value = scenario.name;
    option.textContent = scenario.name;
    select.append(option);
  });
  select.value = selected;
}
async function saveScenario() {
  const name = document.querySelector("#scenario-name").value.trim();
  if (!name) {
    document.querySelector("#route-results").className = "route-results warning";
    document.querySelector("#route-results").textContent = "Enter a scenario name before saving.";
    return;
  }
  const response = await fetch(`/api/scenarios/${scenarioId}/save`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name, start_point: startPoint }),
  });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not save scenario.");
  await loadSavedScenarios();
  document.querySelector("#saved-scenarios").value = name;
  document.querySelector("#route-results").className = "route-results";
  document.querySelector("#route-results").textContent = `Saved scenario “${name}”.`;
}
async function loadScenario() {
  const name = document.querySelector("#saved-scenarios").value;
  if (!name) {
    document.querySelector("#route-results").className = "route-results warning";
    document.querySelector("#route-results").textContent = "Choose a saved scenario to load.";
    return;
  }
  if (!window.confirm(`Replace the current road, shelter, and start-point changes with “${name}”?`)) return;
  const response = await fetch(`/api/scenarios/${scenarioId}/load/${encodeURIComponent(name)}`, { method: "POST" });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not load scenario.");
  const saved = await response.json();
  await loadRoads();
  await reloadScenarioShelters();
  if (saved.start_point) {
    setStartPoint({ ...saved.start_point, restored: true });
  } else if (startMarker) {
    map.removeLayer(startMarker);
    startMarker = undefined;
    startPoint = undefined;
  }
  if (routeOverlay) map.removeLayer(routeOverlay);
  if (selectedRouteOverlay) map.removeLayer(selectedRouteOverlay);
  routeOverlay = undefined;
  selectedRouteOverlay = undefined;
  document.querySelector("#scenario-name").value = saved.name;
  document.querySelector("#route-results").className = "route-results";
  document.querySelector("#route-results").textContent = `Loaded scenario “${saved.name}”. Recalculate routes to view updated access.`;
}
async function reloadScenarioShelters() {
  await loadScenarioShelterChanges();
  await loadShelters();
  await loadAddedShelters();
  await loadLiveShelters();
}
async function showMapActionPopup(latlng) {
  const content = L.DomUtil.create("div", "map-action-popup");
  content.innerHTML = "<strong>Checking nearby road…</strong>";
  const popup = L.popup({ closeButton: true, autoClose: true }).setLatLng(latlng).setContent(content).openOn(map);
  const response = await fetch(`/api/snap?longitude=${encodeURIComponent(latlng.lng)}&latitude=${encodeURIComponent(latlng.lat)}`);
  if (!response.ok) throw new Error((await response.json()).detail || "Could not check nearby roads.");
  const snap = await response.json();
  content.innerHTML = "<strong>What would you like to add?</strong>";
  if (snap.can_set_start) {
    const startButton = L.DomUtil.create("button", "", content);
    startButton.type = "button";
    startButton.textContent = `Set start point (snap ${Math.round(snap.distance_meters)} m)`;
    L.DomEvent.on(startButton, "click", () => setStartPoint(snap));
    const shelterLabel = L.DomUtil.create("label", "shelter-name-label", content);
    shelterLabel.textContent = "Shelter name";
    const shelterName = L.DomUtil.create("input", "shelter-name-input", content);
    shelterName.type = "text";
    shelterName.value = `New Shelter ${addedShelterCount + 1}`;
    shelterName.maxLength = 100;
    const shelterButton = L.DomUtil.create("button", "", content);
    shelterButton.type = "button";
    shelterButton.textContent = `Add shelter (snap ${Math.round(snap.distance_meters)} m)`;
    L.DomEvent.on(shelterButton, "click", () => addShelter(snap, shelterName.value.trim()).catch(error => alert(error.message)));
  } else {
    const unavailable = L.DomUtil.create("div", "start-unavailable", content);
    unavailable.textContent = `Start point and shelter unavailable: nearest road is ${Math.round(snap.distance_meters)} m away (limit ${snap.max_distance_meters} m).`;
  }
  L.DomEvent.disableClickPropagation(content);
  if (map._popup === popup) popup.update();
}
async function calculateRoutes() {
  const resultBox = document.querySelector("#route-results");
  if (!startPoint) {
    resultBox.className = "route-results warning";
    resultBox.textContent = "Set a start point on the map before calculating routes.";
    return;
  }
  const maxMinutes = Number(document.querySelector("#route-minutes").value);
  if (!Number.isFinite(maxMinutes) || maxMinutes <= 0 || maxMinutes > 720) {
    resultBox.className = "route-results warning";
    resultBox.textContent = "Enter a travel-time limit from 1 to 720 minutes.";
    return;
  }
  resultBox.className = "route-results";
  resultBox.textContent = "Calculating routes against the current road scenario…";
  const response = await fetch(`/api/scenarios/${scenarioId}/routes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ origin: startPoint, shelters: [...routeShelters.values()], max_minutes: maxMinutes }),
  });
  if (!response.ok) throw new Error((await response.json()).detail || "Could not calculate routes.");
  const result = await response.json();
  const reachable = result.results.filter(item => item.reachable);
  drawRouteOverlay(result.route_features);
  const details = reachable.length ? `<ul class="route-list">${reachable.map(item => `<li><button class="route-link" type="button" data-shelter-id="${item.id}">${item.name}: ${item.minutes} min</button></li>`).join("")}</ul>` : "";
  resultBox.innerHTML = `<strong>${result.reachable_shelters} of ${result.total_shelters}</strong> shelters are reachable within ${result.max_minutes} minutes.${details}`;
  resultBox.querySelectorAll(".route-link").forEach(button => {
    button.addEventListener("click", () => focusRoute(result, button.dataset.shelterId));
  });
  zoomToReachableShelters(result);
}
function drawRouteOverlay(features) {
  if (routeOverlay) map.removeLayer(routeOverlay);
  if (selectedRouteOverlay) map.removeLayer(selectedRouteOverlay);
  routeOverlay = L.geoJSON({ type: "FeatureCollection", features }, {
    style: { color: "#f6c814", weight: 5, opacity: .75, lineCap: "round", lineJoin: "round", interactive: false },
  }).addTo(map);
  selectedRouteOverlay = undefined;
}
function zoomToReachableShelters(result) {
  const reachable = result.results.filter(item => item.reachable);
  if (!reachable.length) return;
  const points = [[startPoint.latitude, startPoint.longitude], ...reachable.map(item => [item.latitude, item.longitude])];
  map.fitBounds(L.latLngBounds(points), { padding: [45, 45], maxZoom: 13 });
}
function focusRoute(result, shelterId) {
  const selected = result.results.find(item => item.id === shelterId);
  if (!selected) return;
  const wanted = new Set(selected.segment_ids);
  const features = result.route_features.filter(feature => wanted.has(feature.properties.segment_id));
  if (selectedRouteOverlay) map.removeLayer(selectedRouteOverlay);
  selectedRouteOverlay = L.geoJSON({ type: "FeatureCollection", features }, {
    style: { color: "#086fd6", weight: 7, opacity: 1, lineCap: "round", lineJoin: "round", interactive: false },
  }).addTo(map);
  const bounds = features.length
    ? selectedRouteOverlay.getBounds()
    : L.latLngBounds([[startPoint.latitude, startPoint.longitude], [selected.latitude, selected.longitude]]);
  bounds.extend([startPoint.latitude, startPoint.longitude]);
  bounds.extend([selected.latitude, selected.longitude]);
  map.fitBounds(bounds, { padding: [55, 55], maxZoom: 14 });
}
async function load() {
  const health = await (await fetch("/api/health")).json();
  if (!health.baseline_ready) throw new Error("Baseline is not prepared. Run the Docker prep command in the README.");
  const scope = await (await fetch("/api/scope")).json();
  const [minLon, minLat, maxLon, maxLat] = scope.display_bounds || scope.bounds;
  const scopeBounds = L.latLngBounds([[minLat, minLon], [maxLat, maxLon]]);
  const minimumZoom = map.getBoundsZoom(scopeBounds);
  lakeCountyView = { center: scopeBounds.getCenter(), zoom: minimumZoom };
  map.setMaxBounds(scopeBounds);
  map.setMinZoom(minimumZoom);
  map.fitBounds(scopeBounds, { padding: [10, 10] });
  await loadStudyArea();
  await loadRoads();
  await reloadScenarioShelters();
  await loadSavedScenarios();
  map.on("click", event => showMapActionPopup(event.latlng).catch(error => {
    document.querySelector("#status").textContent = error.message;
  }));
  map.on("moveend", () => {
    clearTimeout(loadTimer);
    loadTimer = setTimeout(() => loadRoads().catch(error => { document.querySelector("#status").textContent = error.message; }), 150);
  });
  map.on("zoomend", resizeScenarioMarkers);
  document.querySelectorAll("#road-type-filters input").forEach(input => {
    input.addEventListener("change", () => loadRoads().catch(error => { document.querySelector("#status").textContent = error.message; }));
  });
  document.querySelector("#clear-road-changes").addEventListener("click", () => {
    clearRoadChanges().catch(error => { document.querySelector("#status").textContent = error.message; });
  });
  document.querySelector("#reset-shelters").addEventListener("click", () => {
    resetShelters().catch(error => { document.querySelector("#shelter-changes").textContent = error.message; });
  });
  document.querySelector("#calculate-routes").addEventListener("click", () => {
    calculateRoutes().catch(error => {
      document.querySelector("#route-results").className = "route-results warning";
      document.querySelector("#route-results").textContent = error.message;
    });
  });
  document.querySelector("#save-scenario").addEventListener("click", () => {
    saveScenario().catch(error => { document.querySelector("#route-results").textContent = error.message; });
  });
  document.querySelector("#load-scenario").addEventListener("click", () => {
    loadScenario().catch(error => { document.querySelector("#route-results").textContent = error.message; });
  });
  const guide = document.querySelector("#how-it-works-dialog");
  document.querySelector("#how-it-works").addEventListener("click", () => guide.showModal());
  document.querySelector("#close-how-it-works").addEventListener("click", () => guide.close());
  guide.addEventListener("click", event => { if (event.target === guide) guide.close(); });
  window.setInterval(() => {
    loadLiveShelters().catch(error => { document.querySelector("#live-shelter-status").textContent = error.message; });
  }, 5 * 60 * 1000);
}
load().catch(error => { document.querySelector("#status").textContent = error.message; });
