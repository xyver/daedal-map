const map = L.map("map", { preferCanvas: true }).setView([38.9, -121.7], 9);
NetworkBasemap.setup(map, "network");

const selector = document.getElementById("pack");
const featureBox = document.getElementById("feature");
const resultBox = document.getElementById("result");
const sourceBox = document.getElementById("source");
const closeButton = document.getElementById("close");
const routeButton = document.getElementById("route");
const downstreamButton = document.getElementById("downstream");
const upstreamButton = document.getElementById("upstream");
const scenarioName = document.getElementById("scenario-name");
const saveButton = document.getElementById("save");
const savedScenarios = document.getElementById("saved-scenarios");
const loadButton = document.getElementById("load");
const deleteButton = document.getElementById("delete");
const closed = new Set();
let layer = null;
let selected = null;
let activePack = null;

function style(feature) {
  const id = feature.properties.feature_id;
  return { color: closed.has(id) ? "#e34b4b" : id === selected ? "#e9a527" : "#2766ad",
    weight: id === selected ? 5 : 2.5, opacity: 0.88 };
}

async function json(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.detail || response.statusText);
  return payload;
}

async function loadPack() {
  activePack = selector.value;
  selected = null;
  closed.clear();
  closeButton.disabled = true;
  routeButton.disabled = true;
  downstreamButton.disabled = true;
  upstreamButton.disabled = true;
  saveButton.disabled = true;
  loadButton.disabled = true;
  deleteButton.disabled = true;
  featureBox.textContent = "Click a network line.";
  resultBox.textContent = "No analysis yet.";
  if (layer) map.removeLayer(layer);
  const manifest = (await json("/api/packs")).packs.find(item => item.pack_id === activePack);
  if (!manifest) return;
  saveButton.disabled = !manifest.capabilities.includes("close_feature");
  closeButton.textContent = manifest.network_type === "river" ? "Exclude reach from trace" : "Close selected feature";
  sourceBox.textContent = `${manifest.network_type} · ${manifest.source.name} · ${manifest.source.snapshot} · ${manifest.source.licence}`;
  const collection = await json(`/api/packs/${encodeURIComponent(activePack)}/features`);
  layer = L.geoJSON(collection, { style, onEachFeature(feature, line) {
    line.on("click", async () => {
      selected = feature.properties.feature_id;
      layer.setStyle(style);
      try {
        const details = await json(`/api/packs/${encodeURIComponent(activePack)}/features/${encodeURIComponent(selected)}`);
        featureBox.textContent = JSON.stringify({ properties: details.feature.properties, links: details.links }, null, 2);
        closeButton.disabled = !manifest.capabilities.includes("close_feature");
        routeButton.disabled = !manifest.capabilities.includes("route_distance") || details.links.length !== 1;
        downstreamButton.disabled = !manifest.capabilities.includes("trace_downstream") || details.links.length !== 1;
        upstreamButton.disabled = !manifest.capabilities.includes("trace_upstream") || details.links.length !== 1;
        routeButton.dataset.start = details.links[0]?.from || "";
        routeButton.dataset.end = details.links[0]?.to || "";
        downstreamButton.dataset.start = details.links[0]?.from || "";
        upstreamButton.dataset.start = details.links[0]?.to || "";
      } catch (error) { featureBox.textContent = error.message; }
    });
  }}).addTo(map);
  if (layer.getBounds().isValid()) map.fitBounds(layer.getBounds(), { padding: [18, 18] });
  await refreshScenarios();
}

async function refreshScenarios() {
  const payload = await json(`/api/packs/${encodeURIComponent(activePack)}/scenarios`);
  savedScenarios.replaceChildren(...payload.scenario_ids.map(id => {
    const option = document.createElement("option"); option.value = id; option.textContent = id; return option;
  }));
  loadButton.disabled = payload.scenario_ids.length === 0;
  deleteButton.disabled = payload.scenario_ids.length === 0;
}

closeButton.onclick = () => {
  if (!selected) return;
  if (closed.has(selected)) closed.delete(selected); else closed.add(selected);
  layer.setStyle(style);
  resultBox.textContent = `${closed.size} feature(s) changed in this browser session.`;
};
document.getElementById("clear").onclick = () => { closed.clear(); if (layer) layer.setStyle(style); resultBox.textContent = "Changes cleared."; };
routeButton.onclick = async () => {
  try {
    const payload = await json(`/api/packs/${encodeURIComponent(activePack)}/route`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ start_node: routeButton.dataset.start, end_node: routeButton.dataset.end, closed_features: [...closed] }),
    });
    resultBox.textContent = JSON.stringify(payload, null, 2);
  } catch (error) { resultBox.textContent = error.message; }
};
async function trace(direction, startNode) {
  try {
    const payload = await json(`/api/packs/${encodeURIComponent(activePack)}/trace`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ start_node: startNode, direction, closed_features: [...closed] }),
    });
    resultBox.textContent = JSON.stringify({ direction, reached_nodes: payload.node_ids.length,
      traversed_features: payload.feature_ids.length, feature_ids: payload.feature_ids }, null, 2);
  } catch (error) { resultBox.textContent = error.message; }
}
downstreamButton.onclick = () => trace("downstream", downstreamButton.dataset.start);
upstreamButton.onclick = () => trace("upstream", upstreamButton.dataset.start);
saveButton.onclick = async () => {
  try {
    const id = scenarioName.value.trim();
    const payload = await json(`/api/packs/${encodeURIComponent(activePack)}/scenarios/${encodeURIComponent(id)}`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ closed_features: [...closed] }),
    });
    await refreshScenarios();
    savedScenarios.value = payload.scenario_id;
    resultBox.textContent = `Saved ${payload.scenario_id} for this pack version.`;
  } catch (error) { resultBox.textContent = error.message; }
};
loadButton.onclick = async () => {
  try {
    const payload = await json(`/api/packs/${encodeURIComponent(activePack)}/scenarios/${encodeURIComponent(savedScenarios.value)}`);
    closed.clear();
    payload.closed_features.forEach(id => closed.add(id));
    layer.setStyle(style);
    scenarioName.value = payload.scenario_id;
    resultBox.textContent = `Loaded ${payload.scenario_id}: ${closed.size} changed feature(s).`;
  } catch (error) { resultBox.textContent = error.message; }
};
deleteButton.onclick = async () => {
  try {
    const id = savedScenarios.value;
    await json(`/api/packs/${encodeURIComponent(activePack)}/scenarios/${encodeURIComponent(id)}`, { method: "DELETE" });
    await refreshScenarios();
    resultBox.textContent = `Deleted saved scenario ${id}. Current browser changes remain.`;
  } catch (error) { resultBox.textContent = error.message; }
};
selector.onchange = () => loadPack().catch(error => { resultBox.textContent = error.message; });
json("/api/packs").then(({ packs }) => {
  selector.replaceChildren(...packs.map(pack => {
    const option = document.createElement("option"); option.value = pack.pack_id;
    option.textContent = `${pack.network_type}: ${pack.pack_id}`; return option;
  }));
  if (packs.length) loadPack(); else resultBox.textContent = "No validated network packs installed.";
}).catch(error => { resultBox.textContent = error.message; });
