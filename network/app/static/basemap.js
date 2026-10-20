/* Shared raster-tile preference for the local Mapping host and Network views.
 * The Network override is optional; without it both pages use Mapping's choice.
 * These preferences are browser-local until the downloadable Mapping runtime
 * provides a project settings store.
 */
(function () {
  const MAPPING_KEY = "daedalmap.mapping.basemap.v1";
  const NETWORK_KEY = "daedalmap.network.basemap.v1";
  const STARTER = {
    url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution: "© OpenStreetMap contributors",
  };

  function valid(config) {
    return config && typeof config.url === "string" && typeof config.attribution === "string" &&
      /^(https?:\/\/|\/(?!\/))/.test(config.url) &&
      ["{z}", "{x}", "{y}"].every(token => config.url.includes(token)) &&
      config.attribution.trim().length > 0;
  }

  function read(key) {
    try {
      const value = JSON.parse(localStorage.getItem(key));
      return valid(value) ? value : null;
    } catch (_error) { return null; }
  }

  function save(key, config) {
    config = { url: config.url.trim(), attribution: config.attribution.trim() };
    if (!valid(config)) throw new Error("Enter an HTTP(S) or /local XYZ URL with {z}, {x}, {y}, plus attribution.");
    localStorage.setItem(key, JSON.stringify(config));
  }

  function choice(scope) {
    return (scope === "network" && read(NETWORK_KEY)) || read(MAPPING_KEY) || STARTER;
  }

  function escapeHtml(value) {
    return value.replace(/[&<>"']/g, character => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;",
      '"': "&quot;", "'": "&#39;" })[character]);
  }

  function setup(map, scope) {
    const layer = L.tileLayer(choice(scope).url, {
      maxZoom: 19, attribution: escapeHtml(choice(scope).attribution),
    }).addTo(map);
    const status = document.getElementById("basemap-status");
    function apply() {
      const config = choice(scope);
      map.removeLayer(layer);
      layer.setUrl(config.url);
      layer.options.attribution = escapeHtml(config.attribution);
      map.addLayer(layer);
      status.textContent = `Using ${config.url}`;
    }
    layer.on("tileerror", () => { status.textContent = "Map tiles could not load. Check the URL and your connection."; });

    const url = document.getElementById("basemap-url");
    const attribution = document.getElementById("basemap-attribution");
    const source = document.getElementById("basemap-source");
    const saveButton = document.getElementById("basemap-save");
    const resetButton = document.getElementById("basemap-reset");
    function refreshControls() {
      if (scope === "network") {
        source.value = read(NETWORK_KEY) ? "network" : "mapping";
        const config = read(NETWORK_KEY) || choice("mapping");
        url.value = config.url;
        attribution.value = config.attribution;
        url.disabled = attribution.disabled = source.value === "mapping";
      } else {
        const config = choice("mapping");
        url.value = config.url;
        attribution.value = config.attribution;
      }
    }
    if (source) source.onchange = () => {
      url.disabled = attribution.disabled = source.value === "mapping";
      if (source.value === "mapping") {
        const config = choice("mapping");
        url.value = config.url;
        attribution.value = config.attribution;
      }
    };
    saveButton.onclick = () => {
      try {
        if (scope === "network" && source.value === "mapping") localStorage.removeItem(NETWORK_KEY);
        else save(scope === "network" ? NETWORK_KEY : MAPPING_KEY,
          { url: url.value, attribution: attribution.value });
        apply();
        refreshControls();
      } catch (error) { status.textContent = error.message; }
    };
    resetButton.onclick = () => {
      localStorage.removeItem(scope === "network" ? NETWORK_KEY : MAPPING_KEY);
      refreshControls();
      apply();
    };
    refreshControls();
    status.textContent = `Using ${choice(scope).url}`;
    return layer;
  }

  window.NetworkBasemap = { setup, choice };
})();
