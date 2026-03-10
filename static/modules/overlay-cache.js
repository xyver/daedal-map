/**
 * Shared overlay cache state and cache-related helpers.
 */

// Cache for loaded overlay data (full unfiltered datasets)
export const dataCache = {};

// Cache for metrics/choropleth data from order system
// sourceId -> { geojson, year_data, year_range, loadedAt }
export const metricCache = {};

// Track which time ranges have been loaded per overlay
// Each entry is an array of {start, end} (millisecond timestamps)
export const loadedRanges = {};

// Legacy: loadedYears kept as derived view for compatibility with render logic
export const loadedYears = {};

// Track current displayed year per overlay
export const displayedYear = {};

// Cache year ranges per overlay (for recalculating combined range when overlays change)
export const yearRangeCache = {};

// Active filter overrides per overlay (for chat-based filter modifications)
export const activeFilters = {};

// Track filters that were used when data was loaded
export const loadedFilters = {};

// All climate variables to fetch together (optimization: one API call for all)
export const CLIMATE_VARIABLES = [
  'temp_c', 'humidity', 'snow_depth_m',
  'precipitation_mm', 'cloud_cover_pct', 'pressure_hpa',
  'solar_radiation', 'soil_temp_c', 'soil_moisture'
];

export const CLIMATE_OVERLAY_MAP = {
  'temperature': 'temp_c',
  'humidity': 'humidity',
  'snow-depth': 'snow_depth_m',
  'precipitation': 'precipitation_mm',
  'cloud-cover': 'cloud_cover_pct',
  'pressure': 'pressure_hpa',
  'solar-radiation': 'solar_radiation',
  'soil-temp': 'soil_temp_c',
  'soil-moisture': 'soil_moisture'
};

export const VARIABLE_OVERLAY_MAP = {
  'temp_c': 'temperature',
  'humidity': 'humidity',
  'snow_depth_m': 'snow-depth',
  'precipitation_mm': 'precipitation',
  'cloud_cover_pct': 'cloud-cover',
  'pressure_hpa': 'pressure',
  'solar_radiation': 'solar-radiation',
  'soil_temp_c': 'soil-temp',
  'soil_moisture': 'soil-moisture'
};

/**
 * Calculate total cache size - exact bytes via JSON serialization.
 * @returns {{ totalFeatures: number, bytes: number, sizeMB: string, perOverlay: Object }}
 */
export function calculateCacheSize() {
  let totalFeatures = 0;
  let totalBytes = 0;
  const perOverlay = {};

  for (const overlayId of Object.keys(dataCache)) {
    const features = dataCache[overlayId]?.features || [];
    if (features.length > 0) {
      const bytes = new Blob([JSON.stringify(features)]).size;
      perOverlay[overlayId] = { features: features.length, bytes, type: 'events' };
      totalFeatures += features.length;
      totalBytes += bytes;
    }
  }

  for (const sourceId of Object.keys(metricCache)) {
    const cached = metricCache[sourceId];
    const features = cached?.geojson?.features || [];
    if (features.length > 0 || cached?.year_data) {
      const dataToSize = { features, year_data: cached?.year_data || {} };
      const bytes = new Blob([JSON.stringify(dataToSize)]).size;
      perOverlay[sourceId] = { features: features.length, bytes, type: 'metrics' };
      totalFeatures += features.length;
      totalBytes += bytes;
    }
  }

  const sizeMB = (totalBytes / (1024 * 1024)).toFixed(2);
  return { totalFeatures, bytes: totalBytes, sizeMB, perOverlay };
}

/**
 * Build URL for fetching data within a time range.
 * @param {Object} endpoint - Endpoint config from OVERLAY_ENDPOINTS
 * @param {number} startMs - Start timestamp in milliseconds
 * @param {number} endMs - End timestamp in milliseconds
 * @param {string | null} overlayId - Overlay ID for looking up active filters
 * @returns {string}
 */
export function buildRangeUrl(endpoint, startMs, endMs, overlayId = null) {
  const url = new URL(endpoint.baseUrl, window.location.origin);
  const defaultParams = endpoint.params || {};
  const overrides = overlayId ? (activeFilters[overlayId] || {}) : {};
  const effectiveParams = { ...defaultParams };

  if (overrides.minMagnitude !== undefined) {
    effectiveParams.min_magnitude = String(overrides.minMagnitude);
  }
  if (overrides.maxMagnitude !== undefined) {
    effectiveParams.max_magnitude = String(overrides.maxMagnitude);
  }
  if (overrides.minCategory !== undefined) {
    effectiveParams.min_category = `Cat${overrides.minCategory}`;
  }
  if (overrides.minScale !== undefined) {
    effectiveParams.min_scale = `EF${overrides.minScale}`;
  }
  if (overrides.minAreaKm2 !== undefined) {
    effectiveParams.min_area_km2 = String(overrides.minAreaKm2);
  }
  if (overrides.minVei !== undefined) {
    effectiveParams.min_vei = String(overrides.minVei);
  }
  if (overrides.locPrefix !== undefined) {
    effectiveParams.loc_prefix = overrides.locPrefix;
  }
  if (overrides.affectedLocId !== undefined) {
    effectiveParams.affected_loc_id = overrides.affectedLocId;
  }

  for (const [key, value] of Object.entries(effectiveParams)) {
    url.searchParams.set(key, value);
  }

  url.searchParams.set('start', String(startMs));
  url.searchParams.set('end', String(endMs));
  return url.toString();
}
