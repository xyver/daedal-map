/**
 * Overlay data-loading helpers built on top of shared overlay cache state.
 */

import {
  activeFilters,
  buildRangeUrl,
  calculateCacheSize,
  CLIMATE_VARIABLES,
  dataCache,
  loadedFilters,
  loadedRanges,
  loadedYears,
  VARIABLE_OVERLAY_MAP,
  yearRangeCache
} from './overlay-cache.js';
import { fetchMsgpack } from './utils/fetch.js';

/**
 * Load weather grid data for a specific year.
 * Weather data uses a different format than GeoJSON overlays.
 */
export async function loadWeatherYearData(overlayId, year, endpoint, signal = null) {
  if (dataCache[overlayId]?.years?.[year]) {
    console.log(`OverlayController: Using cached data for ${overlayId} year ${year}`);
    return true;
  }

  const missingVars = CLIMATE_VARIABLES.filter((varName) => {
    const varOverlayId = VARIABLE_OVERLAY_MAP[varName];
    return !dataCache[varOverlayId]?.years?.[year];
  });

  if (missingVars.length === 0) {
    console.log(`OverlayController: All climate variables already cached for year ${year}`);
    return true;
  }

  const url = new URL(endpoint.baseUrl, window.location.origin);
  url.searchParams.set('tier', endpoint.params.tier || 'monthly');
  url.searchParams.set('variables', missingVars.join(','));
  url.searchParams.set('year', year);

  console.log(`OverlayController: Fetching ${missingVars.length} climate variable(s) for year ${year}: ${missingVars.join(', ')}`);

  try {
    const fetchOptions = signal ? { signal } : {};
    const data = await fetchMsgpack(url.toString(), fetchOptions);

    if (data.error) {
      console.error('OverlayController: Weather API error:', data.error);
      loadedYears[overlayId]?.delete(year);
      return false;
    }

    if (data.tier && data.requested_tier && data.tier !== data.requested_tier) {
      console.log(`OverlayController: Tier cascade for ${year}: ${data.requested_tier} -> ${data.tier}`);
    }

    if (data.variables && data.color_scales) {
      for (const variable of data.variables) {
        const varOverlayId = VARIABLE_OVERLAY_MAP[variable];
        if (!varOverlayId) continue;

        if (!dataCache[varOverlayId]) {
          dataCache[varOverlayId] = { years: {}, colorScale: null, grid: null };
        }

        dataCache[varOverlayId].years[year] = {
          timestamps: data.timestamps,
          values: data.values[variable],
          tier: data.tier
        };

        if (data.color_scales[variable]) {
          dataCache[varOverlayId].colorScale = data.color_scales[variable];
        }
        if (data.grid) {
          dataCache[varOverlayId].grid = data.grid;
        }

        if (!yearRangeCache[varOverlayId]) {
          yearRangeCache[varOverlayId] = { min: year, max: year, available: [] };
        }
        yearRangeCache[varOverlayId].min = Math.min(yearRangeCache[varOverlayId].min, year);
        yearRangeCache[varOverlayId].max = Math.max(yearRangeCache[varOverlayId].max, year);
        if (!yearRangeCache[varOverlayId].available.includes(year)) {
          yearRangeCache[varOverlayId].available.push(year);
          yearRangeCache[varOverlayId].available.sort((a, b) => a - b);
        }

        if (!loadedYears[varOverlayId]) loadedYears[varOverlayId] = new Set();
        loadedYears[varOverlayId].add(year);
      }

      const frameCount = data.timestamps?.length || 0;
      console.log(`OverlayController: Cached ${data.variables.length} climate variables for year ${year} (${frameCount} frames)`);

      for (const variable of data.variables) {
        const varOverlayId = VARIABLE_OVERLAY_MAP[variable];
        if (varOverlayId) {
          window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: { overlayId: varOverlayId, year } }));
        }
      }

      return true;
    }

    console.error('OverlayController: Unexpected weather response shape (expected multi-variable payload)');
    return false;
  } catch (error) {
    loadedYears[overlayId]?.delete(year);
    if (error.name === 'AbortError') {
      console.log(`OverlayController: Weather fetch aborted for ${overlayId} ${year}`);
      return false;
    }
    console.error(`OverlayController: Failed to load weather ${overlayId} for ${year}:`, error);
    return false;
  }
}

/**
 * Load data for a time range and merge into cache.
 * Skips if range is already fully covered by loaded ranges.
 */
export async function loadRangeData(overlayId, startMs, endMs, endpoint, signal = null) {
  if (!endpoint) return false;

  if (endpoint.isWeatherGrid) {
    const year = new Date(endMs).getFullYear();
    return loadWeatherYearData(overlayId, year, endpoint, signal);
  }

  if (!loadedRanges[overlayId]) {
    loadedRanges[overlayId] = [];
  }

  const isRangeCovered = loadedRanges[overlayId].some((r) => r.start <= startMs && r.end >= endMs);
  if (isRangeCovered) {
    console.log(`OverlayController: ${overlayId} range already cached`);
    return false;
  }

  const rangeEntry = { start: startMs, end: endMs, loading: true };
  loadedRanges[overlayId].push(rangeEntry);

  const url = buildRangeUrl(endpoint, startMs, endMs, overlayId);
  const startDate = new Date(startMs).toISOString().split('T')[0];
  const endDate = new Date(endMs).toISOString().split('T')[0];
  console.log(`OverlayController: Fetching ${overlayId} for ${startDate} to ${endDate}`);
  console.log(`OverlayController: URL = ${url}`);

  try {
    const fetchOptions = signal ? { signal } : {};
    const geojson = await fetchMsgpack(url, fetchOptions);
    const featureCount = geojson.features?.length || 0;

    if (!dataCache[overlayId]) {
      dataCache[overlayId] = { type: 'FeatureCollection', features: [] };
    }

    if (featureCount > 0) {
      const existingIds = new Set(
        dataCache[overlayId].features
          .map((f) => f.properties?.event_id || f.properties?.storm_id || f.id)
          .filter(Boolean)
      );

      const newFeatures = geojson.features.filter((f) => {
        const id = f.properties?.event_id || f.properties?.storm_id || f.id;
        return !id || !existingIds.has(id);
      });

      dataCache[overlayId].features.push(...newFeatures);
      console.log(`OverlayController: Added ${newFeatures.length} ${overlayId} features (total: ${dataCache[overlayId].features.length})`);

      const cacheSize = calculateCacheSize();
      console.log(`OverlayController: Total cache: ${cacheSize.totalFeatures} features (${cacheSize.sizeMB} MB)`);
      window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: cacheSize }));
    } else {
      console.log(`OverlayController: No ${overlayId} events in range`);
    }

    rangeEntry.loading = false;

    if (!loadedYears[overlayId]) {
      loadedYears[overlayId] = new Set();
    }
    const startYear = new Date(startMs).getFullYear();
    const endYear = new Date(endMs).getFullYear();
    const SIX_MONTHS_MS = 180 * 24 * 60 * 60 * 1000;

    for (let y = startYear; y <= endYear; y++) {
      const yearStartMs = new Date(y, 0, 1).getTime();
      const yearEndMs = new Date(y, 11, 31, 23, 59, 59).getTime();
      const loadedStart = Math.max(startMs, yearStartMs);
      const loadedEnd = Math.min(endMs, yearEndMs);
      const loadedDuration = loadedEnd - loadedStart;
      const isFullYearRequest = startMs <= yearStartMs && endMs >= yearEndMs;

      if (isFullYearRequest || loadedDuration >= SIX_MONTHS_MS) {
        loadedYears[overlayId].add(y);
        console.log(`OverlayController: Marked ${overlayId} year ${y} as loaded (${Math.round(loadedDuration / (24 * 60 * 60 * 1000))} days)`);
      } else {
        console.log(`OverlayController: ${overlayId} year ${y} partial load (${Math.round(loadedDuration / (24 * 60 * 60 * 1000))} days) - not marking as loaded`);
      }
    }
    console.log(`OverlayController: ${overlayId} total cached: ${dataCache[overlayId]?.features?.length || 0} features`);

    if (!yearRangeCache[overlayId]) {
      yearRangeCache[overlayId] = { min: startYear, max: endYear, available: [] };
    }
    yearRangeCache[overlayId].min = Math.min(yearRangeCache[overlayId].min, startYear);
    yearRangeCache[overlayId].max = Math.max(yearRangeCache[overlayId].max, endYear);

    const defaultParams = endpoint.params || {};
    const overrides = activeFilters[overlayId] || {};
    const effectiveFilters = { ...defaultParams, ...overrides };

    if (!loadedFilters[overlayId]) {
      loadedFilters[overlayId] = {};
    }
    if (effectiveFilters.min_magnitude !== undefined) {
      const current = loadedFilters[overlayId].minMagnitude;
      loadedFilters[overlayId].minMagnitude = current !== undefined
        ? Math.min(current, effectiveFilters.min_magnitude)
        : effectiveFilters.min_magnitude;
    }
    if (effectiveFilters.min_vei !== undefined) {
      const current = loadedFilters[overlayId].minVei;
      loadedFilters[overlayId].minVei = current !== undefined
        ? Math.min(current, effectiveFilters.min_vei)
        : effectiveFilters.min_vei;
    }
    if (effectiveFilters.min_category !== undefined) {
      loadedFilters[overlayId].minCategory = effectiveFilters.min_category;
    }
    if (effectiveFilters.min_scale !== undefined) {
      loadedFilters[overlayId].minScale = effectiveFilters.min_scale;
    }
    if (effectiveFilters.min_area_km2 !== undefined) {
      const current = loadedFilters[overlayId].minAreaKm2;
      loadedFilters[overlayId].minAreaKm2 = current !== undefined
        ? Math.min(current, effectiveFilters.min_area_km2)
        : effectiveFilters.min_area_km2;
    }

    return featureCount > 0;
  } catch (error) {
    const idx = loadedRanges[overlayId].indexOf(rangeEntry);
    if (idx >= 0) loadedRanges[overlayId].splice(idx, 1);

    if (error.name === 'AbortError') {
      console.log(`OverlayController: Range fetch aborted for ${overlayId}`);
      return false;
    }
    console.error(`OverlayController: Failed to load ${overlayId}:`, error);
    return false;
  }
}
