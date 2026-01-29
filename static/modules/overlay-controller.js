/**
 * Overlay Controller - Orchestrates overlay data loading and rendering.
 * Listens to OverlaySelector changes and fetches/displays data using models.
 *
 * Data flow:
 * 1. Toggle overlay ON -> fetch ALL events from API
 * 2. Cache full dataset
 * 3. Filter by current TimeSlider year and render
 * 4. When TimeSlider changes -> filter cached data and update display
 */

import { TrackAnimator, MultiTrackAnimator, setDependencies as setTrackAnimatorDeps } from './track-animator.js';
import EventAnimator, { AnimationMode, setDependencies as setEventAnimatorDeps } from './event-animator.js';
import { TIME_SYSTEM } from './time-slider.js';
import { CONFIG } from './config.js';
import { DetailedEventCache } from './cache.js';
import { fetchMsgpack } from './utils/fetch.js';
import { WeatherGridModel, setDependencies as setWeatherGridDeps } from './models/model-weather-grid.js';

// Dependencies set via setDependencies
let MapAdapter = null;
let ModelRegistry = null;
let OverlaySelector = null;
let TimeSlider = null;

export function setDependencies(deps) {
  MapAdapter = deps.MapAdapter;
  ModelRegistry = deps.ModelRegistry;
  OverlaySelector = deps.OverlaySelector;
  TimeSlider = deps.TimeSlider;

  // Wire dependencies to TrackAnimator
  setTrackAnimatorDeps({
    MapAdapter: deps.MapAdapter,
    TimeSlider: deps.TimeSlider,
    TrackModel: deps.ModelRegistry?.getModel?.('track')
  });

  // Wire dependencies to EventAnimator
  setEventAnimatorDeps({
    MapAdapter: deps.MapAdapter,
    TimeSlider: deps.TimeSlider,
    ModelRegistry: deps.ModelRegistry,
    TIME_SYSTEM: TIME_SYSTEM
  });

  // Wire dependencies to WeatherGridModel
  setWeatherGridDeps({
    MapAdapter: deps.MapAdapter
  });
}

/**
 * Gardner-Knopoff window calculation for aftershocks.
 * Returns time window in days based on mainshock magnitude.
 * Formula: 10^(0.5*M - 1.5) days
 * @param {number} magnitude - Mainshock magnitude
 * @returns {number} Time window in days
 */
function gardnerKnopoffTimeWindow(magnitude) {
  return Math.pow(10, 0.5 * magnitude - 1.5);
}

// API endpoints for each overlay type
// Severity filters reduce data volume for initial load:
// - Earthquakes: M5.5+ (significant events)
// - Hurricanes: Cat1+ (named hurricanes only, excludes TD/TS)
// - Tornadoes: EF2+ (significant damage)
// - Wildfires: 100km2+ (major fires)
// - Volcanoes, Tsunamis, Floods: no filter (small datasets)
//
// Year-based lazy loading: Data is fetched per-year as user navigates time.
// On overlay enable: fetch current year
// On TimeSlider change: fetch that year if not cached
const OVERLAY_ENDPOINTS = {
  earthquakes: {
    baseUrl: '/api/earthquakes/geojson',
    params: { min_magnitude: '5.5' },
    eventType: 'earthquake',
    yearField: 'year'
  },
  hurricanes: {
    baseUrl: '/api/storms/tracks/geojson',
    params: { min_category: 'Cat1' },
    trackEndpoint: '/api/storms/{storm_id}/track',
    eventType: 'hurricane',
    yearField: 'year'
  },
  volcanoes: {
    baseUrl: '/api/eruptions/geojson',
    params: { exclude_ongoing: 'true' },
    eventType: 'volcano',
    yearField: 'year'
  },
  wildfires: {
    baseUrl: '/api/wildfires/geojson',
    params: { min_area_km2: '500', include_perimeter: 'true' },  // 500km2 (~193 sq mi) = large fires
    eventType: 'wildfire',
    yearField: 'year'
  },
  tsunamis: {
    baseUrl: '/api/tsunamis/geojson',
    params: {},
    animationEndpoint: '/api/tsunamis/{event_id}/animation',
    eventType: 'tsunami',
    yearField: 'year'
  },
  tornadoes: {
    baseUrl: '/api/tornadoes/geojson',
    params: { min_scale: 'EF2' },
    detailEndpoint: '/api/tornadoes/{event_id}',
    eventType: 'tornado',
    yearField: 'year'
  },
  floods: {
    baseUrl: '/api/floods/geojson',
    params: { include_geometry: 'true' },
    geometryEndpoint: '/api/floods/{event_id}/geometry',
    eventType: 'flood',
    yearField: 'year',
    maxYear: 2019  // Flood data ends at 2019
  },
  drought: {
    baseUrl: '/api/drought/geojson',
    params: { country: 'CAN' },
    eventType: 'drought',
    yearField: 'year',
    minYear: 2019  // Canada drought data starts at 2019
  },
  landslides: {
    baseUrl: '/api/landslides/geojson',
    params: { min_deaths: '1', require_coords: 'true' },
    eventType: 'landslide',
    yearField: 'year'
  },
  // Weather/Climate overlays - grid data format (not GeoJSON)
  // Data available from 1940, but UI defaults to 2000 (earlier data via chat)
  temperature: {
    baseUrl: '/api/weather/grid',
    params: { variable: 'temp_c', tier: 'weekly' },
    isWeatherGrid: true,
    minYear: 1940,
    defaultMinYear: 2000
  },
  humidity: {
    baseUrl: '/api/weather/grid',
    params: { variable: 'humidity', tier: 'weekly' },
    isWeatherGrid: true,
    minYear: 1940,
    defaultMinYear: 2000
  },
  'snow-depth': {
    baseUrl: '/api/weather/grid',
    params: { variable: 'snow_depth_m', tier: 'weekly' },
    isWeatherGrid: true,
    minYear: 1940,
    defaultMinYear: 2000
  }
};

/**
 * Event lifecycle configuration for timestamp-based filtering.
 * Each disaster type defines how to calculate start/end times and fade duration.
 * See docs/future/rolling_time.md for full documentation.
 */
const EVENT_LIFECYCLE = {
  earthquake: {
    // Earthquake with expanding "aftershock zone" circle
    // Circle expands over days/weeks based on magnitude (from aftershock data analysis)
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      // End time = when the expanding circle reaches max radius
      // Based on data: M5.5: ~4d, M6: ~9d, M6.5-7.5: ~20-25d, M8+: ~30d
      const start = new Date(f.properties.timestamp).getTime();
      const mag = f.properties.magnitude || 5;
      // Expansion duration scales with magnitude: ~4 days at M5, ~30 days at M8
      const expansionDays = Math.min(30, 4 * Math.pow(1.5, mag - 5));
      return start + expansionDays * 24 * 60 * 60 * 1000;
    },
    defaultDuration: 7 * 24 * 60 * 60 * 1000,  // 7 days default
    // Quick fade after expansion completes (aftershock sequence ending)
    fadeDuration: 7 * 24 * 60 * 60 * 1000,  // 7 days
    // Aftershock wave speed from data analysis (distance/time of aftershocks):
    // M5-6: ~0.3 km/h (7 km/day), M7-8: ~1-2 km/h (25-50 km/day)
    // Bigger earthquakes = faster expansion (more impressive on map)
    getWaveSpeedKmPerMs: (f) => {
      const mag = f.properties.magnitude || 5;
      // Base: 0.3 km/h at M5, doubles per magnitude unit
      // 0.3 km/h = 0.0833 km/min = 0.00139 km/sec = 0.00000139 km/ms
      const baseSpeed = 0.00000139;  // 0.3 km/h in km/ms
      return baseSpeed * Math.pow(2, mag - 5);
    },
    // Max radius from data (use felt_radius_km, default by magnitude)
    getMaxWaveRadiusKm: (f) => {
      if (f.properties.felt_radius_km) return f.properties.felt_radius_km;
      // Default based on magnitude: M5: ~30km, M6: ~80km, M7: ~180km, M8: ~400km
      const mag = f.properties.magnitude || 5;
      return 30 * Math.pow(2.5, mag - 5);
    }
  },

  hurricane: {
    // Track event - spans start_date to end_date
    getStartMs: (f) => new Date(f.properties.start_date).getTime(),
    getEndMs: (f) => new Date(f.properties.end_date).getTime(),
    defaultDuration: 7 * 24 * 60 * 60 * 1000,  // 7 days if missing
    fadeDuration: 7 * 24 * 60 * 60 * 1000      // 7 days after dissipation
  },

  tsunami: {
    // Propagation event - wave expands to furthest runup location
    // Uses max_runup_dist_km from data (distance to furthest observed runup)
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      const start = new Date(f.properties.timestamp).getTime();
      // End time = when wave reaches furthest runup
      // Wave speed ~720 km/h, calculate from max distance
      const maxDist = f.properties.max_runup_dist_km || 500;  // Default 500km
      const travelHours = maxDist / 720;
      return start + travelHours * 60 * 60 * 1000;
    },
    defaultDuration: 2 * 60 * 60 * 1000,  // 2 hours default
    fadeDuration: 7 * 24 * 60 * 60 * 1000,  // 7 days
    // Wave speed: ~720 km/h in open ocean
    waveSpeedKmPerMs: 0.0002,  // 720 km/h in km/ms
    // Max radius from data (furthest runup location)
    getMaxWaveRadiusKm: (f) => {
      return f.properties.max_runup_dist_km || 500;  // Default 500km
    }
  },

  volcano: {
    // Volcanic eruption with expanding ash cloud/felt radius
    // Uses actual felt_radius_km from data, VEI calculation as fallback
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      // If eruption has known duration, use that
      if (f.properties.end_timestamp) {
        return new Date(f.properties.end_timestamp).getTime();
      }
      if (f.properties.duration_days) {
        return new Date(f.properties.timestamp).getTime() +
               f.properties.duration_days * 24 * 60 * 60 * 1000;
      }
      if (f.properties.is_ongoing) {
        return Date.now();  // Still active
      }
      // Expansion time: circle grows to felt_radius over several hours
      // Higher VEI = faster expansion but larger radius, so ~similar duration
      const start = new Date(f.properties.timestamp).getTime();
      const vei = f.properties.VEI || f.properties.vei || 2;
      const maxRadius = f.properties.felt_radius_km || Math.pow(2, vei) * 12.5;
      const speedKmH = 10 * Math.pow(1.6, vei - 2);
      const expansionHours = maxRadius / speedKmH;
      return start + expansionHours * 60 * 60 * 1000;
    },
    defaultDuration: 24 * 60 * 60 * 1000,  // 24 hours default expansion
    fadeDuration: 7 * 24 * 60 * 60 * 1000,   // 7 days fade
    // Ash cloud expansion speed - VEI-based
    // VEI 2: ~10 km/h, VEI 4: ~26 km/h, VEI 6: ~66 km/h
    getWaveSpeedKmPerMs: (f) => {
      const vei = f.properties.VEI || f.properties.vei || 2;
      // Base: 10 km/h at VEI 2, scales with VEI
      const speedKmH = 10 * Math.pow(1.6, vei - 2);
      return speedKmH / 3600000;  // Convert km/h to km/ms
    },
    // Max radius from actual data (felt_radius_km), VEI fallback
    // Data: VEI 2: ~23km, VEI 4: ~105km, VEI 6: ~478km, VEI 7: ~1021km
    getMaxWaveRadiusKm: (f) => {
      if (f.properties.felt_radius_km) return f.properties.felt_radius_km;
      const vei = f.properties.VEI || f.properties.vei || 2;
      return Math.pow(2, vei) * 12.5;  // Fallback calculation
    }
  },

  tornado: {
    // Instant track event
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      // Estimate from track length: ~1 mile/minute typical speed
      const lengthMi = f.properties.tornado_length_mi || 1;
      return new Date(f.properties.timestamp).getTime() + lengthMi * 60 * 1000;
    },
    defaultDuration: 30 * 60 * 1000,       // 30 minutes
    fadeDuration: 7 * 24 * 60 * 60 * 1000  // 7 days
  },

  wildfire: {
    // Duration event with progression
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      if (f.properties.duration_days) {
        return new Date(f.properties.timestamp).getTime() +
               f.properties.duration_days * 24 * 60 * 60 * 1000;
      }
      return new Date(f.properties.timestamp).getTime() + 30 * 24 * 60 * 60 * 1000;
    },
    defaultDuration: 30 * 24 * 60 * 60 * 1000,  // 30 days
    fadeDuration: 7 * 24 * 60 * 60 * 1000       // 7 days
  },

  flood: {
    // Duration event
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      if (f.properties.end_timestamp) {
        return new Date(f.properties.end_timestamp).getTime();
      }
      if (f.properties.duration_days) {
        return new Date(f.properties.timestamp).getTime() +
               f.properties.duration_days * 24 * 60 * 60 * 1000;
      }
      return new Date(f.properties.timestamp).getTime() + 21 * 24 * 60 * 60 * 1000;
    },
    defaultDuration: 21 * 24 * 60 * 60 * 1000,  // 21 days
    fadeDuration: 7 * 24 * 60 * 60 * 1000       // 7 days
  },

  drought: {
    // Monthly snapshot duration event
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      if (f.properties.end_timestamp) {
        return new Date(f.properties.end_timestamp).getTime();
      }
      if (f.properties.duration_days) {
        return new Date(f.properties.timestamp).getTime() +
               f.properties.duration_days * 24 * 60 * 60 * 1000;
      }
      return new Date(f.properties.timestamp).getTime() + 30 * 24 * 60 * 60 * 1000;
    },
    defaultDuration: 30 * 24 * 60 * 60 * 1000,  // 30 days
    fadeDuration: 0  // No fade between monthly snapshots
  },

  landslide: {
    // Point event with expanding circle based on deaths (intensity)
    // Circle expands quickly, stays visible based on severity
    getStartMs: (f) => new Date(f.properties.timestamp).getTime(),
    getEndMs: (f) => {
      const start = new Date(f.properties.timestamp).getTime();
      // Higher intensity (more deaths) = longer visibility: 3-14 days
      const intensity = f.properties.intensity || 1;
      const durationDays = 3 + 2 * intensity;  // 5 days at intensity 1, 13 days at intensity 5
      return start + durationDays * 24 * 60 * 60 * 1000;
    },
    defaultDuration: 7 * 24 * 60 * 60 * 1000,  // 7 days default
    fadeDuration: 7 * 24 * 60 * 60 * 1000,     // 7 days fade
    // Use felt_radius_km from data for circle sizing
    getMaxWaveRadiusKm: (f) => f.properties.felt_radius_km || 10
  }
};

/**
 * Filter and annotate features by lifecycle state.
 * Adds animation properties for expanding circle effects:
 * - _radiusProgress: 0-1 progress through active+animation period (for expanding circles)
 * - _waveRadiusKm: For tsunamis, the current wave radius in km
 * @param {Array} features - GeoJSON features
 * @param {number} currentMs - Current time in milliseconds
 * @param {string} eventType - Event type key (earthquake, hurricane, etc.)
 * @returns {Array} Filtered features with _opacity, _phase, _radiusProgress properties
 */
function filterByLifecycle(features, currentMs, eventType) {
  const config = EVENT_LIFECYCLE[eventType];
  if (!config) {
    // Fallback: show all features at full opacity
    return features.map(f => ({
      ...f,
      properties: { ...f.properties, _opacity: 1.0, _phase: 'active', _radiusProgress: 1.0 }
    }));
  }

  return features.map(f => {
    let startMs, endMs;
    try {
      startMs = config.getStartMs(f);
      endMs = config.getEndMs(f);
    } catch (e) {
      // If timestamp parsing fails, show at full opacity
      return {
        ...f,
        properties: { ...f.properties, _opacity: 1.0, _phase: 'active', _radiusProgress: 1.0 }
      };
    }

    // Handle invalid dates
    if (isNaN(startMs) || isNaN(endMs)) {
      return {
        ...f,
        properties: { ...f.properties, _opacity: 1.0, _phase: 'active', _radiusProgress: 1.0 }
      };
    }

    const fadeDuration = config.getFadeDuration?.(f) || config.fadeDuration;
    const fadeEndMs = endMs + fadeDuration;

    // Not visible yet
    if (currentMs < startMs) return null;

    // Already faded out
    if (currentMs > fadeEndMs) return null;

    // Calculate phase and opacity
    let opacity = 1.0;
    let phase = 'active';
    let radiusProgress = 1.0;

    if (currentMs <= endMs) {
      // In active period - calculate expansion progress
      phase = 'active';
      const activeDuration = Math.max(endMs - startMs, config.defaultDuration || 60000);
      // Animation duration: 10% of active period or 5 days, whichever is smaller
      const animationDuration = Math.min(activeDuration * 0.1, 5 * 24 * 60 * 60 * 1000);
      const elapsed = currentMs - startMs;
      if (elapsed < animationDuration) {
        // Expanding phase - ease out for natural feel
        radiusProgress = easeOutQuad(elapsed / animationDuration);
      } else {
        radiusProgress = 1.0;
      }
    } else {
      // In fade period
      phase = 'fading';
      opacity = 1.0 - (currentMs - endMs) / fadeDuration;
      opacity = Math.max(0, Math.min(1, opacity));  // Clamp 0-1
      radiusProgress = 1.0;  // Full size during fade
    }

    // Build properties with animation data
    const props = {
      ...f.properties,
      _opacity: opacity,
      _phase: phase,
      _radiusProgress: radiusProgress
    };

    // Calculate expanding wave radius based on event type
    const elapsed = currentMs - startMs;

    if (eventType === 'earthquake') {
      // Aftershock zone expansion: ~0.3-3 km/h based on magnitude
      // Data-driven speeds from aftershock distance/time analysis
      const waveSpeed = config.getWaveSpeedKmPerMs?.(f) || config.waveSpeedKmPerMs || 0.00000139;
      const maxRadius = config.getMaxWaveRadiusKm?.(f) || f.properties.felt_radius_km || 300;
      const currentRadius = Math.min(elapsed * waveSpeed, maxRadius);
      props._waveRadiusKm = currentRadius;
      // Also set progress for any layers using it
      props._radiusProgress = maxRadius > 0 ? currentRadius / maxRadius : 1.0;
    }

    if (eventType === 'volcano') {
      // Ash cloud expansion: VEI-based speed (10-100 km/h)
      const waveSpeed = config.getWaveSpeedKmPerMs?.(f) || config.waveSpeedKmPerMs || 0.0000028;
      const maxRadius = config.getMaxWaveRadiusKm?.(f) || 100;
      const currentRadius = Math.min(elapsed * waveSpeed, maxRadius);
      props._waveRadiusKm = currentRadius;
      props._radiusProgress = maxRadius > 0 ? currentRadius / maxRadius : 1.0;
    }

    if (eventType === 'tsunami') {
      // Tsunami waves travel ~720 km/h, expand to furthest runup location
      // All events in events.parquet are sources (runups are in separate file)
      const waveSpeed = config.waveSpeedKmPerMs || 0.0002;  // 720 km/h
      const maxRadius = config.getMaxWaveRadiusKm?.(f) || f.properties.max_runup_dist_km || 500;
      const currentRadius = Math.min(elapsed * waveSpeed, maxRadius);
      props._waveRadiusKm = currentRadius;
      props._radiusProgress = maxRadius > 0 ? currentRadius / maxRadius : 1.0;
    }

    // Hurricane track progressive display - trim LineString based on time progress
    if (eventType === 'hurricane' && f.geometry?.type === 'LineString') {
      const totalDuration = endMs - startMs;
      // Calculate animation progress (0 to 1) based on time within active period
      let animationProgress;
      if (phase === 'active' && totalDuration > 0) {
        animationProgress = Math.min(1, elapsed / totalDuration);
      } else {
        // Fading phase or completed - show full track
        animationProgress = 1.0;
      }
      props._animationProgress = animationProgress;

      // Trim the LineString coordinates to show progressive track
      const coords = f.geometry.coordinates;
      if (coords && coords.length > 1 && animationProgress < 1.0) {
        // Calculate how many points to show (at least 1)
        const numPoints = Math.max(1, Math.ceil(animationProgress * coords.length));
        const trimmedCoords = coords.slice(0, numPoints);

        // Return feature with trimmed geometry
        return {
          type: 'Feature',
          geometry: {
            type: 'LineString',
            coordinates: trimmedCoords
          },
          properties: props
        };
      }
    }

    return {
      ...f,
      properties: props
    };
  }).filter(Boolean);
}

/**
 * Ease out quadratic - starts fast, slows down
 */
function easeOutQuad(t) {
  return t * (2 - t);
}

// Feature flag to enable/disable lifecycle filtering (for gradual rollout)
let useLifecycleFiltering = true;

// Cache for loaded overlay data (full unfiltered datasets)
const dataCache = {};

/**
 * Calculate total cache size - exact bytes via JSON serialization
 * @returns {Object} { totalFeatures, bytes, sizeMB, perOverlay }
 */
function calculateCacheSize() {
  let totalFeatures = 0;
  let totalBytes = 0;
  const perOverlay = {};

  for (const overlayId of Object.keys(dataCache)) {
    const features = dataCache[overlayId]?.features || [];
    if (features.length > 0) {
      // Get exact byte size via JSON serialization
      const bytes = new Blob([JSON.stringify(features)]).size;
      perOverlay[overlayId] = { features: features.length, bytes };
      totalFeatures += features.length;
      totalBytes += bytes;
    }
  }

  const sizeMB = (totalBytes / (1024 * 1024)).toFixed(2);

  return { totalFeatures, bytes: totalBytes, sizeMB, perOverlay };
}

// Track which time ranges have been loaded per overlay
// Each entry is an array of {start, end} (millisecond timestamps)
const loadedRanges = {};  // overlayId -> [{start, end}, ...]

// Legacy: loadedYears kept as derived view for compatibility with render logic
const loadedYears = {};  // overlayId -> Set of years (derived from loadedRanges)

// Track current displayed year per overlay
const displayedYear = {};

// Cache year ranges per overlay (for recalculating combined range when overlays change)
const yearRangeCache = {};

// Active filter overrides per overlay (for chat-based filter modifications)
// These override the defaults in OVERLAY_ENDPOINTS.params
const activeFilters = {};  // overlayId -> {minMagnitude, maxMagnitude, ...}

// Track filters that were used when data was LOADED (for Phase 7 cache awareness)
// This lets chat know if a request can be satisfied from cache without new API call
// Example: loaded with minMagnitude=5.0, display set to 6.0 - can filter to 5.5 from cache
const loadedFilters = {};  // overlayId -> {minMagnitude, minVei, ...}

/**
 * Build URL for fetching data within a time range.
 * @param {Object} endpoint - Endpoint config from OVERLAY_ENDPOINTS
 * @param {number} startMs - Start timestamp in milliseconds
 * @param {number} endMs - End timestamp in milliseconds
 * @param {string} overlayId - Overlay ID for looking up active filters
 * @returns {string} Full URL with start/end and other params
 */
function buildRangeUrl(endpoint, startMs, endMs, overlayId = null) {
  const url = new URL(endpoint.baseUrl, window.location.origin);

  // Start with default params from endpoint config
  const defaultParams = endpoint.params || {};

  // Get active filter overrides (from chat-based filter changes)
  const overrides = overlayId ? (activeFilters[overlayId] || {}) : {};

  // Build effective params, with overrides taking precedence
  const effectiveParams = { ...defaultParams };

  // Map user-friendly filter names to API param names
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

  // Location filters (for disaster queries like "earthquakes in California")
  if (overrides.locPrefix !== undefined) {
    effectiveParams.loc_prefix = overrides.locPrefix;
  }
  if (overrides.affectedLocId !== undefined) {
    effectiveParams.affected_loc_id = overrides.affectedLocId;
  }

  // Add all params to URL
  for (const [key, value] of Object.entries(effectiveParams)) {
    url.searchParams.set(key, value);
  }

  // Add time range filter
  url.searchParams.set('start', String(startMs));
  url.searchParams.set('end', String(endMs));

  return url.toString();
}

/**
 * Load weather grid data for a specific year.
 * Weather data uses a different format than GeoJSON overlays.
 * @param {string} overlayId - Overlay ID (temperature, humidity, snow-depth)
 * @param {number} year - Year to load
 * @param {Object} endpoint - Endpoint config from OVERLAY_ENDPOINTS
 * @param {AbortSignal} signal - Optional abort signal
 * @returns {Promise<boolean>} True if data was loaded
 */
// All climate variables to fetch together (optimization: one API call for all)
const CLIMATE_VARIABLES = [
  'temp_c', 'humidity', 'snow_depth_m',
  'precipitation_mm', 'cloud_cover_pct', 'pressure_hpa',
  'solar_radiation', 'soil_temp_c', 'soil_moisture'
];
const CLIMATE_OVERLAY_MAP = {
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
const VARIABLE_OVERLAY_MAP = {
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

async function loadWeatherYearData(overlayId, year, endpoint, signal = null) {
  // Check if we already have this year's data cached (from a previous batch request)
  if (dataCache[overlayId]?.years?.[year]) {
    console.log(`OverlayController: Using cached data for ${overlayId} year ${year}`);
    return true;
  }

  // Smart batching: check which variables are missing from cache for this year
  const missingVars = CLIMATE_VARIABLES.filter(varName => {
    const varOverlayId = VARIABLE_OVERLAY_MAP[varName];
    return !dataCache[varOverlayId]?.years?.[year];
  });

  // If nothing is missing (shouldn't happen but safety check), we're done
  if (missingVars.length === 0) {
    console.log(`OverlayController: All climate variables already cached for year ${year}`);
    return true;
  }

  // Build URL for weather API - only fetch missing variables
  const url = new URL(endpoint.baseUrl, window.location.origin);
  url.searchParams.set('tier', endpoint.params.tier || 'monthly');
  url.searchParams.set('variables', missingVars.join(','));  // Only missing vars
  url.searchParams.set('year', year);

  console.log(`OverlayController: Fetching ${missingVars.length} climate variable(s) for year ${year}: ${missingVars.join(', ')}`);

  try {
    const fetchOptions = signal ? { signal } : {};
    const data = await fetchMsgpack(url.toString(), fetchOptions);

    if (data.error) {
      console.error(`OverlayController: Weather API error:`, data.error);
      loadedYears[overlayId]?.delete(year);
      return false;
    }

    // Log if tier cascade occurred (e.g., monthly -> hourly for recent data)
    if (data.tier && data.requested_tier && data.tier !== data.requested_tier) {
      console.log(`OverlayController: Tier cascade for ${year}: ${data.requested_tier} -> ${data.tier}`);
    }

    // Multi-variable response: cache all variables at once
    if (data.variables && data.color_scales) {
      for (const variable of data.variables) {
        const varOverlayId = VARIABLE_OVERLAY_MAP[variable];
        if (!varOverlayId) continue;

        // Initialize cache structure
        if (!dataCache[varOverlayId]) {
          dataCache[varOverlayId] = { years: {}, colorScale: null, grid: null };
        }

        // Store the year's data for this variable
        dataCache[varOverlayId].years[year] = {
          timestamps: data.timestamps,
          values: data.values[variable],
          tier: data.tier
        };

        // Store color scale and grid
        if (data.color_scales[variable]) {
          dataCache[varOverlayId].colorScale = data.color_scales[variable];
        }
        if (data.grid) {
          dataCache[varOverlayId].grid = data.grid;
        }

        // Update year range cache
        if (!yearRangeCache[varOverlayId]) {
          yearRangeCache[varOverlayId] = { min: year, max: year, available: [] };
        }
        yearRangeCache[varOverlayId].min = Math.min(yearRangeCache[varOverlayId].min, year);
        yearRangeCache[varOverlayId].max = Math.max(yearRangeCache[varOverlayId].max, year);
        if (!yearRangeCache[varOverlayId].available.includes(year)) {
          yearRangeCache[varOverlayId].available.push(year);
          yearRangeCache[varOverlayId].available.sort((a, b) => a - b);
        }

        // Mark as loaded
        if (!loadedYears[varOverlayId]) loadedYears[varOverlayId] = new Set();
        loadedYears[varOverlayId].add(year);
      }

      const frameCount = data.timestamps?.length || 0;
      console.log(`OverlayController: Cached ${data.variables.length} climate variables for year ${year} (${frameCount} frames)`);

      // Dispatch cache update event for all variables
      for (const variable of data.variables) {
        const varOverlayId = VARIABLE_OVERLAY_MAP[variable];
        if (varOverlayId) {
          window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: { overlayId: varOverlayId, year } }));
        }
      }

      return true;
    }

    // Fallback: single variable response (backwards compatibility)
    if (!dataCache[overlayId]) {
      dataCache[overlayId] = { years: {}, colorScale: null, grid: null };
    }

    dataCache[overlayId].years[year] = {
      timestamps: data.timestamps,
      values: data.values,
      tier: data.tier
    };

    if (data.color_scale) {
      dataCache[overlayId].colorScale = data.color_scale;
    }
    if (data.grid) {
      dataCache[overlayId].grid = data.grid;
    }

    const frameCount = data.timestamps?.length || 0;
    console.log(`OverlayController: Cached weather ${overlayId} year ${year} (${frameCount} frames)`);

    if (!yearRangeCache[overlayId]) {
      yearRangeCache[overlayId] = { min: year, max: year, available: [] };
    }
    yearRangeCache[overlayId].min = Math.min(yearRangeCache[overlayId].min, year);
    yearRangeCache[overlayId].max = Math.max(yearRangeCache[overlayId].max, year);
    if (!yearRangeCache[overlayId].available.includes(year)) {
      yearRangeCache[overlayId].available.push(year);
      yearRangeCache[overlayId].available.sort((a, b) => a - b);
    }

    window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: { overlayId, year } }));

    return true;
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
 * @param {string} overlayId - Overlay ID
 * @param {number} startMs - Start timestamp in milliseconds
 * @param {number} endMs - End timestamp in milliseconds
 * @param {AbortSignal} signal - Optional abort signal
 * @returns {Promise<boolean>} True if new data was loaded
 */
async function loadRangeData(overlayId, startMs, endMs, signal = null) {
  const endpoint = OVERLAY_ENDPOINTS[overlayId];
  if (!endpoint) return false;

  // Weather grid overlays still use year-based loading
  if (endpoint.isWeatherGrid) {
    const year = new Date(endMs).getFullYear();
    return await loadWeatherYearData(overlayId, year, endpoint, signal);
  }

  // Initialize loadedRanges if needed
  if (!loadedRanges[overlayId]) {
    loadedRanges[overlayId] = [];
  }

  // Check if this range is already fully covered
  const isRangeCovered = loadedRanges[overlayId].some(
    r => r.start <= startMs && r.end >= endMs
  );
  if (isRangeCovered) {
    console.log(`OverlayController: ${overlayId} range already cached`);
    return false;
  }

  // Mark range as loading to prevent duplicate requests
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

    // Initialize cache if needed
    if (!dataCache[overlayId]) {
      dataCache[overlayId] = { type: 'FeatureCollection', features: [] };
    }

    // Merge new features (avoid duplicates by event_id if available)
    if (featureCount > 0) {
      const existingIds = new Set(
        dataCache[overlayId].features
          .map(f => f.properties?.event_id || f.properties?.storm_id || f.id)
          .filter(Boolean)
      );

      const newFeatures = geojson.features.filter(f => {
        const id = f.properties?.event_id || f.properties?.storm_id || f.id;
        return !id || !existingIds.has(id);
      });

      dataCache[overlayId].features.push(...newFeatures);
      console.log(`OverlayController: Added ${newFeatures.length} ${overlayId} features (total: ${dataCache[overlayId].features.length})`);

      // Log total cache size when new data received
      const cacheSize = calculateCacheSize();
      console.log(`OverlayController: Total cache: ${cacheSize.totalFeatures} features (${cacheSize.sizeMB} MB)`);

      // Dispatch event for UI to update cache status display
      window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: cacheSize }));
    } else {
      console.log(`OverlayController: No ${overlayId} events in range`);
    }

    // Mark range as loaded (remove loading flag)
    rangeEntry.loading = false;

    // Derive years from range for legacy loadedYears compatibility
    if (!loadedYears[overlayId]) {
      loadedYears[overlayId] = new Set();
    }
    const startYear = new Date(startMs).getFullYear();
    const endYear = new Date(endMs).getFullYear();
    for (let y = startYear; y <= endYear; y++) {
      loadedYears[overlayId].add(y);
    }
    console.log(`OverlayController: Marked ${overlayId} years ${startYear}-${endYear} as loaded. Total cached: ${dataCache[overlayId]?.features?.length || 0} features`);

    // Update year range cache for TimeSlider
    if (!yearRangeCache[overlayId]) {
      yearRangeCache[overlayId] = { min: startYear, max: endYear, available: [] };
    }
    yearRangeCache[overlayId].min = Math.min(yearRangeCache[overlayId].min, startYear);
    yearRangeCache[overlayId].max = Math.max(yearRangeCache[overlayId].max, endYear);

    // Track filters used at load time (Phase 7 cache awareness)
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
    // Remove failed range entry
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

export const OverlayController = {
  // Currently loading overlays (prevent duplicate requests)
  loading: new Set(),

  // AbortControllers for in-flight fetch requests (overlayId -> AbortController)
  abortControllers: new Map(),

  // Last known TimeSlider year (for change detection)
  lastTimeSliderYear: null,

  // Bound listener function (for cleanup if needed)
  _timeChangeListener: null,

  // Active aftershock sequence scale ID
  activeSequenceScaleId: null,

  /**
   * Initialize the overlay controller.
   * Registers as listener to OverlaySelector and TimeSlider.
   */
  init() {
    if (!OverlaySelector) {
      console.warn('OverlayController: OverlaySelector not available');
      return;
    }

    // Listen for overlay toggle events
    OverlaySelector.addListener((overlayId, isActive) => {
      this.handleOverlayChange(overlayId, isActive);
    });

    // Listen for TimeSlider changes (decoupled via listener pattern)
    if (TimeSlider) {
      this._timeChangeListener = (time, source) => {
        this.handleTimeChange(time, source);
      };
      TimeSlider.addChangeListener(this._timeChangeListener);
      console.log('OverlayController: Registered TimeSlider listener');
    }

    // Setup aftershock sequence listener
    this.setupSequenceListener();

    // Setup cross-event linking (volcano<->earthquake)
    this.setupCrossLinkListeners();

    // Setup track drill-down listener for hurricanes
    this.setupTrackDrillDownListener();

    // Listen for live mode events to refresh overlay data
    window.addEventListener('live-data-poll', () => {
      this.refreshLiveOverlays();
    });
    window.addEventListener('live-lock-engaged', () => {
      // Immediate refresh when entering live mode
      this.refreshLiveOverlays();
    });

    console.log('OverlayController initialized');
  },

  /**
   * Setup listener for hurricane track drill-down.
   */
  setupTrackDrillDownListener() {
    document.addEventListener('track-drill-down', async (e) => {
      const { stormId, stormName, eventType, props } = e.detail;
      console.log(`OverlayController: Track drill-down for ${stormName} (${stormId})`);
      await this.handleHurricaneDrillDown(stormId, stormName, props);
    });
    console.log('OverlayController: Registered track drill-down listener');
  },

  /**
   * Setup listener for aftershock sequence selection.
   * When user clicks "View sequence" on an earthquake, adds a 6h granularity tab.
   */
  setupSequenceListener() {
    const model = ModelRegistry?.getModel('point-radius');
    if (model?.onSequenceChange) {
      model.onSequenceChange((sequenceId, eventId) => {
        this.handleSequenceChange(sequenceId, eventId);
      });
      console.log('OverlayController: Registered sequence change listener');
    }
  },

  /**
   * Setup listeners for cross-event linking (volcano<->earthquake).
   */
  setupCrossLinkListeners() {
    const model = ModelRegistry?.getModel('point-radius');
    if (!model) return;

    // Volcano -> Earthquakes: when user searches from a volcano popup
    if (model.onVolcanoEarthquakes) {
      model.onVolcanoEarthquakes((data) => {
        this.handleVolcanoEarthquakes(data);
      });
      console.log('OverlayController: Registered volcano->earthquake cross-link listener');
    }

    // Earthquake -> Volcanoes: when user searches from an earthquake popup
    if (model.onNearbyVolcanoes) {
      model.onNearbyVolcanoes((data) => {
        this.handleNearbyVolcanoes(data);
      });
      console.log('OverlayController: Registered earthquake->volcano cross-link listener');
    }

    // Tsunami -> Runups: when user clicks "View runups" on a tsunami
    if (model.onTsunamiRunups) {
      model.onTsunamiRunups((data) => {
        this.handleTsunamiRunups(data);
      });
      console.log('OverlayController: Registered tsunami runups animation listener');
    }

    // Wildfire -> Animation: when user clicks "View fire progression"
    if (model.onFireAnimation) {
      model.onFireAnimation((data) => {
        this.handleFireAnimation(data);
      });
      console.log('OverlayController: Registered fire animation listener');
    }

    // Wildfire -> Progression: when daily progression data is available
    if (model.onFireProgression) {
      model.onFireProgression((data) => {
        this.handleFireProgression(data);
      });
      console.log('OverlayController: Registered fire progression listener');
    }

    // Tornado -> Sequence: when user clicks a tornado that's part of a sequence
    if (model.onTornadoSequence) {
      model.onTornadoSequence((data) => {
        this.handleTornadoSequence(data);
      });
      console.log('OverlayController: Registered tornado sequence listener');
    }

    // Tornado -> Point Animation: for tornadoes without track data
    if (model.onTornadoPointAnimation) {
      model.onTornadoPointAnimation((data) => {
        this.handleTornadoPointAnimation(data);
      });
      console.log('OverlayController: Registered tornado point animation listener');
    }

    // Flood -> Animation: when user clicks "View flood" on a flood event
    if (model.onFloodAnimation) {
      model.onFloodAnimation((data) => {
        this.handleFloodAnimation(data);
      });
      console.log('OverlayController: Registered flood animation listener');
    }

    // Volcano -> Impact: when user clicks "Impact" on a volcano event
    if (model.onVolcanoImpact) {
      model.onVolcanoImpact((data) => {
        this.handleVolcanoImpact(data);
      });
      console.log('OverlayController: Registered volcano impact animation listener');
    }

    // Wildfire -> Impact: fallback when no progression data (area circle)
    if (model.onWildfireImpact) {
      model.onWildfireImpact((data) => {
        this.handleWildfireImpact(data);
      });
      console.log('OverlayController: Registered wildfire impact animation listener');
    }

    // Wildfire -> Perimeter: single shape fade-in (second preference)
    if (model.onWildfirePerimeter) {
      model.onWildfirePerimeter((data) => {
        this.handleWildfirePerimeter(data);
      });
      console.log('OverlayController: Registered wildfire perimeter animation listener');
    }

    // Flood -> Impact: fallback when no geometry data (area circle)
    if (model.onFloodImpact) {
      model.onFloodImpact((data) => {
        this.handleFloodImpact(data);
      });
      console.log('OverlayController: Registered flood impact animation listener');
    }
  },

  /**
   * Handle earthquakes found near a volcano.
   * Uses the same animation system as aftershock sequences.
   */
  handleVolcanoEarthquakes(data) {
    const { features, volcanoName, volcanoLat, volcanoLon } = data;
    console.log(`OverlayController: Displaying ${features.length} earthquakes triggered by ${volcanoName}`);

    if (features.length === 0) return;

    // Convert API features to GeoJSON format
    const seqEvents = features.map(f => ({
      type: 'Feature',
      geometry: f.geometry,
      properties: f.properties
    }));

    // Find the largest earthquake to use as "mainshock" for animation centering
    let mainshock = seqEvents[0];
    for (const event of seqEvents) {
      if ((event.properties.magnitude || 0) > (mainshock.properties.magnitude || 0)) {
        mainshock = event;
      }
    }

    // Handle case where all events have same timestamp or no valid times
    let minTime = Infinity, maxTime = -Infinity;
    for (const event of seqEvents) {
      const t = new Date(event.properties.timestamp || event.properties.time).getTime();
      if (!isNaN(t)) {
        if (t < minTime) minTime = t;
        if (t > maxTime) maxTime = t;
      }
    }

    if (minTime === Infinity || maxTime === -Infinity || minTime === maxTime) {
      // Just display statically without animation
      const geojson = { type: 'FeatureCollection', features: seqEvents };
      const model = ModelRegistry?.getModel('point-radius');
      if (model) {
        model.update(geojson);
        const maplibre = window.maplibregl || maplibregl;
        const bounds = new maplibre.LngLatBounds();
        bounds.extend([volcanoLon, volcanoLat]);
        for (const f of seqEvents) {
          if (f.geometry?.coordinates) bounds.extend(f.geometry.coordinates);
        }
        if (!bounds.isEmpty()) {
          MapAdapter.map.fitBounds(bounds, { padding: 50, maxZoom: 10 });
        }
      }
      console.log(`OverlayController: Showing ${seqEvents.length} earthquakes statically (no time range)`);
      return;
    }

    // Stop any active animation
    if (EventAnimator.getIsActive()) {
      EventAnimator.stop();
    }

    // Clear normal earthquake display
    const model = ModelRegistry?.getModelForType('earthquake');
    if (model?.clear) {
      model.clear();
    }

    // Create mainshock at volcano location
    const volcanoMainshock = {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [volcanoLon, volcanoLat] },
      properties: {
        ...mainshock.properties,
        is_volcano_origin: true,
        volcano_name: volcanoName
      }
    };

    // Determine granularity based on time range
    const timeRange = maxTime - minTime;
    const stepHours = Math.max(1, Math.ceil((timeRange / (60 * 60 * 1000)) / 200));
    let granularityLabel = '6h';
    if (stepHours < 2) granularityLabel = '1h';
    else if (stepHours < 4) granularityLabel = '2h';
    else if (stepHours < 8) granularityLabel = '6h';
    else if (stepHours < 16) granularityLabel = '12h';
    else if (stepHours < 36) granularityLabel = 'daily';
    else granularityLabel = '2d';

    // Start unified EventAnimator with earthquake mode
    EventAnimator.start({
      id: `volcano-${volcanoName.replace(/\s+/g, '-').substring(0, 12)}`,
      label: `${volcanoName} quakes`,
      mode: AnimationMode.EARTHQUAKE,
      events: seqEvents,
      mainshock: volcanoMainshock,
      eventType: 'earthquake',
      timeField: 'timestamp',
      granularity: granularityLabel,
      renderer: 'point-radius',
      onExit: () => {
        console.log('OverlayController: Volcano earthquake sequence exited');
        // Restore TimeSlider range from cached overlay year ranges
        this.recalculateTimeRange();
        if (TimeSlider) {
          if (TimeSlider.scales?.find(s => s.id === 'primary')) {
            TimeSlider.setActiveScale('primary');
          }
          if (Object.keys(yearRangeCache).length > 0) {
            TimeSlider.show();
          }
        }
        // Restore normal earthquake display for current year
        const currentYear = this.getCurrentYear();
        if (dataCache.earthquakes) {
          this.renderFilteredData('earthquakes', currentYear);
        }
      }
    });

    console.log(`OverlayController: Started volcano earthquake animation with ${seqEvents.length} events`);
  },

  /**
   * Handle volcanoes found near an earthquake.
   * Shows volcano markers temporarily on the map.
   */
  handleNearbyVolcanoes(data) {
    const { features, earthquakeLat, earthquakeLon } = data;
    console.log(`OverlayController: Displaying ${features.length} nearby volcanoes`);

    if (features.length === 0) {
      console.log('OverlayController: No volcanoes to display');
      return;
    }

    // Log found volcanoes - the popup already displays names
    const names = features.map(f => f.properties.volcano_name).join(', ');
    console.log(`OverlayController: Found volcanoes: ${names}`);
    console.log(`OverlayController: Earthquake at [${earthquakeLon}, ${earthquakeLat}]`);

    // Fit map to show the earthquake and nearby volcanoes
    // Use window.maplibregl for ES module compatibility
    const maplibre = window.maplibregl || maplibregl;
    const bounds = new maplibre.LngLatBounds();
    bounds.extend([earthquakeLon, earthquakeLat]);

    for (const f of features) {
      const coords = f.geometry?.coordinates;
      if (coords && coords.length >= 2) {
        console.log(`OverlayController: Adding volcano at [${coords[0]}, ${coords[1]}]`);
        bounds.extend(coords);
      }
    }

    if (!bounds.isEmpty()) {
      console.log(`OverlayController: Fitting bounds`, bounds.toArray());
      MapAdapter.map.fitBounds(bounds, { padding: 80, maxZoom: 8, duration: 1500 });
    } else {
      console.warn('OverlayController: Bounds are empty, cannot zoom');
    }
  },

  /**
   * Handle tsunami runups animation.
   * Uses EventAnimator with RADIAL mode to show wave propagation.
   * Similar to earthquake sequences: zoom to center, start animation,
   * slowly zoom out with expanding wave radius, reveal runups progressively.
   * @param {Object} data - { geojson, eventId, runupCount }
   */
  handleTsunamiRunups(data) {
    const { geojson, eventId, runupCount } = data;
    console.log(`OverlayController: Starting tsunami runups animation for ${eventId} with ${runupCount} runups`);

    if (!geojson || !geojson.features || geojson.features.length < 2) {
      console.warn('OverlayController: Not enough data for tsunami animation');
      return;
    }

    // Find source event (is_source: true)
    const sourceEvent = geojson.features.find(f => f.properties?.is_source === true);
    if (!sourceEvent) {
      console.warn('OverlayController: No source event found in tsunami data');
      return;
    }

    // Get source coordinates for centering
    const sourceCoords = sourceEvent.geometry?.coordinates;
    if (!sourceCoords) {
      console.warn('OverlayController: Source event has no coordinates');
      return;
    }

    // Hide any popups
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Zoom to source location first (like earthquake sequences)
    MapAdapter.map.flyTo({
      center: sourceCoords,
      zoom: 7,
      duration: 1500
    });

    // Start radial animation using EventAnimator
    const animationId = `tsunami-${eventId}`;
    const sourceYear = sourceEvent.properties?.year || new Date().getFullYear();
    const sourceDate = sourceEvent.properties?.timestamp
      ? new Date(sourceEvent.properties.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
      : sourceYear;

    const started = EventAnimator.start({
      id: animationId,
      label: `Tsunami ${sourceDate}`,
      mode: AnimationMode.RADIAL,
      events: geojson.features,
      eventType: 'tsunami',
      timeField: 'timestamp',
      granularity: '12m',  // 12-minute steps for smooth wave animation (5 steps per hour)
      renderer: 'point-radius',
      // Don't auto-center - we already did flyTo above
      rendererOptions: {
        eventType: 'tsunami'  // Tell renderer to use tsunami styling
      },
      onExit: () => {
        console.log('OverlayController: Tsunami animation exited');
        // Restore original tsunami overlay
        const currentYear = this.getCurrentYear();
        if (dataCache.tsunamis) {
          this.renderFilteredData('tsunamis', currentYear);
        }
        // Recalculate time range for TimeSlider
        this.recalculateTimeRange();
        if (TimeSlider && Object.keys(yearRangeCache).length > 0) {
          TimeSlider.show();
        }
      }
    });

    if (started) {
      console.log(`OverlayController: Tsunami animation started with ${geojson.features.length} features`);
    } else {
      console.error('OverlayController: Failed to start tsunami animation');
      // Try to restore the overlay
      const currentYear = this.getCurrentYear();
      if (dataCache.tsunamis) {
        this.renderFilteredData('tsunamis', currentYear);
      }
    }
  },

  /**
   * Handle tornado sequence animation.
   * Uses EventAnimator with TORNADO_SEQUENCE mode for progressive track drawing.
   * @param {Object} data - { geojson, seedEventId, sequenceCount }
   */
  handleTornadoSequence(data) {
    const { geojson, seedEventId, sequenceCount } = data;
    console.log(`OverlayController: Starting tornado sequence animation for ${seedEventId} with ${sequenceCount} tornadoes`);

    if (!geojson || !geojson.features || geojson.features.length === 0) {
      console.warn('OverlayController: No data for tornado sequence animation');
      return;
    }

    // For single tornadoes, only proceed if it has track geometry
    // (otherwise route to point animation)
    if (geojson.features.length === 1) {
      const feature = geojson.features[0];
      if (!feature.properties?.track) {
        console.log('OverlayController: Single tornado without track - routing to point animation');
        // Extract data and trigger point animation
        const props = feature.properties || {};
        this.handleTornadoPointAnimation({
          eventId: props.event_id || seedEventId,
          latitude: props.latitude,
          longitude: props.longitude,
          scale: props.tornado_scale || 'EF0',
          timestamp: props.timestamp || null
        });
        return;
      }
    }

    // Hide any popups
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Find the seed tornado to get initial center
    const seedTornado = geojson.features.find(f =>
      String(f.properties?.event_id) === String(seedEventId)
    ) || geojson.features[0];

    const centerLon = seedTornado.properties?.longitude;
    const centerLat = seedTornado.properties?.latitude;

    // Get time range for animation label
    let minTime = Infinity, maxTime = -Infinity;
    for (const f of geojson.features) {
      const t = new Date(f.properties?.timestamp).getTime();
      if (!isNaN(t)) {
        if (t < minTime) minTime = t;
        if (t > maxTime) maxTime = t;
      }
    }

    // Format label - different for single vs sequence
    const startDate = new Date(minTime);
    const isSingle = geojson.features.length === 1;
    const label = isSingle
      ? `Tornado ${startDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`
      : `Tornado Sequence ${startDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;

    // Start tornado sequence animation using EventAnimator
    const animationId = `tornado-seq-${seedEventId}`;

    const started = EventAnimator.start({
      id: animationId,
      label: label,
      mode: AnimationMode.TORNADO_SEQUENCE,
      events: geojson.features,
      eventType: 'tornado',
      timeField: 'timestamp',
      granularity: '1h',
      renderer: 'point-radius',
      center: centerLat && centerLon ? { lat: centerLat, lon: centerLon } : null,
      zoom: 8,
      rendererOptions: {
        eventType: 'tornado'
      },
      onExit: () => {
        console.log('OverlayController: Tornado sequence animation exited');
        // Restore original tornado overlay
        const currentYear = this.getCurrentYear();
        if (dataCache.tornadoes) {
          this.renderFilteredData('tornadoes', currentYear);
        }
        // Recalculate time range for TimeSlider
        this.recalculateTimeRange();
        if (TimeSlider && Object.keys(yearRangeCache).length > 0) {
          TimeSlider.show();
        }
      }
    });

    if (started) {
      console.log(`OverlayController: Tornado sequence animation started with ${geojson.features.length} tornadoes`);
    } else {
      console.error('OverlayController: Failed to start tornado sequence animation');
      const currentYear = this.getCurrentYear();
      if (dataCache.tornadoes) {
        this.renderFilteredData('tornadoes', currentYear);
      }
    }
  },

  /**
   * Handle point-only tornado animation.
   * For tornadoes without track data - zooms in, shows circle based on EF scale,
   * with TimeSlider-driven animation showing the tornado's duration.
   * @param {Object} data - { eventId, latitude, longitude, scale, timestamp }
   */
  handleTornadoPointAnimation(data) {
    const { eventId, scale, timestamp } = data;
    // Parse coordinates as floats to ensure valid numbers
    const latitude = parseFloat(data.latitude);
    const longitude = parseFloat(data.longitude);
    console.log(`OverlayController: Starting point-only tornado animation for ${eventId} at [${longitude}, ${latitude}]`);

    if (isNaN(latitude) || isNaN(longitude)) {
      console.warn('OverlayController: Invalid coordinates for tornado point animation:', data);
      return;
    }

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Note: We keep the tornado overlay visible so the source point stays on screen
    // The animation circle will appear on top of the existing point

    // Get color and size based on scale
    const scaleColors = {
      'EF0': '#98fb98', 'F0': '#98fb98',
      'EF1': '#32cd32', 'F1': '#32cd32',
      'EF2': '#ffd700', 'F2': '#ffd700',
      'EF3': '#ff8c00', 'F3': '#ff8c00',
      'EF4': '#ff4500', 'F4': '#ff4500',
      'EF5': '#8b0000', 'F5': '#8b0000'
    };
    const scaleRadii = {
      'EF0': 500,   // meters
      'EF1': 800,
      'EF2': 1200,
      'EF3': 1800,
      'EF4': 2500,
      'EF5': 3500
    };

    // Estimated duration in minutes based on EF scale
    // Stronger tornadoes tend to last longer
    const scaleDurations = {
      'EF0': 3, 'F0': 3,     // ~3 minutes (weak, short-lived)
      'EF1': 5, 'F1': 5,     // ~5 minutes
      'EF2': 10, 'F2': 10,   // ~10 minutes
      'EF3': 15, 'F3': 15,   // ~15 minutes
      'EF4': 20, 'F4': 20,   // ~20 minutes
      'EF5': 30, 'F5': 30    // ~30 minutes (violent, long-lived)
    };

    const color = scaleColors[scale] || '#32cd32';
    const radius = scaleRadii[scale] || scaleRadii['EF0'] || 500;
    const durationMinutes = scaleDurations[scale] || 5;
    const layerId = 'tornado-point-animation';
    const sourceId = 'tornado-point-animation-source';

    // Calculate time range
    // Use timestamp if available, otherwise use a default time
    let startMs;
    if (timestamp) {
      startMs = new Date(timestamp).getTime();
    } else {
      // Fallback: use noon on Jan 1 of some year (arbitrary but valid)
      startMs = new Date('2020-01-01T12:00:00Z').getTime();
    }
    const endMs = startMs + (durationMinutes * 60 * 1000);

    // Create GeoJSON for the point
    const geojson = {
      type: 'FeatureCollection',
      features: [{
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [longitude, latitude] },
        properties: { scale: scale, radius: radius }
      }]
    };

    // Zoom to location first
    const zoomLevel = 11;
    MapAdapter.flyTo([longitude, latitude], zoomLevel);

    // Wait for flyTo to complete, then setup TimeSlider and layers
    setTimeout(() => {
      const map = MapAdapter.map;
      if (!map) return;

      // Clean up any previous animation layers
      if (map.getLayer(layerId)) map.removeLayer(layerId);
      if (map.getLayer(layerId + '-outline')) map.removeLayer(layerId + '-outline');
      if (map.getSource(sourceId)) map.removeSource(sourceId);

      // Add source
      map.addSource(sourceId, {
        type: 'geojson',
        data: geojson
      });

      // Convert meters to pixels using proper geographic scaling
      // Note: ['zoom'] can only be used at top-level interpolate/step, so we use
      // interpolate with pre-calculated meters/pixel values at zoom stops:
      // Zoom 8: 611.5 m/px, Zoom 11: 76.44 m/px, Zoom 14: 9.55 m/px
      const metersToPixels = [
        'interpolate', ['exponential', 2], ['zoom'],
        8, ['/', ['get', 'radius'], 611.5],
        11, ['/', ['get', 'radius'], 76.44],
        14, ['/', ['get', 'radius'], 9.55]
      ];

      // Add fill circle layer (starts transparent)
      map.addLayer({
        id: layerId,
        type: 'circle',
        source: sourceId,
        paint: {
          'circle-radius': metersToPixels,
          'circle-color': color,
          'circle-opacity': 0
        }
      });

      // Add outline layer
      map.addLayer({
        id: layerId + '-outline',
        type: 'circle',
        source: sourceId,
        paint: {
          'circle-radius': metersToPixels,
          'circle-color': 'transparent',
          'circle-stroke-color': color,
          'circle-stroke-width': 3,
          'circle-stroke-opacity': 1
        }
      });

      // Setup TimeSlider for tornado animation
      const scaleId = `tornado-point-${eventId.substring(0, 12)}`;
      const tornadoDate = timestamp
        ? new Date(timestamp).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })
        : `${scale} Tornado`;

      // Generate timestamps for each second (tornadoes are short events)
      const timestamps = [];
      const stepMs = 1000; // 1 second steps
      for (let t = startMs; t <= endMs; t += stepMs) {
        timestamps.push(t);
      }

      if (TimeSlider) {
        const added = TimeSlider.addScale({
          id: scaleId,
          label: `Tornado ${tornadoDate}`,
          granularity: 'seconds',
          useTimestamps: true,
          currentTime: startMs,
          timeRange: {
            min: startMs,
            max: endMs,
            available: timestamps
          },
          mapRenderer: 'tornado-point-animation'
        });

        if (added) {
          this.activeTornadoPointScaleId = scaleId;
          TimeSlider.setActiveScale(scaleId);

          // Enter event animation mode with auto-calculated speed
          if (TimeSlider.enterEventAnimation) {
            TimeSlider.enterEventAnimation(startMs, endMs);
          }
        }
      }

      // Store animation state
      this._tornadoPointAnimState = {
        sourceId,
        layerId,
        startMs,
        endMs,
        scaleId,
        color
      };

      // Listen for time changes to update opacity
      this._tornadoPointTimeHandler = (time, source) => {
        if (!this._tornadoPointAnimState) return;

        const { startMs, endMs, layerId } = this._tornadoPointAnimState;
        const progress = Math.max(0, Math.min(1, (time - startMs) / (endMs - startMs)));

        // Update fill opacity based on progress (0 -> 0.7 over duration)
        if (map.getLayer(layerId)) {
          map.setPaintProperty(layerId, 'circle-opacity', progress * 0.7);
        }
      };
      TimeSlider?.addChangeListener(this._tornadoPointTimeHandler);

      // Add exit button
      this._addTornadoPointExitButton(() => this._exitTornadoPointAnimation());

      console.log(`OverlayController: Tornado point animation ready, ${durationMinutes} minutes`);
    }, 1600); // Wait for flyTo to complete
  },

  /**
   * Exit tornado point animation and cleanup.
   * @private
   */
  _exitTornadoPointAnimation() {
    console.log('OverlayController: Exiting tornado point animation');

    const map = MapAdapter.map;

    // Remove layers
    if (this._tornadoPointAnimState) {
      const { sourceId, layerId, scaleId } = this._tornadoPointAnimState;

      if (map.getLayer(layerId)) map.removeLayer(layerId);
      if (map.getLayer(layerId + '-outline')) map.removeLayer(layerId + '-outline');
      if (map.getSource(sourceId)) map.removeSource(sourceId);

      // Remove TimeSlider scale
      if (TimeSlider && scaleId) {
        TimeSlider.removeScale(scaleId);
        if (TimeSlider.exitEventAnimation) {
          TimeSlider.exitEventAnimation();
        }
      }

      this._tornadoPointAnimState = null;
    }

    // Remove time listener
    if (this._tornadoPointTimeHandler && TimeSlider) {
      TimeSlider.removeChangeListener(this._tornadoPointTimeHandler);
      this._tornadoPointTimeHandler = null;
    }

    // Remove exit button
    const exitBtn = document.getElementById('tornado-point-exit-btn');
    if (exitBtn) exitBtn.remove();

    // Recalculate time range
    this.recalculateTimeRange();
  },

  /**
   * Add exit button for tornado point animation.
   * @private
   */
  _addTornadoPointExitButton(onExit) {
    // Remove existing
    const existing = document.getElementById('tornado-point-exit-btn');
    if (existing) existing.remove();

    const btn = document.createElement('button');
    btn.id = 'tornado-point-exit-btn';
    btn.textContent = 'Exit Tornado View';
    btn.style.cssText = `
      position: fixed;
      top: 80px;
      left: 50%;
      transform: translateX(-50%);
      padding: 10px 20px;
      background: #32cd32;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      z-index: 1000;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;
    btn.addEventListener('click', onExit);
    btn.addEventListener('mouseenter', () => { btn.style.background = '#228b22'; });
    btn.addEventListener('mouseleave', () => { btn.style.background = '#32cd32'; });

    document.body.appendChild(btn);
  },

  /**
   * Handle flood animation - shows flood polygon with opacity fade over duration.
   * At flood start time, outline appears. Over the duration, opacity increases.
   * @param {Object} data - { geometry, eventId, durationDays, startTime, endTime, latitude, longitude, eventName }
   */
  handleFloodAnimation(data) {
    const { geometry, eventId, durationDays, startTime, endTime, latitude, longitude, eventName } = data;
    console.log(`OverlayController: Starting flood animation for ${eventId} (${durationDays} days)`);

    // Handle both Feature and FeatureCollection formats
    let geojsonData = geometry;
    if (!geometry) {
      console.warn('OverlayController: No geometry data for flood animation');
      return;
    }

    // If it's a FeatureCollection, use it directly; if it's a Feature, wrap it
    if (geometry.type === 'FeatureCollection') {
      geojsonData = geometry;
    } else if (geometry.type === 'Feature') {
      geojsonData = geometry;
    } else if (geometry.geometry) {
      // Already a Feature with geometry property
      geojsonData = geometry;
    } else {
      console.warn('OverlayController: Invalid geometry format for flood animation');
      return;
    }

    // Calculate time range
    const startMs = new Date(startTime).getTime();
    const endMs = new Date(endTime).getTime();
    const durationMs = endMs - startMs;

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Hide the flood overlay to focus on this flood
    this._hideFloodOverlay();

    // Calculate bounds from geometry for proper zoom that shows the whole area
    let bounds = null;

    // Helper to collect all coordinates from a geometry
    const collectCoords = (geom) => {
      const coords = [];
      if (!geom || !geom.coordinates) return coords;
      if (geom.type === 'Polygon') {
        coords.push(...geom.coordinates[0]);
      } else if (geom.type === 'MultiPolygon') {
        for (const poly of geom.coordinates) {
          coords.push(...poly[0]);
        }
      }
      return coords;
    };

    // Collect all coordinates from geometry
    let allCoords = [];
    if (geojsonData.type === 'FeatureCollection' && geojsonData.features) {
      for (const feature of geojsonData.features) {
        allCoords.push(...collectCoords(feature.geometry));
      }
    } else if (geojsonData.geometry) {
      allCoords = collectCoords(geojsonData.geometry);
    }

    // Calculate bounds from coordinates
    if (allCoords.length > 0) {
      bounds = this._getBoundsFromCoords(allCoords);
    }

    // Zoom to flood - use fitBounds if we have geometry, otherwise flyTo center
    if (bounds) {
      MapAdapter.map.fitBounds(bounds, {
        padding: 60,
        duration: 1500,
        maxZoom: 11
      });
    } else if (longitude && latitude) {
      MapAdapter.map.flyTo({
        center: [longitude, latitude],
        zoom: 8,
        duration: 1500
      });
    }

    // Create flood polygon layer
    const sourceId = 'flood-anim-polygon';
    const layerId = 'flood-anim-fill';
    const strokeId = 'flood-anim-stroke';

    // Remove existing layers
    if (MapAdapter.map.getLayer(layerId)) MapAdapter.map.removeLayer(layerId);
    if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
    if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);

    // Add flood source
    MapAdapter.map.addSource(sourceId, {
      type: 'geojson',
      data: geojsonData
    });

    // Add stroke layer (appears immediately at animation start)
    MapAdapter.map.addLayer({
      id: strokeId,
      type: 'line',
      source: sourceId,
      paint: {
        'line-color': '#0066cc',
        'line-width': 2,
        'line-opacity': 0.8
      }
    });

    // Add fill layer (starts transparent, fades in over duration)
    MapAdapter.map.addLayer({
      id: layerId,
      type: 'fill',
      source: sourceId,
      paint: {
        'fill-color': '#3399ff',
        'fill-opacity': 0
      }
    });

    // Setup TimeSlider for flood animation
    const scaleId = `flood-${eventId.substring(0, 12)}`;
    const floodDate = new Date(startTime).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

    // Generate timestamps for each day
    const timestamps = [];
    for (let t = startMs; t <= endMs; t += 24 * 60 * 60 * 1000) {
      timestamps.push(t);
    }

    if (TimeSlider) {
      const added = TimeSlider.addScale({
        id: scaleId,
        label: eventName ? `${eventName}` : `Flood ${floodDate}`,
        granularity: 'daily',
        useTimestamps: true,
        currentTime: startMs,
        timeRange: {
          min: startMs,
          max: endMs,
          available: timestamps
        },
        mapRenderer: 'flood-animation'
      });

      if (added) {
        this.activeFloodScaleId = scaleId;
        TimeSlider.setActiveScale(scaleId);

        // Enter event animation mode with auto-calculated speed
        if (TimeSlider.enterEventAnimation) {
          TimeSlider.enterEventAnimation(startMs, endMs);
        }
      }
    }

    // Store animation state
    this._floodAnimState = {
      sourceId,
      layerId,
      strokeId,
      startMs,
      endMs,
      scaleId
    };

    // Listen for time changes to update opacity
    this._floodTimeHandler = (time, source) => {
      if (!this._floodAnimState) return;

      const { startMs, endMs, layerId } = this._floodAnimState;
      const progress = Math.max(0, Math.min(1, (time - startMs) / (endMs - startMs)));

      // Update fill opacity based on progress (0 -> 0.6 over duration)
      if (MapAdapter.map.getLayer(layerId)) {
        MapAdapter.map.setPaintProperty(layerId, 'fill-opacity', progress * 0.6);
      }
    };
    TimeSlider?.addChangeListener(this._floodTimeHandler);

    // Add exit button
    this._addFloodExitButton(() => this._exitFloodAnimation());

    console.log(`OverlayController: Flood animation ready, ${durationDays} days starting ${floodDate}`);
  },

  /**
   * Handle volcano impact radius animation.
   * Shows felt and damage radii expanding from the volcano center.
   */
  handleVolcanoImpact(data) {
    const { eventId, volcanoName, latitude, longitude, feltRadius, damageRadius, VEI, timestamp } = data;
    console.log(`OverlayController: Starting volcano impact animation for ${volcanoName} (VEI ${VEI})`);

    if (!latitude || !longitude) {
      console.warn('OverlayController: No coordinates for volcano impact animation');
      return;
    }

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Zoom to volcano
    MapAdapter.map.flyTo({
      center: [longitude, latitude],
      zoom: 7,
      duration: 1500
    });

    // Create impact circle sources
    const feltSourceId = 'volcano-felt-radius';
    const damageSourceId = 'volcano-damage-radius';
    const feltLayerId = 'volcano-felt-fill';
    const damageLayerId = 'volcano-damage-fill';
    const feltStrokeId = 'volcano-felt-stroke';
    const damageStrokeId = 'volcano-damage-stroke';

    // Remove existing layers
    [feltLayerId, damageLayerId, feltStrokeId, damageStrokeId].forEach(id => {
      if (MapAdapter.map.getLayer(id)) MapAdapter.map.removeLayer(id);
    });
    [feltSourceId, damageSourceId].forEach(id => {
      if (MapAdapter.map.getSource(id)) MapAdapter.map.removeSource(id);
    });

    // Create circle GeoJSON (approximation using turf-style circle)
    const createCircle = (centerLon, centerLat, radiusKm, steps = 64) => {
      const coords = [];
      for (let i = 0; i <= steps; i++) {
        const angle = (i / steps) * 2 * Math.PI;
        // Approximate km to degrees (1 degree ~ 111km at equator)
        const latOffset = (radiusKm / 111) * Math.cos(angle);
        const lonOffset = (radiusKm / (111 * Math.cos(centerLat * Math.PI / 180))) * Math.sin(angle);
        coords.push([centerLon + lonOffset, centerLat + latOffset]);
      }
      return {
        type: 'Feature',
        properties: {},
        geometry: {
          type: 'Polygon',
          coordinates: [coords]
        }
      };
    };

    // Add felt radius source and layers (larger, yellow/orange)
    if (feltRadius > 0) {
      MapAdapter.map.addSource(feltSourceId, {
        type: 'geojson',
        data: createCircle(longitude, latitude, feltRadius)
      });

      MapAdapter.map.addLayer({
        id: feltLayerId,
        type: 'fill',
        source: feltSourceId,
        paint: {
          'fill-color': '#ffc107',
          'fill-opacity': 0
        }
      });

      MapAdapter.map.addLayer({
        id: feltStrokeId,
        type: 'line',
        source: feltSourceId,
        paint: {
          'line-color': '#ff9800',
          'line-width': 2,
          'line-opacity': 0
        }
      });
    }

    // Add damage radius source and layers (smaller, red)
    if (damageRadius > 0) {
      MapAdapter.map.addSource(damageSourceId, {
        type: 'geojson',
        data: createCircle(longitude, latitude, damageRadius)
      });

      MapAdapter.map.addLayer({
        id: damageLayerId,
        type: 'fill',
        source: damageSourceId,
        paint: {
          'fill-color': '#f44336',
          'fill-opacity': 0
        }
      });

      MapAdapter.map.addLayer({
        id: damageStrokeId,
        type: 'line',
        source: damageSourceId,
        paint: {
          'line-color': '#d32f2f',
          'line-width': 3,
          'line-opacity': 0
        }
      });
    }

    // Animate the radii expanding (3 second animation)
    const animDuration = 3000;
    const startTime = performance.now();

    const animate = () => {
      const elapsed = performance.now() - startTime;
      const progress = Math.min(1, elapsed / animDuration);
      const easeProgress = 1 - Math.pow(1 - progress, 3); // Ease out cubic

      // Update felt radius opacity (fade in)
      if (feltRadius > 0 && MapAdapter.map.getLayer(feltLayerId)) {
        MapAdapter.map.setPaintProperty(feltLayerId, 'fill-opacity', easeProgress * 0.3);
        MapAdapter.map.setPaintProperty(feltStrokeId, 'line-opacity', easeProgress * 0.8);
      }

      // Update damage radius opacity (fade in slightly delayed)
      if (damageRadius > 0 && MapAdapter.map.getLayer(damageLayerId)) {
        const damageProgress = Math.max(0, (progress - 0.3) / 0.7); // Start at 30%
        const easeDamage = 1 - Math.pow(1 - damageProgress, 3);
        MapAdapter.map.setPaintProperty(damageLayerId, 'fill-opacity', easeDamage * 0.4);
        MapAdapter.map.setPaintProperty(damageStrokeId, 'line-opacity', easeDamage * 0.9);
      }

      if (progress < 1) {
        requestAnimationFrame(animate);
      }
    };

    // Start animation after flyTo completes
    setTimeout(animate, 1600);

    // Store state for cleanup
    this._volcanoImpactState = {
      feltSourceId,
      damageSourceId,
      feltLayerId,
      damageLayerId,
      feltStrokeId,
      damageStrokeId
    };

    // Add exit button
    this._addVolcanoExitButton(() => this._exitVolcanoImpact());

    console.log(`OverlayController: Volcano impact animation started (felt: ${feltRadius}km, damage: ${damageRadius}km)`);
  },

  /**
   * Exit volcano impact animation and cleanup.
   * @private
   */
  _exitVolcanoImpact() {
    console.log('OverlayController: Exiting volcano impact animation');

    if (this._volcanoImpactState) {
      const { feltSourceId, damageSourceId, feltLayerId, damageLayerId, feltStrokeId, damageStrokeId } = this._volcanoImpactState;

      // Remove layers
      [feltLayerId, damageLayerId, feltStrokeId, damageStrokeId].forEach(id => {
        if (MapAdapter.map.getLayer(id)) MapAdapter.map.removeLayer(id);
      });

      // Remove sources
      [feltSourceId, damageSourceId].forEach(id => {
        if (MapAdapter.map.getSource(id)) MapAdapter.map.removeSource(id);
      });

      this._volcanoImpactState = null;
    }

    // Remove exit button
    const exitBtn = document.getElementById('volcano-exit-btn');
    if (exitBtn) exitBtn.remove();
  },

  /**
   * Add exit button for volcano impact animation.
   * @private
   */
  _addVolcanoExitButton(onExit) {
    const existing = document.getElementById('volcano-exit-btn');
    if (existing) existing.remove();

    const btn = document.createElement('button');
    btn.id = 'volcano-exit-btn';
    btn.textContent = 'Exit Impact View';
    btn.style.cssText = `
      position: fixed;
      top: 80px;
      left: 50%;
      transform: translateX(-50%);
      padding: 10px 20px;
      background: #ff5722;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      z-index: 1000;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;

    btn.addEventListener('click', onExit);
    document.body.appendChild(btn);
  },

  /**
   * Handle wildfire impact animation (area circle fallback).
   * Shows a circle representing the burned area.
   */
  handleWildfireImpact(data) {
    const { eventId, fireName, latitude, longitude, areaKm2, radiusKm, timestamp } = data;
    console.log(`OverlayController: Starting wildfire impact animation for ${fireName} (${areaKm2} km2)`);

    if (!latitude || !longitude) {
      console.warn('OverlayController: No coordinates for wildfire impact animation');
      return;
    }

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Hide other wildfire events to focus on this one
    this._hideWildfireOverlay();

    // Zoom to fire
    MapAdapter.map.flyTo({
      center: [longitude, latitude],
      zoom: 9,
      duration: 1500
    });

    // Create area circle
    const sourceId = 'wildfire-impact-radius';
    const fillId = 'wildfire-impact-fill';
    const strokeId = 'wildfire-impact-stroke';

    // Remove existing
    if (MapAdapter.map.getLayer(fillId)) MapAdapter.map.removeLayer(fillId);
    if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
    if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);

    // Create circle GeoJSON
    const createCircle = (centerLon, centerLat, radiusKm, steps = 64) => {
      const coords = [];
      for (let i = 0; i <= steps; i++) {
        const angle = (i / steps) * 2 * Math.PI;
        const latOffset = (radiusKm / 111) * Math.cos(angle);
        const lonOffset = (radiusKm / (111 * Math.cos(centerLat * Math.PI / 180))) * Math.sin(angle);
        coords.push([centerLon + lonOffset, centerLat + latOffset]);
      }
      return {
        type: 'Feature',
        properties: {},
        geometry: { type: 'Polygon', coordinates: [coords] }
      };
    };

    MapAdapter.map.addSource(sourceId, {
      type: 'geojson',
      data: createCircle(longitude, latitude, radiusKm)
    });

    MapAdapter.map.addLayer({
      id: fillId,
      type: 'fill',
      source: sourceId,
      paint: { 'fill-color': '#ff5722', 'fill-opacity': 0 }
    });

    MapAdapter.map.addLayer({
      id: strokeId,
      type: 'line',
      source: sourceId,
      paint: { 'line-color': '#d84315', 'line-width': 3, 'line-opacity': 0 }
    });

    // Animate
    const animDuration = 2000;
    const startTime = performance.now();
    const animate = () => {
      const progress = Math.min(1, (performance.now() - startTime) / animDuration);
      const ease = 1 - Math.pow(1 - progress, 3);
      if (MapAdapter.map.getLayer(fillId)) {
        MapAdapter.map.setPaintProperty(fillId, 'fill-opacity', ease * 0.4);
        MapAdapter.map.setPaintProperty(strokeId, 'line-opacity', ease * 0.9);
      }
      if (progress < 1) requestAnimationFrame(animate);
    };
    setTimeout(animate, 1600);

    this._wildfireImpactState = { sourceId, fillId, strokeId };
    this._addGenericExitButton('wildfire-exit-btn', 'Exit Fire View', '#ff5722', () => this._exitWildfireImpact());
  },

  _exitWildfireImpact(skipRestore = false) {
    if (this._wildfireImpactState) {
      const { sourceId, fillId, strokeId } = this._wildfireImpactState;
      if (MapAdapter.map.getLayer(fillId)) MapAdapter.map.removeLayer(fillId);
      if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
      if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);
      this._wildfireImpactState = null;
    }
    document.getElementById('wildfire-exit-btn')?.remove();
    // Restore wildfire overlay points (unless overlay is being fully disabled)
    if (!skipRestore) this._restoreWildfireOverlay();
  },

  /**
   * Handle wildfire perimeter animation (single shape fade-in).
   * Shows the fire perimeter polygon fading in.
   */
  handleWildfirePerimeter(data) {
    const { eventId, fireName, geometry, latitude, longitude, areaKm2, timestamp } = data;
    console.log(`OverlayController: Starting wildfire perimeter animation for ${fireName}`);

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Calculate bounds from geometry for proper zoom
    let bounds = null;
    if (geometry && geometry.geometry) {
      const coords = geometry.geometry.coordinates;
      if (geometry.geometry.type === 'Polygon') {
        bounds = this._getBoundsFromCoords(coords[0]);
      } else if (geometry.geometry.type === 'MultiPolygon') {
        // Flatten all outer rings
        const allCoords = coords.flatMap(poly => poly[0]);
        bounds = this._getBoundsFromCoords(allCoords);
      }
    }

    // Zoom to fire perimeter
    if (bounds) {
      MapAdapter.map.fitBounds(bounds, {
        padding: 50,
        duration: 1500,
        maxZoom: 12
      });
    } else if (latitude && longitude) {
      MapAdapter.map.flyTo({
        center: [longitude, latitude],
        zoom: 9,
        duration: 1500
      });
    }

    // Layer IDs
    const sourceId = 'wildfire-perimeter';
    const fillId = 'wildfire-perimeter-fill';
    const strokeId = 'wildfire-perimeter-stroke';

    // Remove existing
    if (MapAdapter.map.getLayer(fillId)) MapAdapter.map.removeLayer(fillId);
    if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
    if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);

    // Add the perimeter geometry
    MapAdapter.map.addSource(sourceId, {
      type: 'geojson',
      data: geometry
    });

    MapAdapter.map.addLayer({
      id: fillId,
      type: 'fill',
      source: sourceId,
      paint: { 'fill-color': '#ff5722', 'fill-opacity': 0 }
    });

    MapAdapter.map.addLayer({
      id: strokeId,
      type: 'line',
      source: sourceId,
      paint: { 'line-color': '#d84315', 'line-width': 2, 'line-opacity': 0 }
    });

    // Animate fade-in
    const animDuration = 2500;
    const startTime = performance.now();
    const animate = () => {
      const progress = Math.min(1, (performance.now() - startTime) / animDuration);
      const ease = 1 - Math.pow(1 - progress, 3);
      if (MapAdapter.map.getLayer(fillId)) {
        MapAdapter.map.setPaintProperty(fillId, 'fill-opacity', ease * 0.5);
        MapAdapter.map.setPaintProperty(strokeId, 'line-opacity', ease * 0.9);
      }
      if (progress < 1) requestAnimationFrame(animate);
    };
    setTimeout(animate, 1600);

    this._wildfirePerimeterState = { sourceId, fillId, strokeId };
    this._addGenericExitButton('wildfire-perim-exit-btn', 'Exit Fire View', '#ff5722', () => this._exitWildfirePerimeter());
  },

  _exitWildfirePerimeter(skipRestore = false) {
    if (this._wildfirePerimeterState) {
      const { sourceId, fillId, strokeId } = this._wildfirePerimeterState;
      if (MapAdapter.map.getLayer(fillId)) MapAdapter.map.removeLayer(fillId);
      if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
      if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);
      this._wildfirePerimeterState = null;
    }
    document.getElementById('wildfire-perim-exit-btn')?.remove();
    // Restore wildfire overlay points (unless overlay is being fully disabled)
    if (!skipRestore) this._restoreWildfireOverlay();
  },

  /**
   * Get bounding box from coordinate array.
   * @param {Array} coords - Array of [lng, lat] coordinates
   * @returns {Array} [[minLng, minLat], [maxLng, maxLat]]
   */
  _getBoundsFromCoords(coords) {
    if (!coords || coords.length === 0) return null;
    let minLng = Infinity, maxLng = -Infinity;
    let minLat = Infinity, maxLat = -Infinity;
    for (const [lng, lat] of coords) {
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
    return [[minLng, minLat], [maxLng, maxLat]];
  },

  /**
   * Handle flood impact animation (area circle fallback).
   * Shows a circle representing the flooded area.
   */
  handleFloodImpact(data) {
    const { eventId, eventName, latitude, longitude, areaKm2, radiusKm, durationDays, timestamp } = data;
    console.log(`OverlayController: Starting flood impact animation for ${eventName} (${areaKm2} km2)`);

    if (!latitude || !longitude) {
      console.warn('OverlayController: No coordinates for flood impact animation');
      return;
    }

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Hide other flood events to focus on this one
    this._hideFloodOverlay();

    // Zoom to flood
    MapAdapter.map.flyTo({
      center: [longitude, latitude],
      zoom: 8,
      duration: 1500
    });

    // Create area circle
    const sourceId = 'flood-impact-radius';
    const fillId = 'flood-impact-fill';
    const strokeId = 'flood-impact-stroke';

    // Remove existing
    if (MapAdapter.map.getLayer(fillId)) MapAdapter.map.removeLayer(fillId);
    if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
    if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);

    // Create circle GeoJSON
    const createCircle = (centerLon, centerLat, radiusKm, steps = 64) => {
      const coords = [];
      for (let i = 0; i <= steps; i++) {
        const angle = (i / steps) * 2 * Math.PI;
        const latOffset = (radiusKm / 111) * Math.cos(angle);
        const lonOffset = (radiusKm / (111 * Math.cos(centerLat * Math.PI / 180))) * Math.sin(angle);
        coords.push([centerLon + lonOffset, centerLat + latOffset]);
      }
      return {
        type: 'Feature',
        properties: {},
        geometry: { type: 'Polygon', coordinates: [coords] }
      };
    };

    MapAdapter.map.addSource(sourceId, {
      type: 'geojson',
      data: createCircle(longitude, latitude, radiusKm)
    });

    MapAdapter.map.addLayer({
      id: fillId,
      type: 'fill',
      source: sourceId,
      paint: { 'fill-color': '#2196f3', 'fill-opacity': 0 }
    });

    MapAdapter.map.addLayer({
      id: strokeId,
      type: 'line',
      source: sourceId,
      paint: { 'line-color': '#1565c0', 'line-width': 3, 'line-opacity': 0 }
    });

    // Animate
    const animDuration = 2000;
    const startTime = performance.now();
    const animate = () => {
      const progress = Math.min(1, (performance.now() - startTime) / animDuration);
      const ease = 1 - Math.pow(1 - progress, 3);
      if (MapAdapter.map.getLayer(fillId)) {
        MapAdapter.map.setPaintProperty(fillId, 'fill-opacity', ease * 0.4);
        MapAdapter.map.setPaintProperty(strokeId, 'line-opacity', ease * 0.9);
      }
      if (progress < 1) requestAnimationFrame(animate);
    };
    setTimeout(animate, 1600);

    this._floodImpactState = { sourceId, fillId, strokeId };
    this._addGenericExitButton('flood-impact-exit-btn', 'Exit Flood View', '#2196f3', () => this._exitFloodImpact());
  },

  _exitFloodImpact(skipRestore = false) {
    if (this._floodImpactState) {
      const { sourceId, fillId, strokeId } = this._floodImpactState;
      if (MapAdapter.map.getLayer(fillId)) MapAdapter.map.removeLayer(fillId);
      if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
      if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);
      this._floodImpactState = null;
    }
    document.getElementById('flood-impact-exit-btn')?.remove();
    // Restore flood overlay points (unless overlay is being fully disabled)
    if (!skipRestore) this._restoreFloodOverlay();
  },

  /**
   * Generic exit button helper.
   * @private
   */
  _addGenericExitButton(id, text, color, onExit) {
    document.getElementById(id)?.remove();
    const btn = document.createElement('button');
    btn.id = id;
    btn.textContent = text;
    btn.style.cssText = `
      position: fixed; top: 80px; left: 50%; transform: translateX(-50%);
      padding: 10px 20px; background: ${color}; color: white; border: none;
      border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500;
      z-index: 1000; box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;
    btn.addEventListener('click', onExit);
    document.body.appendChild(btn);
  },

  /**
   * Exit flood animation and cleanup.
   * @private
   */
  _exitFloodAnimation(skipRestore = false) {
    console.log('OverlayController: Exiting flood animation');

    // Remove layers
    if (this._floodAnimState) {
      const { sourceId, layerId, strokeId, scaleId } = this._floodAnimState;

      if (MapAdapter.map.getLayer(layerId)) MapAdapter.map.removeLayer(layerId);
      if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
      if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);

      // Remove TimeSlider scale
      if (TimeSlider && scaleId) {
        TimeSlider.removeScale(scaleId);
        if (TimeSlider.exitEventAnimation) {
          TimeSlider.exitEventAnimation();
        }
      }

      this._floodAnimState = null;
    }

    // Remove time listener
    if (this._floodTimeHandler && TimeSlider) {
      TimeSlider.removeChangeListener(this._floodTimeHandler);
      this._floodTimeHandler = null;
    }

    // Remove exit button
    const exitBtn = document.getElementById('flood-exit-btn');
    if (exitBtn) exitBtn.remove();

    // Restore flood overlay (unless overlay is being fully disabled)
    if (!skipRestore) this._restoreFloodOverlay();

    // Recalculate time range
    this.recalculateTimeRange();
  },

  /**
   * Add exit button for flood animation.
   * @private
   */
  _addFloodExitButton(onExit) {
    // Remove existing
    const existing = document.getElementById('flood-exit-btn');
    if (existing) existing.remove();

    const btn = document.createElement('button');
    btn.id = 'flood-exit-btn';
    btn.textContent = 'Exit Flood View';
    btn.style.cssText = `
      position: fixed;
      top: 80px;
      left: 50%;
      transform: translateX(-50%);
      padding: 10px 20px;
      background: #0066cc;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      z-index: 1000;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;

    btn.addEventListener('click', onExit);
    document.body.appendChild(btn);
  },

  /**
   * Hide flood overlay to focus on a single flood animation.
   * @private
   */
  _hideFloodOverlay() {
    // Clear point-radius model (overview points)
    const model = ModelRegistry?.getModelForType('flood');
    if (model?.clearType) {
      model.clearType('flood');
    } else if (model?.clear) {
      model.clear();
    }
    // Also clear polygon model (split-render polygons)
    const polygonModel = ModelRegistry?.getModel('polygon');
    if (polygonModel?.isTypeActive?.('flood')) {
      polygonModel.clearType('flood');
    }
    console.log('OverlayController: Hid flood overlay for animation');
  },

  /**
   * Restore flood overlay after exiting flood animation.
   * @private
   */
  _restoreFloodOverlay() {
    const currentYear = this.getCurrentYear();
    if (dataCache.floods) {
      this.renderFilteredData('floods', currentYear);
      console.log('OverlayController: Restored flood overlay');
    }
  },

  /**
   * Handle wildfire animation - animates perimeter polygon opacity over fire duration.
   * Simple Option A: Fade in final perimeter from 0% to 100% over duration_days.
   */
  handleFireAnimation(data) {
    const { perimeter, eventId, durationDays, startTime, latitude, longitude } = data;
    console.log(`OverlayController: Starting fire animation for ${eventId} (${durationDays} days)`);

    if (!perimeter || !perimeter.geometry) {
      console.warn('OverlayController: No perimeter data for fire animation');
      return;
    }

    // Calculate time range
    const startMs = new Date(startTime).getTime();
    const durationMs = durationDays * 24 * 60 * 60 * 1000;
    const endMs = startMs + durationMs;

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Hide the wildfire overlay to focus on this fire
    this._hideWildfireOverlay();

    // Get perimeter center for zoom (use provided coords or calculate from geometry)
    let centerLon = longitude || 0;
    let centerLat = latitude || 0;

    // Calculate center from geometry if not provided
    if (!longitude || !latitude) {
      let count = 0;
      centerLon = 0;
      centerLat = 0;
      const coords = perimeter.geometry.coordinates;
      if (perimeter.geometry.type === 'Polygon') {
        for (const pt of coords[0]) {
          centerLon += pt[0];
          centerLat += pt[1];
          count++;
        }
      } else if (perimeter.geometry.type === 'MultiPolygon') {
        for (const poly of coords) {
          for (const pt of poly[0]) {
            centerLon += pt[0];
            centerLat += pt[1];
            count++;
          }
        }
      }
      if (count > 0) {
        centerLon /= count;
        centerLat /= count;
      }
    }

    // Add ignition marker at the fire's starting point
    this._addIgnitionMarker(centerLon, centerLat);

    // Zoom to fire location
    MapAdapter.map.flyTo({
      center: [centerLon, centerLat],
      zoom: 9,
      duration: 1500
    });

    // Create fire perimeter layer
    const sourceId = 'fire-anim-perimeter';
    const layerId = 'fire-anim-fill';
    const strokeId = 'fire-anim-stroke';

    // Remove existing layers
    if (MapAdapter.map.getLayer(layerId)) MapAdapter.map.removeLayer(layerId);
    if (MapAdapter.map.getLayer(strokeId)) MapAdapter.map.removeLayer(strokeId);
    if (MapAdapter.map.getSource(sourceId)) MapAdapter.map.removeSource(sourceId);

    // Add perimeter source
    MapAdapter.map.addSource(sourceId, {
      type: 'geojson',
      data: perimeter
    });

    // Add fill layer (starts transparent)
    MapAdapter.map.addLayer({
      id: layerId,
      type: 'fill',
      source: sourceId,
      paint: {
        'fill-color': '#ff4400',
        'fill-opacity': 0
      }
    });

    // Add stroke layer
    MapAdapter.map.addLayer({
      id: strokeId,
      type: 'line',
      source: sourceId,
      paint: {
        'line-color': '#ff6600',
        'line-width': 2,
        'line-opacity': 0
      }
    });

    // Setup TimeSlider for fire animation
    const scaleId = `fire-${eventId.substring(0, 12)}`;
    const fireDate = new Date(startTime).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

    // Generate timestamps for each day
    const timestamps = [];
    for (let t = startMs; t <= endMs; t += 24 * 60 * 60 * 1000) {
      timestamps.push(t);
    }

    if (TimeSlider) {
      const added = TimeSlider.addScale({
        id: scaleId,
        label: `Fire ${fireDate}`,
        granularity: 'daily',
        useTimestamps: true,
        currentTime: startMs,
        timeRange: {
          min: startMs,
          max: endMs,
          available: timestamps
        },
        mapRenderer: 'fire-animation'
      });

      if (added) {
        this.activeFireScaleId = scaleId;
        TimeSlider.setActiveScale(scaleId);

        // Enter event animation mode with auto-calculated speed
        if (TimeSlider.enterEventAnimation) {
          TimeSlider.enterEventAnimation(startMs, endMs);
        }
      }
    }

    // Store animation state
    this._fireAnimState = {
      sourceId,
      layerId,
      strokeId,
      startMs,
      endMs,
      scaleId
    };

    // Listen for time changes to update opacity
    this._fireTimeHandler = (time, source) => {
      if (!this._fireAnimState) return;

      const { startMs, endMs, layerId, strokeId } = this._fireAnimState;
      const progress = Math.max(0, Math.min(1, (time - startMs) / (endMs - startMs)));

      // Update fill and stroke opacity based on progress
      if (MapAdapter.map.getLayer(layerId)) {
        MapAdapter.map.setPaintProperty(layerId, 'fill-opacity', progress * 0.6);
      }
      if (MapAdapter.map.getLayer(strokeId)) {
        MapAdapter.map.setPaintProperty(strokeId, 'line-opacity', progress * 0.9);
      }
    };
    TimeSlider?.addChangeListener(this._fireTimeHandler);

    // Add exit button
    this._addFireExitButton(() => this._exitFireAnimation());

    console.log(`OverlayController: Fire animation ready, ${durationDays} days starting ${fireDate}`);
  },

  /**
   * Handle fire progression animation with daily snapshots.
   * Shows actual fire spread day-by-day using pre-computed perimeters.
   * @param {Object} data - {snapshots, eventId, totalDays, startTime, latitude, longitude}
   */
  handleFireProgression(data) {
    const { snapshots, eventId, totalDays, startTime, latitude, longitude } = data;
    console.log(`OverlayController: Starting fire progression for ${eventId} (${totalDays} daily snapshots)`);

    if (!snapshots || snapshots.length === 0) {
      console.warn('OverlayController: No snapshots for fire progression');
      return;
    }

    // Build timestamp -> snapshot lookup
    const snapshotMap = new Map();
    const timestamps = [];
    let minTime = Infinity, maxTime = -Infinity;

    for (const snap of snapshots) {
      const t = new Date(snap.date + 'T00:00:00Z').getTime();
      snapshotMap.set(t, snap);
      timestamps.push(t);
      if (t < minTime) minTime = t;
      if (t > maxTime) maxTime = t;
    }
    timestamps.sort((a, b) => a - b);

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Hide the wildfire overlay to focus on this fire
    this._hideWildfireOverlay();

    // Get first snapshot for initial display
    const firstSnap = snapshots[0];

    // Get center from first snapshot for zoom (use provided coords or calculate)
    let centerLon = longitude || 0;
    let centerLat = latitude || 0;

    if (!longitude || !latitude) {
      let count = 0;
      centerLon = 0;
      centerLat = 0;
      const geom = firstSnap.geometry;
      if (geom.type === 'Polygon') {
        for (const pt of geom.coordinates[0]) {
          centerLon += pt[0];
          centerLat += pt[1];
          count++;
        }
      } else if (geom.type === 'MultiPolygon') {
        for (const poly of geom.coordinates) {
          for (const pt of poly[0]) {
            centerLon += pt[0];
            centerLat += pt[1];
            count++;
          }
        }
      }
      if (count > 0) {
        centerLon /= count;
        centerLat /= count;
      }
    }

    // Add ignition marker at the fire's starting point
    this._addIgnitionMarker(centerLon, centerLat);

    // Zoom to fire location
    MapAdapter.map.flyTo({
      center: [centerLon, centerLat],
      zoom: 9,
      duration: 1500
    });

    // Create two sets of fire perimeter layers for cross-fading
    // This enables smooth transitions between daily snapshots
    const sourceIdA = 'fire-prog-perimeter-a';
    const sourceIdB = 'fire-prog-perimeter-b';
    const layerIdA = 'fire-prog-fill-a';
    const layerIdB = 'fire-prog-fill-b';
    const strokeIdA = 'fire-prog-stroke-a';
    const strokeIdB = 'fire-prog-stroke-b';

    // Remove existing layers from both sets
    [layerIdA, layerIdB, strokeIdA, strokeIdB].forEach(id => {
      if (MapAdapter.map.getLayer(id)) MapAdapter.map.removeLayer(id);
    });
    [sourceIdA, sourceIdB].forEach(id => {
      if (MapAdapter.map.getSource(id)) MapAdapter.map.removeSource(id);
    });

    // Add two perimeter sources for cross-fading
    MapAdapter.map.addSource(sourceIdA, {
      type: 'geojson',
      data: { type: 'Feature', geometry: firstSnap.geometry, properties: { day: 1 } }
    });
    MapAdapter.map.addSource(sourceIdB, {
      type: 'geojson',
      data: { type: 'Feature', geometry: firstSnap.geometry, properties: { day: 1 } }
    });

    // Add fill layers (A starts visible, B starts hidden)
    MapAdapter.map.addLayer({
      id: layerIdA,
      type: 'fill',
      source: sourceIdA,
      paint: {
        'fill-color': '#ff4400',
        'fill-opacity': 0.5,
        'fill-opacity-transition': { duration: 300, delay: 0 }
      }
    });
    MapAdapter.map.addLayer({
      id: layerIdB,
      type: 'fill',
      source: sourceIdB,
      paint: {
        'fill-color': '#ff4400',
        'fill-opacity': 0,
        'fill-opacity-transition': { duration: 300, delay: 0 }
      }
    });

    // Add stroke layers (A starts visible, B starts hidden)
    MapAdapter.map.addLayer({
      id: strokeIdA,
      type: 'line',
      source: sourceIdA,
      paint: {
        'line-color': '#ff6600',
        'line-width': 2,
        'line-opacity': 0.9,
        'line-opacity-transition': { duration: 300, delay: 0 }
      }
    });
    MapAdapter.map.addLayer({
      id: strokeIdB,
      type: 'line',
      source: sourceIdB,
      paint: {
        'line-color': '#ff6600',
        'line-width': 2,
        'line-opacity': 0,
        'line-opacity-transition': { duration: 300, delay: 0 }
      }
    });

    // Legacy variable names for backward compatibility
    const sourceId = sourceIdA;
    const layerId = layerIdA;
    const strokeId = strokeIdA;

    // Setup TimeSlider
    const scaleId = `fireprog-${eventId.substring(0, 10)}`;
    const fireDate = new Date(minTime).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

    if (TimeSlider) {
      const added = TimeSlider.addScale({
        id: scaleId,
        label: `Fire ${fireDate} (${totalDays}d)`,
        granularity: 'daily',
        useTimestamps: true,
        currentTime: minTime,
        timeRange: {
          min: minTime,
          max: maxTime,
          available: timestamps
        },
        mapRenderer: 'fire-progression'
      });

      if (added) {
        this.activeFireScaleId = scaleId;
        TimeSlider.setActiveScale(scaleId);

        // Enter event animation mode
        if (TimeSlider.enterEventAnimation) {
          TimeSlider.enterEventAnimation(minTime, maxTime);
        }
      }
    }

    // Store animation state with both layer sets for cross-fading
    this._fireAnimState = {
      sourceId,
      layerId,
      strokeId,
      sourceIdA,
      sourceIdB,
      layerIdA,
      layerIdB,
      strokeIdA,
      strokeIdB,
      startMs: minTime,
      endMs: maxTime,
      scaleId,
      snapshotMap,  // For progression: lookup by timestamp
      timestamps,   // For progression: sorted list
      currentLayer: 'A',  // Track which layer is currently visible
      lastSnapshotTime: minTime  // Track last snapshot to detect changes
    };

    // Listen for time changes to update geometry with cross-fade
    this._fireTimeHandler = (time, source) => {
      if (!this._fireAnimState || !this._fireAnimState.snapshotMap) return;

      const state = this._fireAnimState;
      const { snapshotMap, timestamps, lastSnapshotTime, currentLayer } = state;

      // Find closest snapshot <= current time
      let closestTime = timestamps[0];
      for (const t of timestamps) {
        if (t <= time) closestTime = t;
        else break;
      }

      // If snapshot hasn't changed, no need to update
      if (closestTime === lastSnapshotTime) return;

      const snap = snapshotMap.get(closestTime);
      if (!snap) return;

      // Cross-fade: update the hidden layer with new geometry, then swap visibility
      const newLayer = currentLayer === 'A' ? 'B' : 'A';
      const newSourceId = newLayer === 'A' ? state.sourceIdA : state.sourceIdB;
      const newFillId = newLayer === 'A' ? state.layerIdA : state.layerIdB;
      const newStrokeId = newLayer === 'A' ? state.strokeIdA : state.strokeIdB;
      const oldFillId = currentLayer === 'A' ? state.layerIdA : state.layerIdB;
      const oldStrokeId = currentLayer === 'A' ? state.strokeIdA : state.strokeIdB;

      // Update the hidden layer's geometry
      const newSource = MapAdapter.map.getSource(newSourceId);
      if (newSource) {
        newSource.setData({
          type: 'Feature',
          geometry: snap.geometry,
          properties: { day: snap.day_num, area_km2: snap.area_km2, date: snap.date }
        });
      }

      // Cross-fade: fade in the new layer, fade out the old
      if (MapAdapter.map.getLayer(newFillId)) {
        MapAdapter.map.setPaintProperty(newFillId, 'fill-opacity', 0.5);
      }
      if (MapAdapter.map.getLayer(newStrokeId)) {
        MapAdapter.map.setPaintProperty(newStrokeId, 'line-opacity', 0.9);
      }
      if (MapAdapter.map.getLayer(oldFillId)) {
        MapAdapter.map.setPaintProperty(oldFillId, 'fill-opacity', 0);
      }
      if (MapAdapter.map.getLayer(oldStrokeId)) {
        MapAdapter.map.setPaintProperty(oldStrokeId, 'line-opacity', 0);
      }

      // Update tracking state
      state.currentLayer = newLayer;
      state.lastSnapshotTime = closestTime;
    };
    TimeSlider?.addChangeListener(this._fireTimeHandler);

    // Add exit button
    this._addFireExitButton(() => this._exitFireAnimation());

    console.log(`OverlayController: Fire progression ready, ${totalDays} days starting ${fireDate}`);
  },

  /**
   * Exit fire animation and cleanup.
   * @private
   */
  _exitFireAnimation(skipRestore = false) {
    console.log('OverlayController: Exiting fire animation');

    // Remove layers (both A and B layer sets for cross-fade)
    if (this._fireAnimState) {
      const {
        sourceIdA, sourceIdB,
        layerIdA, layerIdB,
        strokeIdA, strokeIdB,
        scaleId
      } = this._fireAnimState;

      // Remove all fill and stroke layers
      [layerIdA, layerIdB, strokeIdA, strokeIdB].forEach(id => {
        if (id && MapAdapter.map.getLayer(id)) MapAdapter.map.removeLayer(id);
      });

      // Remove all sources
      [sourceIdA, sourceIdB].forEach(id => {
        if (id && MapAdapter.map.getSource(id)) MapAdapter.map.removeSource(id);
      });

      // Remove TimeSlider scale
      if (TimeSlider && scaleId) {
        TimeSlider.removeScale(scaleId);
        if (TimeSlider.exitEventAnimation) {
          TimeSlider.exitEventAnimation();
        }
      }

      this._fireAnimState = null;
    }

    // Remove time listener
    if (this._fireTimeHandler && TimeSlider) {
      TimeSlider.removeChangeListener(this._fireTimeHandler);
      this._fireTimeHandler = null;
    }

    // Remove exit button
    const exitBtn = document.getElementById('fire-exit-btn');
    if (exitBtn) exitBtn.remove();

    // Remove ignition marker
    this._removeIgnitionMarker();

    // Restore wildfire overlay (unless overlay is being fully disabled)
    if (!skipRestore) this._restoreWildfireOverlay();

    // Recalculate time range
    this.recalculateTimeRange();
  },

  /**
   * Add exit button for fire animation.
   * @private
   */
  _addFireExitButton(onExit) {
    // Remove existing
    const existing = document.getElementById('fire-exit-btn');
    if (existing) existing.remove();

    const btn = document.createElement('button');
    btn.id = 'fire-exit-btn';
    btn.textContent = 'Exit Fire View';
    btn.style.cssText = `
      position: fixed;
      top: 80px;
      left: 50%;
      transform: translateX(-50%);
      padding: 10px 20px;
      background: #ff6600;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      z-index: 1000;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;

    btn.addEventListener('click', onExit);
    document.body.appendChild(btn);
  },

  /**
   * Hide wildfire overlay to focus on a single fire animation.
   * Clears the map layers but preserves the cached data.
   * @private
   */
  _hideWildfireOverlay() {
    // Clear point-radius model (overview points)
    const model = ModelRegistry?.getModelForType('wildfire');
    if (model?.clearType) {
      model.clearType('wildfire');
    } else if (model?.clear) {
      model.clear();
    }
    // Also clear polygon model (split-render polygons)
    const polygonModel = ModelRegistry?.getModel('polygon');
    if (polygonModel?.isTypeActive?.('wildfire')) {
      polygonModel.clearType('wildfire');
    }
    console.log('OverlayController: Hid wildfire overlay for animation');
  },

  /**
   * Restore wildfire overlay after exiting fire animation.
   * @private
   */
  _restoreWildfireOverlay() {
    const currentYear = this.getCurrentYear();
    if (dataCache.wildfires) {
      this.renderFilteredData('wildfires', currentYear);
      console.log('OverlayController: Restored wildfire overlay');
    }
  },

  /**
   * Add ignition marker for wildfire animation.
   * Shows a fire icon/marker at the ignition point.
   * @private
   */
  _addIgnitionMarker(lon, lat) {
    if (!MapAdapter?.map) return;

    const map = MapAdapter.map;
    const sourceId = 'fire-ignition-marker';
    const layerId = 'fire-ignition-point';
    const glowId = 'fire-ignition-glow';

    // Remove existing marker
    this._removeIgnitionMarker();

    // Add marker source
    map.addSource(sourceId, {
      type: 'geojson',
      data: {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [lon, lat] },
        properties: { type: 'ignition' }
      }
    });

    // Add glow effect layer
    map.addLayer({
      id: glowId,
      type: 'circle',
      source: sourceId,
      paint: {
        'circle-radius': 16,
        'circle-color': '#ff4400',
        'circle-opacity': 0.4,
        'circle-blur': 0.8
      }
    });

    // Add main marker layer (fire symbol)
    map.addLayer({
      id: layerId,
      type: 'circle',
      source: sourceId,
      paint: {
        'circle-radius': 8,
        'circle-color': '#ff6600',
        'circle-stroke-color': '#ffcc00',
        'circle-stroke-width': 2
      }
    });

    console.log(`OverlayController: Added ignition marker at [${lon.toFixed(3)}, ${lat.toFixed(3)}]`);
  },

  /**
   * Remove ignition marker.
   * @private
   */
  _removeIgnitionMarker() {
    if (!MapAdapter?.map) return;

    const map = MapAdapter.map;
    const sourceId = 'fire-ignition-marker';
    const layerId = 'fire-ignition-point';
    const glowId = 'fire-ignition-glow';

    if (map.getLayer(glowId)) map.removeLayer(glowId);
    if (map.getLayer(layerId)) map.removeLayer(layerId);
    if (map.getSource(sourceId)) map.removeSource(sourceId);
  },

  /**
   * Hide hurricane overlay to focus on a single track animation.
   * Clears the track model layers but preserves the cached data.
   * @private
   */
  _hideHurricaneOverlay() {
    const model = ModelRegistry?.getModel('track');
    if (model?.clear) {
      model.clear();
    }
    console.log('OverlayController: Hid hurricane overlay for track drill-down');
  },

  /**
   * Restore hurricane overlay after exiting track drill-down.
   * @private
   */
  _restoreHurricaneOverlay() {
    const currentYear = this.getCurrentYear();
    if (dataCache.hurricanes) {
      this.renderFilteredData('hurricanes', currentYear);
      console.log('OverlayController: Restored hurricane overlay');
    }
  },

  /**
   * Handle hurricane track drill-down animation.
   * Fetches detailed track data and shows animated path.
   * @param {string} stormId - Storm ID
   * @param {string} stormName - Storm name
   * @param {Object} props - Storm properties
   */
  async handleHurricaneDrillDown(stormId, stormName, props) {
    console.log(`OverlayController: Starting hurricane drill-down for ${stormName} (${stormId})`);

    // Hide popup
    MapAdapter?.hidePopup?.();
    MapAdapter.popupLocked = false;

    // Check cache first - avoids duplicate API calls on rewind/replay
    // TODO: Consider using loc_id as cache key once unified across event types
    let data;
    const cached = DetailedEventCache.get(stormId);
    if (cached) {
      console.log(`OverlayController: Using cached track data for ${stormId}`);
      data = cached.data;
    } else {
      // Fetch from API
      const trackUrl = OVERLAY_ENDPOINTS.hurricanes.trackEndpoint.replace('{storm_id}', stormId);
      try {
        data = await fetchMsgpack(trackUrl);
        // Cache for future use
        DetailedEventCache.set(stormId, data, 'hurricane');
      } catch (err) {
        console.error('OverlayController: Error fetching hurricane track:', err);
        return;
      }
    }

    try {
      if (!data || (!data.positions && !data.features)) {
        console.warn('OverlayController: No track data for storm', stormId);
        return;
      }

      // Normalize to positions array
      let positions = data.positions;
      if (!positions && data.features) {
        // FeatureCollection format
        positions = data.features.map(f => ({
          timestamp: f.properties.timestamp,
          latitude: f.geometry.coordinates[1],
          longitude: f.geometry.coordinates[0],
          wind_kt: f.properties.wind_kt,
          category: f.properties.category,
          ...f.properties
        }));
      }

      if (!positions || positions.length === 0) {
        console.warn('OverlayController: Empty track positions for storm', stormId);
        return;
      }

      // Hide hurricane overlay to focus on this track
      this._hideHurricaneOverlay();

      // Use TrackAnimator for proper animation with moving marker and wind radii
      TrackAnimator.start(stormId, positions, {
        stormName,
        onExit: () => {
          console.log('TrackAnimator: Animation exited');
          this._restoreHurricaneOverlay();
          this.recalculateTimeRange();
        }
      });

      // Add exit button (TrackAnimator handles its own TimeSlider setup)
      this._addGenericExitButton('track-exit-btn', 'Exit Track View', '#9c27b0', () => this._exitTrackDrillDown());

      console.log(`OverlayController: Hurricane track animation started (${positions.length} positions)`);
    } catch (err) {
      console.error('OverlayController: Error fetching hurricane track:', err);
    }
  },

  /**
   * Cleanup any stray MultiTrackAnimator animations when overlay is disabled.
   * Note: Rolling mode is deprecated - progressive tracks now handled by filterByLifecycle.
   */
  stopHurricaneRollingAnimation() {
    MultiTrackAnimator.stopAll();
  },

  /**
   * Exit track drill-down and restore hurricane overlay.
   * @private
   */
  _exitTrackDrillDown() {
    console.log('OverlayController: Exiting track drill-down');

    // Stop TrackAnimator (handles all cleanup including TimeSlider scale)
    if (TrackAnimator.isActive) {
      TrackAnimator.stop();
    }

    // Also clear track model layers in case they were used
    const trackModel = ModelRegistry?.getModel('track');
    if (trackModel?.clear) {
      trackModel.clear();
    }

    // Exit event animation mode
    if (TimeSlider?.exitEventAnimation) {
      TimeSlider.exitEventAnimation();
    }

    // Remove exit button
    document.getElementById('track-exit-btn')?.remove();

    // Restore hurricane overlay
    this._restoreHurricaneOverlay();

    // Recalculate time range
    this.recalculateTimeRange();
  },

  /**
   * Handle aftershock sequence selection/deselection.
   * Fetches full sequence data from API (not filtered by magnitude).
   * Uses unified EventAnimator with EARTHQUAKE mode.
   * @param {string|null} sequenceId - Sequence ID or null to clear
   * @param {string|null} eventId - Optional mainshock event_id for accurate aftershock query
   */
  async handleSequenceChange(sequenceId, eventId = null) {
    console.log('OverlayController.handleSequenceChange called with:', sequenceId, eventId);

    // Stop any active animation
    if (EventAnimator.getIsActive()) {
      EventAnimator.stop();
    }

    // If no sequence selected, restore normal earthquake display and we're done
    if (!sequenceId && !eventId) {
      console.log('OverlayController: Cleared aftershock sequence');
      // Re-render all earthquakes for current year
      const currentYear = this.getCurrentYear();
      if (dataCache.earthquakes) {
        this.renderFilteredData('earthquakes', currentYear);
      }
      return;
    }

    // Fetch full sequence from API (includes ALL aftershocks regardless of magnitude filter)
    // Use eventId if provided for accurate aftershock query (handles nested sequences)
    const seqEvents = await this.fetchSequenceData(sequenceId, 'earthquake', eventId);

    if (!seqEvents || seqEvents.length === 0) {
      console.warn(`OverlayController: No events found for sequence ${sequenceId}`);
      return;
    }

    console.log(`OverlayController: Loaded ${seqEvents.length} events for sequence ${sequenceId}`);

    // Find mainshock (largest magnitude or flagged is_mainshock)
    let mainshock = seqEvents.find(f => f.properties.is_mainshock);
    if (!mainshock) {
      // Fallback: largest magnitude
      mainshock = seqEvents.reduce((max, f) =>
        (f.properties.magnitude || 0) > (max.properties.magnitude || 0) ? f : max
      );
    }

    const mainMag = mainshock.properties.magnitude || 5.5;
    const mainTime = new Date(mainshock.properties.timestamp || mainshock.properties.time).getTime();

    // Calculate aftershock window end using Gardner-Knopoff
    const windowDays = gardnerKnopoffTimeWindow(mainMag);
    const windowMs = windowDays * 24 * 60 * 60 * 1000;
    const windowEnd = mainTime + windowMs;

    // Find actual min/max timestamps in sequence
    let minTime = mainTime;
    let maxTime = mainTime;
    for (const event of seqEvents) {
      const t = new Date(event.properties.timestamp || event.properties.time).getTime();
      if (t < minTime) minTime = t;
      if (t > maxTime) maxTime = t;
    }

    // Extend max to theoretical window end if needed
    maxTime = Math.max(maxTime, windowEnd);

    // Determine granularity based on time range
    const timeRange = maxTime - minTime;
    const stepHours = Math.max(1, Math.ceil((timeRange / (60 * 60 * 1000)) / 200));
    let granularityLabel = '6h';
    if (stepHours < 2) granularityLabel = '1h';
    else if (stepHours < 4) granularityLabel = '2h';
    else if (stepHours < 8) granularityLabel = '6h';
    else if (stepHours < 16) granularityLabel = '12h';
    else if (stepHours < 36) granularityLabel = 'daily';
    else granularityLabel = '2d';

    // Format label
    const mainDate = new Date(mainTime);
    const label = `M${mainMag.toFixed(1)} ${mainDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;

    // Track which overlays are currently active (to restore on exit)
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
    const overlaysToRestore = activeOverlays.filter(id =>
      id !== 'demographics' && OVERLAY_ENDPOINTS[id]
    );

    // Clear normal earthquake display before starting sequence animation
    const model = ModelRegistry?.getModelForType('earthquake');
    if (model?.clear) {
      model.clear();
    }

    // Start unified EventAnimator with EARTHQUAKE mode
    EventAnimator.start({
      id: `seq-${sequenceId.substring(0, 8)}`,
      label: label,
      mode: AnimationMode.EARTHQUAKE,
      events: seqEvents,
      mainshock: mainshock,
      eventType: 'earthquake',
      timeField: 'timestamp',
      granularity: granularityLabel,
      renderer: 'point-radius',
      onExit: () => {
        console.log('OverlayController: Earthquake sequence exit callback triggered');
        // Restore TimeSlider range from cached overlay year ranges
        this.recalculateTimeRange();
        // Switch back to primary scale if it exists
        if (TimeSlider) {
          if (TimeSlider.scales?.find(s => s.id === 'primary')) {
            TimeSlider.setActiveScale('primary');
          }
          // Show TimeSlider if we have year data
          if (Object.keys(yearRangeCache).length > 0) {
            TimeSlider.show();
          }
        }
        // Restore all overlays that were active before animation
        const currentYear = this.getCurrentYear();
        for (const overlayId of overlaysToRestore) {
          if (dataCache[overlayId]) {
            this.renderFilteredData(overlayId, currentYear);
            console.log(`OverlayController: Restored ${overlayId} overlay`);
          }
        }
      }
    });

    const durationDays = (timeRange / (24 * 60 * 60 * 1000)).toFixed(1);
    console.log(`OverlayController: Started earthquake sequence ${sequenceId} with ${seqEvents.length} events (${durationDays} days)`);
  },

  /**
   * Fetch full sequence data from API.
   * Returns all events in the sequence regardless of magnitude filter.
   * Extensible for future cross-event linking (volcanoes, tsunamis, etc.)
   *
   * @param {string} sequenceId - Sequence ID to fetch
   * @param {string} eventType - Event type (default 'earthquake', future: 'volcano', 'tsunami')
   * @param {string} eventId - Optional event_id for mainshock-based query
   * @returns {Promise<Array>} Array of GeoJSON features
   */
  async fetchSequenceData(sequenceId, eventType = 'earthquake', eventId = null) {
    try {
      // Cache key: prefer eventId, fall back to sequenceId
      // TODO: Consider using loc_id as cache key once unified across event types
      const cacheKey = eventId || sequenceId;

      // Check cache first - avoids duplicate API calls on rewind/replay
      const cached = DetailedEventCache.get(cacheKey);
      if (cached) {
        console.log(`OverlayController: Using cached sequence data for ${cacheKey}`);
        return cached.data;
      }

      // Build API endpoint based on event type
      let endpoint;
      if (eventType === 'earthquake') {
        // Use aftershocks endpoint if eventId is provided (more accurate for nested sequences)
        // Otherwise fall back to sequence_id query
        if (eventId) {
          endpoint = `/api/earthquakes/aftershocks/${encodeURIComponent(eventId)}`;
        } else {
          endpoint = `/api/earthquakes/sequence/${encodeURIComponent(sequenceId)}`;
        }
      } else {
        // Future: add endpoints for cross-event sequences
        // e.g., /api/events/sequence/{id} for cross-type sequences
        console.warn(`OverlayController: Sequence fetch not yet supported for ${eventType}`);
        return [];
      }

      console.log(`OverlayController: Fetching sequence from ${endpoint}`);
      const data = await fetchMsgpack(endpoint);

      if (!data.features || data.features.length === 0) {
        console.warn(`OverlayController: No features in sequence response`);
        return [];
      }

      // Cache the features array for future use
      DetailedEventCache.set(cacheKey, data.features, eventType);

      return data.features;

    } catch (error) {
      console.error('OverlayController: Error fetching sequence data:', error);
      return [];
    }
  },

  // Track last timestamp for lifecycle filtering (to avoid redundant renders)
  lastTimeSliderTimestamp: null,

  /**
   * Handle TimeSlider change event from listener.
   * @param {number} time - Current time (year or timestamp)
   * @param {string} source - What triggered: 'slider' | 'playback' | 'api'
   */
  handleTimeChange(time, source) {
    // If EventAnimator is active, forward timestamp to it
    // Note: Don't check time value - pre-1970 events have negative timestamps
    if (EventAnimator.getIsActive()) {
      EventAnimator.setTime(time);
      return;  // Don't do normal year-based filtering
    }

    // If TrackAnimator (focused mode) is active, it handles its own rendering
    if (TrackAnimator.isActive) {
      return;
    }

    // Determine if this is a timestamp (for lifecycle filtering)
    const isTimestamp = Math.abs(time) >= 50000;

    if (useLifecycleFiltering && isTimestamp) {
      // NEW: Timestamp-based lifecycle filtering
      // Throttle updates to avoid excessive re-renders (render every ~6 hours of slider time)
      const SIX_HOURS_MS = 6 * 60 * 60 * 1000;
      if (this.lastTimeSliderTimestamp === null ||
          Math.abs(time - this.lastTimeSliderTimestamp) >= SIX_HOURS_MS) {
        this.lastTimeSliderTimestamp = time;
        this.onTimeChangeTimestamp(time);
      }
    } else {
      // LEGACY: Year-based filtering
      const year = this.getYearFromTime(time);
      if (year !== this.lastTimeSliderYear) {
        this.lastTimeSliderYear = year;
        this.onTimeChange(year);
      }
    }
  },

  /**
   * Convert time to year (handles both year int and timestamp ms).
   * Uses same detection as TimeSlider: |value| < 50000 = year, else timestamp.
   * @param {number} time - Time value
   * @returns {number} Year
   */
  getYearFromTime(time) {
    if (!time && time !== 0) return null;
    // If absolute value is small, it's a year (-50000 to 50000)
    // Otherwise it's a timestamp (handles both positive and negative)
    if (Math.abs(time) < 50000) {
      return time;
    }
    // It's a timestamp - convert to year
    return new Date(time).getUTCFullYear();
  },

  /**
   * Get current year from TimeSlider.
   * TimeSlider.currentTime is always stored as timestamp (ms) internally.
   * @returns {number|null}
   */
  getCurrentYear() {
    if (!TimeSlider?.currentTime) return null;

    // currentTime is always a timestamp since Phase 8 unification
    // Use TimeSlider's helper if available, otherwise convert directly
    if (TimeSlider.timestampToYear) {
      return TimeSlider.timestampToYear(TimeSlider.currentTime);
    }
    return new Date(TimeSlider.currentTime).getFullYear();
  },

  /**
   * Get current timestamp from TimeSlider.
   * @returns {number|null}
   */
  getCurrentTimestamp() {
    return TimeSlider?.currentTime || null;
  },

  /**
   * Render overlay with current time using appropriate filtering mode.
   * Uses lifecycle filtering if enabled, otherwise falls back to year-based.
   * @param {string} overlayId - Overlay ID
   */
  renderCurrentData(overlayId) {
    if (useLifecycleFiltering) {
      const timestamp = this.getCurrentTimestamp();
      if (timestamp) {
        this.renderFilteredData(overlayId, timestamp, { useTimestamp: true });
      }
    } else {
      const year = this.getCurrentYear();
      this.renderFilteredData(overlayId, year);
    }
  },

  /**
   * Handle TimeSlider year change - update all active overlays.
   * LEGACY: Year-based filtering (used when useLifecycleFiltering is false)
   * Auto-fetches data for the year if not already cached.
   * @param {number} year - New year
   */
  onTimeChange(year) {
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];

    for (const overlayId of activeOverlays) {
      if (overlayId === 'demographics') continue;

      // Handle weather grid overlays
      const overlayConfig = OverlaySelector?.getOverlayConfig(overlayId);
      if (overlayConfig?.model === 'weather-grid') {
        this.reloadWeatherGridForYear(overlayId, overlayConfig, year);
        continue;
      }

      const endpoint = OVERLAY_ENDPOINTS[overlayId];
      if (!endpoint || !endpoint.yearField) continue;

      // Auto-fetch data for the year if not cached, then render
      this.loadAndRenderYear(overlayId, year);
    }
  },

  /**
   * Load data for a specific year if not cached, then render.
   * @param {string} overlayId - Overlay ID
   * @param {number} year - Year to load
   */
  async loadAndRenderYear(overlayId, year) {
    // Use loadedYears set for simpler year tracking
    const yearAlreadyLoaded = loadedYears[overlayId]?.has(year);

    if (!yearAlreadyLoaded) {
      console.log(`OverlayController: AUTO-FETCHING ${overlayId} for year ${year} (legacy handler)`);
      const yearStart = new Date(year, 0, 1).getTime();
      const yearEnd = new Date(year, 11, 31, 23, 59, 59).getTime();
      await loadRangeData(overlayId, yearStart, yearEnd);
    }

    // Render the data for this year
    if (dataCache[overlayId]) {
      if (useLifecycleFiltering && TimeSlider?.currentTime) {
        this.renderFilteredData(overlayId, TimeSlider.currentTime, { useTimestamp: true });
      } else {
        this.renderFilteredData(overlayId, year);
      }
    }
  },

  /**
   * Reload weather grid data for a specific year.
   * Called when time slider year changes.
   * @param {string} overlayId - Overlay ID
   * @param {Object} config - Overlay config
   * @param {number} year - Year to load
   */
  async reloadWeatherGridForYear(overlayId, config, year) {
    // Check if already cached
    const alreadyCached = loadedYears[overlayId]?.has(year);

    if (alreadyCached) {
      console.log(`OverlayController: Using cached weather ${overlayId} for year ${year}`);
    } else {
      // Load via the cache system (year boundaries for weather grid)
      const yearStart = new Date(year, 0, 1).getTime();
      const yearEnd = new Date(year, 11, 31, 23, 59, 59).getTime();
      await loadRangeData(overlayId, yearStart, yearEnd);
    }

    // Get cached data and pass to display model
    const cachedData = dataCache[overlayId];
    if (cachedData?.years?.[year]) {
      // Display from cache (instances are created automatically)
      WeatherGridModel.displayFromCache(
        overlayId,
        cachedData.years[year],
        cachedData.colorScale,
        cachedData.grid
      );

      // Render at current time slider position
      if (TimeSlider?.currentTime) {
        WeatherGridModel.renderAtTimestamp(overlayId, TimeSlider.currentTime);
      }

      console.log(`OverlayController: Weather grid ${overlayId} displayed for ${year}`);
    }
  },

  /**
   * Handle TimeSlider timestamp change - update all active overlays with lifecycle filtering.
   * NEW: Timestamp-based filtering (used when useLifecycleFiltering is true)
   * Also handles hurricane rolling animation (progressive track drawing during active period).
   * Auto-fetches data for the year if not already cached.
   * @param {number} timestamp - Current timestamp in milliseconds
   */
  onTimeChangeTimestamp(timestamp) {
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
    const year = this.getYearFromTime(timestamp);

    for (const overlayId of activeOverlays) {
      if (overlayId === 'demographics') continue;

      // Handle weather grid overlays
      const overlayConfig = OverlaySelector?.getOverlayConfig(overlayId);
      if (overlayConfig?.model === 'weather-grid') {
        if (WeatherGridModel.hasInstance(overlayId)) {
          const range = WeatherGridModel.getTimestampRange(overlayId);
          if (range && (timestamp < range.min || timestamp > range.max)) {
            this.reloadWeatherGridForYear(overlayId, overlayConfig, year);
          } else {
            WeatherGridModel.renderAtTimestamp(overlayId, timestamp);
          }
        }
        continue;
      }

      const endpoint = OVERLAY_ENDPOINTS[overlayId];
      if (!endpoint) continue;

      // Track last loaded year per overlay to avoid duplicate fetches
      if (!this._lastLoadedYear) this._lastLoadedYear = {};

      // Check if we need to load this year's data
      const yearKey = `${overlayId}_${year}`;
      const yearAlreadyLoaded = loadedYears[overlayId]?.has(year);
      const currentlyLoading = this._loadingYears?.has(yearKey);

      console.log(`OverlayController: ${overlayId} year=${year}, loaded=${yearAlreadyLoaded}, loading=${currentlyLoading}`);

      if (!yearAlreadyLoaded && !currentlyLoading) {
        // Track that we're loading this year
        if (!this._loadingYears) this._loadingYears = new Set();
        this._loadingYears.add(yearKey);

        console.log(`OverlayController: AUTO-FETCHING ${overlayId} for year ${year}`);
        // Auto-fetch data for this year, then render
        this.loadYearAndRender(overlayId, year, timestamp).finally(() => {
          this._loadingYears?.delete(yearKey);
        });
      } else if (dataCache[overlayId]) {
        // Render from cache
        this.renderFilteredData(overlayId, timestamp, { useTimestamp: true });

        if (overlayId === 'hurricanes') {
          this.checkHurricaneRollingAnimation(timestamp);
        }
      }
    }
  },

  /**
   * Load a year's data and render.
   * Used for lazy loading when user navigates to a year not yet cached.
   * @param {string} overlayId - Overlay ID
   * @param {number} year - Year to load
   * @param {number} timestamp - Optional timestamp for lifecycle filtering
   */
  async loadYearAndRender(overlayId, year, timestamp = null) {
    const endpoint = OVERLAY_ENDPOINTS[overlayId];
    if (!endpoint) return;

    console.log(`OverlayController: Auto-fetching ${overlayId} for year ${year}`);

    // Load the year data (year boundaries)
    const yearStart = new Date(year, 0, 1).getTime();
    const yearEnd = new Date(year, 11, 31, 23, 59, 59).getTime();
    const loaded = await loadRangeData(overlayId, yearStart, yearEnd);

    // Check if overlay is still active
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
    if (!activeOverlays.includes(overlayId)) return;

    // After loading, always render with CURRENT time (not the timestamp that triggered load)
    // This fixes gaps during fast playback where animation moves while data is loading
    if (useLifecycleFiltering && TimeSlider?.currentTime) {
      const currentTimestamp = TimeSlider.currentTime;
      this.renderFilteredData(overlayId, currentTimestamp, { useTimestamp: true });

      // For hurricanes, check if we should start rolling animation
      if (overlayId === 'hurricanes') {
        this.checkHurricaneRollingAnimation(currentTimestamp);
      }
    } else if (timestamp && useLifecycleFiltering) {
      this.renderFilteredData(overlayId, timestamp, { useTimestamp: true });
    } else {
      // Year-based rendering
      this.renderFilteredData(overlayId, year);
    }

    if (loaded) {
      console.log(`OverlayController: Loaded and rendered ${overlayId} for year ${year}`);
    }
  },

  /**
   * DEPRECATED: No-op. Hurricane animation now handled by filterByLifecycle.
   * Kept for API compatibility - callers don't need to be updated.
   */
  checkHurricaneRollingAnimation() {
    // filterByLifecycle handles progressive track display via timestamp trimming
  },

  /**
   * Handle overlay toggle event.
   * @param {string} overlayId - Overlay ID (e.g., 'earthquakes')
   * @param {boolean} isActive - Whether overlay is now active
   */
  async handleOverlayChange(overlayId, isActive) {
    console.log(`OverlayController: ${overlayId} ${isActive ? 'ON' : 'OFF'}`);

    // Demographics controls choropleth visibility AND loads countries
    if (overlayId === 'demographics') {
      if (isActive) {
        // Load countries if not already loaded (lazy load on first demographics enable)
        const App = window.App;  // Get App reference
        if (App && typeof App.loadCountries === 'function') {
          // Check if countries are already loaded by checking if there's geojson data
          if (!App.currentData?.geojson) {
            console.log('OverlayController: Loading countries for demographics overlay');
            await App.loadCountries();
          }
        }
        if (MapAdapter) {
          MapAdapter.setChoroplethVisible(true);
        }
      } else {
        if (MapAdapter) {
          MapAdapter.setChoroplethVisible(false);
        }
      }
      return;
    }

    // Weather grid overlays (temperature, humidity, snow-depth)
    const overlayConfig = OverlaySelector?.getOverlayConfig(overlayId);
    if (overlayConfig?.model === 'weather-grid') {
      if (isActive) {
        await this.loadWeatherGridOverlay(overlayId, overlayConfig);
      } else {
        this.clearWeatherGridOverlay(overlayId);
      }
      return;
    }

    if (isActive) {
      await this.loadOverlay(overlayId);
    } else {
      this.hideOverlay(overlayId);
    }
  },

  /**
   * Load and display a weather grid overlay.
   * @param {string} overlayId - Overlay ID (temperature, humidity, snow-depth)
   * @param {Object} config - Overlay configuration from OverlaySelector
   */
  async loadWeatherGridOverlay(overlayId, config) {
    // Determine year based on current time slider position
    let year = new Date().getFullYear();

    if (TimeSlider?.currentTime) {
      const currentDate = new Date(TimeSlider.currentTime);
      year = currentDate.getFullYear();
    }

    console.log(`OverlayController: Loading weather grid ${overlayId} for year ${year}`);

    // Load data via cache system (year boundaries for weather grid)
    const yearStart = new Date(year, 0, 1).getTime();
    const yearEnd = new Date(year, 11, 31, 23, 59, 59).getTime();
    await loadRangeData(overlayId, yearStart, yearEnd);

    // Get cached data and display (instances are created automatically)
    const cachedData = dataCache[overlayId];
    if (cachedData?.years?.[year]) {
      WeatherGridModel.displayFromCache(
        overlayId,
        cachedData.years[year],
        cachedData.colorScale,
        cachedData.grid
      );

      // Set TimeSlider to default range (2000-present, data exists back to 1940 via chat)
      if (TimeSlider) {
        const minDate = new Date(Date.UTC(2000, 0, 1));  // Jan 1, 2000 (default view)
        const maxDate = new Date();  // Now
        TimeSlider.setTimeRange({
          min: minDate.getTime(),
          max: maxDate.getTime(),
          granularity: 'timestamp',
          available: null
        });
        TimeSlider.show();

        // Position slider at start of loaded data
        const yearData = cachedData.years[year];
        if (yearData?.timestamps?.length > 0) {
          TimeSlider.setTime(yearData.timestamps[0]);
        }
      }

      console.log(`OverlayController: Weather grid ${overlayId} loaded for year ${year}`);
    } else {
      console.error(`OverlayController: Failed to load weather grid ${overlayId}`);
    }
  },

  /**
   * Clear a weather grid overlay.
   * @param {string} overlayId - Overlay ID
   */
  clearWeatherGridOverlay(overlayId) {
    WeatherGridModel.hide(overlayId);
    console.log(`OverlayController: Cleared weather grid ${overlayId}`);
  },

  /**
   * Load and display an overlay.
   * Uses year-based lazy loading: only loads current year initially.
   * Additional years are loaded on-demand as user navigates time.
   * @param {string} overlayId - Overlay ID
   */
  async loadOverlay(overlayId) {
    const endpoint = OVERLAY_ENDPOINTS[overlayId];
    if (!endpoint) {
      console.warn(`OverlayController: No endpoint for overlay: ${overlayId}`);
      return;
    }

    // Prevent duplicate loads
    if (this.loading.has(overlayId)) {
      console.log(`OverlayController: Already loading ${overlayId}`);
      return;
    }

    // Abort any existing request for this overlay
    if (this.abortControllers.has(overlayId)) {
      this.abortControllers.get(overlayId).abort();
    }

    // Create new AbortController for this request
    const abortController = new AbortController();
    this.abortControllers.set(overlayId, abortController);

    this.loading.add(overlayId);

    try {
      // If range already loaded (cache exists), just re-render without fetching
      // This handles re-enable after hide (0 features is still "loaded")
      if (loadedRanges[overlayId]?.length > 0) {
        console.log(`OverlayController: ${overlayId} already loaded, re-rendering from cache`);
        this.loading.delete(overlayId);
        this.renderCurrentData(overlayId);

        // If live mode is active, immediately fetch delta to catch up
        if (TimeSlider?.isLiveMode) {
          const FIVE_MIN = 5 * 60 * 1000;
          const now = Math.floor(Date.now() / FIVE_MIN) * FIVE_MIN;
          const ranges = loadedRanges[overlayId].filter(r => !r.loading);
          const lastEnd = Math.max(...ranges.map(r => r.end));
          if (now > lastEnd) {
            console.log(`OverlayController: ${overlayId} catching up delta in live mode`);
            loadRangeData(overlayId, lastEnd, now).then(loaded => {
              if (loaded) this.renderCurrentData(overlayId);
            });
          }
        }
        return;
      }

      // Load past 30 days of data (one-time initial load)
      // Round to 5-minute intervals to prevent duplicate fetches from ms drift
      const FIVE_MIN = 5 * 60 * 1000;
      const now = Math.floor(Date.now() / FIVE_MIN) * FIVE_MIN;
      const thirtyDaysAgo = now - (30 * 24 * 60 * 60 * 1000);

      // Respect maxYear constraint (e.g., floods end at 2019)
      let endMs = now;
      let startMs = thirtyDaysAgo;
      if (endpoint.maxYear) {
        const maxEndMs = new Date(endpoint.maxYear, 11, 31).getTime();
        if (endMs > maxEndMs) {
          endMs = maxEndMs;
          startMs = endMs - (30 * 24 * 60 * 60 * 1000);
        }
      }

      console.log(`OverlayController: Loading ${overlayId} (past 30 days)`);

      // Load the range data
      const loaded = await loadRangeData(overlayId, startMs, endMs, abortController.signal);

      // Check if overlay was disabled while we were fetching
      const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
      if (!activeOverlays.includes(overlayId)) {
        console.log(`OverlayController: ${overlayId} was disabled during fetch, discarding data`);
        return;
      }

      // Initialize TimeSlider for this overlay
      if (endpoint.yearField && TimeSlider) {
        const currentYear = new Date().getFullYear();
        const minYear = 2000;
        const maxYear = currentYear;

        // Initialize year range cache
        if (!yearRangeCache[overlayId]) {
          yearRangeCache[overlayId] = {
            min: currentYear,
            max: currentYear,
            available: loaded ? [currentYear] : []
          };
        }

        TimeSlider.setTimeRange({
          min: minYear,
          max: maxYear,
          granularity: 'yearly',
          available: null
        });
        TimeSlider.show();
        console.log(`OverlayController: TimeSlider range ${minYear}-${maxYear}, loaded past 30 days`);
      }

      // Render with current time (uses lifecycle filtering if enabled)
      this.renderCurrentData(overlayId);

    } catch (error) {
      if (error.name === 'AbortError') {
        console.log(`OverlayController: Fetch aborted for ${overlayId}`);
        return;
      }
      console.error(`OverlayController: Failed to load ${overlayId}:`, error);
      this.showError(overlayId, error.message);
    } finally {
      this.loading.delete(overlayId);
      this.abortControllers.delete(overlayId);
    }
  },

  /**
   * Filter cached data and render.
   * Supports both year-based filtering (legacy) and timestamp-based lifecycle filtering (new).
   * @param {string} overlayId - Overlay ID
   * @param {number|null} yearOrTimestamp - Year or timestamp to filter by
   * @param {object} options - Optional settings
   * @param {boolean} options.useTimestamp - If true, treat value as timestamp for lifecycle filtering
   */
  renderFilteredData(overlayId, yearOrTimestamp, options = {}) {
    const endpoint = OVERLAY_ENDPOINTS[overlayId];
    const cachedData = dataCache[overlayId];

    if (!endpoint || !cachedData) return;

    let filteredGeojson;
    const useTimestamp = options.useTimestamp && useLifecycleFiltering;

    if (useTimestamp && yearOrTimestamp) {
      // NEW: Timestamp-based lifecycle filtering
      // Hurricane tracks are progressively trimmed by filterByLifecycle based on _animationProgress
      const currentMs = yearOrTimestamp;
      const filtered = filterByLifecycle(
        cachedData.features,
        currentMs,
        endpoint.eventType
      );

      filteredGeojson = {
        type: 'FeatureCollection',
        features: filtered
      };
      const dateStr = new Date(currentMs).toISOString().split('T')[0];
      console.log(`OverlayController: Lifecycle filtered ${cachedData.features.length} -> ${filtered.length} for ${dateStr}`);
    } else if (endpoint.yearField && yearOrTimestamp) {
      // LEGACY: Year-based filtering
      const yearNum = parseInt(yearOrTimestamp);
      const filtered = cachedData.features.filter(f => {
        const propYear = f.properties[endpoint.yearField];
        if (propYear == null) return false;
        return parseInt(propYear) === yearNum;
      });
      filteredGeojson = {
        type: 'FeatureCollection',
        features: filtered
      };
      console.log(`OverlayController: Filtered ${cachedData.features.length} -> ${filtered.length} for year ${yearNum}`);
    } else {
      filteredGeojson = cachedData;
    }

    // Track displayed year (for legacy compatibility)
    displayedYear[overlayId] = useTimestamp ? this.getYearFromTime(yearOrTimestamp) : yearOrTimestamp;

    // Render using appropriate model
    const rendered = ModelRegistry?.render(filteredGeojson, endpoint.eventType, {
      onEventClick: (props) => this.handleEventClick(overlayId, props)
    });

    if (rendered) {
      const timeStr = useTimestamp
        ? ` at ${new Date(yearOrTimestamp).toISOString().split('T')[0]}`
        : (yearOrTimestamp ? ` for ${yearOrTimestamp}` : ' (all years)');
      console.log(`OverlayController: Rendered ${filteredGeojson.features?.length || 0} ${overlayId}${timeStr}`);
    }
  },

  /**
   * Clear an overlay from the map.
   * @param {string} overlayId - Overlay ID
   */
  /**
   * Hide overlay from map without clearing cache.
   * Called when overlay is toggled off - data stays in cache for re-enable.
   * @param {string} overlayId - Overlay ID
   */
  hideOverlay(overlayId) {
    const endpoint = OVERLAY_ENDPOINTS[overlayId];
    if (!endpoint) return;

    // Abort any in-flight fetch request for this overlay
    if (this.abortControllers.has(overlayId)) {
      this.abortControllers.get(overlayId).abort();
      this.abortControllers.delete(overlayId);
      console.log(`OverlayController: Aborted pending fetch for ${overlayId}`);
    }
    this.loading.delete(overlayId);

    // Stop any active animations/drill-downs for this overlay
    this._cleanupOverlayAnimations(overlayId);

    // Clear visual layers from map (but keep dataCache intact)
    const model = ModelRegistry?.getModelForType(endpoint.eventType);
    if (model) {
      if (model.clearType) {
        model.clearType(endpoint.eventType);
      } else if (model.clear) {
        model.clear();
      }
    }

    // Also clear polygon layers for split-render types
    const eventType = endpoint.eventType;
    if (eventType === 'wildfire' || eventType === 'flood') {
      const polygonModel = ModelRegistry?.getModel('polygon');
      if (polygonModel?.isTypeActive?.(eventType)) {
        polygonModel.clearType(eventType);
      }
    }

    // Hide popup if showing this overlay's data
    if (MapAdapter?.popup?.isOpen?.()) {
      MapAdapter.hidePopup();
      MapAdapter.popupLocked = false;
    }

    // Recalculate TimeSlider range from remaining active overlays
    this.recalculateTimeRange();

    console.log(`OverlayController: Hidden ${overlayId} (cache preserved)`);
  },

  /**
   * Clear overlay completely - removes from map AND deletes cache.
   * Called from Loaded tab "Clear" button.
   * @param {string} overlayId - Overlay ID
   */
  clearOverlay(overlayId) {
    // First hide from map
    this.hideOverlay(overlayId);

    // Then clear caches
    delete dataCache[overlayId];
    delete yearRangeCache[overlayId];
    delete loadedYears[overlayId];
    delete loadedRanges[overlayId];

    // Dispatch cache update for Loaded tab
    window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: calculateCacheSize() }));

    console.log(`OverlayController: Cleared ${overlayId} (cache deleted)`);
  },

  /**
   * Cleanup any active animations or drill-down layers for a specific overlay.
   * Called when an overlay is toggled off to prevent orphaned layers on the map.
   * @private
   * @param {string} overlayId - Overlay ID being disabled
   */
  _cleanupOverlayAnimations(overlayId) {
    // When called from clearOverlay, skip restore since the overlay is being disabled
    const skipRestore = true;

    switch (overlayId) {
      case 'hurricanes':
        this.stopHurricaneRollingAnimation();
        break;

      case 'wildfires':
        // Exit any active wildfire animations (skip restore - overlay is being disabled)
        if (this._wildfireImpactState) this._exitWildfireImpact(skipRestore);
        if (this._wildfirePerimeterState) this._exitWildfirePerimeter(skipRestore);
        if (this._fireAnimState) this._exitFireAnimation(skipRestore);
        break;

      case 'floods':
        // Exit any active flood animations (skip restore - overlay is being disabled)
        if (this._floodAnimState) this._exitFloodAnimation(skipRestore);
        if (this._floodImpactState) this._exitFloodImpact(skipRestore);
        break;

      case 'volcanoes':
        // Exit volcano impact radius animation
        if (this._volcanoImpactState) this._exitVolcanoImpact();
        break;

      case 'tornadoes':
        // Exit tornado point animation
        if (this._tornadoPointAnimState) this._exitTornadoPointAnimation();
        // Also stop EventAnimator if running a tornado sequence
        if (EventAnimator.getIsActive() && EventAnimator.config?.rendererOptions?.eventType === 'tornado') {
          EventAnimator.stop();
        }
        break;

      case 'earthquakes':
        // Stop EventAnimator if running an aftershock sequence
        if (EventAnimator.getIsActive() && EventAnimator.config?.rendererOptions?.eventType === 'earthquake') {
          EventAnimator.stop();
        }
        break;

      case 'tsunamis':
        // Stop EventAnimator if running a tsunami wave animation
        if (EventAnimator.getIsActive() && EventAnimator.config?.rendererOptions?.eventType === 'tsunami') {
          EventAnimator.stop();
        }
        break;
    }
  },

  /**
   * Recalculate TimeSlider range from all active overlays.
   * Called when an overlay is disabled to contract the range.
   */
  recalculateTimeRange() {
    if (!TimeSlider) return;

    // Get remaining cached year ranges
    const activeRanges = Object.values(yearRangeCache);

    if (activeRanges.length === 0) {
      // No active overlays with year data - hide slider or reset to default
      console.log('OverlayController: No active overlays, TimeSlider range unchanged');
      return;
    }

    // Calculate combined range (union of all active overlays)
    let combinedMin = Infinity;
    let combinedMax = -Infinity;
    const allYears = new Set();

    for (const range of activeRanges) {
      if (range.min < combinedMin) combinedMin = range.min;
      if (range.max > combinedMax) combinedMax = range.max;
      for (const year of range.available) {
        allYears.add(year);
      }
    }

    const sortedYears = Array.from(allYears).sort((a, b) => a - b);

    // Update TimeSlider with REPLACE mode (contract range if needed)
    TimeSlider.setTimeRange({
      min: combinedMin,
      max: combinedMax,
      granularity: 'yearly',
      available: sortedYears,
      replace: true  // Allow contracting the range
    });

    console.log(`OverlayController: Recalculated TimeSlider range ${combinedMin}-${combinedMax} from ${activeRanges.length} overlays`);
  },

  /**
   * Handle click on an event feature.
   * @param {string} overlayId - Overlay ID
   * @param {Object} props - Feature properties
   * @param {Array} coords - Optional coordinates [lng, lat] for popup placement
   */
  handleEventClick(overlayId, props, coords = null) {
    console.log(`OverlayController: Clicked ${overlayId} event:`, props);

    // For hurricanes, show popup with View Track button
    if (overlayId === 'hurricanes' && props.storm_id) {
      this._showHurricanePopup(props, coords);
    }
  },

  /**
   * Show popup for hurricane track with View Track button.
   * @private
   */
  _showHurricanePopup(props, coords) {
    const map = MapAdapter?.map;
    if (!map) return;

    const stormId = props.storm_id;
    const stormName = props.name || stormId;

    // Build popup content
    const lines = [`<strong>${stormName}</strong>`];
    if (props.year) lines.push(`Year: ${props.year}`);
    if (props.basin) lines.push(`Basin: ${props.basin}`);
    if (props.max_category) lines.push(`Max Category: ${props.max_category}`);
    if (props.max_wind_kt) lines.push(`Max Wind: ${props.max_wind_kt} kt`);
    if (props.min_pressure_mb) lines.push(`Min Pressure: ${props.min_pressure_mb} mb`);
    if (props.start_date && props.end_date) {
      lines.push(`Dates: ${props.start_date.split('T')[0]} to ${props.end_date.split('T')[0]}`);
    }
    if (props.made_landfall) lines.push('<em>Made landfall</em>');

    // Add View Track button
    const buttonId = `view-track-${stormId.replace(/[^a-zA-Z0-9]/g, '-')}`;
    lines.push(`<br><button id="${buttonId}" style="background:#3b82f6;color:white;border:none;padding:6px 12px;border-radius:4px;cursor:pointer;margin-top:8px;">View Track</button>`);

    // Determine popup position - use center of track if no coords provided
    let popupCoords = coords;
    if (!popupCoords) {
      // Try to get coords from the feature geometry center (approximate)
      popupCoords = [-80, 25]; // Default to Atlantic
    }

    // Create popup
    const popup = new maplibregl.Popup({ closeOnClick: true, maxWidth: '280px' })
      .setLngLat(popupCoords)
      .setHTML(lines.join('<br>'))
      .addTo(map);

    // Setup button click handler after popup is added to DOM
    setTimeout(() => {
      const button = document.getElementById(buttonId);
      if (button) {
        button.addEventListener('click', () => {
          popup.remove();
          this.drillDownHurricane(stormId, stormName);
        });
      }
    }, 0);
  },

  /**
   * Drill down into a hurricane track for animation.
   * Uses global IBTrACS API endpoint.
   * @param {string} stormId - Storm ID (e.g., "2005236N23285" for Katrina)
   * @param {string} stormName - Storm name
   */
  async drillDownHurricane(stormId, stormName) {
    try {
      // Hide the hurricane overlay to focus on this single track
      this._hideHurricaneOverlay();

      // Check cache first - avoids duplicate API calls on rewind/replay
      // TODO: Consider using loc_id as cache key once unified across event types
      let data;
      const cached = DetailedEventCache.get(stormId);
      if (cached) {
        console.log(`OverlayController: Using cached track data for ${stormId}`);
        data = cached.data;
      } else {
        // Fetch from API
        data = await fetchMsgpack(`/api/storms/${encodeURIComponent(stormId)}/track`);
        // Cache for future use
        DetailedEventCache.set(stormId, data, 'hurricane');
      }

      if (!data.positions || data.positions.length === 0) {
        console.warn(`OverlayController: No positions found for storm ${stormId}`);
        return;
      }

      // Build GeoJSON from positions
      const features = data.positions.map((pos, idx) => ({
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: [pos.longitude, pos.latitude]
        },
        properties: {
          storm_id: stormId,
          name: data.name || stormName,
          timestamp: pos.timestamp,
          wind_kt: pos.wind_kt,
          pressure_mb: pos.pressure_mb,
          category: pos.category,
          status: pos.status,
          // Wind radii (may be null for older storms)
          r34_ne: pos.r34_ne,
          r34_se: pos.r34_se,
          r34_sw: pos.r34_sw,
          r34_nw: pos.r34_nw,
          r50_ne: pos.r50_ne,
          r50_se: pos.r50_se,
          r50_sw: pos.r50_sw,
          r50_nw: pos.r50_nw,
          r64_ne: pos.r64_ne,
          r64_se: pos.r64_se,
          r64_sw: pos.r64_sw,
          r64_nw: pos.r64_nw,
          position_index: idx
        }
      }));

      const trackGeojson = {
        type: 'FeatureCollection',
        features: features
      };

      // Get TrackModel and render track
      const trackModel = ModelRegistry?.getModel('track');
      if (trackModel) {
        trackModel.renderTrack(trackGeojson);
        trackModel.fitBounds(trackGeojson);

        // Store track data for click handling
        this._currentTrackData = {
          stormId,
          stormName,
          positions: features
        };

        // Add click handler for track position dots to show wind radii
        this._setupTrackPositionClickHandler(trackModel);
      }

      console.log(`OverlayController: Loaded track for ${stormName} (${data.count} positions)`);

      // Add "Animate Track" button for 6-hour animation mode
      this._addAnimateTrackButton(stormId, stormName, data.positions);

    } catch (error) {
      console.error(`OverlayController: Failed to load hurricane track:`, error);
    }
  },

  /**
   * Add track control buttons (Animate Track + Back to Storms).
   * Positioned at top center to avoid overlapping TimeSlider.
   * @private
   */
  _addAnimateTrackButton(stormId, stormName, positions) {
    // Remove existing buttons if any
    const existing = document.getElementById('track-controls-container');
    if (existing) existing.remove();

    // Create container for both buttons - positioned at top center
    const container = document.createElement('div');
    container.id = 'track-controls-container';
    container.style.cssText = `
      position: fixed;
      top: 80px;
      left: 50%;
      transform: translateX(-50%);
      display: flex;
      gap: 12px;
      z-index: 1000;
    `;

    // Animate Track button
    const animateBtn = document.createElement('button');
    animateBtn.id = 'animate-track-btn';
    animateBtn.textContent = 'Animate Track';
    animateBtn.style.cssText = `
      padding: 10px 20px;
      background: #3b82f6;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;

    animateBtn.addEventListener('click', () => {
      container.remove();
      this._startTrackAnimation(stormId, stormName, positions);
    });

    // Back to Storms button
    const backBtn = document.createElement('button');
    backBtn.id = 'back-to-storms-btn';
    backBtn.textContent = 'Back to Storms';
    backBtn.style.cssText = `
      padding: 10px 20px;
      background: #6b7280;
      color: white;
      border: none;
      border-radius: 6px;
      cursor: pointer;
      font-size: 14px;
      font-weight: 500;
      box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    `;

    backBtn.addEventListener('click', () => {
      container.remove();
      this._exitTrackView();
    });

    container.appendChild(animateBtn);
    container.appendChild(backBtn);
    document.body.appendChild(container);
  },

  /**
   * Exit track view and return to yearly storm overview.
   * @private
   */
  _exitTrackView() {
    // Clear track display
    const trackModel = ModelRegistry?.getModel('track');
    if (trackModel) {
      trackModel.clearTrack();
      trackModel.clearWindRadii();
    }

    // Clear track data
    this._currentTrackData = null;

    // Restore hurricane overlay to show yearly overview
    this._restoreHurricaneOverlay();

    console.log('OverlayController: Returned to storms overview');
  },

  /**
   * Start track animation using TrackAnimator.
   * @private
   */
  _startTrackAnimation(stormId, stormName, positions) {
    // Clear the static track display first
    const trackModel = ModelRegistry?.getModel('track');
    if (trackModel) {
      trackModel.clearTrack();
      trackModel.clearWindRadii();
    }

    // Start TrackAnimator
    TrackAnimator.start(stormId, positions, {
      stormName,
      onExit: () => {
        // When animation exits, reload the static track view
        console.log('TrackAnimator: Exited, reloading static track');
        this.drillDownHurricane(stormId, stormName);
      }
    });
  },

  /**
   * Setup click handler for track position dots.
   * Shows wind radii and popup when clicking on a position.
   * @private
   */
  _setupTrackPositionClickHandler(trackModel) {
    const map = MapAdapter?.map;
    if (!map) return;

    // Remove existing handler if any
    if (this._trackPositionClickHandler) {
      map.off('click', CONFIG.layers.hurricaneCircle + '-track-dots', this._trackPositionClickHandler);
    }

    this._trackPositionClickHandler = (e) => {
      if (!e.features || e.features.length === 0) return;

      const feature = e.features[0];
      const props = feature.properties;
      const coords = feature.geometry.coordinates;

      // Show wind radii if available
      const hasWindRadii = props.r34_ne || props.r34_se || props.r34_sw || props.r34_nw;
      if (hasWindRadii) {
        trackModel.renderWindRadii({
          longitude: coords[0],
          latitude: coords[1],
          properties: props
        });
      } else {
        trackModel.clearWindRadii();
      }

      // Build popup content
      const lines = [`<strong>${props.name || 'Storm Position'}</strong>`];
      if (props.timestamp) {
        const date = new Date(props.timestamp);
        lines.push(date.toLocaleString());
      }
      if (props.category) lines.push(`Category: ${props.category}`);
      if (props.wind_kt) lines.push(`Wind: ${props.wind_kt} kt`);
      if (props.pressure_mb) lines.push(`Pressure: ${props.pressure_mb} mb`);
      if (props.status) lines.push(`Status: ${props.status}`);

      // Wind radii info
      if (hasWindRadii) {
        lines.push('<br><em>Wind Radii (nm):</em>');
        if (props.r34_ne) lines.push(`34kt: NE=${props.r34_ne} SE=${props.r34_se} SW=${props.r34_sw} NW=${props.r34_nw}`);
        if (props.r50_ne) lines.push(`50kt: NE=${props.r50_ne} SE=${props.r50_se} SW=${props.r50_sw} NW=${props.r50_nw}`);
        if (props.r64_ne) lines.push(`64kt: NE=${props.r64_ne} SE=${props.r64_se} SW=${props.r64_sw} NW=${props.r64_nw}`);
      } else {
        lines.push('<em>(No wind radii data for this position)</em>');
      }

      // Show popup
      new maplibregl.Popup({ closeOnClick: true })
        .setLngLat(coords)
        .setHTML(lines.join('<br>'))
        .addTo(map);
    };

    map.on('click', CONFIG.layers.hurricaneCircle + '-track-dots', this._trackPositionClickHandler);

    // Hover cursor
    map.on('mouseenter', CONFIG.layers.hurricaneCircle + '-track-dots', () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', CONFIG.layers.hurricaneCircle + '-track-dots', () => {
      map.getCanvas().style.cursor = '';
    });
  },

  /**
   * Refresh all active overlays (e.g., when time changes).
   */
  async refreshActive() {
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];

    for (const overlayId of activeOverlays) {
      if (overlayId !== 'demographics' && OVERLAY_ENDPOINTS[overlayId]) {
        await this.loadOverlay(overlayId);
      }
    }
  },

  /**
   * Show error notification for failed overlay load.
   * @param {string} overlayId - Overlay ID
   * @param {string} message - Error message
   */
  showError(overlayId, message) {
    // For now, just console error
    // TODO: Add toast notification UI
    console.error(`Failed to load ${overlayId}: ${message}`);
  },

  /**
   * Get cached data for an overlay.
   * @param {string} overlayId - Overlay ID
   * @returns {Object|null} Cached GeoJSON or null
   */
  getCachedData(overlayId) {
    return dataCache[overlayId] || null;
  },

  /**
   * Re-render all active overlays from cache (no data fetching).
   * Use after map style changes that clear layers but shouldn't reload data.
   */
  rerenderFromCache() {
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];

    for (const overlayId of activeOverlays) {
      if (overlayId === 'demographics') continue;
      if (!dataCache[overlayId]) continue;

      // Use current time slider state to render
      if (useLifecycleFiltering && TimeSlider?.currentTime) {
        this.renderFilteredData(overlayId, TimeSlider.currentTime, { useTimestamp: true });
      } else {
        const year = TimeSlider?.currentTime ? this.getYearFromTime(TimeSlider.currentTime) : new Date().getFullYear();
        this.renderFilteredData(overlayId, year);
      }
    }

    console.log('OverlayController: Re-rendered overlays from cache');
  },

  /**
   * Clear all overlay caches.
   */
  clearCache() {
    for (const key in dataCache) {
      delete dataCache[key];
    }
    for (const key in loadedYears) {
      delete loadedYears[key];
    }
    for (const key in loadedRanges) {
      delete loadedRanges[key];
    }
    for (const key in yearRangeCache) {
      delete yearRangeCache[key];
    }
    for (const key in loadedFilters) {
      delete loadedFilters[key];
    }
  },

  /**
   * Get loaded years for an overlay.
   * @param {string} overlayId - Overlay ID
   * @returns {Array} Array of loaded years
   */
  getLoadedYears(overlayId) {
    return loadedYears[overlayId] ? Array.from(loadedYears[overlayId]).sort((a, b) => a - b) : [];
  },

  /**
   * Get the filter thresholds that were used when loading data.
   * This tells chat what data is actually in cache vs what's currently displayed.
   * Example: loaded M5.0+ but displaying M6.0+ - can filter to M5.5+ from cache.
   * @param {string} overlayId - Overlay ID
   * @returns {Object} Filter thresholds used at load time
   */
  getLoadedFilters(overlayId) {
    return loadedFilters[overlayId] || {};
  },

  /**
   * Get cache statistics for monitoring memory usage.
   * Call from console: OverlayController.getCacheStats()
   * @returns {Object} Cache statistics
   */
  getCacheStats() {
    // Get exact size measurements
    const sizeInfo = calculateCacheSize();

    const stats = {
      overlays: {},
      totals: {
        features: 0,
        bytes: 0,
        yearsLoaded: 0,
        overlaysActive: 0
      }
    };

    for (const overlayId of Object.keys(OVERLAY_ENDPOINTS)) {
      const features = dataCache[overlayId]?.features || [];
      const years = loadedYears[overlayId] ? Array.from(loadedYears[overlayId]).sort((a, b) => a - b) : [];
      const ranges = (loadedRanges[overlayId] || []).filter(r => !r.loading);
      const overlaySize = sizeInfo.perOverlay[overlayId] || { features: 0, bytes: 0 };

      if (features.length > 0 || years.length > 0) {
        // Compute overall time range from loadedRanges
        let rangeStart = null;
        let rangeEnd = null;
        if (ranges.length > 0) {
          rangeStart = Math.min(...ranges.map(r => r.start));
          rangeEnd = Math.max(...ranges.map(r => r.end));
        }

        stats.overlays[overlayId] = {
          features: features.length,
          sizeMB: (overlaySize.bytes / (1024 * 1024)).toFixed(2),
          yearsLoaded: years.length,
          years: years,
          yearRange: years.length > 0 ? `${years[0]}-${years[years.length - 1]}` : 'none',
          ranges: ranges,
          rangeStart: rangeStart,
          rangeEnd: rangeEnd
        };

        stats.totals.features += features.length;
        stats.totals.bytes += overlaySize.bytes;
        stats.totals.yearsLoaded += years.length;
        stats.totals.overlaysActive++;
      }
    }

    stats.totals.sizeMB = (stats.totals.bytes / (1024 * 1024)).toFixed(2);

    console.table(stats.overlays);
    console.log(`Total: ${stats.totals.features} features across ${stats.totals.yearsLoaded} year-loads (${stats.totals.sizeMB} MB)`);

    return stats;
  },

  /**
   * Get current filter settings for an overlay.
   * Returns active overrides merged with defaults from OVERLAY_ENDPOINTS.
   * @param {string} overlayId - Overlay ID
   * @returns {Object} Current filter settings
   */
  getActiveFilters(overlayId) {
    const config = OVERLAY_ENDPOINTS[overlayId];
    if (!config) return {};

    // Start with defaults from config
    const filters = {};

    // Map API params to user-friendly filter names
    if (config.params.min_magnitude) {
      filters.minMagnitude = parseFloat(config.params.min_magnitude);
    }
    if (config.params.min_category) {
      filters.minCategory = config.params.min_category;
    }
    if (config.params.min_scale) {
      filters.minScale = config.params.min_scale;
    }
    if (config.params.min_area_km2) {
      filters.minAreaKm2 = parseFloat(config.params.min_area_km2);
    }

    // Override with active filter settings (from chat-based changes)
    const overrides = activeFilters[overlayId] || {};
    return { ...filters, ...overrides };
  },

  /**
   * Update filter settings for an overlay.
   * Triggers cache clear and data reload.
   * @param {string} overlayId - Overlay ID
   * @param {Object} newFilters - New filter values to apply
   */
  updateFilters(overlayId, newFilters) {
    if (!OVERLAY_ENDPOINTS[overlayId]) {
      console.warn(`Unknown overlay: ${overlayId}`);
      return;
    }

    // Merge with existing overrides
    activeFilters[overlayId] = {
      ...(activeFilters[overlayId] || {}),
      ...newFilters
    };

    console.log(`OverlayController: Updated filters for ${overlayId}:`, activeFilters[overlayId]);
  },

  /**
   * Clear filter overrides for an overlay (revert to defaults).
   * @param {string} overlayId - Overlay ID
   */
  clearFilters(overlayId) {
    delete activeFilters[overlayId];
    console.log(`OverlayController: Cleared filters for ${overlayId}`);
  },

  /**
   * Reload an overlay with current filter settings.
   * Clears cache and refetches data.
   * @param {string} overlayId - Overlay ID
   */
  async reloadOverlay(overlayId) {
    if (!OVERLAY_ENDPOINTS[overlayId]) {
      console.warn(`Unknown overlay: ${overlayId}`);
      return;
    }

    console.log(`OverlayController: Reloading ${overlayId} with filters:`, this.getActiveFilters(overlayId));

    // Clear cache for this overlay
    delete dataCache[overlayId];
    delete loadedYears[overlayId];
    delete loadedRanges[overlayId];
    delete yearRangeCache[overlayId];

    // Check if overlay is currently active
    const isActive = OverlaySelector?.getActiveOverlays()?.includes(overlayId);
    if (!isActive) {
      console.log(`OverlayController: ${overlayId} not active, skipping reload`);
      return;
    }

    // Reload the overlay
    await this.loadOverlay(overlayId);
  },

  /**
   * Refresh all active overlays with new data since last fetch.
   * Called by live-data-poll (every 5 min) and live-lock-engaged events.
   * Only fetches the delta (from last loaded end to now), not the full 30 days.
   */
  async refreshLiveOverlays() {
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
    if (activeOverlays.length === 0) return;

    const FIVE_MIN = 5 * 60 * 1000;
    const now = Math.floor(Date.now() / FIVE_MIN) * FIVE_MIN;

    console.log(`OverlayController: Live refresh for ${activeOverlays.length} overlays`);

    for (const overlayId of activeOverlays) {
      const endpoint = OVERLAY_ENDPOINTS[overlayId];
      if (!endpoint || endpoint.isWeatherGrid) continue;

      // Skip if no ranges loaded yet (overlay hasn't done initial load)
      const ranges = loadedRanges[overlayId];
      if (!ranges || ranges.length === 0) continue;

      // Find the latest end time across all loaded ranges
      const lastEnd = Math.max(...ranges.filter(r => !r.loading).map(r => r.end));
      if (now <= lastEnd) {
        // Already up to date (within same 5-min window)
        continue;
      }

      console.log(`OverlayController: Refreshing ${overlayId} delta (${new Date(lastEnd).toISOString()} to ${new Date(now).toISOString()})`);

      try {
        const loaded = await loadRangeData(overlayId, lastEnd, now);
        if (loaded !== false) {
          this.renderCurrentData(overlayId);
        }
      } catch (err) {
        console.warn(`OverlayController: Live refresh failed for ${overlayId}:`, err.message);
      }
    }
  },

  /**
   * Ingest order result data into the overlay cache.
   * Called by the order/chat system when a disaster data order completes.
   * Merges the GeoJSON features into existing cache and re-renders.
   * @param {string} overlayId - Overlay ID (e.g., 'earthquakes', 'hurricanes')
   * @param {Object} geojson - GeoJSON FeatureCollection from the order result
   * @param {Object} rangeMeta - Optional range metadata {start, end} in ms
   */
  ingestOrderResult(overlayId, geojson, rangeMeta = null) {
    if (!geojson?.features || !OVERLAY_ENDPOINTS[overlayId]) {
      console.warn(`OverlayController: Cannot ingest - invalid data or unknown overlay: ${overlayId}`);
      return;
    }

    // Initialize cache if needed
    if (!dataCache[overlayId]) {
      dataCache[overlayId] = { type: 'FeatureCollection', features: [] };
    }

    // Merge new features (dedup by event_id)
    const existingIds = new Set(
      dataCache[overlayId].features
        .map(f => f.properties?.event_id || f.properties?.storm_id || f.id)
        .filter(Boolean)
    );

    const newFeatures = geojson.features.filter(f => {
      const id = f.properties?.event_id || f.properties?.storm_id || f.id;
      return !id || !existingIds.has(id);
    });

    if (newFeatures.length > 0) {
      dataCache[overlayId].features.push(...newFeatures);
      console.log(`OverlayController: Ingested ${newFeatures.length} ${overlayId} features from order (total: ${dataCache[overlayId].features.length})`);
    } else {
      console.log(`OverlayController: Order result had no new ${overlayId} features (all duplicates)`);
    }

    // Track the loaded range if metadata provided
    if (rangeMeta && rangeMeta.start && rangeMeta.end) {
      if (!loadedRanges[overlayId]) {
        loadedRanges[overlayId] = [];
      }
      loadedRanges[overlayId].push({ start: rangeMeta.start, end: rangeMeta.end, loading: false });

      // Update loadedYears for compatibility
      if (!loadedYears[overlayId]) {
        loadedYears[overlayId] = new Set();
      }
      const startYear = new Date(rangeMeta.start).getFullYear();
      const endYear = new Date(rangeMeta.end).getFullYear();
      for (let y = startYear; y <= endYear; y++) {
        loadedYears[overlayId].add(y);
      }
    }

    // Update cache size
    const cacheSize = calculateCacheSize();
    window.dispatchEvent(new CustomEvent('overlayCacheUpdated', { detail: cacheSize }));

    // Re-render if overlay is active
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
    if (activeOverlays.includes(overlayId)) {
      this.renderCurrentData(overlayId);
    }
  }
};
