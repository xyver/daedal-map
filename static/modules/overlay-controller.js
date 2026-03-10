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
import {
  calculateCacheSize,
  dataCache,
  displayedYear,
  loadedRanges,
  loadedYears,
  yearRangeCache
} from './overlay-cache.js';
import {
  clearAllOverlayCaches,
  clearMetricCacheEntry,
  clearOverlayData,
  clearOverlayFilters,
  getActiveFiltersForOverlay,
  getCachedData as getOverlayCachedData,
  getCachedMetricData as getOverlayCachedMetricData,
  getCacheStats as getOverlayCacheStats,
  getLoadedFiltersForOverlay,
  getLoadedYearsForOverlay,
  ingestMetricData as ingestOverlayMetricData,
  refreshGeometryFromCache as refreshCachedGeometry,
  removeEventData as removeOverlayEventData,
  removeGeometryData as removeOverlayGeometryData,
  removeMetricData as removeOverlayMetricData,
  renderGeometryData as renderOverlayGeometryData,
  updateOverlayFilters
} from './overlay-cache-ops.js';
import { loadRangeData, loadWeatherYearData } from './overlay-data-loader.js';
import {
  addAnimateTrackButton as addHurricaneTrackButton,
  drillDownHurricane as showHurricaneTrackDetail,
  exitTrackDrillDown,
  exitTrackView as exitHurricaneTrackView,
  handleHurricaneDrillDown as runHurricaneDrillDown,
  hideHurricaneOverlay,
  restoreHurricaneOverlay,
  setupTrackPositionClickHandler as bindTrackPositionClickHandler,
  showHurricanePopup,
  stopHurricaneRollingAnimation as stopRollingHurricanes,
  startTrackAnimation as startHurricaneTrackAnimation
} from './overlay-hurricane.js';
import { addGenericExitButton } from './overlay-disaster-common.js';
import {
  exitFireAnimation,
  exitWildfireImpact,
  exitWildfirePerimeter,
  handleFireAnimation as runFireAnimation,
  handleFireProgression as runFireProgression,
  handleWildfireImpact as runWildfireImpact,
  handleWildfirePerimeter as runWildfirePerimeter
} from './overlay-wildfire.js';
import {
  exitFloodAnimation,
  exitFloodImpact,
  handleFloodAnimation as runFloodAnimation,
  handleFloodImpact as runFloodImpact
} from './overlay-flood.js';
import {
  exitTornadoPointAnimation,
  handleTornadoPointAnimation as runTornadoPointAnimation,
  handleTornadoSequence as runTornadoSequence
} from './overlay-tornado.js';
import {
  handleTsunamiRunups as runTsunamiRunups
} from './overlay-tsunami.js';
import {
  handleSequenceChange as runSequenceChange
} from './overlay-earthquake.js';
import {
  exitVolcanoImpact,
  handleVolcanoImpact as runVolcanoImpact
} from './overlay-volcano.js';
import { fetchMsgpack } from './utils/fetch.js';
import { WeatherGridModel, setDependencies as setWeatherGridDeps } from './models/model-weather-grid.js';
import { GeometryModel } from './models/model-geometry.js';

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

  // Pending geometry data to render when geography overlay is enabled
  // Format: { geojson, geometryType, options }
  pendingGeometry: null,

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
    runTsunamiRunups(this, data, { EventAnimator, MapAdapter, TimeSlider, dataCache, yearRangeCache });
  },

  /**
   * Handle tornado sequence animation.
   * Uses EventAnimator with TORNADO_SEQUENCE mode for progressive track drawing.
   * @param {Object} data - { geojson, seedEventId, sequenceCount }
   */
  handleTornadoSequence(data) {
    runTornadoSequence(this, data, {
      EventAnimator,
      MapAdapter,
      TimeSlider,
      dataCache,
      yearRangeCache,
      onFallbackPointAnimation: (payload) => this.handleTornadoPointAnimation(payload)
    });
  },

  /**
   * Handle point-only tornado animation.
   * For tornadoes without track data - zooms in, shows circle based on EF scale,
   * with TimeSlider-driven animation showing the tornado's duration.
   * @param {Object} data - { eventId, latitude, longitude, scale, timestamp }
   */
  handleTornadoPointAnimation(data) {
    runTornadoPointAnimation(this, data, { MapAdapter, TimeSlider });
  },

  /**
   * Exit tornado point animation and cleanup.
   * @private
   */
  _exitTornadoPointAnimation() {
    exitTornadoPointAnimation(this, { MapAdapter, TimeSlider });
  },

  /**
   * Handle flood animation - shows flood polygon with opacity fade over duration.
   * At flood start time, outline appears. Over the duration, opacity increases.
   * @param {Object} data - { geometry, eventId, durationDays, startTime, endTime, latitude, longitude, eventName }
   */
  handleFloodAnimation(data) {
    runFloodAnimation(this, data, { MapAdapter, TimeSlider, ModelRegistry, dataCache });
  },

  /**
   * Handle volcano impact radius animation.
   * Shows felt and damage radii expanding from the volcano center.
   */
  handleVolcanoImpact(data) {
    runVolcanoImpact(this, data, { MapAdapter });
  },

  /**
   * Exit volcano impact animation and cleanup.
   * @private
   */
  _exitVolcanoImpact() {
    exitVolcanoImpact(this, { MapAdapter });
  },

  /**
   * Handle wildfire impact animation (area circle fallback).
   * Shows a circle representing the burned area.
   */
  handleWildfireImpact(data) {
    runWildfireImpact(this, data, { MapAdapter, ModelRegistry, dataCache });
  },

  _exitWildfireImpact(skipRestore = false) {
    exitWildfireImpact(this, { MapAdapter, ModelRegistry, dataCache }, skipRestore);
  },

  /**
   * Handle wildfire perimeter animation (single shape fade-in).
   * Shows the fire perimeter polygon fading in.
   */
  handleWildfirePerimeter(data) {
    runWildfirePerimeter(this, data, { MapAdapter, ModelRegistry, dataCache });
  },

  _exitWildfirePerimeter(skipRestore = false) {
    exitWildfirePerimeter(this, { MapAdapter, ModelRegistry, dataCache }, skipRestore);
  },

  /**
   * Handle flood impact animation (area circle fallback).
   * Shows a circle representing the flooded area.
   */
  handleFloodImpact(data) {
    runFloodImpact(this, data, { MapAdapter, ModelRegistry, dataCache });
  },

  _exitFloodImpact(skipRestore = false) {
    exitFloodImpact(this, { MapAdapter, ModelRegistry, dataCache }, skipRestore);
  },

  /**
   * Generic exit button helper.
   * @private
   */
  _addGenericExitButton(id, text, color, onExit) {
    addGenericExitButton(id, text, color, onExit);
  },

  /**
   * Exit flood animation and cleanup.
   * @private
   */
  _exitFloodAnimation(skipRestore = false) {
    exitFloodAnimation(this, { MapAdapter, TimeSlider, ModelRegistry, dataCache }, skipRestore);
  },

  /**
   * Handle wildfire animation - animates perimeter polygon opacity over fire duration.
   * Simple Option A: Fade in final perimeter from 0% to 100% over duration_days.
   */
  handleFireAnimation(data) {
    runFireAnimation(this, data, { MapAdapter, TimeSlider, ModelRegistry, dataCache });
  },

  /**
   * Handle fire progression animation with daily snapshots.
   * Shows actual fire spread day-by-day using pre-computed perimeters.
   * @param {Object} data - {snapshots, eventId, totalDays, startTime, latitude, longitude}
   */
  handleFireProgression(data) {
    runFireProgression(this, data, { MapAdapter, TimeSlider, ModelRegistry, dataCache });
  },

  /**
   * Exit fire animation and cleanup.
   * @private
   */
  _exitFireAnimation(skipRestore = false) {
    exitFireAnimation(this, { MapAdapter, TimeSlider, ModelRegistry, dataCache }, skipRestore);
  },

  /**
   * Hide hurricane overlay to focus on a single track animation.
   * Clears the track model layers but preserves the cached data.
   * @private
   */
  _hideHurricaneOverlay() {
    hideHurricaneOverlay({ modelRegistry: ModelRegistry });
  },

  /**
   * Restore hurricane overlay after exiting track drill-down.
   * @private
   */
  _restoreHurricaneOverlay() {
    restoreHurricaneOverlay(this, { dataCache });
  },

  /**
   * Handle hurricane track drill-down animation.
   * Fetches detailed track data and shows animated path.
   * @param {string} stormId - Storm ID
   * @param {string} stormName - Storm name
   * @param {Object} props - Storm properties
   */
  async handleHurricaneDrillDown(stormId, stormName, props) {
    await runHurricaneDrillDown(this, stormId, stormName, {
      mapAdapter: MapAdapter,
      overlayEndpoints: OVERLAY_ENDPOINTS,
      fetcher: fetchMsgpack,
      modelRegistry: ModelRegistry,
      timeSlider: TimeSlider,
      dataCache,
      addExitButton: addGenericExitButton
    });
  },

  /**
   * Cleanup any stray MultiTrackAnimator animations when overlay is disabled.
   * Note: Rolling mode is deprecated - progressive tracks now handled by filterByLifecycle.
   */
  stopHurricaneRollingAnimation() {
    stopRollingHurricanes();
  },

  /**
   * Exit track drill-down and restore hurricane overlay.
   * @private
   */
  _exitTrackDrillDown() {
    exitTrackDrillDown(this, { modelRegistry: ModelRegistry, timeSlider: TimeSlider, dataCache });
  },

  /**
   * Handle aftershock sequence selection/deselection.
   * Fetches full sequence data from API (not filtered by magnitude).
   * Uses unified EventAnimator with EARTHQUAKE mode.
   * @param {string|null} sequenceId - Sequence ID or null to clear
   * @param {string|null} eventId - Optional mainshock event_id for accurate aftershock query
   */
  async handleSequenceChange(sequenceId, eventId = null) {
    await runSequenceChange(this, sequenceId, eventId, {
      EventAnimator,
      ModelRegistry,
      OverlaySelector,
      OVERLAY_ENDPOINTS,
      TimeSlider,
      dataCache,
      yearRangeCache,
      gardnerKnopoffTimeWindow,
      fetchMsgpack
    });
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
      await loadRangeData(overlayId, yearStart, yearEnd, OVERLAY_ENDPOINTS[overlayId]);
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
      await loadRangeData(overlayId, yearStart, yearEnd, OVERLAY_ENDPOINTS[overlayId]);
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
    const loaded = await loadRangeData(overlayId, yearStart, yearEnd, OVERLAY_ENDPOINTS[overlayId]);

    // Check if overlay is still active
    const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
    if (!activeOverlays.includes(overlayId)) return;

    // After loading, always render with CURRENT time (not the timestamp that triggered load)
    // This fixes gaps during fast playback where animation moves while data is loading
    if (useLifecycleFiltering && TimeSlider?.currentTime) {
      const currentTimestamp = TimeSlider.currentTime;
      this.renderFilteredData(overlayId, currentTimestamp, { useTimestamp: true });

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
   * Handle overlay toggle event.
   * @param {string} overlayId - Overlay ID (e.g., 'earthquakes')
   * @param {boolean} isActive - Whether overlay is now active
   */
  async handleOverlayChange(overlayId, isActive) {
    console.log(`OverlayController: ${overlayId} ${isActive ? 'ON' : 'OFF'}`);

    // Demographics controls choropleth visibility AND loads countries
    // Note: Can coexist with geometry overlays (separate layer systems)
    if (overlayId === 'demographics') {
      if (isActive) {
        // Load countries if choropleth layers don't exist yet
        const choroplethLayerExists = MapAdapter?.map?.getLayer('regions-fill');
        if (!choroplethLayerExists) {
          const App = window.App;
          if (App && typeof App.loadCountries === 'function') {
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

    // Geography overlay controls geometry layers (ZCTA, tribal, watersheds, etc.)
    // These are rendered via GeometryModel with type-specific layers
    // Handle both the category toggle ('geography') and individual toggles ('zip_codes', etc.)
    // Note: Can coexist with demographics (separate layer systems)
    const geometryOverlayIds = ['geography', 'zip_codes', 'tribal_areas', 'watersheds', 'parks'];
    if (geometryOverlayIds.includes(overlayId)) {
      if (MapAdapter?.map) {
        if (isActive) {
          // If there's pending geometry data, store it in cache first
          if (this.pendingGeometry) {
            const { geojson, geometryType, sourceId, options } = this.pendingGeometry;
            console.log(`OverlayController: Storing pending geometry (${geometryType}, ${geojson.features?.length || 0} features)`);
            this.renderGeometryData(sourceId, geojson, geometryType, options);
            this.pendingGeometry = null;
          }
          // Render all geometry from cache
          this.refreshGeometryFromCache();
          // Show all geometry layers
          const geometryTypes = ['zcta', 'tribal', 'watershed', 'park', 'geometry'];
          for (const geoType of geometryTypes) {
            const fillId = `${geoType}-geometry-fill`;
            const strokeId = `${geoType}-geometry-stroke`;
            const labelId = `${geoType}-geometry-label`;
            if (MapAdapter.map.getLayer(fillId)) {
              MapAdapter.map.setLayoutProperty(fillId, 'visibility', 'visible');
            }
            if (MapAdapter.map.getLayer(strokeId)) {
              MapAdapter.map.setLayoutProperty(strokeId, 'visibility', 'visible');
            }
            if (MapAdapter.map.getLayer(labelId)) {
              MapAdapter.map.setLayoutProperty(labelId, 'visibility', 'visible');
            }
          }
          console.log(`OverlayController: Geography layers shown`);
        } else {
          // Hide all geometry layers
          const geometryTypes = ['zcta', 'tribal', 'watershed', 'park', 'geometry'];
          for (const geoType of geometryTypes) {
            const fillId = `${geoType}-geometry-fill`;
            const strokeId = `${geoType}-geometry-stroke`;
            const labelId = `${geoType}-geometry-label`;
            if (MapAdapter.map.getLayer(fillId)) {
              MapAdapter.map.setLayoutProperty(fillId, 'visibility', 'none');
            }
            if (MapAdapter.map.getLayer(strokeId)) {
              MapAdapter.map.setLayoutProperty(strokeId, 'visibility', 'none');
            }
            if (MapAdapter.map.getLayer(labelId)) {
              MapAdapter.map.setLayoutProperty(labelId, 'visibility', 'none');
            }
          }
          console.log(`OverlayController: Geography layers hidden`);
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
    await loadRangeData(overlayId, yearStart, yearEnd, OVERLAY_ENDPOINTS[overlayId]);

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
            loadRangeData(overlayId, lastEnd, now, OVERLAY_ENDPOINTS[overlayId]).then(loaded => {
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
      const loaded = await loadRangeData(overlayId, startMs, endMs, endpoint, abortController.signal);

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
    showHurricanePopup(props, coords, {
      map: MapAdapter?.map,
      onViewTrack: (stormId, stormName) => this.drillDownHurricane(stormId, stormName)
    });
  },

  /**
   * Drill down into a hurricane track for animation.
   * Uses global IBTrACS API endpoint.
   * @param {string} stormId - Storm ID (e.g., "2005236N23285" for Katrina)
   * @param {string} stormName - Storm name
   */
  async drillDownHurricane(stormId, stormName) {
    try {
      await showHurricaneTrackDetail(stormId, stormName, {
        fetcher: fetchMsgpack,
        hideHurricaneOverlay: () => this._hideHurricaneOverlay(),
        modelRegistry: ModelRegistry,
        onAddAnimateTrackButton: (nextStormId, nextStormName, positions) => this._addAnimateTrackButton(nextStormId, nextStormName, positions),
        onSetCurrentTrackData: (trackData) => {
          this._currentTrackData = trackData;
        },
        onSetupTrackPositionClickHandler: (trackModel) => this._setupTrackPositionClickHandler(trackModel)
      });
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
    addHurricaneTrackButton(stormId, stormName, positions, {
      onExitTrackView: () => this._exitTrackView(),
      onStartTrackAnimation: (nextStormId, nextStormName, nextPositions) => this._startTrackAnimation(nextStormId, nextStormName, nextPositions)
    });
  },

  /**
   * Exit track view and return to yearly storm overview.
   * @private
   */
  _exitTrackView() {
    exitHurricaneTrackView({
      modelRegistry: ModelRegistry,
      onRestoreHurricaneOverlay: () => this._restoreHurricaneOverlay(),
      onSetCurrentTrackData: (trackData) => {
        this._currentTrackData = trackData;
      }
    });
  },

  /**
   * Start track animation using TrackAnimator.
   * @private
   */
  _startTrackAnimation(stormId, stormName, positions) {
    startHurricaneTrackAnimation(stormId, stormName, positions, {
      modelRegistry: ModelRegistry,
      onReloadTrack: (nextStormId, nextStormName) => this.drillDownHurricane(nextStormId, nextStormName)
    });
  },

  /**
   * Setup click handler for track position dots.
   * Shows wind radii and popup when clicking on a position.
   * @private
   */
  _setupTrackPositionClickHandler(trackModel) {
    bindTrackPositionClickHandler(trackModel, {
      map: MapAdapter?.map,
      currentHandler: this._trackPositionClickHandler,
      onSetHandler: (handler) => {
        this._trackPositionClickHandler = handler;
      }
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
    return getOverlayCachedData(overlayId);
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
    clearAllOverlayCaches();
  },

  /**
   * Get loaded years for an overlay.
   * @param {string} overlayId - Overlay ID
   * @returns {Array} Array of loaded years
   */
  getLoadedYears(overlayId) {
    return getLoadedYearsForOverlay(overlayId);
  },

  /**
   * Get the filter thresholds that were used when loading data.
   * This tells chat what data is actually in cache vs what's currently displayed.
   * Example: loaded M5.0+ but displaying M6.0+ - can filter to M5.5+ from cache.
   * @param {string} overlayId - Overlay ID
   * @returns {Object} Filter thresholds used at load time
   */
  getLoadedFilters(overlayId) {
    return getLoadedFiltersForOverlay(overlayId);
  },

  /**
   * Get cache statistics for monitoring memory usage.
   * Call from console: OverlayController.getCacheStats()
   * @returns {Object} Cache statistics
   */
  getCacheStats() {
    return getOverlayCacheStats(OVERLAY_ENDPOINTS);
  },

  /**
   * Get current filter settings for an overlay.
   * Returns active overrides merged with defaults from OVERLAY_ENDPOINTS.
   * @param {string} overlayId - Overlay ID
   * @returns {Object} Current filter settings
   */
  getActiveFilters(overlayId) {
    return getActiveFiltersForOverlay(overlayId, OVERLAY_ENDPOINTS);
  },

  /**
   * Update filter settings for an overlay.
   * Triggers cache clear and data reload.
   * @param {string} overlayId - Overlay ID
   * @param {Object} newFilters - New filter values to apply
   */
  updateFilters(overlayId, newFilters) {
    updateOverlayFilters(overlayId, newFilters, OVERLAY_ENDPOINTS);
  },

  /**
   * Clear filter overrides for an overlay (revert to defaults).
   * @param {string} overlayId - Overlay ID
   */
  clearFilters(overlayId) {
    clearOverlayFilters(overlayId);
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
    clearOverlayData(overlayId);

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
        const loaded = await loadRangeData(overlayId, lastEnd, now, endpoint);
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
  },

  /**
   * Ingest metrics/choropleth data from order system into cache.
   * Called by the chat system when a metrics order completes.
   * @param {string} sourceId - Source ID (e.g., 'owid_co2', 'census_population')
   * @param {Object} geojson - GeoJSON FeatureCollection from the order result
   * @param {Object} yearData - Optional year_data for multi-year results
   * @param {Object} yearRange - Optional year range metadata {min, max, available_years}
   */
  ingestMetricData(sourceId, geojson, yearData = null, yearRange = null) {
    ingestOverlayMetricData(sourceId, geojson, yearData, yearRange);
  },

  /**
   * Get cached metric data for a source.
   * @param {string} sourceId - Source ID
   * @returns {Object|null} Cached data or null if not cached
   */
  getCachedMetricData(sourceId) {
    return getOverlayCachedMetricData(sourceId);
  },

  /**
   * Clear metric data for a source from cache.
   * @param {string} sourceId - Source ID to clear
   */
  clearMetricCache(sourceId) {
    clearMetricCacheEntry(sourceId);
  },

  // -------------------------------------------------------------------------
  // Geometry Cache (for geometry orders - ZCTA, tribal, watersheds, etc.)
  // Deduplicates by loc_id, similar to event_id dedup for events.
  // -------------------------------------------------------------------------
  // Geometry Order Rendering
  // Note: Backend SessionCache handles deduplication. Frontend just renders.
  // -------------------------------------------------------------------------

  /**
   * Render geometry data from a chat order (ZCTA, tribal, etc.)
   * Backend SessionCache handles deduplication - frontend just renders.
   * @param {string} sourceId - Source ID (e.g., 'geometry_zcta')
   * @param {Object} geojson - GeoJSON FeatureCollection from the order result
   * @param {string} geometryType - Geometry type for rendering ('zcta', 'tribal', etc.)
   * @param {Object} options - Render options (showLabels, etc.)
   * @returns {number} Number of features rendered
   */
  renderGeometryData(sourceId, geojson, geometryType = 'zcta', options = {}) {
    return renderOverlayGeometryData(sourceId, geojson, geometryType, options);
  },

  /**
   * Refresh geometry display from cache.
   * Called when overlay is turned on or when new data arrives while overlay is already on.
   */
  refreshGeometryFromCache() {
    refreshCachedGeometry();
  },

  /**
   * Remove geometry features from cache and re-render.
   * Supports two removal modes (backend-driven preferred):
   * 1. loc_ids: Exact list from backend (keeps caches in sync)
   * 2. regions: Prefix-based removal (fallback)
   *
   * @param {string} sourceId - Source ID (e.g., 'geometry_zcta')
   * @param {Object} criteria - Removal criteria
   * @param {Array} [criteria.loc_ids] - Specific loc_ids to remove (preferred, from backend)
   * @param {Array} [criteria.regions] - Regions to remove by prefix (e.g., ['USA-FL'])
   * @param {string} geometryType - Geometry type for rendering ('zcta', 'tribal', etc.)
   * @returns {Object} { removed: number, remaining: number }
   */
  removeGeometryData(sourceId, criteria, geometryType = 'zcta') {
    return removeOverlayGeometryData(sourceId, criteria, geometryType);
  },

  /**
   * Remove event data from cache by event_ids.
   * Like removing rows from a feature collection.
   *
   * @param {string} sourceId - Source ID (e.g., 'earthquakes_usgs')
   * @param {Object} criteria - Removal criteria
   * @param {Array} [criteria.event_ids] - Specific event_ids to remove
   * @param {Array} [criteria.regions] - Regions to remove by loc_id prefix
   * @returns {Object} { removed: number, remaining: number }
   */
  removeEventData(sourceId, criteria) {
    return removeOverlayEventData(sourceId, criteria);
  },

  /**
   * Remove metric data from cache - like deleting a column from a spreadsheet.
   * Removes all values for a specific metric, optionally filtered by region/years.
   *
   * @param {string} sourceId - Source ID (e.g., 'census')
   * @param {Object} criteria - Removal criteria
   * @param {Array} [criteria.loc_ids] - Specific loc_ids to remove from
   * @param {Array} [criteria.years] - Specific years to remove from
   * @param {string} [criteria.metric] - Metric column to remove
   * @returns {Object} { removed: number, remaining: number }
   */
  removeMetricData(sourceId, criteria) {
    return removeOverlayMetricData(sourceId, criteria);
  },

  /**
   * Clear geometry display for a specific type.
   * @param {string} geometryType - Geometry type for layer cleanup (zcta, tribal, etc.)
   */
  clearGeometryDisplay(geometryType = 'zcta') {
    GeometryModel.clearType(geometryType);
    console.log(`OverlayController: Cleared ${geometryType} geometry display`);
  }
};
