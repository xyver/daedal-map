/**
 * Map Adapter - Abstraction layer for map library.
 * Swap this module to change map libraries (MapLibre, Leaflet, deck.gl, etc.)
 */

import { CONFIG } from './config.js';
import { LocationInfoCache } from './cache.js';
import { PointRadiusModel } from './models/model-point-radius.js';
import { TrackModel } from './models/model-track.js';
import { fetchMsgpack } from './utils/fetch.js';

// Dependencies set via setDependencies to avoid circular imports
let ViewportLoader = null;
let NavigationManager = null;
let App = null;
let PopupBuilder = null;
let OverlayController = null;

export function setDependencies(deps) {
  ViewportLoader = deps.ViewportLoader;
  NavigationManager = deps.NavigationManager;
  App = deps.App;
  PopupBuilder = deps.PopupBuilder;
  OverlayController = deps.OverlayController;
}

// ============================================================================
// MAP ADAPTER - Abstraction layer for map library
// ============================================================================

export const MapAdapter = {
  map: null,
  popup: null,
  hoveredFeatureId: null,
  popupLocked: false,  // When true, popup stays visible on mouseleave
  isShowingPopup: false,  // True while showing popup (prevents close event from unlocking)
  handlersSetup: false,  // Track if event handlers have been added
  lastZoom: null,
  citiesLoaded: false,
  currentStateLocId: null,
  currentRegionGeojson: null,  // Store current regions for parent outline
  focusedParentId: null,  // Parent ID of focal area (center of viewport)
  clickTimeout: null,  // Timer to distinguish single vs double click
  pendingClickFeature: null,  // Feature from pending single click

  /**
   * Initialize the map
   */
  init() {
    this.map = new maplibregl.Map({
      container: 'map',
      style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      center: CONFIG.defaultCenter,
      zoom: CONFIG.defaultZoom,
      doubleClickZoom: false  // Disable default double-click zoom
    });

    // Create popup instance - compact sizing, no fixed width
    this.popup = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: false,
      maxWidth: 'none'  // Let content determine width naturally
    });

    // Unlock popup when close button is clicked (but not when we're just re-showing)
    this.popup.on('close', () => {
      // Only unlock if this is a real close (user clicked X), not a re-show
      if (!this.isShowingPopup) {
        this.popupLocked = false;
      }
    });

    // Globe projection disabled - using flat mercator for smoother panning
    // To re-enable globe: uncomment the enableGlobe() call below
    // this.map.on('style.load', () => {
    //   this.enableGlobe();
    // });

    // Setup zoom-based navigation
    this.map.on('zoomend', () => this.handleZoomChange());
    this.map.on('zoom', () => this.updateZoomDisplay(this.map.getZoom()));

    // Setup viewport-based loading events
    this.map.on('zoomend', () => ViewportLoader?.onZoomEnd());
    this.map.on('moveend', () => ViewportLoader?.onMoveEnd());

    return new Promise((resolve) => {
      this.map.on('load', () => {
        console.log('Map loaded');
        this.lastZoom = this.map.getZoom();
        this.updateZoomDisplay(this.lastZoom);
        resolve();
      });
    });
  },

  /**
   * Handle zoom changes - update display
   * Navigation is now handled by ViewportLoader.onViewportChange()
   * Globe projection switching disabled - use mercator only for stability
   */
  handleZoomChange() {
    const currentZoom = this.map.getZoom();
    this.updateZoomDisplay(currentZoom);
    this.lastZoom = currentZoom;
  },

  /**
   * Update the zoom level display
   */
  updateZoomDisplay(zoom) {
    const zoomEl = document.getElementById('zoomLevel');
    if (zoomEl) {
      zoomEl.textContent = `Zoom: ${zoom.toFixed(1)}`;
    }
  },


  /**
   * Navigate up one level in hierarchy
   */
  navigateUp() {
    // Check if we can go up and if navigation isn't already in progress
    if (NavigationManager?.path.length <= 1) {
      console.log('Already at world level, cannot go up');
      return;
    }
    if (NavigationManager?.isNavigating) {
      console.log('Navigation in progress, skipping navigateUp');
      return;
    }

    // Temporarily disable zoom navigation to prevent loops
    this.zoomNavigationEnabled = false;

    const targetIndex = NavigationManager.path.length - 2;
    console.log(`Navigating up to index ${targetIndex}: ${NavigationManager.path[targetIndex].name}`);
    NavigationManager.navigateTo(targetIndex);

    // Re-enable after navigation completes
    setTimeout(() => {
      this.zoomNavigationEnabled = true;
      this.lastZoom = this.map.getZoom();
    }, 1500);
  },

  /**
   * Drill down into the currently hovered feature
   */
  drillDownHovered() {
    if (this.hoveredFeatureId === null) return;
    if (NavigationManager?.isNavigating) {
      console.log('Navigation in progress, skipping drillDownHovered');
      return;
    }

    // Get the hovered feature's properties
    const features = this.map.querySourceFeatures(CONFIG.layers.source, {
      filter: ['==', ['id'], this.hoveredFeatureId]
    });

    if (features.length > 0) {
      const feature = features[0];
      const locId = feature.properties.loc_id;
      const name = feature.properties.name || 'Unknown';

      if (locId) {
        // Temporarily disable zoom navigation
        this.zoomNavigationEnabled = false;

        App?.drillDown(locId, name);

        // Re-enable after navigation completes
        setTimeout(() => {
          this.zoomNavigationEnabled = true;
          this.lastZoom = this.map.getZoom();
        }, 1500);
      }
    }
  },

  /**
   * Enable globe projection (3D sphere view)
   */
  enableGlobe() {
    try {
      this.map.setProjection({ type: 'globe' });
      console.log('Globe projection enabled');

      // Add space/atmosphere effect
      this.map.setSky({
        'sky-color': '#000011',           // Deep space blue-black
        'horizon-color': '#000033',       // Slightly lighter at horizon
        'fog-color': '#000011',           // Match space color
        'fog-ground-blend': 0.5,
        'atmosphere-blend': ['interpolate', ['linear'], ['zoom'], 0, 1, 5, 0.5, 10, 0]
      });

      // Add atmosphere glow around the globe
      this.map.setFog({
        'color': 'rgb(20, 30, 50)',        // Dark blue-gray fog
        'high-color': 'rgb(10, 15, 30)',   // Darker at high altitudes
        'horizon-blend': 0.1,
        'space-color': 'rgb(5, 5, 15)',    // Deep space color
        'star-intensity': 0.3              // Subtle stars
      });

    } catch (e) {
      console.log('Globe projection not available:', e.message);
    }
  },

  /**
   * Disable globe projection (switch back to flat mercator)
   */
  disableGlobe() {
    try {
      this.map.setProjection({ type: 'mercator' });
      console.log('Mercator projection enabled');

      // Remove atmosphere effects
      this.map.setSky({});
      this.map.setFog({});

    } catch (e) {
      console.log('Failed to disable globe:', e.message);
    }
  },

  /**
   * Toggle globe projection on/off
   * @param {boolean} enabled - True for globe, false for flat mercator
   */
  toggleGlobe(enabled) {
    if (enabled) {
      this.enableGlobe();
    } else {
      this.disableGlobe();
    }
  },

  // Track satellite mode state
  satelliteMode: false,

  // Map style URLs
  STYLES: {
    dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    satellite: {
      version: 8,
      sources: {
        'satellite': {
          type: 'raster',
          tiles: [
            'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
          ],
          tileSize: 256,
          attribution: 'Tiles: Esri - Source: Esri, Maxar, Earthstar Geographics'
        }
      },
      layers: [
        {
          id: 'satellite-layer',
          type: 'raster',
          source: 'satellite',
          minzoom: 0,
          maxzoom: 19
        }
      ]
    }
  },

  /**
   * Toggle satellite view on/off
   * @param {boolean} enabled - True for satellite, false for dark map
   */
  toggleSatellite(enabled) {
    this.satelliteMode = enabled;
    const style = enabled ? this.STYLES.satellite : this.STYLES.dark;

    // Store current projection state
    const wasGlobeEnabled = this.map.getProjection()?.type === 'globe';

    this.map.setStyle(style);

    // Re-apply projection and overlays after style loads
    this.map.once('style.load', () => {
      // Restore globe if it was enabled
      if (wasGlobeEnabled) {
        this.enableGlobe();
      }

      // Re-render overlays from cache (don't reload data)
      if (OverlayController) {
        OverlayController.rerenderFromCache();
      }

      console.log(`Satellite mode: ${enabled ? 'ON' : 'OFF'}`);
    });
  },

  /**
   * Load GeoJSON data onto the map
   * @param {Object} geojson - GeoJSON FeatureCollection
   * @param {boolean} debugMode - If true, use hierarchy-depth colors
   */
  loadGeoJSON(geojson, debugMode = false) {
    // Add unique IDs to features for hover state
    geojson.features.forEach((feature, index) => {
      feature.id = index;
    });

    // Store current geojson for use as parent outline later
    this.currentRegionGeojson = geojson;

    // Update focused parent based on center of viewport
    this.updateFocusedParent(geojson);

    // Remove existing source and layers
    this.clearLayers();

    // Add source
    this.map.addSource(CONFIG.layers.source, {
      type: 'geojson',
      data: geojson,
      generateId: true
    });

    // Determine fill color based on debug mode or focal coloring
    const fillColor = debugMode
      ? this.getDebugFillColorExpression()
      : this.getFocalFillColorExpression();

    // Determine fill opacity (higher for focal area)
    const fillOpacity = debugMode
      ? [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.colors.fillHoverOpacity,
          CONFIG.colors.fillOpacity
        ]
      : [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.colors.fillHoverOpacity,
          ['==', ['get', 'parent_id'], this.focusedParentId || ''],
          CONFIG.colors.focalFillOpacity,
          CONFIG.colors.fillOpacity
        ];

    // Add fill layer
    this.map.addLayer({
      id: CONFIG.layers.fill,
      type: 'fill',
      source: CONFIG.layers.source,
      paint: {
        'fill-color': fillColor,
        'fill-opacity': fillOpacity
      }
    });

    // Determine stroke color based on focal coloring
    const strokeColor = debugMode
      ? CONFIG.colors.stroke
      : this.getFocalStrokeColorExpression();

    // Add stroke layer
    // Strokes are hidden for focal features (siblings) to avoid internal lines
    this.map.addLayer({
      id: CONFIG.layers.stroke,
      type: 'line',
      source: CONFIG.layers.source,
      paint: {
        'line-color': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.colors.strokeHover,
          strokeColor
        ],
        'line-width': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.colors.strokeHoverWidth,
          CONFIG.colors.strokeWidth
        ],
        'line-opacity': this.getFocalStrokeOpacityExpression()
      }
    });

    // Setup event handlers (only once)
    if (!this.handlersSetup) {
      this.setupEventHandlers();
      this.handlersSetup = true;
    }

    // Update stats
    document.getElementById('totalAreas').textContent = geojson.features.length;
  },

  /**
   * Update just the source data without recreating layers.
   * Much faster than loadGeoJSON - use for time slider updates.
   * @param {Object} geojson - GeoJSON FeatureCollection
   */
  updateSourceData(geojson) {
    // Add unique IDs to features for hover state
    geojson.features.forEach((feature, index) => {
      feature.id = index;
    });

    // Get the existing source and update its data
    const source = this.map.getSource(CONFIG.layers.source);
    if (source) {
      source.setData(geojson);
    }
  },

  /**
   * Load GeoJSON with instant swap for viewport changes.
   * Used by ViewportLoader for zoom-based layer changes.
   * @param {Object} geojson - GeoJSON FeatureCollection
   */
  loadGeoJSONWithFade(geojson) {
    // Add unique IDs to features for hover state
    geojson.features.forEach((feature, index) => {
      feature.id = index;
    });

    // Update focused parent based on center of viewport
    this.updateFocusedParent(geojson);

    const fillLayer = this.map.getLayer(CONFIG.layers.fill);
    const source = this.map.getSource(CONFIG.layers.source);

    if (fillLayer && source) {
      // Instant swap - no fade animation
      source.setData(geojson);

      // Update focal coloring based on new focused parent (skips if debug mode)
      this.updateFocalColors();
    } else {
      // No existing layer - just do a normal load
      this.loadGeoJSON(geojson);
    }

    // Update stats
    document.getElementById('totalAreas').textContent = geojson.features.length;
  },

  /**
   * Get MapLibre expression for debug fill color based on coverage ratio
   * Coverage = actual_depth / expected_depth (0 to 1)
   */
  getDebugFillColorExpression() {
    // Use step expression based on coverage value (0-1)
    return [
      'case',
      ['boolean', ['feature-state', 'hover'], false],
      '#ffffff',  // White on hover for debug mode
      [
        'step',
        ['coalesce', ['get', 'coverage'], 0],
        CONFIG.debugColors.none,   // Default: gray (no data)
        0.01, CONFIG.debugColors.low,    // 0-49%: red
        0.50, CONFIG.debugColors.medium, // 50-74%: orange
        0.75, CONFIG.debugColors.high,   // 75-99%: yellow
        1.0, CONFIG.debugColors.full     // 100%: green
      ]
    ];
  },

  /**
   * Update fill colors based on debug mode
   * @param {boolean} debugMode - Whether debug mode is on
   */
  updateDebugColors(debugMode) {
    if (!this.map.getLayer(CONFIG.layers.fill)) return;

    const fillColor = debugMode
      ? this.getDebugFillColorExpression()
      : [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.colors.fillHover,
          CONFIG.colors.fill
        ];

    this.map.setPaintProperty(CONFIG.layers.fill, 'fill-color', fillColor);
    console.log(`Fill colors updated for ${debugMode ? 'debug' : 'normal'} mode`);
  },

  /**
   * Update the focused parent ID based on the feature at center of viewport
   * @param {Object} geojson - GeoJSON FeatureCollection to search
   */
  updateFocusedParent(geojson) {
    if (!this.map || !geojson || !geojson.features) {
      this.focusedParentId = null;
      return;
    }

    const center = this.map.getCenter();
    const centerLng = center.lng;
    const centerLat = center.lat;

    // Find the feature closest to center (using centroid)
    let closestFeature = null;
    let closestDist = Infinity;

    for (const feature of geojson.features) {
      const props = feature.properties || {};
      const lon = props.centroid_lon;
      const lat = props.centroid_lat;

      if (lon == null || lat == null) continue;

      // Simple euclidean distance (good enough for finding closest)
      const dist = Math.pow(lon - centerLng, 2) + Math.pow(lat - centerLat, 2);
      if (dist < closestDist) {
        closestDist = dist;
        closestFeature = feature;
      }
    }

    // Set the focused parent ID
    if (closestFeature && closestFeature.properties) {
      this.focusedParentId = closestFeature.properties.parent_id || null;
    } else {
      this.focusedParentId = null;
    }
  },

  /**
   * Get MapLibre expression for focal fill color
   * Features matching focusedParentId get green, others get blue
   */
  getFocalFillColorExpression() {
    if (!this.focusedParentId) {
      // No focal parent - use default blue
      return [
        'case',
        ['boolean', ['feature-state', 'hover'], false],
        CONFIG.colors.fillHover,
        CONFIG.colors.fill
      ];
    }

    // Color based on parent_id match
    return [
      'case',
      ['boolean', ['feature-state', 'hover'], false],
      CONFIG.colors.fillHover,
      ['==', ['get', 'parent_id'], this.focusedParentId],
      CONFIG.colors.focalFill,
      CONFIG.colors.fill
    ];
  },

  /**
   * Get MapLibre expression for focal stroke color
   */
  getFocalStrokeColorExpression() {
    if (!this.focusedParentId) {
      return CONFIG.colors.stroke;
    }

    return [
      'case',
      ['==', ['get', 'parent_id'], this.focusedParentId],
      CONFIG.colors.focalStroke,
      CONFIG.colors.stroke
    ];
  },

  /**
   * Get MapLibre expression for focal stroke opacity
   * Focal features (siblings sharing parent_id) get 0 opacity to hide internal lines
   */
  getFocalStrokeOpacityExpression() {
    if (!this.focusedParentId) {
      return 1;  // Full opacity when no focal parent
    }

    return [
      'case',
      ['==', ['get', 'parent_id'], this.focusedParentId],
      0,  // Hide strokes between siblings
      1   // Show strokes for non-focal features
    ];
  },

  /**
   * Update focal coloring based on current focusedParentId
   */
  updateFocalColors() {
    if (!this.map.getLayer(CONFIG.layers.fill)) return;

    // Skip if in debug mode (debug mode has its own coloring via getDebugFillColorExpression)
    if (App?.debugMode) return;

    const fillColor = this.getFocalFillColorExpression();
    const strokeColor = this.getFocalStrokeColorExpression();

    this.map.setPaintProperty(CONFIG.layers.fill, 'fill-color', fillColor);
    this.map.setPaintProperty(CONFIG.layers.stroke, 'line-color', [
      'case',
      ['boolean', ['feature-state', 'hover'], false],
      CONFIG.colors.strokeHover,
      strokeColor
    ]);
  },

  /**
   * Clear all layers and sources
   */
  clearLayers() {
    if (this.map.getLayer(CONFIG.layers.fill)) {
      this.map.removeLayer(CONFIG.layers.fill);
    }
    if (this.map.getLayer(CONFIG.layers.stroke)) {
      this.map.removeLayer(CONFIG.layers.stroke);
    }
    if (this.map.getSource(CONFIG.layers.source)) {
      this.map.removeSource(CONFIG.layers.source);
    }
  },

  /**
   * Show or hide all choropleth/demographics layers.
   * Used when toggling the Demographics overlay.
   * @param {boolean} visible - Whether to show (true) or hide (false)
   */
  setChoroplethVisible(visible) {
    if (!this.map) return;

    const visibility = visible ? 'visible' : 'none';

    // Main choropleth layers
    const choroplethLayers = [
      CONFIG.layers.fill,
      CONFIG.layers.stroke,
      CONFIG.layers.parentFill,
      CONFIG.layers.parentStroke,
      CONFIG.layers.cityCircle,
      CONFIG.layers.cityCircle + '-glow-outer',
      CONFIG.layers.cityCircle + '-glow-mid',
      CONFIG.layers.cityCircle + '-glow-inner',
      CONFIG.layers.cityLabel
    ];

    for (const layerId of choroplethLayers) {
      if (this.map.getLayer(layerId)) {
        this.map.setLayoutProperty(layerId, 'visibility', visibility);
      }
    }

    // Also toggle choropleth legend (use class, not inline style)
    const legend = document.getElementById('choroplethLegend');
    if (legend) {
      if (visible) {
        legend.classList.add('visible');
      } else {
        legend.classList.remove('visible');
      }
    }

    console.log(`MapAdapter: Choropleth layers ${visible ? 'shown' : 'hidden'}`);
  },

  /**
   * Setup mouse and click event handlers
   */
  setupEventHandlers() {
    const fillLayer = CONFIG.layers.fill;

    // Click handler - locks popup and fetches enriched data
    this.map.on('click', fillLayer, async (e) => {
      // Check if click was on an event/overlay layer - if so, skip base layer handling
      // Event layers should take priority over base geometry
      const eventFeatures = this.map.queryRenderedFeatures(e.point, {
        layers: [CONFIG.layers.eventCircle, CONFIG.layers.hurricaneMarker, CONFIG.layers.polygonFill].filter(
          layerId => this.map.getLayer(layerId)
        )
      });
      if (eventFeatures.length > 0) {
        return; // Let event layer handler deal with this click
      }

      if (e.features.length > 0) {
        const feature = e.features[0];
        this.popupLocked = true;
        // Show basic popup immediately
        App?.handleFeatureHover(feature, e.lngLat);
        // Fetch enriched data and update popup
        const locId = feature.properties.loc_id;
        if (locId) {
          const locationInfo = await LocationInfoCache.fetch(locId);
          if (locationInfo && this.popupLocked) {
            // Update popup with enriched data
            const popupHtml = PopupBuilder?.build(feature.properties, App?.currentData, locationInfo);
            this.showPopup([e.lngLat.lng, e.lngLat.lat], popupHtml);
            // Wire up tab click delegation for tabbed popups
            this.setupPopupTabHandlers();
          }
        }
      }
    });

    // Click on map (not on feature) - unlock and hide popup
    this.map.on('click', (e) => {
      // Check if click was on any interactive feature (choropleth or event layer)
      // We need to check multiple layers to avoid interfering with event click handlers
      const fillFeatures = this.map.queryRenderedFeatures(e.point, { layers: [fillLayer] });
      // Only query event-circle layer if it exists
      const eventFeatures = this.map.getLayer('event-circle')
        ? this.map.queryRenderedFeatures(e.point, { layers: ['event-circle'] })
        : [];
      const allFeatures = [...fillFeatures, ...eventFeatures];

      if (allFeatures.length === 0 && this.popupLocked) {
        this.popupLocked = false;
        this.hidePopup();
      }
    });

    // Double-click handler for drill-down - DISABLED (using zoom controls instead)
    // this.map.on('dblclick', fillLayer, (e) => {
    //   e.preventDefault();
    //   if (e.features.length > 0) {
    //     const feature = e.features[0];
    //     this.popupLocked = false;
    //     App?.handleFeatureDrillDown(feature);
    //   }
    // });

    // Hover handlers - show popup on hover (unless locked)
    this.map.on('mousemove', fillLayer, (e) => {
      if (e.features.length > 0) {
        const feature = e.features[0];

        // Reset previous hover state
        if (this.hoveredFeatureId !== null) {
          this.map.setFeatureState(
            { source: CONFIG.layers.source, id: this.hoveredFeatureId },
            { hover: false }
          );
        }

        // Set new hover state
        this.hoveredFeatureId = feature.id;
        this.map.setFeatureState(
          { source: CONFIG.layers.source, id: this.hoveredFeatureId },
          { hover: true }
        );

        this.map.getCanvas().style.cursor = 'pointer';

        // Show popup on hover (only if not locked to another location)
        if (!this.popupLocked) {
          App?.handleFeatureHover(feature, e.lngLat);
        }
      }
    });

    this.map.on('mouseleave', fillLayer, () => {
      if (this.hoveredFeatureId !== null) {
        this.map.setFeatureState(
          { source: CONFIG.layers.source, id: this.hoveredFeatureId },
          { hover: false }
        );
      }
      this.hoveredFeatureId = null;
      this.map.getCanvas().style.cursor = '';
      // Only hide popup if not locked
      if (!this.popupLocked) {
        this.hidePopup();
      }
    });
  },

  /**
   * Show popup at location
   * @param {Array} lngLat - [longitude, latitude]
   * @param {string} html - Popup HTML content
   */
  showPopup(lngLat, html) {
    // Set flag to prevent close event from unlocking
    this.isShowingPopup = true;
    this.popup
      .setLngLat(lngLat)
      .setHTML(html)
      .addTo(this.map);
    // Clear flag after a short delay (after close event would have fired)
    setTimeout(() => {
      this.isShowingPopup = false;
    }, 50);
  },

  /**
   * Hide popup and unlock
   */
  hidePopup() {
    // Set flag so close event doesn't also try to unlock
    this.isShowingPopup = true;
    this.popup.remove();
    this.popupLocked = false;
    setTimeout(() => {
      this.isShowingPopup = false;
    }, 50);
  },

  /**
   * Setup click delegation for popup tab switching.
   */
  setupPopupTabHandlers() {
    const el = this.popup.getElement();
    if (!el) return;
    el.addEventListener('click', (e) => {
      if (!e.target.classList.contains('popup-tab')) return;
      const tabName = e.target.dataset.tab;
      if (!tabName) return;
      // Toggle active tab buttons
      el.querySelectorAll('.popup-tab').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
      });
      // Toggle active content panels
      el.querySelectorAll('.popup-tab-content').forEach(panel => {
        panel.classList.toggle('active', panel.dataset.tab === tabName);
      });
    });
  },

  /**
   * Fly to a location
   * @param {Array} center - [longitude, latitude]
   * @param {number} zoom - Zoom level
   */
  flyTo(center, zoom) {
    this.map.flyTo({
      center: center,
      zoom: zoom,
      duration: 1500
    });
  },

  // Fixed center points for countries with problematic bounding boxes
  countryFixedCenters: {
    'USA': { center: [-98.5, 39.5], zoom: 4 },  // Center of contiguous US
    'RUS': { center: [100, 60], zoom: 3 },      // Russia spans many time zones
    'FJI': { center: [178, -18], zoom: 6 }      // Fiji crosses date line
  },

  /**
   * Fit map to GeoJSON bounds
   * @param {Object} geojson - GeoJSON FeatureCollection
   * @param {Object} options - Optional settings like minZoom
   */
  fitToBounds(geojson, options = {}) {
    if (!geojson || !geojson.features || geojson.features.length === 0) return;

    // Check if this is a single country with a fixed center
    if (geojson.features.length > 1) {
      const firstFeature = geojson.features[0];
      const parentId = firstFeature.properties?.parent_id;
      if (parentId && this.countryFixedCenters[parentId]) {
        const fixed = this.countryFixedCenters[parentId];
        this.map.flyTo({
          center: fixed.center,
          zoom: options.minZoom || fixed.zoom,
          duration: 1000
        });
        return;
      }
    }

    // Calculate bounds from all features
    const bounds = new maplibregl.LngLatBounds();

    geojson.features.forEach(feature => {
      if (feature.geometry) {
        this.extendBoundsWithGeometry(bounds, feature.geometry);
      }
    });

    if (!bounds.isEmpty()) {
      this.map.fitBounds(bounds, {
        padding: 50,
        duration: 1000,
        maxZoom: options.maxZoom || 10,
        minZoom: options.minZoom || undefined
      });
    }
  },

  /**
   * Extend bounds with geometry coordinates
   * @param {LngLatBounds} bounds - MapLibre bounds object
   * @param {Object} geometry - GeoJSON geometry
   */
  extendBoundsWithGeometry(bounds, geometry) {
    const type = geometry.type;
    const coords = geometry.coordinates;

    if (type === 'Point') {
      bounds.extend(coords);
    } else if (type === 'Polygon') {
      coords[0].forEach(coord => bounds.extend(coord));
    } else if (type === 'MultiPolygon') {
      coords.forEach(polygon => {
        polygon[0].forEach(coord => bounds.extend(coord));
      });
    }
  },

  /**
   * Get current map view including bounds and admin level
   * @returns {Object} {center, zoom, bounds, adminLevel}
   */
  getView() {
    const bounds = this.map.getBounds();
    return {
      center: this.map.getCenter(),
      zoom: this.map.getZoom(),
      bounds: {
        west: bounds.getWest(),
        south: bounds.getSouth(),
        east: bounds.getEast(),
        north: bounds.getNorth()
      },
      adminLevel: ViewportLoader?.currentAdminLevel || 0
    };
  },

  /**
   * Load city markers for a location (state or county)
   * @param {string} locId - Location loc_id (e.g., "USA-CA" for state, "USA-CA-06037" for county)
   */
  async loadCityOverlay(locId) {
    // Only load if we're in a US location and haven't already loaded for this location
    if (!locId || !locId.startsWith('USA-') || locId === this.currentStateLocId) {
      return;
    }

    console.log(`Loading city overlay for ${locId}`);
    this.currentStateLocId = locId;

    try {
      // Fetch cities for this location
      const result = await fetchMsgpack(`/geometry/${locId}/places`);
      if (!result.geojson || !result.geojson.features || result.geojson.features.length === 0) {
        console.log('No cities found for', locId);
        return;
      }

      // Remove existing city layers
      this.clearCityOverlay();

      // Add city source
      this.map.addSource(CONFIG.layers.citySource, {
        type: 'geojson',
        data: result.geojson
      });

      // Add outer glow layer (largest, most transparent)
      this.map.addLayer({
        id: CONFIG.layers.cityCircle + '-glow-outer',
        type: 'circle',
        source: CONFIG.layers.citySource,
        minzoom: CONFIG.layers.cityMinZoom,
        paint: {
          'circle-radius': 16,
          'circle-color': '#00ffff',
          'circle-opacity': 0.15,
          'circle-blur': 1
        }
      });

      // Add middle glow layer
      this.map.addLayer({
        id: CONFIG.layers.cityCircle + '-glow-mid',
        type: 'circle',
        source: CONFIG.layers.citySource,
        minzoom: CONFIG.layers.cityMinZoom,
        paint: {
          'circle-radius': 10,
          'circle-color': '#00ffff',
          'circle-opacity': 0.3,
          'circle-blur': 0.8
        }
      });

      // Add inner glow layer
      this.map.addLayer({
        id: CONFIG.layers.cityCircle + '-glow-inner',
        type: 'circle',
        source: CONFIG.layers.citySource,
        minzoom: CONFIG.layers.cityMinZoom,
        paint: {
          'circle-radius': 6,
          'circle-color': '#66ffff',
          'circle-opacity': 0.5,
          'circle-blur': 0.5
        }
      });

      // Add city circle markers (bright center point)
      this.map.addLayer({
        id: CONFIG.layers.cityCircle,
        type: 'circle',
        source: CONFIG.layers.citySource,
        minzoom: CONFIG.layers.cityMinZoom,
        paint: {
          'circle-radius': 3,
          'circle-color': '#ffffff',
          'circle-opacity': 1
        }
      });

      // Add city labels (bright white text for dark maps)
      this.map.addLayer({
        id: CONFIG.layers.cityLabel,
        type: 'symbol',
        source: CONFIG.layers.citySource,
        minzoom: CONFIG.layers.cityMinZoom + 1,
        layout: {
          'text-field': ['get', 'name'],
          'text-size': 12,
          'text-offset': [0, 1.5],
          'text-anchor': 'top',
          'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold']
        },
        paint: {
          'text-color': '#ffffff',
          'text-halo-color': 'rgba(0, 40, 80, 0.8)',
          'text-halo-width': 2
        }
      });

      // Add click handler for cities
      this.map.on('click', CONFIG.layers.cityCircle, (e) => {
        if (e.features.length > 0) {
          const feature = e.features[0];
          const props = feature.properties;
          const name = props.name || 'Unknown City';
          const html = `<strong>${name}</strong><br>Population: ${props.population ? props.population.toLocaleString() : 'N/A'}`;
          this.showPopup([e.lngLat.lng, e.lngLat.lat], html);
        }
      });

      // Hover cursor for cities
      this.map.on('mouseenter', CONFIG.layers.cityCircle, () => {
        this.map.getCanvas().style.cursor = 'pointer';
      });
      this.map.on('mouseleave', CONFIG.layers.cityCircle, () => {
        this.map.getCanvas().style.cursor = '';
      });

      this.citiesLoaded = true;
      console.log(`Loaded ${result.geojson.features.length} cities for ${locId}`);

    } catch (error) {
      console.log('Error loading cities:', error.message);
    }
  },

  /**
   * Clear city overlay layers
   */
  clearCityOverlay() {
    // Remove label layer
    if (this.map.getLayer(CONFIG.layers.cityLabel)) {
      this.map.removeLayer(CONFIG.layers.cityLabel);
    }
    // Remove center circle
    if (this.map.getLayer(CONFIG.layers.cityCircle)) {
      this.map.removeLayer(CONFIG.layers.cityCircle);
    }
    // Remove glow layers
    if (this.map.getLayer(CONFIG.layers.cityCircle + '-glow-inner')) {
      this.map.removeLayer(CONFIG.layers.cityCircle + '-glow-inner');
    }
    if (this.map.getLayer(CONFIG.layers.cityCircle + '-glow-mid')) {
      this.map.removeLayer(CONFIG.layers.cityCircle + '-glow-mid');
    }
    if (this.map.getLayer(CONFIG.layers.cityCircle + '-glow-outer')) {
      this.map.removeLayer(CONFIG.layers.cityCircle + '-glow-outer');
    }
    // Remove source
    if (this.map.getSource(CONFIG.layers.citySource)) {
      this.map.removeSource(CONFIG.layers.citySource);
    }
    this.citiesLoaded = false;
    this.currentStateLocId = null;
  },

  /**
   * Set the parent outline layer (shows the region you drilled into)
   * @param {Object} geojson - GeoJSON FeatureCollection of the parent region
   */
  setParentOutline(geojson) {
    // Clear existing parent outline
    this.clearParentOutline();

    if (!geojson || !geojson.features || geojson.features.length === 0) {
      return;
    }

    // Add parent source
    this.map.addSource(CONFIG.layers.parentSource, {
      type: 'geojson',
      data: geojson
    });

    // Add subtle fill for parent region (very low opacity, below children)
    this.map.addLayer({
      id: CONFIG.layers.parentFill,
      type: 'fill',
      source: CONFIG.layers.parentSource,
      paint: {
        'fill-color': '#ff7800',
        'fill-opacity': 0.08
      }
    }, CONFIG.layers.fill);  // Insert below the main fill layer

    // Add parent outline stroke (thicker, on top of everything to be visible)
    this.map.addLayer({
      id: CONFIG.layers.parentStroke,
      type: 'line',
      source: CONFIG.layers.parentSource,
      paint: {
        'line-color': '#cc4400',
        'line-width': 4,
        'line-opacity': 0.9
      }
    });  // No 'before' parameter = add on top

    console.log('Parent outline layer added');
  },

  /**
   * Clear the parent outline layer
   */
  clearParentOutline() {
    if (this.map.getLayer(CONFIG.layers.parentStroke)) {
      this.map.removeLayer(CONFIG.layers.parentStroke);
    }
    if (this.map.getLayer(CONFIG.layers.parentFill)) {
      this.map.removeLayer(CONFIG.layers.parentFill);
    }
    if (this.map.getSource(CONFIG.layers.parentSource)) {
      this.map.removeSource(CONFIG.layers.parentSource);
    }
  },

  /**
   * Load navigation locations as a highlighted layer
   * Used for "show me X" navigation without data request
   * @param {Object} geojson - GeoJSON FeatureCollection of locations to highlight
   */
  loadNavigationLayer(geojson) {
    if (!geojson || !geojson.features || geojson.features.length === 0) {
      return;
    }

    // Clear any existing navigation layer
    this.clearNavigationLayer();

    // Add unique IDs to features
    geojson.features.forEach((feature, index) => {
      feature.id = index;
    });

    // Add source for navigation locations
    this.map.addSource(CONFIG.layers.selectionSource, {
      type: 'geojson',
      data: geojson,
      generateId: true
    });

    // Add fill layer with selection colors (orange/amber)
    this.map.addLayer({
      id: CONFIG.layers.selectionFill,
      type: 'fill',
      source: CONFIG.layers.selectionSource,
      paint: {
        'fill-color': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.selectionColors.hoverFill,
          CONFIG.selectionColors.fill
        ],
        'fill-opacity': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.selectionColors.hoverOpacity,
          CONFIG.selectionColors.fillOpacity
        ]
      }
    });

    // Add stroke layer
    this.map.addLayer({
      id: CONFIG.layers.selectionStroke,
      type: 'line',
      source: CONFIG.layers.selectionSource,
      paint: {
        'line-color': CONFIG.selectionColors.stroke,
        'line-width': [
          'case',
          ['boolean', ['feature-state', 'hover'], false],
          CONFIG.selectionColors.hoverStrokeWidth,
          CONFIG.selectionColors.strokeWidth
        ]
      }
    });

    console.log(`Navigation layer loaded with ${geojson.features.length} features`);
  },

  /**
   * Clear the navigation layer
   */
  clearNavigationLayer() {
    if (this.map.getLayer(CONFIG.layers.selectionFill)) {
      this.map.removeLayer(CONFIG.layers.selectionFill);
    }
    if (this.map.getLayer(CONFIG.layers.selectionStroke)) {
      this.map.removeLayer(CONFIG.layers.selectionStroke);
    }
    if (this.map.getSource(CONFIG.layers.selectionSource)) {
      this.map.removeSource(CONFIG.layers.selectionSource);
    }
  },

  /**
   * Full memory cleanup - call when switching major views
   */
  cleanup() {
    this.clearLayers();
    this.clearParentOutline();
    this.clearCityOverlay();
    this.clearNavigationLayer();
    this.clearHurricaneLayer();
    this.clearHurricaneTrack();
    this.clearEventLayer();
    this.currentRegionGeojson = null;
    this.hoveredFeatureId = null;
  },

  // ============================================================================
  // HURRICANE/STORM LAYERS
  // ============================================================================

  /**
   * Load hurricane/storm point markers onto the map.
   * Delegates to TrackModel.
   * @param {Object} geojson - GeoJSON FeatureCollection with Point features
   * @param {Function} onStormClick - Callback when a storm marker is clicked (stormId, stormName)
   */
  loadHurricaneLayer(geojson, onStormClick = null) {
    TrackModel.render(geojson, 'hurricane', { onStormClick });
  },

  /**
   * Clear hurricane point layer.
   * Delegates to TrackModel.
   */
  clearHurricaneLayer() {
    TrackModel.clearMarkers();
  },

  /**
   * Load a hurricane track (line + animated current position).
   * Delegates to TrackModel.
   * @param {Object} trackGeojson - GeoJSON with track points
   * @param {Object} lineGeojson - GeoJSON LineString for the track path
   * @param {Object} currentPosition - {longitude, latitude, category} for animated marker
   */
  loadHurricaneTrack(trackGeojson, lineGeojson = null, currentPosition = null) {
    TrackModel.renderTrack(trackGeojson, lineGeojson, currentPosition);
  },

  /**
   * Update the current position marker on a track (for animation).
   * Delegates to TrackModel.
   * @param {number} longitude
   * @param {number} latitude
   * @param {string} category - Storm category for color
   */
  updateTrackPosition(longitude, latitude, category) {
    TrackModel.updatePosition(longitude, latitude, category);
  },

  /**
   * Clear hurricane track layers.
   * Delegates to TrackModel.
   */
  clearHurricaneTrack() {
    TrackModel.clearTrack();
  },

  // ============================================================================
  // EVENT LAYERS (Earthquakes, Volcanoes, etc.)
  // ============================================================================

  eventClickHandler: null,

  /**
   * Load event layer (earthquakes, volcanoes, etc.) onto the map.
   * Delegates to appropriate display model via PointRadiusModel.
   * @param {Object} geojson - GeoJSON FeatureCollection with Point features
   * @param {string} eventType - 'earthquake', 'volcano', 'wildfire', etc.
   * @param {Object} options - {showFeltRadius, showDamageRadius, onEventClick}
   */
  loadEventLayer(geojson, eventType = 'earthquake', options = {}) {
    // Delegate to PointRadiusModel for point-based events
    PointRadiusModel.render(geojson, eventType, options);
  },

  /**
   * Update event layer data (for time-based filtering).
   * Delegates to PointRadiusModel.
   * @param {Object} geojson - Filtered GeoJSON FeatureCollection
   */
  updateEventLayer(geojson) {
    PointRadiusModel.update(geojson);
  },

  /**
   * Clear event layer.
   * Delegates to PointRadiusModel.
   */
  clearEventLayer() {
    PointRadiusModel.clear();
  },

  /**
   * Fit map to event bounds.
   * Delegates to PointRadiusModel.
   * @param {Object} geojson - Event GeoJSON
   */
  fitToEventBounds(geojson) {
    PointRadiusModel.fitBounds(geojson);
  }
};
