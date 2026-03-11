/**
 * App - Main application controller.
 * Orchestrates all modules and handles initialization.
 */

import { CONFIG } from './config.js';
import { GeometryCache } from './cache.js';
import { fetchMsgpack } from './utils/fetch.js';
import { ViewportLoader, setDependencies as setViewportDeps } from './viewport-loader.js';
import { MapAdapter, setDependencies as setMapDeps } from './map-adapter.js';
import { NavigationManager, setDependencies as setNavDeps } from './navigation.js';
import { PopupBuilder, setDependencies as setPopupDeps } from './popup-builder.js';
import { ChatManager, OrderManager, setDependencies as setChatDeps } from './chat-panel.js';
import { TimeSlider, setDependencies as setTimeDeps } from './time-slider.js';
import { ChoroplethManager, setDependencies as setChoroDeps } from './choropleth.js';
import { ResizeManager, SidebarResizer, SettingsManager } from './sidebar.js';
import { SelectionManager, setDependencies as setSelectionDeps } from './selection-manager.js';
import { HurricaneHandler, setDependencies as setHurricaneDeps } from './hurricane-handler.js';
import { OverlaySelector, setDependencies as setOverlayDeps } from './overlay-selector.js';
import { ModelRegistry } from './models/model-registry.js';
import { OverlayController, setDependencies as setOverlayControllerDeps } from './overlay-controller.js';
import { DisasterPopup, setDependencies as setDisasterPopupDeps } from './disaster-popup.js';
import { GeometryModel, setDependencies as setGeometryDeps } from './models/model-geometry.js';
import { AuthManager } from './auth.js';

// ============================================================================
// APP - Main application controller
// ============================================================================

export const App = {
  currentData: null,
  debugMode: false,  // Toggle with 'D' key - shows hierarchy depth colors
  geometryOverlayActive: false,  // True when geometry overlay (ZCTA, tribal, etc.) is displayed

  /**
   * Merge new multi-year data into existing data (same source).
   * Combines geojson features, year_data, and expands year_range.
   */
  mergeMultiYearData(existing, incoming) {
    if (!existing || !incoming) return incoming;

    // Merge GeoJSON features (by loc_id to avoid duplicates)
    const existingLocIds = new Set(
      existing.geojson?.features?.map(f => f.properties?.loc_id || f.id) || []
    );
    const newFeatures = incoming.geojson?.features?.filter(
      f => !existingLocIds.has(f.properties?.loc_id || f.id)
    ) || [];
    const mergedFeatures = [
      ...(existing.geojson?.features || []),
      ...newFeatures
    ];

    // Merge year_data: {year: {loc_id: {metric: value}}}
    const mergedYearData = { ...(existing.year_data || {}) };
    for (const [year, locData] of Object.entries(incoming.year_data || {})) {
      if (!mergedYearData[year]) {
        mergedYearData[year] = {};
      }
      for (const [locId, metrics] of Object.entries(locData)) {
        if (!mergedYearData[year][locId]) {
          mergedYearData[year][locId] = {};
        }
        Object.assign(mergedYearData[year][locId], metrics);
      }
    }

    // Expand year_range
    const mergedYearRange = {
      min: Math.min(existing.year_range?.min || Infinity, incoming.year_range?.min || Infinity),
      max: Math.max(existing.year_range?.max || -Infinity, incoming.year_range?.max || -Infinity),
      available_years: [
        ...new Set([
          ...(existing.year_range?.available_years || []),
          ...(incoming.year_range?.available_years || [])
        ])
      ].sort((a, b) => a - b)
    };

    // Merge available_metrics
    const mergedMetrics = [
      ...new Set([
        ...(existing.available_metrics || []),
        ...(incoming.available_metrics || [])
      ])
    ];

    // Merge metric_year_ranges
    const mergedMetricYearRanges = {
      ...(existing.metric_year_ranges || {}),
      ...(incoming.metric_year_ranges || {})
    };

    return {
      ...incoming,
      geojson: { type: 'FeatureCollection', features: mergedFeatures },
      year_data: mergedYearData,
      year_range: mergedYearRange,
      available_metrics: mergedMetrics,
      metric_year_ranges: mergedMetricYearRanges,
      count: mergedFeatures.length
    };
  },

  /**
   * Initialize the application
   */
  async init() {
    console.log('Initializing Map Explorer...');

    // Wire up circular dependencies
    setViewportDeps({ MapAdapter, NavigationManager, App, TimeSlider });
    setMapDeps({ ViewportLoader, NavigationManager, App, PopupBuilder, OverlayController });
    setNavDeps({ MapAdapter, ViewportLoader, App });
    setPopupDeps({ App, ChoroplethManager });
    setChatDeps({ MapAdapter, App, SelectionManager, OverlayController, OverlaySelector });
    setTimeDeps({ MapAdapter, ChoroplethManager });
    setChoroDeps({ MapAdapter });
    setSelectionDeps({ MapAdapter, ChatManager });
    setHurricaneDeps({ TimeSlider, MapAdapter });
    setOverlayDeps({ MapAdapter, ModelRegistry });
    ModelRegistry.setDependencies({ MapAdapter, TimeSlider });
    setOverlayControllerDeps({ MapAdapter, ModelRegistry, OverlaySelector, TimeSlider });
    setDisasterPopupDeps({ MapAdapter });
    setGeometryDeps({ MapAdapter });

    await AuthManager.init();

    // Initialize components
    ChatManager.init();
    OrderManager.init();
    SettingsManager.init();
    ResizeManager.init();
    SidebarResizer.init();

    // Initialize TimeSlider early (UI setup only, no data)
    // This ensures the slider is visible and listener system is ready
    // before overlays are enabled
    TimeSlider.initSlider();

    await OverlaySelector.init();
    OverlayController.init();

    // Initialize map
    await MapAdapter.init();

    // Load reference data for popups (non-blocking)
    PopupBuilder.loadAdminLevels();

    // Setup keyboard handler for debug mode
    this.setupKeyboardHandler();

    // Don't load countries at startup - wait for demographics overlay to be enabled
    // This keeps the map clean until user selects what they want to see

    // Initialize viewport-based navigation with current viewport area
    const bounds = MapAdapter.map.getBounds();
    ViewportLoader.currentAdminLevel = ViewportLoader.getAdminLevelForViewport(bounds);
    console.log('Viewport navigation ready (area-based thresholds)');

    // Initialize admin level buttons
    NavigationManager.initLevelButtons();
    NavigationManager.updateLevelButtons(ViewportLoader.currentAdminLevel);

    // Setup globe toggle checkbox
    this.setupGlobeToggle();

    // Setup satellite toggle checkbox
    this.setupSatelliteToggle();

    console.log('Map Explorer ready');
    console.log('Press D to toggle debug mode (hierarchy depth colors)');
  },

  /**
   * Setup keyboard handler for debug mode toggle
   */
  setupKeyboardHandler() {
    document.addEventListener('keydown', (e) => {
      // Ignore if typing in an input
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
        return;
      }

      if (e.key.toLowerCase() === 'd') {
        this.toggleDebugMode();
      }
    });
  },

  /**
   * Setup globe/3D toggle checkbox
   */
  setupGlobeToggle() {
    const checkbox = document.getElementById('globeCheckbox');
    if (checkbox) {
      // Restore saved state
      try {
        const saved = localStorage.getItem('countymap_globe_enabled');
        if (saved === 'true') {
          checkbox.checked = true;
          MapAdapter.toggleGlobe(true);
        }
      } catch (e) {}

      checkbox.addEventListener('change', (e) => {
        MapAdapter.toggleGlobe(e.target.checked);
        // Save state
        try {
          localStorage.setItem('countymap_globe_enabled', e.target.checked ? 'true' : 'false');
        } catch (err) {}
      });
    }
  },

  /**
   * Setup satellite view toggle checkbox
   */
  setupSatelliteToggle() {
    const checkbox = document.getElementById('satCheckbox');
    if (checkbox) {
      // Restore saved state
      try {
        const saved = localStorage.getItem('countymap_satellite_enabled');
        if (saved === 'true') {
          checkbox.checked = true;
          MapAdapter.toggleSatellite(true);
        }
      } catch (e) {}

      checkbox.addEventListener('change', (e) => {
        MapAdapter.toggleSatellite(e.target.checked);
        // Save state
        try {
          localStorage.setItem('countymap_satellite_enabled', e.target.checked ? 'true' : 'false');
        } catch (err) {}
      });
    }
  },

  /**
   * Clear map view settings (called by New Chat)
   */
  clearMapViewSettings() {
    try {
      localStorage.removeItem('countymap_globe_enabled');
      localStorage.removeItem('countymap_satellite_enabled');
    } catch (e) {}
    // Reset checkboxes
    const globeCheckbox = document.getElementById('globeCheckbox');
    const satCheckbox = document.getElementById('satCheckbox');
    if (globeCheckbox) {
      globeCheckbox.checked = false;
      MapAdapter.toggleGlobe(false);
    }
    if (satCheckbox) {
      satCheckbox.checked = false;
      MapAdapter.toggleSatellite(false);
    }
  },

  /**
   * Toggle debug mode (hierarchy depth visualization)
   */
  async toggleDebugMode() {
    this.debugMode = !this.debugMode;
    console.log(`Debug mode: ${this.debugMode ? 'ON' : 'OFF'}`);

    // Only reload if we're at world level showing countries
    if (NavigationManager.currentLevel === 'world') {
      await this.loadCountries();
    }

    // Update fill colors based on debug mode
    MapAdapter.updateDebugColors(this.debugMode);
  },

  /**
   * Load world countries
   */
  async loadCountries() {
    // Note: Geometry overlays (ZCTA, tribal) use separate layers, so they can coexist
    // with the main choropleth display. No need to skip.
    try {
      console.log('Loading countries...');

      // Only reset time slider if NO active overlay needs it (OR gate logic)
      const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
      const anyOverlayNeedsSlider = activeOverlays.some(id => {
        const config = OverlaySelector.getOverlayConfig(id);
        return config?.hasYearFilter === true;
      });

      if (!anyOverlayNeedsSlider) {
        TimeSlider.reset();
      }
      ChoroplethManager.reset();

      // Re-enable viewport loading (exit order mode)
      ViewportLoader.orderMode = false;

      // Add debug param if debug mode is on
      const url = this.debugMode
        ? `${CONFIG.api.countries}?debug=true`
        : CONFIG.api.countries;
      const result = await fetchMsgpack(url);

      if (result.geojson && result.geojson.features.length > 0) {
        this.currentData = {
          geojson: result.geojson,
          dataset_name: 'World Countries',
          source_name: 'Natural Earth'
        };

        NavigationManager.reset();
        MapAdapter.clearParentOutline();  // Clear parent outline at world level
        MapAdapter.clearCityOverlay();    // Clear city overlay
        MapAdapter.clearNavigationLayer(); // Clear navigation highlights
        MapAdapter.loadGeoJSON(result.geojson, this.debugMode);
        // Don't fitToBounds for world view - use CONFIG.defaultCenter instead
        // (fitToBounds on 256 countries averages to 0,0 which is Gulf of Guinea)

        console.log(`Loaded ${result.count} countries${this.debugMode ? ' (debug mode)' : ''}`);
      }
    } catch (error) {
      console.error('Error loading countries:', error);
    }
  },

  /**
   * Handle hover over a feature - show popup
   */
  handleFeatureHover(feature, lngLat) {
    const properties = feature.properties;
    const popupHtml = PopupBuilder.build(properties, this.currentData);
    MapAdapter.showPopup([lngLat.lng, lngLat.lat], popupHtml);
  },

  /**
   * Handle single click on a feature - fly to location
   */
  handleFeatureClick(feature, lngLat) {
    const properties = feature.properties;

    // Get coordinates for fly-to
    let coords = null;
    if (properties.coordinates) {
      try {
        coords = JSON.parse(properties.coordinates);
      } catch (e) {}
    }

    if (coords && coords.length === 2) {
      const zoom = NavigationManager.getZoomForLevel() + 1;
      MapAdapter.flyTo(coords, zoom);
    }
  },

  /**
   * Handle double-click drill-down
   */
  async handleFeatureDrillDown(feature) {
    const locId = feature.properties.loc_id;
    const name = feature.properties.name || 'Unknown';

    if (locId) {
      MapAdapter.hidePopup();
      await this.drillDown(locId, name);
    }
  },

  /**
   * Drill down into a location
   * @param {string} locId - Location ID
   * @param {string} name - Display name
   * @param {boolean} skipPush - Skip adding to navigation path (used for back navigation)
   */
  async drillDown(locId, name, skipPush = false) {
    // Prevent duplicate navigation (unless this is a back-navigation call)
    if (!skipPush && NavigationManager.isNavigating) {
      console.log('Navigation already in progress, skipping drillDown');
      return;
    }
    if (!skipPush) {
      NavigationManager.isNavigating = true;
    }

    try {
      console.log(`Drilling down: ${locId}`);

      // Before loading children, find the parent feature to use as outline
      let parentGeojson = null;
      if (MapAdapter.currentRegionGeojson && MapAdapter.currentRegionGeojson.features) {
        const parentFeature = MapAdapter.currentRegionGeojson.features.find(
          f => f.properties && f.properties.loc_id === locId
        );
        if (parentFeature) {
          parentGeojson = {
            type: 'FeatureCollection',
            features: [parentFeature]
          };
        }
      }

      const url = CONFIG.api.children.replace('{loc_id}', locId);
      const result = await fetchMsgpack(url);

      if (result.geojson && result.geojson.features.length > 0) {
        this.currentData = {
          geojson: result.geojson,
          dataset_name: `${name} - ${result.level}`,
          source_name: 'Geometry'
        };

        if (!skipPush) {
          NavigationManager.push(locId, name, result.level);
        }

        MapAdapter.loadGeoJSON(result.geojson);

        // Set the parent outline (the region we drilled into)
        if (parentGeojson) {
          MapAdapter.setParentOutline(parentGeojson);
        }

        // Zoom closer when drilling into countries (minZoom based on level)
        const zoomOptions = {};
        if (result.level === 'us_state' || result.level === 'state') {
          zoomOptions.minZoom = 4;  // Zoom to at least 4 for states
        } else if (result.level === 'us_county' || result.level === 'county') {
          zoomOptions.minZoom = 6;  // Zoom to at least 6 for counties
        } else if (result.level === 'city') {
          zoomOptions.minZoom = 8;  // Zoom to at least 8 for cities
        }
        MapAdapter.fitToBounds(result.geojson, zoomOptions);

        // Load city overlay based on navigation level
        // Cities are parented to counties, so load when viewing a county
        const locIdParts = locId.split('-');
        if (locIdParts.length === 3 && locIdParts[0] === 'USA') {
          // We're in a county (USA-XX-XXXXX) - load cities for this county
          MapAdapter.loadCityOverlay(locId);
        } else if (result.level === 'us_county' && locId.startsWith('USA-')) {
          // We drilled into a state and see counties - clear any previous city overlay
          MapAdapter.clearCityOverlay();
        } else {
          // Clear city overlay for other cases
          MapAdapter.clearCityOverlay();
        }

        console.log(`Loaded ${result.count} ${result.level} features`);
      } else {
        console.log(`No children found for ${locId}`);
        if (result.message) {
          console.log(result.message);
        }
      }
    } catch (error) {
      console.error('Error drilling down:', error);
    } finally {
      // Clear navigation lock
      if (!skipPush) {
        NavigationManager.isNavigating = false;
      }
    }
  },

  /**
   * Display data from chat query
   */
  displayData(data) {
    // Check if we should merge with existing data (same source, multi-year)
    const shouldMerge = this.currentData &&
      this.currentData.multi_year &&
      data.multi_year &&
      this.currentData.source_id === data.source_id;

    if (shouldMerge) {
      console.log(`Merging data: existing ${this.currentData.count} + new ${data.count} features`);
      data = this.mergeMultiYearData(this.currentData, data);
      console.log(`After merge: ${data.count} total features`);
    }

    this.currentData = data;

    // Suspend viewport loading when displaying order data
    // This prevents viewport API from overwriting our ordered data
    ViewportLoader.orderMode = true;

    // Clear any existing layers first
    MapAdapter.clearHurricaneLayer();
    MapAdapter.clearHurricaneTrack();
    MapAdapter.clearEventLayer();

    // Handle removal orders first (works for all data types)
    // Removal payloads are minimal - just identifiers, not full data
    if (data.action === 'remove') {
      console.log(`Removal order: ${data.data_type}, source: ${data.source_id}`);
      let result = { removed: 0, remaining: 0 };

      if (data.data_type === 'geometry') {
        // Geometry: remove by loc_ids
        let geometryType = data.geographic_level || 'zcta';
        if (data.source_id) {
          const match = data.source_id.match(/geometry_(\w+)/);
          if (match) geometryType = match[1];
        }
        result = OverlayController.removeGeometryData(
          data.source_id,
          { loc_ids: data.loc_ids, regions: data.regions },
          geometryType
        );
      } else if (data.data_type === 'events') {
        // Events: remove by event_ids
        result = OverlayController.removeEventData(
          data.source_id,
          { event_ids: data.event_ids, regions: data.regions }
        );
      } else if (data.data_type === 'metrics') {
        // Metrics: remove column (loc_ids + years + metric)
        result = OverlayController.removeMetricData(
          data.source_id,
          { loc_ids: data.loc_ids, years: data.years, metric: data.metric }
        );
      }

      // Trigger overlay refresh (same as add - turn on overlay, which refreshes from cache)
      if (data.data_type === 'geometry') {
        const geometryTypeToOverlayId = {
          'zcta': 'zip_codes',
          'tribal': 'tribal_areas',
          'watershed': 'watersheds',
          'park': 'parks'
        };
        let geometryType = data.geographic_level || 'zcta';
        if (data.source_id) {
          const match = data.source_id.match(/geometry_(\w+)/);
          if (match) geometryType = match[1];
        }
        const overlayId = geometryTypeToOverlayId[geometryType] || 'zip_codes';
        OverlayController.handleOverlayChange(overlayId, true);
      }

      // Update summary display
      const summaryEl = document.getElementById('queryStatus');
      if (summaryEl) {
        summaryEl.textContent = data.summary || `Removed ${result.removed} items (${result.remaining} remaining)`;
      }
      return;
    }

    // Check if this is geometry overlay data (ZCTA, tribal, watersheds, etc.)
    if (data.data_type === 'geometry') {
      // Determine geometry type early
      let geometryType = 'geometry';
      if (data.source_id) {
        const match = data.source_id.match(/geometry_(\w+)/);
        if (match) geometryType = match[1];
      }
      if (geometryType === 'geometry' && data.geographic_level) {
        geometryType = data.geographic_level;
      }

      console.log(`Geometry overlay detected: ${data.source_id}, ${data.geojson?.features?.length || 0} features`);

      // Set geometry overlay flag - prevents loadCountries from overwriting
      App.geometryOverlayActive = true;
      ViewportLoader.orderMode = true;

      TimeSlider.reset();
      ChoroplethManager.reset();

      // Render geometry if we have features (geometryType already computed above)
      if (data.geojson && data.geojson.features && data.geojson.features.length > 0) {
        // Map geometryType to overlay ID
        const geometryTypeToOverlayId = {
          'zcta': 'zip_codes',
          'tribal': 'tribal_areas',
          'watershed': 'watersheds',
          'park': 'parks'
        };
        const overlayId = geometryTypeToOverlayId[geometryType] || 'zip_codes';

        // Store geometry data for OverlayController to render when overlay is enabled
        // This ensures render happens AFTER overlay is toggled ON
        OverlayController.pendingGeometry = {
          geojson: data.geojson,
          geometryType: geometryType,
          sourceId: data.source_id,
          options: { showLabels: false }
        };

        // Enable the overlay - handleOverlayChange will render from pendingGeometry
        // If already active, it will refresh the display
        if (OverlaySelector && !OverlaySelector.isActive(overlayId)) {
          OverlaySelector.setActive(overlayId, true);
        }
        // Always notify - if already on, this triggers a refresh
        OverlayController.handleOverlayChange(overlayId, true);

        console.log(`Geometry queued for render as type: ${geometryType}`);
      }

      // Update summary display
      const summaryEl = document.getElementById('queryStatus');
      if (summaryEl) {
        summaryEl.textContent = data.summary || `${data.geojson?.features?.length || 0} areas`;
      }

      return;
    }

    // Check if this is event mode data (earthquakes, volcanoes, etc.)
    if (data.type === 'events') {
      console.log(`Event data detected: ${data.event_type}, ${data.count} events`);

      TimeSlider.reset();
      ChoroplethManager.reset();

      // Load event layer with appropriate styling
      MapAdapter.loadEventLayer(data.geojson, data.event_type, {
        showFeltRadius: true,
        showDamageRadius: true,
        onEventClick: (props) => {
          console.log('Event clicked:', props);
          // Show detailed popup on click
          const html = MapAdapter._buildEventPopupHtml(props, data.event_type);
          MapAdapter.popup.setHTML(html);
          MapAdapter.popupLocked = true;
        }
      });

      // Fit map to event locations
      MapAdapter.fitToEventBounds(data.geojson);

      // Update summary display
      const summaryEl = document.getElementById('queryStatus');
      if (summaryEl) {
        summaryEl.textContent = data.summary || `${data.count} ${data.event_type} events`;
      }

      return;
    }

    // Check if this is hurricane/storm point data
    const isHurricaneData = data.source_id === 'ibtracs' ||
      data.dataset_name?.toLowerCase().includes('hurricane') ||
      data.dataset_name?.toLowerCase().includes('storm') ||
      data.metric_key === 'storm_count' ||
      (data.geojson?.features?.[0]?.properties?.storm_id);

    if (isHurricaneData && data.geojson?.features?.[0]?.geometry?.type === 'Point') {
      // Hurricane point data - use special hurricane layer with click drill-down
      console.log('Hurricane data detected, using hurricane layer');

      TimeSlider.reset();
      ChoroplethManager.reset();

      // Load hurricane markers with drill-down click handler
      MapAdapter.loadHurricaneLayer(data.geojson, (stormId, stormName) => {
        console.log(`Storm clicked: ${stormId} - ${stormName}`);
        HurricaneHandler.drillDown(stormId, stormName);
      });

      // Fit map to storm locations
      MapAdapter.fitToBounds(data.geojson);

    } else if (data.multi_year && data.year_data && data.year_range) {
      // Multi-year mode: initialize time slider
      console.log('Multi-year data detected, initializing time slider');
      console.log(`Year range: ${data.year_range.min} - ${data.year_range.max}`);
      console.log('DEBUG app.js: metric_year_ranges from response:', data.metric_year_ranges);

      // Auto-enable demographics overlay for demographic data from chat orders
      // This ensures viewport-based admin level filtering works
      const OverlaySelector = window.OverlaySelector;
      if (OverlaySelector && !OverlaySelector.isActive('demographics')) {
        console.log('Auto-enabling demographics overlay for chat order data');
        OverlaySelector.setActive('demographics', true);
      }

      // Hide any existing slider/legend first
      TimeSlider.reset();
      ChoroplethManager.reset();

      // Initialize time slider with the data
      TimeSlider.init(
        data.year_range,
        data.year_data,
        data.geojson,
        data.metric_key,
        data.available_metrics,  // Explicit list of metrics from order
        data.metric_year_ranges  // Per-metric year ranges for slider adjustment
      );

      // Fit map to the data, then apply initial admin level filter
      MapAdapter.fitToBounds(data.geojson);

      // Set initial admin level filter based on viewport after fit completes
      // Use setTimeout to let fitToBounds animation complete
      setTimeout(() => {
        const bounds = MapAdapter.map?.getBounds();
        if (bounds) {
          const level = ViewportLoader.getAdminLevelForViewport(bounds);
          ViewportLoader.currentAdminLevel = level;
          TimeSlider.setAdminLevelFilter(level);
        }
      }, 100);

    } else {
      // Single-year mode: hide time slider, display normally
      TimeSlider.reset();
      ChoroplethManager.reset();

      if (data.geojson && data.geojson.type === 'FeatureCollection') {
        MapAdapter.loadGeoJSON(data.geojson);
        MapAdapter.fitToBounds(data.geojson);
      }
    }

    // Collapse sidebar on mobile
    if (window.innerWidth < 500) {
      ChatManager.elements.sidebar.classList.add('collapsed');
      ChatManager.elements.toggle.style.display = 'flex';
    }
  },

  /**
   * Display navigation locations as highlighted overlay
   * Used when user says "show me X" without requesting data
   * @param {Object} geojson - GeoJSON with location geometries
   * @param {Array} locations - Location metadata array
   */
  displayNavigationLocations(geojson, locations) {
    if (!geojson || !geojson.features || geojson.features.length === 0) {
      console.warn('No features to display for navigation');
      return;
    }

    console.log(`Displaying ${geojson.features.length} navigation locations`);

    // Suspend viewport loading while showing navigation locations
    ViewportLoader.orderMode = true;

    // Reset any previous data display state
    TimeSlider.reset();
    ChoroplethManager.reset();

    // Load the navigation locations using selection layer (orange/amber highlighting)
    // This uses the same layer as disambiguation but for a different purpose
    MapAdapter.loadNavigationLayer(geojson);

    // Store reference for popups (minimal data, just location info)
    this.currentData = {
      geojson: geojson,
      dataset_name: 'Navigation',
      source_name: 'Location View',
      isNavigation: true
    };

    // Store locations for click handling
    this.navigationLocations = locations;

    // Set up click handler for navigation layer selection
    this.setupNavigationClickHandler();
  },

  /**
   * Set up click handler for navigation layer
   * Allows user to select one location from multiple candidates
   */
  setupNavigationClickHandler() {
    if (!MapAdapter?.map) return;

    // Remove any existing handler
    if (this._navigationClickHandler) {
      MapAdapter.map.off('click', CONFIG.layers.selectionFill, this._navigationClickHandler);
    }

    // Create click handler
    this._navigationClickHandler = (e) => {
      if (!e.features || e.features.length === 0) return;

      const feature = e.features[0];
      const locId = feature.properties?.loc_id;

      // Find matching location from stored locations
      const location = this.navigationLocations?.find(loc => loc.loc_id === locId);

      if (location) {
        this.handleNavigationSelection(location, feature);
      }
    };

    // Add click handler
    MapAdapter.map.on('click', CONFIG.layers.selectionFill, this._navigationClickHandler);

    // Change cursor on hover
    MapAdapter.map.on('mouseenter', CONFIG.layers.selectionFill, () => {
      MapAdapter.map.getCanvas().style.cursor = 'pointer';
    });
    MapAdapter.map.on('mouseleave', CONFIG.layers.selectionFill, () => {
      MapAdapter.map.getCanvas().style.cursor = '';
    });
  },

  /**
   * Handle selection of a location in navigation mode
   * @param {Object} location - The selected location object
   * @param {Object} feature - The GeoJSON feature that was clicked
   */
  handleNavigationSelection(location, feature) {
    const name = location.matched_term || location.loc_id;
    const country = location.country_name || location.iso3 || '';

    console.log(`Navigation selection: ${name} (${country})`);

    // Add message to chat
    const displayName = country ? `${name} (${country})` : name;
    ChatManager.addMessage(`Selected: ${displayName}. What data would you like to see for this location?`, 'assistant');

    // Update order panel to show just this location
    OrderManager.setNavigationLocations([location]);

    // Clear the navigation layer and show just the selected location
    MapAdapter.clearNavigationLayer();

    // Reload with just the selected feature
    const selectedGeojson = {
      type: 'FeatureCollection',
      features: [feature]
    };
    MapAdapter.loadNavigationLayer(selectedGeojson);

    // Clean up - only keep selected location
    this.navigationLocations = [location];

    // Remove click handler (no longer needed after selection)
    if (this._navigationClickHandler) {
      MapAdapter.map.off('click', CONFIG.layers.selectionFill, this._navigationClickHandler);
      this._navigationClickHandler = null;
    }
  },

  /**
   * Clear navigation mode and return to normal map state
   */
  clearNavigationMode() {
    MapAdapter.clearNavigationLayer();
    this.navigationLocations = null;

    if (this._navigationClickHandler && MapAdapter?.map) {
      MapAdapter.map.off('click', CONFIG.layers.selectionFill, this._navigationClickHandler);
      this._navigationClickHandler = null;
    }

    // Clear geometry overlay layers (zcta, tribal, etc.) if active
    if (this.geometryOverlayActive) {
      // Clear all geometry layers via GeometryModel
      GeometryModel.clear();
    }

    // Clear geometry overlay flag and re-enable viewport loading
    this.geometryOverlayActive = false;
    ViewportLoader.orderMode = false;
  }
};

// ============================================================================
// INITIALIZATION
// ============================================================================

// Start the app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  App.init();
});

// Export for global access if needed
if (typeof window !== 'undefined') {
  window.App = App;
  window.OverlayController = OverlayController;  // For debugging: OverlayController.getCacheStats()
  window.TimeSlider = TimeSlider;  // For settings to update live timezone
}
