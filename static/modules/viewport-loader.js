/**
 * Viewport-based geometry loading.
 * Handles when and what to load based on map viewport changes.
 * This is where loading strategy and performance tuning lives.
 */

import { CONFIG } from './config.js';
import { GeometryCache } from './cache.js';
import { fetchMsgpack, postMsgpack } from './utils/fetch.js';

// These will be set by app.js to avoid circular dependencies
let MapAdapter = null;
let NavigationManager = null;
let App = null;
let TimeSlider = null;

function isAdminLayersActive() {
  const OverlaySelector = window.OverlaySelector;
  const activeOverlays = OverlaySelector?.getActiveOverlays?.() || [];
  return activeOverlays.includes('admin_layers') || activeOverlays.includes('demographics');
}

export function setDependencies(deps) {
  MapAdapter = deps.MapAdapter;
  NavigationManager = deps.NavigationManager;
  App = deps.App;
  TimeSlider = deps.TimeSlider;
}

// ============================================================================
// VIEWPORT LOADER - Debounced viewport-based geometry loading
// ============================================================================

export const ViewportLoader = {
  loadTimeout: null,
  loadResolve: null,
  currentAdminLevel: 0,
  enabled: true,  // Always enabled - viewport is the only navigation mode
  orderMode: false,  // When true, viewport loading is suspended (order data is displayed)
  levelLocked: false,  // When true, admin level doesn't change with zoom
  lockedLevel: null,   // The locked admin level (0-6)
  abortController: null,  // For cancelling stale requests
  activeRequest: null,  // Owns in-flight coverage and ids until completion/cancellation
  pendingSameLevel: false,  // Coalesce wheel zooms into one latest-viewport follow-up
  requestId: 0,  // Counter to track which request is current
  lastRequestedLevel: null,  // Track the level of the in-flight request
  lastZoom: null,  // Track zoom level to distinguish zoom from pan
  lastRenderedLevel: 0,
  orderModeLevelHoldUntil: 0,
  tabSuspended: false,

  suspendForHiddenTab() {
    this.tabSuspended = true;
    if (this.loadTimeout) {
      clearTimeout(this.loadTimeout);
      this.loadTimeout = null;
      this.loadResolve?.(false);
      this.loadResolve = null;
    }
    this.cancelActiveRequest();
  },

  cancelActiveRequest() {
    this.pendingSameLevel = false;
    this.abortController?.abort();
    this.abortController = null;
    const active = this.activeRequest;
    if (active) {
      GeometryCache.clearLocIdsInFlight(active.level, active.locIds);
      GeometryCache.finishInFlight(active.level, active.key);
      this.activeRequest = null;
    }
  },

  resumeFromHiddenTab() {
    if (!this.tabSuspended) return;
    this.tabSuspended = false;
    if (this.orderMode) {
      this.onViewportChange();
    } else {
      this.load(this.currentAdminLevel);
    }
  },

  holdOrderModeLevel(level = this.currentAdminLevel, durationMs = 1200) {
    this.currentAdminLevel = level;
    this.orderModeLevelHoldUntil = Date.now() + durationMs;
  },

  renderViewportFromCache(level = this.currentAdminLevel, bboxOverride = null) {
    if (!MapAdapter?.map || !isAdminLayersActive()) return 0;
    const bounds = MapAdapter.map.getBounds();
    const bboxObject = bboxOverride || {
      west: Number(bounds.getWest().toFixed(3)),
      south: Number(bounds.getSouth().toFixed(3)),
      east: Number(bounds.getEast().toFixed(3)),
      north: Number(bounds.getNorth().toFixed(3))
    };
    const visibleCached = GeometryCache.getForViewport(level, bboxObject);
    if (visibleCached.length > 0 || GeometryCache.isCovered(level, bboxObject)) {
      MapAdapter.loadGeoJSONWithFade({
        type: 'FeatureCollection',
        features: visibleCached
      });
      document.getElementById('totalAreas').textContent = visibleCached.length;
      this.lastRenderedLevel = level;
    }
    return visibleCached.length;
  },

  renderBestAvailableViewport(preferredLevel = this.currentAdminLevel, bboxOverride = null) {
    const tryLevels = [];
    for (let level = preferredLevel; level >= 0; level -= 1) {
      tryLevels.push(level);
    }
    if (!tryLevels.includes(this.lastRenderedLevel)) {
      tryLevels.push(this.lastRenderedLevel);
    }

    for (const level of tryLevels) {
      // A covered level may have no shapes because this country does not
      // publish that depth. Never clear the previous level while searching.
      const bounds = bboxOverride || MapAdapter.map.getBounds();
      const bbox = bboxOverride || {
        west: Number(bounds.getWest().toFixed(3)),
        south: Number(bounds.getSouth().toFixed(3)),
        east: Number(bounds.getEast().toFixed(3)),
        north: Number(bounds.getNorth().toFixed(3))
      };
      if (GeometryCache.getForViewport(level, bbox).length > 0) {
        this.renderViewportFromCache(level, bbox);
        NavigationManager?.updateLevelDisplay(level);
        return level;
      }
    }
    return null;
  },

  async holdPreviousAdminLevel(adminLevel, targetLevel, bboxObject) {
    if (adminLevel <= this.lastRenderedLevel) return false;
    const previousLevel = Math.min(adminLevel - 1, this.lastRenderedLevel);
    if (previousLevel < 0) return false;
    if (GeometryCache.getForViewport(previousLevel, bboxObject).length > 0) {
      this.renderViewportFromCache(previousLevel, bboxObject);
      NavigationManager?.updateLevelDisplay(previousLevel);
      return true;
    }
    // The previous shapes may not cover a new pan at the same zoom. Request
    // the next shallower level, stopping at the last level that was rendered.
    if (!GeometryCache.isCovered(previousLevel, bboxObject)) {
      await this.doLoad(adminLevel - 1, targetLevel);
      return true;
    }
    return false;
  },

  // Viewport area thresholds (in square degrees) for admin level selection
  // These are tunable - smaller areas = deeper admin levels
  // Area roughly corresponds to zoom: zoom 10 ~ 1-2 sq deg, zoom 14 ~ 0.01 sq deg
  //
  // Navigation layers (contiguous, smooth zoom):
  //   0: Countries, 1: States, 2: Counties, 3: Tracts, 4: Block Groups,
  //   5: Blocks / comparable local detail, 6: source-native micro areas.
  //
  // This is the canonical admin path. Parallel geometry tracks such as ZCTA,
  // tribal, watersheds, and parks are handled separately through overlays /
  // special geometry sources, not by reinterpreting admin levels.
  //
  // Country deep profiles can extend the spine differently. The loader asks
  // for the next canonical depth and falls back gracefully when that country
  // has no rows at the requested level.
  areaThresholds: {
    level0: 3000,   // > 3000 sq deg = countries (world/continent view, zoom ~2-4)
    level1: 300,    // > 300 sq deg = states (large country view, zoom ~4-6)
    level2: 30,     // > 30 sq deg = counties (state/region view, zoom ~6-8)
    level3: 3,      // > 3 sq deg = census tracts (single county view, zoom ~9)
    level4: 0.3,    // > 0.3 sq deg = block groups (city/district view, zoom ~12-13)
    level5: 0.001   // > 0.001 sq deg = blocks / comparable local detail
                    // < 0.001 sq deg = source-native micro areas
  },

  /**
   * Calculate viewport area in square degrees
   */
  getViewportArea(bounds) {
    const width = Math.abs(bounds.getEast() - bounds.getWest());
    const height = Math.abs(bounds.getNorth() - bounds.getSouth());
    return width * height;
  },

  expandBboxForIndex(bboxObject, adminLevel) {
    if (!bboxObject) return bboxObject;
    if (adminLevel !== 1 && adminLevel !== 2) {
      return bboxObject;
    }

    const width = Math.max(0.001, bboxObject.east - bboxObject.west);
    const height = Math.max(0.001, bboxObject.north - bboxObject.south);
    const haloFactor = adminLevel === 1 ? 0.12 : 0.08;
    const haloLon = width * haloFactor;
    const haloLat = height * haloFactor;

    return {
      west: Number((bboxObject.west - haloLon).toFixed(3)),
      south: Number((bboxObject.south - haloLat).toFixed(3)),
      east: Number((bboxObject.east + haloLon).toFixed(3)),
      north: Number((bboxObject.north + haloLat).toFixed(3))
    };
  },

  /**
   * Get admin level based on viewport area (smarter than fixed zoom thresholds)
   * Larger viewport = shallower level, smaller viewport = deeper level
   *
   * Navigation levels (contiguous, smooth zoom):
   *   0 = Countries
   *   1 = States/Provinces
   *   2 = Counties
   *   3 = Census Tracts
   *   4 = Block Groups
   *   5 = Blocks / comparable local detail
   *   6 = Source-native micro areas where a strict-nested country spine has them
   *
   * These are canonical admin levels only. Cities/places, tribal lands,
   * ZCTAs, watersheds, and similar geometries live on separate overlay tracks.
   */
  getAdminLevelForViewport(bounds) {
    const area = this.getViewportArea(bounds);

    if (area > this.areaThresholds.level0) return 0;  // Countries
    if (area > this.areaThresholds.level1) return 1;  // States
    if (area > this.areaThresholds.level2) return 2;  // Counties
    if (area > this.areaThresholds.level3) return 3;  // Census Tracts
    if (area > this.areaThresholds.level4) return 4;  // Block Groups
    if (area > this.areaThresholds.level5) return 5;  // Blocks / comparable local detail
    return 6;  // Source-native micro areas
  },

  /**
   * Get the target viewport area for a given admin level
   * Returns a value solidly within the range for that level
   * (biased toward the upper end to account for projection distortion)
   */
  getTargetAreaForAdminLevel(level) {
    switch(level) {
      case 0: return 5000;    // Countries: > 3000 sq deg, target ~5000
      case 1: return 1000;    // States: 300-3000 sq deg, target ~1000
      case 2: return 150;     // Counties: 30-300 sq deg, target ~150 (zoom ~7.5)
      case 3: return 15;      // Census Tracts: 3-30 sq deg, target ~15
      case 4: return 1.5;     // Block Groups: 0.3-3 sq deg, target ~1.5
      case 5: return 0.15;    // Blocks: < 0.3 sq deg, target ~0.15
      case 6: return 0.0005;  // Source-native micro areas: < 0.001 sq deg
      default: return 15;     // Fallback to tracts
    }
  },

  /**
   * Calculate zoom level needed to achieve target viewport area at current center
   * Uses the relationship: area scales by ~4x per zoom level (2x each dimension)
   * @param {number} targetArea - Target viewport area in square degrees
   * @returns {number} Zoom level to achieve that area
   */
  getZoomForTargetArea(targetArea) {
    if (!MapAdapter?.map) return 2;

    const bounds = MapAdapter.map.getBounds();
    const currentArea = this.getViewportArea(bounds);
    const currentZoom = MapAdapter.map.getZoom();

    // Each zoom level change roughly quarters/quadruples the viewport area
    // zoom_delta = log2(current_area / target_area) / 2
    // (divide by 2 because area scales with zoom^2)
    const zoomDelta = Math.log2(currentArea / targetArea) / 2;
    const targetZoom = currentZoom + zoomDelta;

    // Clamp to reasonable zoom range
    return Math.max(1, Math.min(18, targetZoom));
  },

  /**
   * Get zoom level for a given admin level based on current viewport
   * Calculates the zoom needed to achieve the target area for that level
   */
  getZoomForAdminLevel(level) {
    const targetArea = this.getTargetAreaForAdminLevel(level);
    return this.getZoomForTargetArea(targetArea);
  },

  /**
   * Toggle level lock on/off
   * When locked, zoom changes don't affect admin level
   */
  toggleLock() {
    if (this.levelLocked) {
      // Unlock
      this.levelLocked = false;
      this.lockedLevel = null;
      console.log('Admin level unlocked - will change with zoom');
    } else {
      // Lock at current level
      this.levelLocked = true;
      this.lockedLevel = this.currentAdminLevel;
      console.log(`Admin level locked at ${this.lockedLevel}`);
    }
    return this.levelLocked;
  },

  /**
   * Set lock state directly
   * @param {boolean} locked - Whether to lock
   * @param {number} level - Level to lock at (optional, uses current if not provided)
   */
  setLock(locked, level = null) {
    this.levelLocked = locked;
    if (locked) {
      this.lockedLevel = level !== null ? level : this.currentAdminLevel;
      console.log(`Admin level locked at ${this.lockedLevel}`);
    } else {
      this.lockedLevel = null;
      console.log('Admin level unlocked');
    }
  },

  /**
   * Load geometry for current viewport
   * Uses short debounce (300ms) to batch rapid viewport changes
   */
  async load(adminLevel) {
    if (this.tabSuspended || (typeof document !== 'undefined' && document.hidden)) {
      return false;
    }
    if (this.loadTimeout) {
      clearTimeout(this.loadTimeout);
      this.loadTimeout = null;
      this.loadResolve?.(false);
      this.loadResolve = null;
    }

    // Return the actual load promise. Overlay activation awaits this method
    // before reporting how many shapes are visible; previously it awaited only
    // the debounce setup and immediately announced "Loaded 0 items".
    return new Promise((resolve) => {
      this.loadResolve = resolve;
      this.loadTimeout = setTimeout(async () => {
        this.loadTimeout = null;
        this.loadResolve = null;
        try {
          await this.doLoad(adminLevel);
          resolve(true);
        } catch (error) {
          console.error('Viewport geometry load failed:', error);
          resolve(false);
        }
      }, CONFIG.viewport.debounceMs || 300);
    });
  },

  /**
   * Actually perform the load
   */
  async doLoad(adminLevel, targetLevel = adminLevel) {
    if (!MapAdapter?.map || this.tabSuspended || !isAdminLayersActive()) return;
    let missingLocIds = [];
    let requestKey = null;

    const bounds = MapAdapter.map.getBounds();
    const bboxObject = {
      west: Number(bounds.getWest().toFixed(3)),
      south: Number(bounds.getSouth().toFixed(3)),
      east: Number(bounds.getEast().toFixed(3)),
      north: Number(bounds.getNorth().toFixed(3))
    };
    // Round to 3 decimal places (~100m precision) - more than enough for viewport queries
    const bbox = [
      bboxObject.west.toFixed(3),
      bboxObject.south.toFixed(3),
      bboxObject.east.toFixed(3),
      bboxObject.north.toFixed(3)
    ].join(',');
    const indexBboxObject = this.expandBboxForIndex(bboxObject, adminLevel);
    const indexBbox = [
      indexBboxObject.west.toFixed(3),
      indexBboxObject.south.toFixed(3),
      indexBboxObject.east.toFixed(3),
      indexBboxObject.north.toFixed(3)
    ].join(',');

    // Show cached shapes for the latest viewport, even while a previous
    // same-level request is still completing.
    if (this.renderBestAvailableViewport(adminLevel, bboxObject) === null && GeometryCache.features.size > 0) {
      MapAdapter.loadGeoJSONWithFade({ type: 'FeatureCollection', features: [] });
      document.getElementById('totalAreas').textContent = 0;
    }

    if (this.abortController) {
      if (this.lastRequestedLevel === adminLevel) {
        this.pendingSameLevel = true;
        console.log(`Level ${adminLevel} request in flight; queued latest viewport`);
        return;
      }
      this.cancelActiveRequest();
      console.log(`Admin level changed, cancelling previous level ${this.lastRequestedLevel} request`);
    }

    if (this.lastRequestedLevel !== adminLevel) this.pendingSameLevel = false;
    this.lastRequestedLevel = adminLevel;
    const requestController = new AbortController();
    this.abortController = requestController;
    const thisRequestId = ++this.requestId;

    if (GeometryCache.isCovered(adminLevel, bboxObject)) {
      console.log(`[${thisRequestId}] Level ${adminLevel} viewport already covered, rendering from cache`);
      if (GeometryCache.getForViewport(adminLevel, bboxObject).length > 0) {
        this.renderViewportFromCache(adminLevel, bboxObject);
        if (adminLevel !== targetLevel) NavigationManager?.updateLevelDisplay(adminLevel);
      } else if (!await this.holdPreviousAdminLevel(adminLevel, targetLevel, bboxObject)) {
        this.renderViewportFromCache(adminLevel, bboxObject);
      }
      if (this.abortController === requestController) this.abortController = null;
      return;
    }

    requestKey = GeometryCache.startInFlight(adminLevel, bboxObject);
    if (requestKey === null) {
      console.log(`[${thisRequestId}] Level ${adminLevel} viewport already in flight, rendering cached subset`);
      this.renderViewportFromCache(adminLevel, bboxObject);
      if (this.abortController === requestController) this.abortController = null;
      return;
    }
    this.activeRequest = { id: thisRequestId, level: adminLevel, key: requestKey, locIds: [] };

    try {
      const indexUrl = `/geometry/index?admin_level=${adminLevel}&bbox=${indexBbox}`;
      console.log(`[${thisRequestId}] Fetching geometry index level ${adminLevel}`);
      const indexData = await fetchMsgpack(indexUrl, { signal: requestController.signal });
      const requestedLocIds = (indexData.rows || [])
        .map((row) => row.loc_id)
        .filter(Boolean);
      const loadedLocIds = requestedLocIds.filter((locId) => GeometryCache.hasLocId(locId));
      const inFlightLocIds = requestedLocIds.filter((locId) => GeometryCache.isLocIdInFlight(adminLevel, locId));
      missingLocIds = requestedLocIds.filter((locId) =>
        !GeometryCache.hasLocId(locId) && !GeometryCache.isLocIdInFlight(adminLevel, locId)
      );

      if (missingLocIds.length === 0) {
        const currentBbox = this.getCurrentBbox();
        const visibleCached = GeometryCache.getForViewport(adminLevel, currentBbox);
        const fullyLoaded = inFlightLocIds.length === 0 && loadedLocIds.length === requestedLocIds.length;
        console.log(`[${thisRequestId}] Level ${adminLevel} has no newly missing ids (${loadedLocIds.length} loaded, ${inFlightLocIds.length} in flight)`);
        if (fullyLoaded) {
          GeometryCache.markCoverage(adminLevel, indexBboxObject);
        }
        if (visibleCached.length > 0) {
          this.renderViewportFromCache(adminLevel, currentBbox);
          if (adminLevel !== targetLevel) NavigationManager?.updateLevelDisplay(adminLevel);
        } else if (fullyLoaded || requestedLocIds.length === 0) {
          if (!await this.holdPreviousAdminLevel(adminLevel, targetLevel, currentBbox)) {
            this.renderViewportFromCache(adminLevel, currentBbox);
          }
        }
        return;
      }

      GeometryCache.markLocIdsInFlight(adminLevel, missingLocIds);
      if (this.activeRequest?.id === thisRequestId) {
        this.activeRequest.locIds = missingLocIds;
      }
      console.log(`[${thisRequestId}] Fetching ${missingLocIds.length} missing geometry ids at level ${adminLevel}`);
      const batchSize = CONFIG.viewport.geometryBatchSize || 500;
      const features = [];
      for (let offset = 0; offset < missingLocIds.length; offset += batchSize) {
        const batch = missingLocIds.slice(offset, offset + batchSize);
        const batchData = await postMsgpack(
          '/geometry/display-features',
          { loc_ids: batch },
          { signal: requestController.signal }
        );
        if (batchData.features?.length) features.push(...batchData.features);
        if (thisRequestId !== this.requestId || targetLevel !== this.currentAdminLevel) break;
      }
      const data = { type: 'FeatureCollection', features };

      if (data.features?.length) {
        GeometryCache.add(data.features);
      }

      // Check if this request was superseded by a newer one
      if (!isAdminLayersActive()) return;
      if (thisRequestId !== this.requestId) {
        console.log(`[${thisRequestId}] Discarding stale response (current is ${this.requestId})`);
        if (targetLevel === this.currentAdminLevel) {
          this.renderBestAvailableViewport(adminLevel);
        }
        return;
      }

      // Double-check we're still on the same level (user might have zoomed while parsing)
      if (targetLevel !== this.currentAdminLevel) {
        console.log(`[${thisRequestId}] Level changed during load (was ${targetLevel}, now ${this.currentAdminLevel}), discarding`);
        this.renderBestAvailableViewport(this.currentAdminLevel);
        return;
      }

      const featureCount = data.features?.length || 0;
      const currentBbox = this.getCurrentBbox();
      console.log(`[${thisRequestId}] Level ${adminLevel} response: ${featureCount} features`);

      if (featureCount > 0) {
        // A country read can fail while other countries in the same batch
        // succeed. Keep those shapes, but leave coverage open so the missing
        // IDs can be fetched on the next viewport load.
        if (requestedLocIds.every((locId) => GeometryCache.hasLocId(locId))) {
          GeometryCache.markCoverage(adminLevel, indexBboxObject);
        } else {
          console.warn(`[${thisRequestId}] Level ${adminLevel} returned a partial shape batch; coverage remains open`);
        }
        this.renderViewportFromCache(adminLevel, currentBbox);
        if (adminLevel !== targetLevel) NavigationManager?.updateLevelDisplay(adminLevel);
      } else {
        this.renderBestAvailableViewport(Math.min(adminLevel - 1, this.lastRenderedLevel), currentBbox);
      }
    } catch (err) {
      // Ignore abort errors - they're expected when we cancel requests
      if (err.name === 'AbortError') {
        console.log(`[${thisRequestId}] Request aborted`);
        return;
      }
      console.error('Viewport load failed:', err);
      // Keep displaying cached data - user sees no change
      const currentBbox = this.getCurrentBbox();
      const cached = GeometryCache.getForViewport(adminLevel, currentBbox);
      if (cached.length === 0) {
        console.warn('No cached data available');
        this.renderBestAvailableViewport(Math.min(adminLevel - 1, this.lastRenderedLevel), currentBbox);
      }
    } finally {
      if (this.activeRequest?.id === thisRequestId) {
        GeometryCache.clearLocIdsInFlight(adminLevel, missingLocIds);
        GeometryCache.finishInFlight(adminLevel, requestKey);
        this.activeRequest = null;
      }
      if (this.abortController === requestController) this.abortController = null;
      if (this.pendingSameLevel && !this.abortController) {
        this.pendingSameLevel = false;
        if (this.currentAdminLevel === adminLevel && MapAdapter?.map &&
            !this.tabSuspended && isAdminLayersActive()) {
          const latestBbox = this.getCurrentBbox();
          if (GeometryCache.isCovered(adminLevel, latestBbox)) {
            this.renderBestAvailableViewport(adminLevel, latestBbox);
          } else {
            void this.load(adminLevel);
          }
        }
      }
    }
  },

  getCurrentBbox() {
    const bounds = MapAdapter.map.getBounds();
    return {
      west: Number(bounds.getWest().toFixed(3)),
      south: Number(bounds.getSouth().toFixed(3)),
      east: Number(bounds.getEast().toFixed(3)),
      north: Number(bounds.getNorth().toFixed(3))
    };
  },

  /**
   * Handle zoom/move change - check if admin level should change based on viewport area
   */
  onViewportChange() {
    if (!this.enabled || !MapAdapter?.map || this.tabSuspended) return;

    const bounds = MapAdapter.map.getBounds();
    const area = this.getViewportArea(bounds);
    const calculatedLevel = this.getAdminLevelForViewport(bounds);

    // Use locked level if lock is active, otherwise use calculated level
    const newLevel = this.levelLocked ? this.lockedLevel : calculatedLevel;

    // In order mode, filter displayed data by admin level instead of loading new data
    if (this.orderMode) {
      if (Date.now() < this.orderModeLevelHoldUntil && newLevel > this.currentAdminLevel) {
        return;
      }
      if (newLevel !== this.currentAdminLevel) {
        const lockInfo = this.levelLocked ? ' [LOCKED]' : '';
        console.log(`Order mode: Viewport area ${area.toFixed(0)} sq deg -> Admin level ${newLevel}${lockInfo}`);
        this.currentAdminLevel = newLevel;
        if (App?.hasLazyMetricOrder?.()) {
          App.ensureMetricLevelLoaded(newLevel).then((loaded) => {
            if (this.currentAdminLevel !== newLevel) return;
            if (loaded) {
              App.applyOrderModeLevelFilter?.(newLevel);
            }
          }).catch((error) => {
            console.warn(`Lazy metric level load failed for admin_${newLevel}:`, error.message);
          });
        } else if (App?.applyOrderModeLevelFilter) {
          App.applyOrderModeLevelFilter(newLevel);
        } else {
          TimeSlider?.setAdminLevelFilter(newLevel);
        }
      }
      return;
    }

    // Only load when admin level CHANGES - not on every zoom step
    if (newLevel !== this.currentAdminLevel) {
      const lockInfo = this.levelLocked ? ' [LOCKED]' : '';
      console.log(`Viewport area ${area.toFixed(0)} sq deg -> Admin level ${newLevel}${lockInfo}`);

      this.currentAdminLevel = newLevel;
      this.pendingSameLevel = false;
      this.load(newLevel);

      // Update breadcrumb to show current level
      NavigationManager?.updateLevelDisplay(newLevel);
    }
    // Note: Same-level pan reloads are handled by onMoveEnd, not here
  },

  /**
   * Handle pan/move - reload same level for new viewport area
   * Only triggers on actual panning, not zoom changes
   */
  onPanEnd() {
    if (!this.enabled || !MapAdapter?.map || this.orderMode) return;

    // Reload current level for the new viewport position
    this.load(this.currentAdminLevel);
  },

  /**
   * Handle zoom end - just update lastZoom for tracking
   * The actual level-change logic is handled by onMoveEnd to avoid double-firing
   */
  onZoomEnd() {
    // No-op: moveend handles everything now
    // This method exists for API compatibility but does nothing
  },

  /**
   * Handle move end - detect zoom vs pan and respond appropriately
   * moveend fires for both zoom and pan, so we check if zoom changed
   */
  onMoveEnd() {
    if (!MapAdapter?.map) return;

    // Admin Layers owns viewport-based spine loading/filtering. Keep
    // demographics as a legacy alias so older restored sessions still work.
    if (!isAdminLayersActive()) {
      return;
    }

    const currentZoom = MapAdapter.map.getZoom();

    // Check if this was a zoom or a pan
    const wasZoom = this.lastZoom === null || Math.abs(currentZoom - this.lastZoom) >= 0.01;

    // Update lastZoom for next comparison
    this.lastZoom = currentZoom;

    // In order mode, filter displayed data by admin level instead of loading new data
    if (this.orderMode) {
      if (wasZoom) {
        this.onViewportChange();  // This will filter the order data by admin level
      }
      return;  // Don't load new data in order mode
    }

    if (wasZoom) {
      // A zoom can move the map center without crossing an admin-level
      // threshold. Refresh that level for the new bbox as well.
      const previousLevel = this.currentAdminLevel;
      this.onViewportChange();
      if (this.currentAdminLevel === previousLevel) {
        this.load(this.currentAdminLevel);
      }
    } else {
      // Pure pan - reload same level for new viewport position
      this.onPanEnd();
    }
  }
};
