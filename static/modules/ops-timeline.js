/**
 * Shared retained-snapshot scrubber for Ops.
 *
 * The server sends the retained frame index once. This module chooses compact
 * per-feed payloads locally and lazily fetches only a selected large
 * alert/point geometry frame. It intentionally does not reuse the Explore
 * animator.
 */

import { postMsgpack } from './utils/fetch.js';
import { NwsAlertsOverlay } from './overlay-nws-alerts.js';
import { getLivePointOverlay } from './live-point-overlay.js';
import { formatOpsTime } from './ops-time-display.js';

const CURSOR_STEP_MS = 5 * 60 * 1000;
const NWS_BACKGROUND_BATCH_SIZE = 24;
const INTERACTIVE_GRACE_MS = 250;
const NWS_SCRUB_DEBOUNCE_MS = 120;
const HISTORY_PRELOAD_METHODS = {
  nws_alerts: '_preloadNwsFrames',
  live_point: '_preloadPointFrames',
};

function toMs(value) {
  const result = Date.parse(String(value || ''));
  return Number.isFinite(result) ? result : null;
}

function floorToCursor(ms) {
  return Math.floor(ms / CURSOR_STEP_MS) * CURSOR_STEP_MS;
}

function formatCursor(ms) {
  return formatOpsTime(ms);
}

function frameCacheKey(frame, overlayId = '') {
  const prefix = overlayId ? `${String(overlayId)}:` : '';
  return `${prefix}${String(frame?.start_at || '')}:${String(frame?.payload_hash || '')}`;
}

function buildNwsLifecycleFrame(bundle, ms) {
  const definitions = bundle?.definitions && typeof bundle.definitions === 'object' ? bundle.definitions : {};
  const features = [];
  const county_geometry_references = [];
  for (const interval of bundle?.intervals || []) {
    const start = toMs(interval?.start_at);
    const end = toMs(interval?.end_at);
    if (start === null || start > ms || (end !== null && ms >= end)) continue;
    const alert = definitions[interval.definition];
    if (!alert) continue;
    const props = {
      alert_id: alert.alert_id, event: alert.event, severity: alert.severity,
      urgency: alert.urgency, certainty: alert.certainty, headline: alert.headline,
      area: alert.area, issued_at: alert.issued_at, expires: alert.expires,
      detail_available: Boolean(alert.detail_available),
    };
    if (alert.geometry?.type) features.push({ type: 'Feature', geometry: alert.geometry, properties: { ...props, display: 'polygon' } });
    else if (Array.isArray(alert.loc_ids) && alert.loc_ids.length) county_geometry_references.push({ loc_ids: alert.loc_ids, properties: { ...props, display: 'county' } });
    if (Array.isArray(alert.point) && alert.point.length >= 2) features.push({ type: 'Feature', geometry: { type: 'Point', coordinates: alert.point }, properties: { ...props, display: 'marker' } });
  }
  return { type: 'FeatureCollection', features, county_geometry_references, detail_at: new Date(ms).toISOString() };
}

function hasNwsLifecycleChanges(bundle) {
  const intervals = Array.isArray(bundle?.intervals) ? bundle.intervals : [];
  if (!intervals.length) return false;
  const starts = new Set(intervals.map((interval) => String(interval?.start_at || '')).filter(Boolean));
  return starts.size > 1 || intervals.some((interval) => Boolean(interval?.end_at));
}

export const OpsTimeline = {
  enabled: false,
  element: null,
  input: null,
  timeLabel: null,
  onFrame: null,
  timeline: null,
  selectedMs: null,
  externalProviders: new Map(),
  nwsFrameCache: new Map(),
  nwsLifecycleBundle: null,
  nwsInteractiveRequestedAt: 0,
  nwsFrameRequestController: null,
  nwsFrameRequestToken: 0,
  nwsFrameRequestTimer: 0,
  pointFrameCache: new Map(),
  // Point feeds are independent overlays. A single global request token would
  // let an AirNow request cancel an in-flight buoy frame (or the reverse),
  // leaving one station layer visually stuck on an older cursor value.
  pointRequestTokens: new Map(),
  pointInteractiveRequestedAt: 0,
  hurricaneReplayData: new Map(),
  hurricaneWarmupPromise: null,
  hurricaneWarmupRun: 0,
  backgroundPrefetchTimers: [],
  backgroundPrefetchRun: 0,
  selectedDisplayPayloads: new Map(),
  selectedDisplayKeys: new Map(),
  pendingSelectAt: null,
  selectRenderRequest: 0,

  init({ onFrame } = {}) {
    this.element = document.getElementById('opsTimelineContainer');
    this.enabled = Boolean(this.element);
    this.onFrame = typeof onFrame === 'function' ? onFrame : null;
    if (!this.enabled && this.element) this.element.hidden = true;
  },

  clear() {
    this.timeline = null;
    this.selectedMs = null;
    this.nwsFrameCache.clear();
    this.nwsLifecycleBundle = null;
    if (this.nwsFrameRequestTimer) clearTimeout(this.nwsFrameRequestTimer);
    this.nwsFrameRequestTimer = 0;
    this.nwsFrameRequestController?.abort?.();
    this.nwsFrameRequestController = null;
    this.nwsFrameRequestToken += 1;
    this.pointFrameCache.clear();
    this.pointRequestTokens.clear();
    this.hurricaneReplayData.clear();
    this.selectedDisplayPayloads.clear();
    this.selectedDisplayKeys.clear();
    this.hurricaneWarmupRun += 1;
    this.hurricaneWarmupPromise = null;
    if (this.selectRenderRequest) cancelAnimationFrame(this.selectRenderRequest);
    this.selectRenderRequest = 0;
    this.pendingSelectAt = null;
    for (const timer of this.backgroundPrefetchTimers) clearTimeout(timer);
    this.backgroundPrefetchTimers = [];
    this.backgroundPrefetchRun += 1;
    if (this.element) {
      this.element.hidden = true;
      this.element.replaceChildren();
    }
  },

  setExternalProvider(id, frames, renderAt) {
    const normalized = String(id || '').trim();
    if (!normalized) return;
    const usable = Array.isArray(frames) ? frames.filter((frame) => toMs(frame?.start_at) !== null) : [];
    // A raster may currently hold only one authoritative Ops frame.  Keep it
    // registered so it still receives the shared cursor while another feed
    // supplies the timeline's multiple selectable snapshots.
    if (!usable.length || typeof renderAt !== 'function') {
      this.externalProviders.delete(normalized);
      this._mergeExternalProviders();
      return;
    }
    this.externalProviders.set(normalized, { frames: usable, renderAt });
    this._mergeExternalProviders();
  },

  _mergeExternalProviders() {
    if (!this.timeline || !this.externalProviders.size) return;
    // Rebuild external entries from the provider registry.  Keeping an old
    // external:* entry here would make a disabled raster keep responding to
    // the cursor after its overlay was turned off.
    const feeds = Object.fromEntries(
      Object.entries(this.timeline.feeds || {}).filter(([id]) => !id.startsWith('external:'))
    );
    for (const [id, provider] of this.externalProviders) feeds[`external:${id}`] = provider.frames;
    const forecastEnd = toMs(this.timeline.forecast_end);
    const providerRangeEnd = Math.max(
      this.timeline.currentMs,
      Number.isFinite(forecastEnd) ? forecastEnd : 0,
      ...Object.values(feeds).flatMap((frames) => (Array.isArray(frames) ? frames : []))
        .map((frame) => toMs(frame?.start_at)).filter(Number.isFinite)
    );
    this.timeline = { ...this.timeline, feeds, rangeEnd: providerRangeEnd };
    this._render();
    this.selectAt(this.selectedMs || this.timeline.currentMs, { preserveCurrent: true });
  },

  async load({ sessionId, watchId, watchContext, timelineFeeds = [] } = {}) {
    if (!this.enabled) return null;
    const response = await postMsgpack('/api/local/ops/timeline', {
      sessionId,
      watch_id: watchId,
      watch_context: watchContext,
      timeline_feeds: timelineFeeds,
    });
    await this.setTimeline(response?.timeline);
    return response;
  },

  async setTimeline(timeline) {
    const feedFrames = timeline?.feeds;
    if (!this.enabled || !feedFrames || typeof feedFrames !== 'object') {
      this.clear();
      return;
    }
    const suppliedRangeStart = toMs(timeline.range_start);
    const currentMs = toMs(timeline.range_end);
    if (!Number.isFinite(suppliedRangeStart) || !Number.isFinite(currentMs)) {
      this.clear();
      return;
    }
    const rangeStart = suppliedRangeStart;
    const frameCount = Object.values(feedFrames).reduce(
      (total, frames) => total + (Array.isArray(frames) ? frames.length : 0),
      0
    ) + Array.from(this.externalProviders.values()).reduce(
      (total, provider) => total + provider.frames.length,
      0
    );
    // A single current snapshot is timestamped, but has no changing state to
    // inspect.  Keep the control invisible until an active provider offers a
    // real choice of frames.
    if (frameCount < 2) {
      this.clear();
      return;
    }
    const mergedFeeds = { ...feedFrames };
    for (const [id, provider] of this.externalProviders) mergedFeeds[`external:${id}`] = provider.frames;
    const forecastEnd = toMs(timeline.forecast_end);
    const rangeEnd = Math.max(
      currentMs,
      Number.isFinite(forecastEnd) ? forecastEnd : 0,
      ...Object.values(mergedFeeds).flatMap((frames) => (Array.isArray(frames) ? frames : []))
        .map((frame) => toMs(frame?.start_at)).filter(Number.isFinite)
    );
    this.timeline = {
      ...timeline,
      feeds: mergedFeeds,
      rangeStart,
      currentMs,
      rangeEnd,
      historyHours: Math.max(1, Math.round((currentMs - rangeStart) / 3_600_000)),
    };
    this.hurricaneReplayData.clear();
    for (const [feedId, replay] of Object.entries(timeline?.hurricane_replay || {})) {
      if (replay?.type === 'hurricane_replay') {
        this.hurricaneReplayData.set(feedId, replay);
      }
    }
    this._render();
    this.selectAt(currentMs, { preserveCurrent: true });
    await this._warmRequiredHistory();
    this._scheduleBackgroundPrefetch();
  },

  _render() {
    const { rangeStart, rangeEnd, currentMs } = this.timeline;
    this.element.hidden = false;
    this.element.innerHTML = `
      <div class="ops-timeline-header"><strong>Ops snapshots</strong><span data-ops-time></span></div>
      <div class="ops-timeline-track">
        <span class="ops-timeline-past" aria-hidden="true"></span>
        <span class="ops-timeline-forecast" aria-hidden="true"></span>
        <button type="button" class="ops-timeline-now" data-ops-now title="Jump to live now" aria-label="Jump to live now"></button>
        <input data-ops-timeline-input type="range" step="${CURSOR_STEP_MS}" aria-label="Ops snapshot time">
      </div>
      <div class="ops-timeline-labels"><span>${this.timeline.historyHours >= 48 && this.timeline.historyHours % 24 === 0 ? `${this.timeline.historyHours / 24}d` : `${this.timeline.historyHours}h`} history</span><button type="button" class="ops-timeline-now-label" data-ops-now>Now</button><span>${this.timeline.rangeEnd > currentMs ? `${Math.max(1, Math.round((this.timeline.rangeEnd - currentMs) / 60_000))}m forecast` : 'No forecast'}</span></div>
    `;
    this.input = this.element.querySelector('[data-ops-timeline-input]');
    this.timeLabel = this.element.querySelector('[data-ops-time]');
    this.input.min = String(floorToCursor(rangeStart));
    this.input.max = String(floorToCursor(rangeEnd));
    this.input.value = String(floorToCursor(currentMs));
    this.input.disabled = this._hasPendingRequiredHistoryWarmup();
    const nowPercent = ((currentMs - rangeStart) / (rangeEnd - rangeStart)) * 100;
    this.element.style.setProperty('--ops-now-position', `${Math.max(0, Math.min(100, nowPercent))}%`);
    this.input.addEventListener('input', () => this.selectAt(Number(this.input.value)));
    this.element.querySelectorAll('[data-ops-now]').forEach((control) => {
      control.addEventListener('click', () => this.selectAt(this.timeline.currentMs));
    });
  },

  selectAt(ms, { preserveCurrent = false } = {}) {
    if (!this.timeline || !Number.isFinite(ms)) return;
    this.selectedMs = ms;
    if (this.input && Number(this.input.value) !== ms) this.input.value = String(ms);
    if (this.timeLabel) this.timeLabel.textContent = formatCursor(ms);
    this.pendingSelectAt = { ms, preserveCurrent };
    if (this.selectRenderRequest) return;
    this.selectRenderRequest = requestAnimationFrame(() => {
      this.selectRenderRequest = 0;
      const pending = this.pendingSelectAt;
      this.pendingSelectAt = null;
      if (pending) this._renderSelectedFrame(pending.ms, { preserveCurrent: pending.preserveCurrent });
    });
  },

  _renderSelectedFrame(ms, { preserveCurrent = false } = {}) {
    if (!this.timeline || !Number.isFinite(ms)) return;
    const specialFrames = [];
    const updatedFeedIds = new Set();
    const clearFeedIds = new Set();
    for (const [feedId, frames] of Object.entries(this.timeline.feeds || {})) {
      if (!Array.isArray(frames)) continue;
      let selected = null;
      for (const frame of frames) {
        const start = toMs(frame?.start_at);
        const end = toMs(frame?.end_at);
        if (start === null || start > ms) break;
        selected = (end === null || ms < end) ? frame : null;
      }
      if (selected?.timeline_provider === 'nws_alerts') {
        if (ms > this.timeline.currentMs) void NwsAlertsOverlay.setOpsTimelineFrame?.({ type: 'FeatureCollection', features: [] });
        else if (this.nwsLifecycleBundle) void NwsAlertsOverlay.setOpsTimelineFrame?.(buildNwsLifecycleFrame(this.nwsLifecycleBundle, ms));
        else void this._loadNwsFrame(selected, ms);
      } else if (selected?.timeline_provider === 'live_point') {
        const pointOverlay = getLivePointOverlay(selected.overlay_id);
        if (ms > this.timeline.currentMs) pointOverlay?.setOpsTimelineFrame?.({ type: 'FeatureCollection', features: [] });
        else void this._loadPointFrame(selected, ms);
      } else if (frames[0]?.timeline_provider === 'live_point') {
        const pointOverlay = getLivePointOverlay(frames[0].overlay_id);
        if (ms > this.timeline.currentMs) pointOverlay?.setOpsTimelineFrame?.({ type: 'FeatureCollection', features: [] });
        else pointOverlay?.setOpsTimelineFrame?.({ type: 'FeatureCollection', features: [] });
      } else if (this.hurricaneReplayData.has(feedId)) {
        if (this._isPastHurricaneReplayEnd(feedId, ms)) {
          this.selectedDisplayPayloads.delete(feedId);
          this.selectedDisplayKeys.delete(feedId);
          updatedFeedIds.add(feedId);
          clearFeedIds.add(feedId);
          continue;
        }
        const replayPayload = this._buildHurricaneReplayDisplayPayload(feedId, ms, selected);
        if (replayPayload) {
          const renderKey = String(replayPayload.ops_render_key || '');
          if (renderKey && this.selectedDisplayKeys.get(feedId) === renderKey) {
            this.selectedDisplayPayloads.set(feedId, replayPayload);
          } else {
            this.selectedDisplayPayloads.set(feedId, replayPayload);
            if (renderKey) this.selectedDisplayKeys.set(feedId, renderKey);
            updatedFeedIds.add(feedId);
          }
        }
      } else if (selected?.display_payload?.ops_timeline_provider) {
        specialFrames.push(selected.display_payload);
      } else if (selected?.display_payload) {
        const displayPayload = selected.display_payload;
        this.selectedDisplayPayloads.set(feedId, displayPayload);
        this.selectedDisplayKeys.delete(feedId);
        updatedFeedIds.add(feedId);
      } else if (!feedId.startsWith('external:')) {
        this.selectedDisplayPayloads.delete(feedId);
        this.selectedDisplayKeys.delete(feedId);
        updatedFeedIds.add(feedId);
        clearFeedIds.add(feedId);
      }
      if (feedId.startsWith('external:')) {
        const provider = this.externalProviders.get(feedId.slice('external:'.length));
        provider?.renderAt?.(ms);
      }
    }
    // Keep the last painted frame on screen until a feed has a real
    // replacement. Slider ticks between storm fixes update the label/thumb
    // only; they must not become empty timeline renders.
    if (updatedFeedIds.size > 0) {
      this.onFrame?.(Array.from(this.selectedDisplayPayloads.values()), {
        at: new Date(ms).toISOString(),
        opsTimelineUpdate: true,
        opsTimelineFeedIds: Array.from(updatedFeedIds),
        preserveMissing: clearFeedIds.size === 0,
      });
    }
    for (const frame of specialFrames) {
      if (frame.ops_timeline_provider === 'nws_alerts') {
        void NwsAlertsOverlay.setOpsTimelineFrame?.(frame.geojson);
      }
    }
  },

  async _loadNwsFrame(frame, selectedMs) {
    // A retained-frame timestamp is the cursor identity. Some collectors
    // legitimately reuse a payload hash for a compact/no-op envelope; using
    // only that hash would make a second active provider appear to freeze NWS
    // on the first cached alert frame.
    const key = `${String(frame?.start_at || '')}:${String(frame?.payload_hash || '')}`;
    if (!key) return;
    this.nwsInteractiveRequestedAt = Date.now();
    let loaded = this.nwsFrameCache.get(key);
    if (loaded?.geojson) {
      if (this.selectedMs !== selectedMs) return;
      void NwsAlertsOverlay.setOpsTimelineFrame?.(loaded.geojson);
      return;
    }
    // Slider input can emit dozens of positions while the user drags. Abort
    // the obsolete request and wait briefly for the thumb to settle. This
    // keeps a slow Railway hot-store lookup from blocking every newer cursor
    // position behind it while Aurora and local raster frames continue moving.
    if (this.nwsFrameRequestTimer) clearTimeout(this.nwsFrameRequestTimer);
    this.nwsFrameRequestController?.abort?.();
    const token = ++this.nwsFrameRequestToken;
    this.nwsFrameRequestTimer = setTimeout(() => {
      this.nwsFrameRequestTimer = 0;
      void this._fetchNwsFrame({ frame, selectedMs, key, token });
    }, NWS_SCRUB_DEBOUNCE_MS);
  },

  async _fetchNwsFrame({ frame, selectedMs, key, token }) {
    if (token !== this.nwsFrameRequestToken || selectedMs !== this.selectedMs) return;
    const controller = new AbortController();
    this.nwsFrameRequestController = controller;
    try {
      const response = await postMsgpack('/api/local/ops/timeline/nws-frame', {
        payload_hash: frame?.payload_hash,
        at: frame?.start_at || new Date(selectedMs).toISOString(),
      }, { signal: controller.signal, silent: true });
      const loaded = response?.frame;
      if (loaded) this.nwsFrameCache.set(key, loaded);
      if (token === this.nwsFrameRequestToken && selectedMs === this.selectedMs && loaded?.geojson) {
        void NwsAlertsOverlay.setOpsTimelineFrame?.(loaded.geojson);
      }
    } catch (error) {
      if (error?.name !== 'AbortError') console.warn('OpsTimeline: retained NWS frame failed', error);
    } finally {
      if (this.nwsFrameRequestController === controller) this.nwsFrameRequestController = null;
    }
  },

  async _loadPointFrame(frame, selectedMs) {
    const overlayId = String(frame?.overlay_id || '');
    const key = `${overlayId}:${String(frame?.start_at || '')}:${String(frame?.payload_hash || '')}`;
    if (!overlayId || !key) return;
    const token = (this.pointRequestTokens.get(overlayId) || 0) + 1;
    this.pointRequestTokens.set(overlayId, token);
    this.pointInteractiveRequestedAt = Date.now();
    let loaded = this.pointFrameCache.get(key);
    if (!loaded) {
      try {
        const response = await postMsgpack('/api/local/ops/timeline/point-frame', {
          overlay_id: overlayId,
          at: frame.start_at || new Date(selectedMs).toISOString(),
        });
        loaded = response?.frame;
        if (loaded) this.pointFrameCache.set(key, loaded);
      } catch (error) {
        console.warn('OpsTimeline: retained point frame failed', error);
        return;
      }
    }
    if (token !== this.pointRequestTokens.get(overlayId) || this.selectedMs !== selectedMs || !loaded?.geojson) return;
    void getLivePointOverlay(overlayId)?.setOpsTimelineFrame?.(loaded.geojson);
  },

  _hasPendingRequiredHistoryWarmup() {
    return false;
  },

  async _warmRequiredHistory() {
    this.hurricaneWarmupPromise = null;
  },

  _scheduleBackgroundPrefetch() {
    for (const timer of this.backgroundPrefetchTimers) clearTimeout(timer);
    this.backgroundPrefetchTimers = [];
    const run = ++this.backgroundPrefetchRun;
    const timeline = this.timeline;
    const contracts = timeline?.preload_history || {};
    for (const [feedId, contract] of Object.entries(contracts)) {
      if (!contract?.preload_history) continue;
      const provider = String(contract.provider || '');
      const methodName = HISTORY_PRELOAD_METHODS[provider];
      const preload = methodName && this[methodName];
      const frames = Array.isArray(timeline?.feeds?.[feedId])
        ? timeline.feeds[feedId].filter((frame) => frame?.timeline_provider === provider).reverse()
        : [];
      if (!frames.length || typeof preload !== 'function') continue;
      // Current data has already rendered.  Each declared provider warms its
      // own fixed retained window silently, in its measured batch size.
      void preload.call(this, frames, timeline, run, contract.batch_size, contract);
    }
  },

  async _preloadNwsFrames(frames, timeline, run, declaredBatchSize = NWS_BACKGROUND_BATCH_SIZE) {
    if (!this.nwsLifecycleBundle) {
      try {
        const response = await postMsgpack('/api/local/ops/timeline/nws-bundle', {}, { silent: true });
        if (run !== this.backgroundPrefetchRun || timeline !== this.timeline) return;
        if (
          response?.bundle?.definitions &&
          hasNwsLifecycleChanges(response.bundle)
        ) {
          this.nwsLifecycleBundle = response.bundle;
          this._renderSelectedFrame(this.selectedMs || timeline.currentMs, { preserveCurrent: true });
          return;
        }
      } catch (_) {
        // Old deployments use the existing bounded per-frame fallback below.
      }
    }
    const batchSize = Math.max(1, Math.min(24, Number(declaredBatchSize) || NWS_BACKGROUND_BATCH_SIZE));
    for (let index = 0; index < frames.length; index += batchSize) {
      if (run !== this.backgroundPrefetchRun || timeline !== this.timeline) return;
      // Never let a background batch compete with an immediately preceding
      // slider action. The visible selected frame always wins.
      while (Date.now() - this.nwsInteractiveRequestedAt < INTERACTIVE_GRACE_MS) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        if (run !== this.backgroundPrefetchRun || timeline !== this.timeline) return;
      }
      const batch = frames.slice(index, index + batchSize);
      const missing = batch.filter((frame) => {
        const key = frameCacheKey(frame);
        return key && !this.nwsFrameCache.has(key);
      });
      if (!missing.length) continue;
      try {
        const response = await postMsgpack('/api/local/ops/timeline/nws-frames', {
          at: missing.map((frame) => frame.start_at),
        }, { silent: true });
        for (const loaded of response?.frames || []) {
          const key = frameCacheKey(loaded);
          if (key) this.nwsFrameCache.set(key, loaded);
        }
      } catch (error) {
        console.warn('OpsTimeline: background NWS frame batch failed', error);
        return;
      }
      // Preserve interactive requests and map rendering between batches.
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  },

  async _preloadPointFrames(frames, timeline, run, declaredBatchSize, contract = {}) {
    const overlayId = String(contract.overlay_id || '');
    if (!overlayId) return;
    const batchSize = Math.max(1, Math.min(24, Number(declaredBatchSize) || 24));
    for (let index = 0; index < frames.length; index += batchSize) {
      if (run !== this.backgroundPrefetchRun || timeline !== this.timeline) return;
      while (Date.now() - this.pointInteractiveRequestedAt < INTERACTIVE_GRACE_MS) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        if (run !== this.backgroundPrefetchRun || timeline !== this.timeline) return;
      }
      const batch = frames.slice(index, index + batchSize);
      const missing = batch.filter((frame) => {
        const key = `${overlayId}:${String(frame?.start_at || '')}:${String(frame?.payload_hash || '')}`;
        return key && !this.pointFrameCache.has(key);
      });
      if (!missing.length) continue;
      try {
        const response = await postMsgpack('/api/local/ops/timeline/point-frames', {
          overlay_id: overlayId,
          at: missing.map((frame) => frame.start_at),
        }, { silent: true });
        for (const loaded of response?.frames || []) {
          const key = `${overlayId}:${String(loaded?.start_at || '')}:${String(loaded?.payload_hash || '')}`;
          if (key) this.pointFrameCache.set(key, loaded);
        }
      } catch (error) {
        console.warn(`OpsTimeline: background ${overlayId} point frame batch failed`, error);
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  },

  _normalizeHurricaneCoord(storm, point) {
    const lat = Number(point?.latitude);
    let lon = Number(point?.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    if (String(storm?.source || '').trim().toUpperCase() === 'NHC' && lon > 0 && lon <= 180) {
      lon = -lon;
    }
    if (lon < -180 || lon > 180 || lat < -90 || lat > 90) return null;
    return [lon, lat];
  },

  _hurricanePointMs(point) {
    return toMs(point?.timestamp) ?? toMs(point?.valid_at) ?? toMs(point?.time) ?? toMs(point?.issued_at);
  },

  _hurricaneCategoryFromWind(windKt) {
    const wind = Number(windKt);
    if (!Number.isFinite(wind)) return null;
    if (wind >= 137) return 'Cat5';
    if (wind >= 113) return 'Cat4';
    if (wind >= 96) return 'Cat3';
    if (wind >= 83) return 'Cat2';
    if (wind >= 64) return 'Cat1';
    if (wind >= 34) return 'TS';
    if (wind > 0) return 'TD';
    return null;
  },

  _hurricaneForecastItems(storm, selectedMs, currentMs) {
    const pointItems = (Array.isArray(storm?.forecast_points) ? storm.forecast_points : [])
      .map((point) => ({ point, timeMs: this._hurricanePointMs(point), coord: this._normalizeHurricaneCoord(storm, point), isTimed: true }))
      .filter((item) => Number.isFinite(item.timeMs) && item.coord && (!Number.isFinite(currentMs) || item.timeMs >= currentMs) && item.timeMs <= selectedMs)
      .sort((a, b) => a.timeMs - b.timeMs);
    if (pointItems.length) return pointItems;
    const track = storm?.forecast_track;
    const coords = track?.type === 'LineString' && Array.isArray(track.coordinates) ? track.coordinates : [];
    if (!coords.length || !Number.isFinite(currentMs) || selectedMs < currentMs) return [];
    const horizonHours = Math.max(1, Math.min(168, Number(storm?.forecast_horizon_hours) || 120));
    const stepMs = coords.length > 1 ? (horizonHours * 60 * 60 * 1000) / (coords.length - 1) : 0;
    return coords
      .map((coord, index) => {
        const point = { longitude: coord?.[0], latitude: coord?.[1] };
        return {
          point,
          timeMs: currentMs + (index * stepMs),
          coord: this._normalizeHurricaneCoord(storm, point),
          isTimed: false
        };
      })
      .filter((item) => item.coord && item.timeMs <= selectedMs);
  },

  _hurricaneForecastEndMs(storm, currentMs) {
    const pointTimes = (Array.isArray(storm?.forecast_points) ? storm.forecast_points : [])
      .map((point) => this._hurricanePointMs(point))
      .filter(Number.isFinite);
    if (pointTimes.length) return Math.max(...pointTimes);
    const track = storm?.forecast_track;
    const coords = track?.type === 'LineString' && Array.isArray(track.coordinates) ? track.coordinates : [];
    if (coords.length && Number.isFinite(currentMs)) {
      const horizonHours = Math.max(1, Math.min(168, Number(storm?.forecast_horizon_hours) || 120));
      return currentMs + (horizonHours * 60 * 60 * 1000);
    }
    return null;
  },

  _hurricaneReplayEndMs(feedId) {
    const replay = this.hurricaneReplayData.get(feedId);
    const currentMs = this.timeline?.currentMs;
    if (!replay || !Array.isArray(replay.storms)) return Number.isFinite(currentMs) ? currentMs : null;
    const endTimes = [];
    if (Number.isFinite(currentMs)) endTimes.push(currentMs);
    for (const storm of replay.storms) {
      const forecastEnd = this._hurricaneForecastEndMs(storm, currentMs);
      if (Number.isFinite(forecastEnd)) endTimes.push(forecastEnd);
      for (const point of storm?.observed_track || []) {
        const at = this._hurricanePointMs(point);
        if (Number.isFinite(at)) endTimes.push(at);
      }
    }
    return endTimes.length ? Math.max(...endTimes) : null;
  },

  _isPastHurricaneReplayEnd(feedId, selectedMs) {
    const endMs = this._hurricaneReplayEndMs(feedId);
    return Number.isFinite(endMs) && selectedMs > endMs;
  },

  _buildHurricaneReplayDisplayPayload(feedId, selectedMs, selectedFrame = null) {
    const replay = this.hurricaneReplayData.get(feedId);
    if (!replay || !Array.isArray(replay.storms)) return null;
    const historyHours = Math.max(1, Number(replay.history_hours) || 72);
    const currentMs = this.timeline?.currentMs;
    const observedCursorMs = Number.isFinite(currentMs) ? Math.min(selectedMs, currentMs) : selectedMs;
    const showForecast = Number.isFinite(currentMs) && selectedMs >= currentMs;
    const cutoffMs = observedCursorMs - (historyHours * 60 * 60 * 1000);
    const features = [];
    const renderParts = [];
    let stormCount = 0;
    for (const storm of replay.storms) {
      const points = Array.isArray(storm?.observed_track) ? storm.observed_track : [];
      const usable = points
        .map((point) => ({ point, timeMs: this._hurricanePointMs(point), coord: this._normalizeHurricaneCoord(storm, point) }))
        .filter((item) => Number.isFinite(item.timeMs) && item.coord && item.timeMs >= cutoffMs && item.timeMs <= observedCursorMs)
        .sort((a, b) => a.timeMs - b.timeMs);
      if (!usable.length) continue;
      const latest = usable[usable.length - 1];
      const forecastItems = this._hurricaneForecastItems(storm, selectedMs, currentMs);
      const timedForecastItems = forecastItems.filter((item) => item.isTimed);
      const displayLatest = showForecast && timedForecastItems.length ? timedForecastItems[timedForecastItems.length - 1] : latest;
      renderParts.push([
        storm.storm_id,
        latest.point.timestamp || latest.timeMs,
        latest.coord.join(','),
        displayLatest.point.valid_at || displayLatest.point.timestamp || displayLatest.timeMs,
        displayLatest.coord.join(','),
        usable.length,
        forecastItems.length,
        showForecast ? 'forecast' : 'history'
      ].join(':'));
      const baseProps = {
        storm_id: storm.storm_id,
        storm_color: storm.storm_color,
        name: storm.name,
        basin: storm.basin,
        source: storm.source,
        selected_observed_source: storm.selected_observed_source,
        selected_forecast_source: storm.selected_forecast_source,
        source_name: storm.source_name || storm.source || 'Tropical cyclone advisory source',
        source_url: storm.source_page_url || storm.source_url || storm.source_product_url,
        source_page_url: storm.source_page_url,
        source_product_url: storm.source_product_url,
        advisory_number: storm.advisory_number,
        issued_at: storm.issued_at,
        wind_kt: displayLatest.point.wind_kt,
        max_wind_kt: Math.max(...[...usable, ...forecastItems].map((item) => Number(item.point.wind_kt)).filter(Number.isFinite), 0),
        category: displayLatest.point.category || this._hurricaneCategoryFromWind(displayLatest.point.wind_kt),
        max_category: displayLatest.point.category || this._hurricaneCategoryFromWind(displayLatest.point.wind_kt),
        event_type: 'hurricane',
        track_state: 'replay',
        track_opacity: 0.95,
        last_observed_at: latest.point.timestamp,
        track_kind: 'observed',
      };
      stormCount += 1;
      if (usable.length >= 2) {
        features.push({
          type: 'Feature',
          geometry: { type: 'LineString', coordinates: usable.map((item) => item.coord) },
          properties: { ...baseProps, line_style: 'solid' },
        });
      }
      features.push({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: displayLatest.coord },
        properties: {
          ...baseProps,
          ...displayLatest.point,
          longitude: displayLatest.coord[0],
          latitude: displayLatest.coord[1],
          track_kind: 'current',
          forecast_valid_at: showForecast && forecastItems.length ? (displayLatest.point.valid_at || displayLatest.point.timestamp || null) : null,
        },
      });
      if (showForecast && forecastItems.length) {
        const forecastCoords = [latest.coord, ...forecastItems.map((item) => item.coord)];
        const forecastTimes = [latest.point.timestamp, ...forecastItems.map((item) => item.point.valid_at || item.point.timestamp || item.point.time || item.point.issued_at || null)];
        if (forecastCoords.length >= 2) {
          features.push({
            type: 'Feature',
            geometry: { type: 'LineString', coordinates: forecastCoords },
            properties: {
              ...baseProps,
              track_kind: 'forecast',
              line_style: 'dotted',
              forecast_timestamps: forecastTimes,
            },
          });
        }
        if (storm.uncertainty_geometry && ['Polygon', 'MultiPolygon'].includes(storm.uncertainty_geometry.type)) {
          features.push({
            type: 'Feature',
            geometry: storm.uncertainty_geometry,
            properties: {
              ...baseProps,
              track_kind: 'forecast_uncertainty',
            },
          });
        }
      }
    }
    if (!features.length) return null;
    return {
      type: 'data',
      data_type: 'events',
      event_type: 'hurricane',
      source_id: 'hurricanes_live_ops',
      snapshot_hash: selectedFrame?.payload_hash || null,
      dataset_name: 'Hurricanes',
      source_name: 'Tropical cyclone advisory sources',
      summary: `Showing ${stormCount} recent storm tracks from advisory sources.`,
      count: stormCount,
      fit: false,
      ops_default_view: 'history',
      ops_render_key: renderParts.join('|'),
      geojson: { type: 'FeatureCollection', features },
    };
  },

};
