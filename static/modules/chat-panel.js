/**
 * Chat Panel - Sidebar chat functionality and order management.
 * Map-specific orchestrator that imports reusable chat/order modules.
 */

import { CONFIG } from './config.js';
import { postMsgpack, getApiCallsForRecovery, clearApiCalls, logExecutedOrder, getExecutedOrdersForRecovery, clearExecutedOrders } from './utils/fetch.js';

// Reusable modules
import {
  getOrCreateSessionId,
  resetSessionId,
  saveChatState,
  restoreChatState,
  clearChatStorage
} from './chat/session.js';

import {
  addMessage as renderMessage,
  showTypingIndicator as renderTypingIndicator
} from './chat/message-renderer.js';

import { sendStreamingRequest, sendChatRequest } from './chat/api.js';
import { OrderPanel } from './order/manager.js';
import { OrderTracker as OrderTrackerClass } from './order/tracker.js';
import * as SavedOrders from './order/saved.js';
import { onAuthChanged } from './auth.js';

// Dependencies set via setDependencies to avoid circular imports
let MapAdapter = null;
let App = null;
let SelectionManager = null;
let OverlayController = null;
let OverlaySelector = null;

export function setDependencies(deps) {
  MapAdapter = deps.MapAdapter;
  App = deps.App;
  SelectionManager = deps.SelectionManager;
  OverlayController = deps.OverlayController;
  OverlaySelector = deps.OverlaySelector;
}

// Welcome message shown on first load and new chat
const WELCOME_MESSAGE =
  'Welcome! Ask me anything about global data -- earthquakes, hurricanes, ' +
  'climate indicators, and more. Enable the Demographics overlay to zoom through ' +
  'the countries, states, and territories.<br><br>' +
  'To explore datasets, type a question in natural language. ' +
  'Type "help" or "how do you work?" anytime for a full guide.<br><br>' +
  '<button class="chat-action-btn" data-action="preload-disasters-2020">Load disasters 2020-2025</button>';

// Map event_type from API responses to overlay IDs
const EVENT_TYPE_TO_OVERLAY = {
  earthquake: 'earthquakes',
  volcano: 'volcanoes',
  tsunami: 'tsunamis',
  hurricane: 'hurricanes',
  wildfire: 'wildfires',
  tornado: 'tornadoes',
  flood: 'floods',
  drought: 'drought',
  landslide: 'landslides'
};

// =============================================================================
// Loaded Data Tracker - tracks what data has been loaded for LLM context
// =============================================================================

/**
 * Tracks loaded data for LLM context.
 * Each entry: { source_id, source_name, region, metric, years, data_type, overlay_type }
 */
let loadedDataList = [];

/**
 * Register loaded data from an executed order.
 * Called when orders complete successfully.
 * @param {Object} order - The executed order
 * @param {Object} response - The API response
 */
function registerLoadedData(order, response) {
  if (!order?.items) return;

  const dataType = response?.data_type || 'metrics';

  for (const item of order.items) {
    // Skip removal items
    if (item.action === 'remove') continue;

    const entry = {
      source_id: item.source_id,
      region: item.region || 'global',
      metric: item.metric_label || item.metric || null,
      data_type: dataType,
      overlay_type: item.overlay_type || null
    };

    // Add year info
    if (item.year_start && item.year_end) {
      entry.years = `${item.year_start}-${item.year_end}`;
    } else if (item.year) {
      entry.years = String(item.year);
    } else {
      entry.years = 'latest';
    }

    // Dedupe: don't add if same source+region+metric already exists
    const exists = loadedDataList.some(e =>
      e.source_id === entry.source_id &&
      e.region === entry.region &&
      e.metric === entry.metric
    );

    if (!exists) {
      loadedDataList.push(entry);
      console.log('[LoadedData] Registered:', entry);
    }
  }
}

/**
 * Remove loaded data entries matching criteria.
 * Called when removal orders complete.
 * @param {Object} order - The removal order
 */
function unregisterLoadedData(order) {
  if (!order?.items) return;

  for (const item of order.items) {
    if (item.action !== 'remove') continue;

    const sourceId = item.source_id;
    const region = item.region;

    // Remove matching entries
    const before = loadedDataList.length;
    loadedDataList = loadedDataList.filter(e =>
      !(e.source_id === sourceId && (e.region === region || region === 'global'))
    );

    if (loadedDataList.length < before) {
      console.log('[LoadedData] Unregistered:', { source_id: sourceId, region });
    }
  }
}

/**
 * Get loaded data summary for LLM context.
 * @returns {Array} List of loaded data entries
 */
export function getLoadedDataList() {
  return [...loadedDataList];
}

/**
 * Clear all loaded data (on session reset).
 */
export function clearLoadedDataList() {
  loadedDataList = [];
  console.log('[LoadedData] Cleared');
}

/**
 * Route event-type order results to OverlayController for cache ingestion.
 * @param {Object} response - API response with data_type 'events'
 */
function ingestEventsToOverlay(response) {
  if (!OverlayController?.ingestOrderResult) return;
  if (!response?.geojson?.features) return;

  // Use source_id from response, fall back to event_type mapping for legacy support
  const overlayId = response.source_id || EVENT_TYPE_TO_OVERLAY[response.event_type];
  if (!overlayId) {
    console.warn('ingestEventsToOverlay: No overlayId for response', response.source_id, response.event_type);
    return;
  }

  // Build range metadata from response if available
  const rangeMeta = (response.time_range && response.time_range.min && response.time_range.max)
    ? { start: response.time_range.min, end: response.time_range.max }
    : (response.year_range && response.year_range.length === 2)
      ? { start: new Date(response.year_range[0], 0, 1).getTime(),
          end: new Date(response.year_range[1], 11, 31).getTime() }
      : null;

  OverlayController.ingestOrderResult(overlayId, response.geojson, rangeMeta);
}

/**
 * Route metrics order results to OverlayController for cache ingestion.
 * @param {Object} response - API response with data_type 'metrics'
 */
function ingestMetricsToCache(response) {
  if (!OverlayController?.ingestMetricData) return;
  if (!response?.geojson?.features) return;

  const sourceId = response.source_id;
  if (!sourceId) {
    console.warn('ingestMetricsToCache: No source_id in response');
    return;
  }

  // Build year range metadata
  const yearRange = response.year_range || null;

  OverlayController.ingestMetricData(sourceId, response.geojson, response.year_data, yearRange);
}

/**
 * Route geometry order results to OverlayController for rendering.
 * Backend SessionCache handles deduplication - this just renders.
 * @param {Object} response - API response with data_type 'geometry'
 */
function renderGeometryOrder(response) {
  if (!OverlayController?.renderGeometryData) return;
  if (!response?.geojson?.features) return;

  const sourceId = response.source_id || 'geometry_zcta';
  const geometryType = response.geographic_level || response.overlay_type || 'zcta';

  // Render geometry (backend handles dedup via SessionCache)
  OverlayController.renderGeometryData(sourceId, response.geojson, geometryType, {});
}

// Module-level instances (created during init)
let orderPanel = null;
let orderTracker = null;

// ============================================================================
// CHAT MANAGER - Sidebar chat functionality (map-specific orchestrator)
// ============================================================================

export const ChatManager = {
  history: [],
  sessionId: null,
  elements: {},
  lastDisambiguationOptions: null,

  /**
   * Initialize chat manager
   */
  init() {
    this.sessionId = getOrCreateSessionId();

    // Cache DOM elements
    this.elements = {
      sidebar: document.getElementById('sidebar'),
      toggle: document.getElementById('sidebarToggle'),
      close: document.getElementById('closeSidebar'),
      newChat: document.getElementById('newChatBtn'),
      messages: document.getElementById('chatMessages'),
      form: document.getElementById('chatForm'),
      input: document.getElementById('chatInput'),
      sendBtn: document.getElementById('sendBtn')
    };

    // Restore chat state from localStorage
    this.restoreState();

    // Setup UI event listeners
    this.setupEventListeners();

    // Initialize order panel and tracker
    this.initOrderPanel();

    onAuthChanged(() => {
      window.location.reload();
    });
  },

  /**
   * Initialize OrderPanel and OrderTracker with map-specific callbacks.
   */
  initOrderPanel() {
    orderPanel = new OrderPanel({
      elements: {
        panel: document.getElementById('orderPanel'),
        count: document.getElementById('orderCount'),
        summary: document.getElementById('orderSummary'),
        items: document.getElementById('orderItems'),
        confirmBtn: document.getElementById('orderConfirmBtn'),
        cancelBtn: document.getElementById('orderCancelBtn'),
        orderTabBtn: document.getElementById('orderTabBtn'),
        loadedTabBtn: document.getElementById('loadedTabBtn'),
        orderTabContent: document.getElementById('orderTabContent'),
        loadedTabContent: document.getElementById('loadedTabContent'),
        loadedItems: document.getElementById('loadedItems'),
        loadedActions: document.getElementById('loadedActions'),
        loadedClearAllBtn: document.getElementById('loadedClearAllBtn')
      },
      onConfirm: async (order) => {
        await this.executeOrder(order);
      },
      onQueue: async (order) => {
        await this.queueOrder(order);
      },
      onClear: () => {
        App?.clearNavigationMode();
        // Turn off demographics overlay - user can re-enable to see countries
        if (OverlaySelector?.isActive('demographics')) {
          OverlaySelector.toggle('demographics');
        }
      },
      onClearSource: (overlayId) => {
        if (!OverlayController) return;
        OverlayController.clearOverlay(overlayId);
        // Uncheck the overlay if it's active
        if (OverlaySelector?.isActive(overlayId)) {
          OverlaySelector.toggle(overlayId);
        }
        // Clear backend session cache for this source (keeps caches in sync)
        const sessionId = getOrCreateSessionId();
        postMsgpack('/api/session/clear-source', {
          sessionId,
          sourceId: overlayId
        }).catch(err => console.warn('Failed to clear backend source cache:', err.message));
      },
      getCacheStats: () => {
        if (!OverlayController || !OverlayController.getCacheStats) return null;
        return OverlayController.getCacheStats();
      },
      addMessage: (text, type) => this.addMessage(text, type)
    });
    orderPanel.init();

    orderTracker = new OrderTrackerClass({
      container: document.getElementById('orderItems'),
      onReady: (queueId, result) => {
        if (result && (result.type === 'data' || result.type === 'events')) {
          const count = result.count || result.geojson?.features?.length || 0;
          this.addMessage(`Loaded ${count} locations.`, 'assistant');
          if (result.type === 'events') {
            ingestEventsToOverlay(result);
          }
          App?.displayData(result);
        }
      },
      onFailed: (queueId, error) => {
        this.addMessage(`Order failed: ${error || 'Unknown error'}`, 'assistant');
      }
    });

    // Make globally available for any legacy onclick handlers
    if (typeof window !== 'undefined') {
      window.OrderManager = orderPanel;
      window.OrderTracker = orderTracker;
    }
  },

  /**
   * Execute a confirmed order - send to backend and display results.
   * @param {Object} order - The order to execute
   * @param {Object} options - Options {skipLog: boolean, force: boolean} - skip logging, force re-fetch (bypass dedup)
   */
  async executeOrder(order, options = {}) {
    const apiUrl = (typeof API_BASE_URL !== 'undefined' && API_BASE_URL)
      ? `${API_BASE_URL}/chat`
      : '/chat';

    // Check for mixed geometry orders (different source_ids with overlay_type)
    // Split them into separate calls so backend processes each geometry type correctly
    const geometryItems = (order.items || []).filter(item => item.overlay_type);
    if (geometryItems.length > 0) {
      const sourceIds = new Set(geometryItems.map(item => item.source_id));
      if (sourceIds.size > 1) {
        console.log('Mixed geometry order detected, splitting by source_id:', [...sourceIds]);
        // Group items by source_id
        const itemsBySource = {};
        for (const item of order.items) {
          const key = item.source_id || 'default';
          if (!itemsBySource[key]) itemsBySource[key] = [];
          itemsBySource[key].push(item);
        }
        // Execute each group separately (recursive call, but each group has only 1 source_id so won't split again)
        for (const [sourceId, items] of Object.entries(itemsBySource)) {
          const subOrder = { ...order, items, summary: order.summary };
          await this.executeOrder(subOrder, options);
        }
        return;
      }
    }

    console.log('Sending order:', JSON.stringify(order, null, 2));

    const data = await postMsgpack(apiUrl, {
      confirmed_order: order,
      sessionId: this.sessionId,
      force: options.force || false  // Bypass dedup for recovery
    });

    console.log('Received response:', {
      type: data.type,
      multi_year: data.multi_year,
      has_year_data: !!data.year_data,
      year_range: data.year_range,
      feature_count: data.geojson?.features?.length
    });

    if (data.type === 'already_loaded') {
      this.addMessage(data.message || 'This data is already loaded on your map.', 'assistant');
      if (orderPanel.switchTab) orderPanel.switchTab('loaded');
    } else if (data.type === 'error') {
      this.addMessage(data.message || 'Failed to load data.', 'assistant');
      throw new Error(data.message || 'Order execution failed');
    } else if (data.action === 'remove') {
      // Handle removal orders (no geojson, just identifiers)
      const message = data.summary || `Removed ${data.count || 0} ${data.data_type || 'items'}`;
      this.addMessage(message, 'assistant');
      App?.displayData(data);
      unregisterLoadedData(order);  // Track removal for LLM context
      if (orderPanel.switchTab) orderPanel.switchTab('loaded');
    } else if (data.type === 'mixed_order' && data.results) {
      // Handle mixed add/remove orders - process each result
      for (const result of data.results) {
        App?.displayData(result);
      }
      // Track both adds and removes for LLM context
      registerLoadedData(order, data);
      unregisterLoadedData(order);
      const message = data.summary || `Updated map: added ${data.add_count || 0}, removed ${data.remove_count || 0}`;
      this.addMessage(message, 'assistant');
      if (orderPanel.switchTab) orderPanel.switchTab('loaded');
    } else if (data.geojson) {
      // Route by data_type for cache ingestion
      const dataType = data.data_type || (data.type === 'events' ? 'events' : 'metrics');

      if (dataType === 'events') {
        const message = data.summary || `Showing ${data.count} ${data.event_type || 'event'} events`;
        this.addMessage(message, 'assistant');
        ingestEventsToOverlay(data);
      } else if (dataType === 'metrics') {
        const message = data.data_note || `Loaded ${data.count || data.geojson.features?.length || 0} locations`;
        this.addMessage(message, 'assistant');
        ingestMetricsToCache(data);
      } else if (dataType === 'geometry') {
        const message = data.summary || `Showing ${data.count || data.geojson.features?.length || 0} ${data.geographic_level || data.overlay_type || 'geometry'} areas`;
        this.addMessage(message, 'assistant');
        renderGeometryOrder(data);
      } else {
        // Fallback for unknown data_type
        const message = data.summary || data.data_note || `Loaded ${data.count || 0} items`;
        this.addMessage(message, 'assistant');
      }

      // Log order for session recovery (skip during recovery to avoid duplicates)
      if (!options.skipLog) {
        logExecutedOrder(order);
      }

      // Track loaded data for LLM context
      registerLoadedData(order, data);

      App?.displayData(data);
    }
  },

  /**
   * Queue an order for background processing.
   * @param {Object} order - The order to queue
   */
  async queueOrder(order) {
    const apiUrl = (typeof API_BASE_URL !== 'undefined' && API_BASE_URL)
      ? `${API_BASE_URL}/api/orders/queue`
      : '/api/orders/queue';

    const data = await postMsgpack(apiUrl, {
      items: order.items,
      hints: { summary: order.summary },
      session_id: this.sessionId
    });

    if (data.queue_id) {
      console.log('Order queued:', data.queue_id, 'position:', data.position);
      this.addMessage(
        data.position > 1
          ? `Order queued (position ${data.position}). You can continue chatting while it loads.`
          : 'Order queued. Processing...',
        'assistant'
      );
      orderTracker.addOrder(data.queue_id, {
        items: order.items,
        summary: order.summary
      });
    } else {
      throw new Error('No queue_id returned');
    }
  },

  /**
   * Restore chat state from localStorage.
   */
  restoreState() {
    const state = restoreChatState();
    const hadChatSession = state?.history?.some(m => m.role === 'user');

    if (hadChatSession) {
      this.history = state.history;
      if (state.messagesHtml && this.elements.messages) {
        this.elements.messages.innerHTML = state.messagesHtml;
        // Remove any loading/typing indicators that were saved mid-request
        this.elements.messages.querySelectorAll('.loading-indicator, .typing-indicator').forEach(el => el.remove());
        this.elements.messages.scrollTop = this.elements.messages.scrollHeight;
      }

      const apiCalls = getApiCallsForRecovery();
      const executedOrders = getExecutedOrdersForRecovery();
      if (apiCalls.length > 0 || executedOrders.length > 0) {
        this.showRecoveryPrompt(apiCalls.length, executedOrders.length);
      }
    } else {
      // No real chat session saved - always show fresh welcome message
      this.addMessage(WELCOME_MESSAGE, 'assistant', { html: true });
    }
  },

  /**
   * Save current chat state to localStorage.
   */
  saveState() {
    const html = this.elements.messages ? this.elements.messages.innerHTML : '';
    saveChatState(this.history, html);
  },

  /**
   * Clear current session and start fresh.
   */
  async clearSession() {
    const oldSessionId = this.sessionId;

    // Clear state
    this.history = [];
    this.lastDisambiguationOptions = null;
    if (this.elements.messages) {
      this.elements.messages.innerHTML = '';
    }

    // Clear order panel
    if (orderPanel) orderPanel.clearOrder();

    // Clear map-specific state
    if (window.OverlaySelector?.clearState) window.OverlaySelector.clearState();
    if (window.TimeSlider?.clearSliderSettings) window.TimeSlider.clearSliderSettings();
    if (window.App?.clearMapViewSettings) window.App.clearMapViewSettings();
    clearApiCalls();
    clearExecutedOrders();
    clearLoadedDataList();  // Clear loaded data tracker

    // Reset session
    this.sessionId = resetSessionId();
    clearChatStorage();

    // Notify backend (fire and forget)
    if (oldSessionId) {
      try {
        await postMsgpack('/api/session/clear', { sessionId: oldSessionId });
      } catch (e) {
        console.log('[Session] Backend clear skipped:', e.message);
      }
    }

    console.log('[Session] Session cleared, new session:', this.sessionId);
    return this.sessionId;
  },

  /**
   * Show recovery prompt for map data.
   * @param {number} overlayCount - Number of overlay API calls to recover
   * @param {number} orderCount - Number of executed orders to recover
   */
  showRecoveryPrompt(overlayCount, orderCount = 0) {
    const { messages } = this.elements;
    if (!messages) return;

    // Build summary of what can be recovered
    const parts = [];
    if (orderCount > 0) {
      parts.push(`${orderCount} data order${orderCount === 1 ? '' : 's'}`);
    }
    if (overlayCount > 0) {
      parts.push(`${overlayCount} overlay request${overlayCount === 1 ? '' : 's'}`);
    }
    const dataSummary = parts.join(' and ');

    const div = document.createElement('div');
    div.className = 'chat-message assistant recovery-prompt';
    div.innerHTML = `
      <strong>Welcome Back</strong><br><br>
      Your previous session: <b>${dataSummary}</b><br><br>
      Click <b>Recover Data</b> to reload your map data, or <b>New Chat</b> above to start fresh.
      <div class="recovery-buttons" style="margin-top: 12px;">
        <button class="recovery-btn recover" data-action="recover">Recover Data</button>
      </div>
    `;

    messages.appendChild(div);
    div.querySelector('[data-action="recover"]').addEventListener('click', () => {
      this.handleRecoveryChoice('recover');
    });
    messages.scrollTop = messages.scrollHeight;
  },

  /**
   * Handle user's recovery choice.
   */
  async handleRecoveryChoice(choice) {
    const { messages } = this.elements;

    // Remove the recovery prompt
    const prompt = messages.querySelector('.recovery-prompt');
    if (prompt) prompt.remove();

    if (choice === 'recover') {
      const apiCalls = getApiCallsForRecovery();
      const executedOrders = getExecutedOrdersForRecovery();

      if (apiCalls.length === 0 && executedOrders.length === 0) {
        this.addMessage('No data to recover.', 'assistant');
        return;
      }

      let totalRecovered = 0;
      let totalFailed = 0;

      // 1. Recover executed orders (metrics data)
      if (executedOrders.length > 0) {
        this.addMessage(`Recovering ${executedOrders.length} data order${executedOrders.length === 1 ? '' : 's'}...`, 'assistant');

        for (const record of executedOrders) {
          try {
            // Re-execute the order with skipLog and force to bypass dedup
            await this.executeOrder(record.order, { skipLog: true, force: true });
            totalRecovered++;
          } catch (e) {
            console.warn('[Session] Failed to recover order:', record.summary, e.message);
            totalFailed++;
          }
        }
      }

      // 2. Recover overlay API calls (disaster data)
      if (apiCalls.length > 0) {
        // Parse URLs to extract overlay IDs and years
        const overlayYears = new Map();
        for (const url of apiCalls) {
          const yearMatch = url.match(/[?&]year=(\d+)/);
          if (!yearMatch) continue;
          const year = parseInt(yearMatch[1], 10);

          let overlayId = null;
          if (url.includes('/api/earthquakes/')) overlayId = 'earthquakes';
          else if (url.includes('/api/storms/')) overlayId = 'hurricanes';
          else if (url.includes('/api/volcanoes/')) overlayId = 'volcanoes';
          else if (url.includes('/api/wildfires/')) overlayId = 'wildfires';
          else if (url.includes('/api/tornadoes/')) overlayId = 'tornadoes';
          else if (url.includes('/api/tsunamis/')) overlayId = 'tsunamis';
          else if (url.includes('/api/floods/')) overlayId = 'floods';

          if (overlayId) {
            if (!overlayYears.has(overlayId)) overlayYears.set(overlayId, new Set());
            overlayYears.get(overlayId).add(year);
          }
        }

        let overlayLoads = 0;
        for (const years of overlayYears.values()) overlayLoads += years.size;

        if (overlayLoads > 0) {
          this.addMessage(`Recovering ${overlayLoads} overlay data set${overlayLoads === 1 ? '' : 's'}...`, 'assistant');

          try {
            const loadPromises = [];
            for (const [overlayId, years] of overlayYears) {
              for (const year of years) {
                if (OverlayController?.loadYearAndRender) {
                  loadPromises.push(
                    OverlayController.loadYearAndRender(overlayId, year).catch(e => {
                      console.warn('[Session] Failed to load:', overlayId, year, e.message);
                      return null;
                    })
                  );
                }
              }
            }

            const results = await Promise.all(loadPromises);
            totalRecovered += results.filter(r => r !== null).length;
            totalFailed += results.filter(r => r === null).length;

            if (window.OverlayController?.recalculateTimeRange) {
              window.OverlayController.recalculateTimeRange();
            }
            if (window.TimeSlider?.refreshDisplay) {
              window.TimeSlider.refreshDisplay();
            }
          } catch (e) {
            console.error('[Session] Overlay recovery failed:', e);
            totalFailed++;
          }
        }
      }

      // Final summary
      if (totalFailed === 0) {
        this.addMessage(`Recovery complete. Restored ${totalRecovered} data set${totalRecovered === 1 ? '' : 's'}.`, 'assistant');
      } else {
        this.addMessage(`Recovery complete. Restored ${totalRecovered}, failed ${totalFailed}.`, 'assistant');
      }
    } else {
      await this.clearSession();
      this.addMessage(WELCOME_MESSAGE, 'assistant', { html: true });
    }
  },

  /**
   * Setup event listeners
   */
  setupEventListeners() {
    const { sidebar, toggle, close, newChat, form, input } = this.elements;

    // Sidebar toggle
    toggle.addEventListener('click', () => {
      sidebar.classList.remove('collapsed');
      toggle.style.display = 'none';
    });

    close.addEventListener('click', () => {
      sidebar.classList.add('collapsed');
      toggle.style.display = 'flex';
    });

    // New Chat button
    if (newChat) {
      newChat.addEventListener('click', async () => {
        if (confirm('Start a new chat? This will clear your current conversation.')) {
          await this.clearSession();
          this.addMessage(WELCOME_MESSAGE, 'assistant', { html: true });
        }
      });
    }

    // Auto-resize textarea
    input.addEventListener('input', () => {
      input.style.height = 'auto';
      input.style.height = Math.min(input.scrollHeight, 120) + 'px';
    });

    // Enter to send
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        form.dispatchEvent(new Event('submit'));
      }
    });

    // Form submission
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      await this.handleSubmit();
    });

    // Delegated handler for chat action buttons (e.g. preload buttons in welcome message)
    this.elements.messages.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const action = btn.dataset.action;
      if (action === 'preload-disasters-2020') {
        await this.handlePreloadDisasters2020(btn);
      }
    });
  },

  /**
   * Handle the "Load disasters 2020-2025" preload button.
   * Makes one ranged API call per disaster type and caches the results in the browser.
   */
  async handlePreloadDisasters2020(btn) {
    btn.disabled = true;
    btn.textContent = 'Loading...';
    try {
      const disasterIds = ['earthquakes', 'hurricanes', 'volcanoes', 'wildfires', 'tsunamis', 'tornadoes'];

      // Enable overlays that aren't already active
      for (const id of disasterIds) {
        if (!window.OverlaySelector?.isActive(id)) {
          window.OverlaySelector?.toggle(id);
        }
      }

      // Move trim handles to 2020-2025 (overall range stays at default 2000-present)
      window.TimeSlider?.setTrimBounds(2020, 2025);

      // Preload all data into browser cache
      const summary = await window.OverlayController?.preloadDisasters2020to2025();
      const loaded = summary ? Object.values(summary).filter(r => r.loaded).length : 0;
      btn.textContent = `Loaded (${loaded}/6 datasets)`;
    } catch (e) {
      console.error('Preload failed:', e);
      btn.textContent = 'Load disasters 2020-2025';
      btn.disabled = false;
    }
  },

  /**
   * Handle form submission - send query, handle response types.
   */
  async handleSubmit() {
    const { input, sendBtn } = this.elements;
    const query = input.value.trim();
    if (!query) return;

    // Check for "recover" command
    if (query.toLowerCase() === 'recover') {
      input.value = '';
      this.handleRecoveryChoice('recover');
      return;
    }

    // Add user message
    this.addMessage(query, 'user');
    input.value = '';
    input.style.height = 'auto';

    // Track last query for potential re-send (metric warning)
    this.lastQuery = query;

    // Disable input
    sendBtn.disabled = true;
    input.disabled = true;

    // Show staged loading indicator
    const indicator = this.showTypingIndicator(true);

    try {
      // Build payload with map-specific context
      this.history.push({ role: 'user', content: query });
      const payload = this.buildPayload(query);

      // Send via streaming API
      const response = await sendStreamingRequest(payload, (stage, message) => {
        indicator.updateStage(stage, message);
      });

      if (!response) {
        throw new Error('No response received from server');
      }

      // Track in history
      this.history.push({ role: 'assistant', content: response.message || response.summary });

      // Handle response based on type
      this.handleResponse(response);

    } catch (error) {
      console.error('Chat error:', error);
      this.addMessage('Sorry, something went wrong. Please try again.', 'assistant');
    } finally {
      indicator.remove();
      sendBtn.disabled = false;
      input.disabled = false;
      input.focus();
    }
  },

  /**
   * Handle API response based on type (map-specific routing).
   * @param {Object} response - The API response
   */
  handleResponse(response) {
    switch (response.type) {
      case 'order':
        this.addMessage('Added to your order. Click "Display on Map" when ready.', 'assistant');
        orderPanel.setOrder(response.order, response.summary);
        break;

      case 'already_loaded':
        this.addMessage(response.message || 'This data is already loaded on your map.', 'assistant');
        // Switch to Loaded tab so user can see their data
        if (orderPanel.switchTab) orderPanel.switchTab('loaded');
        break;

      case 'metric_warning': {
        const msgEl = this.addMessage(response.message, 'assistant');
        const btnContainer = document.createElement('div');
        btnContainer.className = 'metric-warning-buttons';

        const yesBtn = document.createElement('button');
        yesBtn.textContent = 'Yes, show all';
        yesBtn.className = 'chat-action-btn confirm';
        yesBtn.addEventListener('click', () => {
          btnContainer.remove();
          this.resendWithForce();
        });

        const noBtn = document.createElement('button');
        noBtn.textContent = 'No, let me narrow it';
        noBtn.className = 'chat-action-btn cancel';
        noBtn.addEventListener('click', () => {
          btnContainer.remove();
          this.addMessage('Sure - what specific metrics would you like?', 'assistant');
        });

        btnContainer.appendChild(yesBtn);
        btnContainer.appendChild(noBtn);
        msgEl.appendChild(btnContainer);
        break;
      }

      case 'clarify':
        this.addMessage(response.message || 'Could you be more specific?', 'assistant');
        break;

      case 'disambiguate':
        this.addMessage(response.message || 'Please select a location:', 'assistant');
        this.lastDisambiguationOptions = response.options || [];
        if (SelectionManager) {
          SelectionManager.enter(response, (selected, originalQuery) => {
            this.handleDisambiguationSelection(selected, originalQuery);
          });
        }
        break;

      case 'navigate':
        this.addMessage(response.message || 'Showing locations.', 'assistant');
        this.handleNavigation(response);
        break;

      case 'drilldown':
        this.addMessage(response.message || 'Loading...', 'assistant');
        if (App && response.loc_id) {
          App.drillDown(response.loc_id, response.name || response.loc_id);
        }
        break;

      case 'data':
        this.addMessage(response.summary || 'Here is your data.', 'assistant');
        App?.displayData(response);
        break;

      case 'events':
        this.addMessage(response.summary || `Showing ${response.count} ${response.event_type} events.`, 'assistant');
        ingestEventsToOverlay(response);
        App?.displayData(response);
        break;

      case 'cache_answer':
        this.addMessage(response.message || 'Here is the current state.', 'assistant');
        break;

      case 'order_response':
        // Handle order execution responses (including removals)
        if (response.action === 'remove') {
          this.addMessage(response.summary || `Removed ${response.count || 0} ${response.data_type || 'items'}.`, 'assistant');
        } else {
          this.addMessage(response.summary || 'Order complete.', 'assistant');
        }
        App?.displayData(response);
        break;

      case 'mixed_order':
        // Handle mixed add/remove orders
        if (response.results) {
          for (const result of response.results) {
            App?.displayData(result);
          }
        }
        this.addMessage(response.summary || `Updated: added ${response.add_count || 0}, removed ${response.remove_count || 0}`, 'assistant');
        break;

      case 'geometry_remove':
        // Legacy: Remove geometry regions from display (now handled by order_response)
        this.addMessage(response.message || 'Removing geometry.', 'assistant');
        App?.displayData({ ...response, action: 'remove', data_type: 'geometry' });
        break;

      case 'filter_update':
        this.addMessage(response.message || 'Updating filters.', 'assistant');
        this.applyFilterUpdate(response);
        break;

      case 'filter_existing':
        this.addMessage(response.message || 'Filtering cached data.', 'assistant');
        if (response.overlay && response.filters && OverlayController) {
          OverlayController.updateFilters(response.overlay, response.filters);
          OverlayController.rerenderFromCache?.();
        }
        break;

      case 'overlay_toggle':
        this.addMessage(response.message || (response.enabled ? 'Enabling overlay.' : 'Disabling overlay.'), 'assistant');
        if (response.overlay && OverlaySelector) {
          const isCurrentlyActive = OverlaySelector.isActive(response.overlay);
          if (response.enabled && !isCurrentlyActive) {
            OverlaySelector.toggle(response.overlay);
          } else if (!response.enabled && isCurrentlyActive) {
            OverlaySelector.toggle(response.overlay);
          }
          if (response.enabled && response.filters && OverlayController) {
            OverlayController.updateFilters(response.overlay, response.filters);
            OverlayController.reloadOverlay(response.overlay);
          }
        }
        break;

      case 'save_order':
        if (response.name) {
          const saved = SavedOrders.save(
            response.name,
            orderPanel?.currentOrder?.items || [],
            orderPanel?.currentOrder?.summary || ''
          );
          if (saved) {
            this.addMessage(`Order saved as "${saved.name}"`, 'assistant');
          } else {
            this.addMessage('No order to save.', 'assistant');
          }
        } else {
          this.addMessage('Please specify a name to save the order (e.g., "save as California Data").', 'assistant');
        }
        break;

      case 'list_orders': {
        const savedOrders = SavedOrders.getAll();
        if (savedOrders.length === 0) {
          this.addMessage('No saved orders. Save an order first with "save as [name]".', 'assistant');
        } else {
          const names = savedOrders.map(o => `- ${o.name}`).join('\n');
          this.addMessage(`Saved orders:\n${names}`, 'assistant');
        }
        break;
      }

      case 'load_order':
        if (response.name) {
          const order = SavedOrders.load(response.name);
          if (order && orderPanel) {
            orderPanel.currentOrder = {
              items: JSON.parse(JSON.stringify(order.items)),
              summary: order.summary || 'Loaded saved order: ' + order.name
            };
            orderPanel.render(orderPanel.currentOrder.summary);
            orderPanel.switchTab('order');
            this.addMessage(`Loaded saved order: "${order.name}"`, 'assistant');
          } else if (!order) {
            this.addMessage(`No saved order found with name "${response.name}".`, 'assistant');
          }
        } else {
          const allOrders = SavedOrders.getAll();
          if (allOrders.length > 0) {
            const names = allOrders.map(o => `- ${o.name}`).join('\n');
            this.addMessage(`Which order? Available:\n${names}`, 'assistant');
          } else {
            this.addMessage('No saved orders available.', 'assistant');
          }
        }
        break;

      case 'delete_order':
        if (response.name) {
          if (SavedOrders.deleteOrder(response.name)) {
            this.addMessage(`Deleted saved order: "${response.name}"`, 'assistant');
          } else {
            this.addMessage(`No saved order found with name "${response.name}".`, 'assistant');
          }
        } else {
          this.addMessage('Please specify which order to delete (e.g., "delete order California Analysis").', 'assistant');
        }
        break;

      case 'error':
        this.addMessage(response.message || 'An error occurred. Please try again.', 'assistant');
        break;

      case 'chat':
      default:
        if (response.geojson && response.geojson.features && response.geojson.features.length > 0) {
          this.addMessage(response.summary || response.message || 'Found data for you.', 'assistant');
          if (response.event_type) {
            ingestEventsToOverlay(response);
          }
          App?.displayData(response);
        } else {
          this.addMessage(response.summary || response.message || 'Could you be more specific?', 'assistant');
        }
        break;
    }
  },

  /**
   * Re-send the last query with force_metrics flag to bypass metric count warning.
   */
  async resendWithForce() {
    if (!this.lastQuery) return;

    const { sendBtn, input } = this.elements;
    sendBtn.disabled = true;
    input.disabled = true;

    const indicator = this.showTypingIndicator(true);

    try {
      const payload = this.buildPayload(this.lastQuery, null, { force_metrics: true });
      const response = await sendStreamingRequest(payload, (stage, message) => {
        indicator.updateStage(stage, message);
      });

      if (response) {
        this.history.push({ role: 'assistant', content: response.message || response.summary });
        this.handleResponse(response);
      }
    } catch (error) {
      console.error('Force metrics re-send error:', error);
      this.addMessage('Sorry, something went wrong. Please try again.', 'assistant');
    } finally {
      indicator.remove();
      sendBtn.disabled = false;
      input.disabled = false;
      input.focus();
    }
  },

  /**
   * Handle user selection from disambiguation mode.
   * @param {Object} selected - The selected location
   * @param {string} originalQuery - The original query to retry
   */
  async handleDisambiguationSelection(selected, originalQuery) {
    const locationName = selected.matched_term || selected.loc_id;
    const countryName = selected.country_name || selected.iso3;

    this.addMessage(`Selected: ${locationName} in ${countryName}`, 'user');

    const { sendBtn, input } = this.elements;
    sendBtn.disabled = true;
    input.disabled = true;

    const indicator = this.showTypingIndicator();

    try {
      this.history.push({ role: 'user', content: originalQuery });
      const resolvedLocation = {
        loc_id: selected.loc_id,
        iso3: selected.iso3,
        matched_term: selected.matched_term,
        country_name: selected.country_name
      };
      const payload = this.buildPayload(originalQuery, resolvedLocation);
      const response = await sendChatRequest(payload);

      if (response) {
        this.history.push({ role: 'assistant', content: response.message || response.summary });
        this.handleResponse(response);
      }
    } catch (error) {
      console.error('Disambiguation retry error:', error);
      this.addMessage('Sorry, something went wrong. Please try again.', 'assistant');
    } finally {
      indicator.remove();
      sendBtn.disabled = false;
      input.disabled = false;
      input.focus();
    }
  },

  /**
   * Handle navigation request - zoom to locations and highlight them.
   * Optionally displays geometry overlay data (ZCTAs, tribal areas, etc.)
   * @param {Object} response - Navigate response with locations, loc_ids, and optional geojson
   */
  async handleNavigation(response) {
    const locIds = response.loc_ids || [];
    const locations = response.locations || [];
    const geometryOverlay = response.geometry_overlay || null;
    const overlayGeojson = response.geojson || null;

    if (locIds.length === 0) {
      console.warn('Navigation: no loc_ids to show');
      return;
    }

    try {
      // If geometry overlay data was returned, display it directly
      // Geometry overlays (ZCTA, tribal, etc.) are complete data - no metrics needed
      if (geometryOverlay && overlayGeojson && overlayGeojson.features && overlayGeojson.features.length > 0) {
        console.log(`Navigation with geometry overlay: ${overlayGeojson.features.length} features`);

        // Display the geometry overlay via the geometry pipeline
        // displayData will handle fitToBounds internally
        App?.displayData({
          data_type: 'geometry',
          geojson: overlayGeojson,
          source_id: geometryOverlay.source_id,
          summary: response.message || `Showing ${overlayGeojson.features.length} areas`
        });

        // Note: Don't call clearOrder() here - it triggers onClear() which calls loadCountries()
        // and would overwrite the geometry we just displayed. Just render the panel to update UI.
        if (orderPanel) {
          orderPanel.currentOrder = null;
          orderPanel.render();
        }
        return;
      }

      // Standard navigation (no geometry overlay) - fetch and highlight location boundaries
      const geojson = await postMsgpack('/geometry/selection', { loc_ids: locIds });

      if (geojson.features && geojson.features.length > 0) {
        // Calculate bounding box
        let minLng = 180, maxLng = -180, minLat = 90, maxLat = -90;

        for (const feature of geojson.features) {
          const props = feature.properties || {};
          if (props.bbox_min_lon !== undefined) {
            minLng = Math.min(minLng, props.bbox_min_lon);
            maxLng = Math.max(maxLng, props.bbox_max_lon);
            minLat = Math.min(minLat, props.bbox_min_lat);
            maxLat = Math.max(maxLat, props.bbox_max_lat);
          } else if (props.centroid_lon !== undefined) {
            minLng = Math.min(minLng, props.centroid_lon - 1);
            maxLng = Math.max(maxLng, props.centroid_lon + 1);
            minLat = Math.min(minLat, props.centroid_lat - 1);
            maxLat = Math.max(maxLat, props.centroid_lat + 1);
          }
        }

        // Fit map to bounds
        if (MapAdapter?.map && minLng < maxLng && minLat < maxLat) {
          MapAdapter.map.fitBounds(
            [[minLng, minLat], [maxLng, maxLat]],
            { padding: 50, duration: 1000 }
          );
        }

        // Display locations as highlight layer
        App?.displayNavigationLocations(geojson, locations);

        // Set up order with these locations
        orderPanel?.setNavigationLocations(locations);
      }
    } catch (error) {
      console.error('Navigation error:', error);
      this.addMessage('Sorry, could not display those locations.', 'assistant');
    }
  },

  /**
   * Build API payload with map-specific context.
   * @param {string} query - User query
   * @param {Object} [resolvedLocation] - Resolved location from disambiguation
   * @returns {Object} Full request payload
   */
  buildPayload(query, resolvedLocation = null, extraOptions = {}) {
    const view = MapAdapter?.getView() || { center: { lat: 0, lng: 0 }, zoom: 2, bounds: null, adminLevel: 0 };

    // Check for navigation location if no explicit resolution
    if (!resolvedLocation) {
      const navLocations = orderPanel?.currentOrder?.navigationLocations;
      if (navLocations && navLocations.length === 1) {
        const loc = navLocations[0];
        resolvedLocation = {
          loc_id: loc.loc_id,
          iso3: loc.iso3,
          matched_term: loc.matched_term,
          country_name: loc.country_name
        };
      }
    }

    return {
      query,
      viewport: {
        center: { lat: view.center.lat, lng: view.center.lng },
        zoom: view.zoom,
        bounds: view.bounds,
        adminLevel: view.adminLevel
      },
      chatHistory: this.history.slice(-CONFIG.chatHistorySendLimit),
      sessionId: this.sessionId,
      resolved_location: resolvedLocation,
      previous_disambiguation_options: this.lastDisambiguationOptions || [],
      activeOverlays: this.getActiveOverlays(),
      cacheStats: this.getCacheStats(),
      timeState: this.getTimeState(),
      savedOrderNames: SavedOrders.getNames(),
      loadedData: getLoadedDataList(),  // Track what data is loaded for LLM context
      ...extraOptions
    };
  },

  /**
   * Add a message to the chat UI (delegates to message-renderer).
   * @param {string} text - Message text
   * @param {string} type - 'user' or 'assistant'
   * @param {Object} [options] - { html: boolean }
   * @returns {HTMLElement} The message element
   */
  addMessage(text, type, options = {}) {
    const div = renderMessage(this.elements.messages, text, type, options);
    this.saveState();
    return div;
  },

  /**
   * Show typing/loading indicator (delegates to message-renderer).
   * @param {boolean} [staged=false] - Show staged indicator
   * @returns {HTMLElement} Indicator with updateStage method
   */
  showTypingIndicator(staged = false) {
    return renderTypingIndicator(this.elements.messages, staged);
  },

  /**
   * Get active overlay state for chat context.
   * @returns {Object} Active overlay info
   */
  getActiveOverlays() {
    const activeList = OverlaySelector?.getActiveOverlays() || [];
    if (activeList.length === 0) {
      return { type: null, filters: {} };
    }

    const primaryOverlay = activeList[0];
    const filters = OverlayController?.getActiveFilters?.(primaryOverlay) || {};

    return {
      type: primaryOverlay,
      filters: filters,
      allActive: activeList
    };
  },

  /**
   * Get cache statistics for chat context.
   * @returns {Object} Cache stats per overlay
   */
  getCacheStats() {
    if (!OverlayController) return {};

    const stats = {};
    const activeList = OverlaySelector?.getActiveOverlays() || [];

    for (const overlayId of activeList) {
      const cached = OverlayController.getCachedData(overlayId);
      if (cached && cached.features) {
        const features = cached.features;
        stats[overlayId] = {
          count: features.length,
          years: OverlayController.getLoadedYears(overlayId),
          loadedFilters: OverlayController.getLoadedFilters?.(overlayId) || {}
        };

        // Overlay-specific stats
        if (overlayId === 'earthquakes') {
          const mags = features.map(f => f.properties?.magnitude).filter(m => m != null);
          if (mags.length > 0) {
            stats[overlayId].minMag = Math.min(...mags);
            stats[overlayId].maxMag = Math.max(...mags);
          }
        } else if (overlayId === 'hurricanes') {
          const cats = features.map(f => f.properties?.max_category).filter(c => c != null);
          if (cats.length > 0) {
            stats[overlayId].categories = [...new Set(cats)].sort();
          }
        } else if (overlayId === 'wildfires') {
          const areas = features.map(f => f.properties?.area_km2).filter(a => a != null);
          if (areas.length > 0) {
            stats[overlayId].minAreaKm2 = Math.min(...areas);
            stats[overlayId].maxAreaKm2 = Math.max(...areas);
          }
        } else if (overlayId === 'volcanoes') {
          const veis = features.map(f => f.properties?.vei).filter(v => v != null);
          if (veis.length > 0) {
            stats[overlayId].minVei = Math.min(...veis);
            stats[overlayId].maxVei = Math.max(...veis);
          }
        } else if (overlayId === 'tornadoes') {
          const scales = features.map(f => f.properties?.scale).filter(s => s != null);
          if (scales.length > 0) {
            stats[overlayId].scales = [...new Set(scales)].sort();
          }
        }
      }
    }

    return stats;
  },

  /**
   * Get current time slider state for chat context.
   * @returns {Object} Time state info
   */
  getTimeState() {
    const TimeSlider = window.TimeSlider;
    if (!TimeSlider) return { available: false };

    return {
      available: true,
      isLiveLocked: TimeSlider.isLiveLocked || false,
      isLiveMode: TimeSlider.isLiveMode || false,
      currentTime: TimeSlider.currentTime,
      currentTimeFormatted: TimeSlider.formatTimeLabel?.(TimeSlider.currentTime) || null,
      minTime: TimeSlider.minTime,
      maxTime: TimeSlider.maxTime,
      granularity: TimeSlider.granularity || 'yearly',
      timezone: TimeSlider.liveTimezone || 'local'
    };
  },

  /**
   * Apply filter update from chat response.
   * @param {Object} response - { overlay, filters }
   */
  applyFilterUpdate(response) {
    const { overlay, filters } = response;

    if (!OverlayController) {
      console.warn('OverlayController not available for filter update');
      return;
    }

    if (filters.clear) {
      OverlayController.clearFilters?.(overlay);
    } else {
      OverlayController.updateFilters?.(overlay, filters);
    }

    OverlayController.reloadOverlay?.(overlay);
  }
};

// Backward-compatible exports
export const OrderManager = {
  init() { /* handled by ChatManager.initOrderPanel() */ },
  get currentOrder() { return orderPanel?.currentOrder; },
  set currentOrder(val) { if (orderPanel) orderPanel.currentOrder = val; },
  setOrder(order, summary) { orderPanel?.setOrder(order, summary); },
  setNavigationLocations(locs) { orderPanel?.setNavigationLocations(locs); },
  clearOrder() { orderPanel?.clearOrder(); },
  removeItem(idx) { orderPanel?.removeItem(idx); },
  render(summary) { orderPanel?.render(summary); },
  switchTab(tab) { orderPanel?.switchTab(tab); },
  renderLoadedTab() { orderPanel?.renderLoadedTab(); }
};

export const OrderTracker = {
  addOrder(queueId, info) { orderTracker?.addOrder(queueId, info); },
  cancel(queueId) { orderTracker?.cancel(queueId); },
  getStats() { return orderTracker?.getStats() || { pending: 0, isPolling: false }; }
};

export const SavedOrdersManager = {
  getAll() { return SavedOrders.getAll(); },
  save(name) {
    const items = orderPanel?.currentOrder?.items;
    const summary = orderPanel?.currentOrder?.summary;
    return SavedOrders.save(name, items || [], summary || '');
  },
  load(nameOrId) { return SavedOrders.load(nameOrId); },
  delete(nameOrId) { return SavedOrders.deleteOrder(nameOrId); },
  getNames() { return SavedOrders.getNames(); },
  getStats() { return SavedOrders.getStats(); },
  clearAll() { return SavedOrders.clearAll(); },
  applyToOrderManager(savedOrder) {
    if (!savedOrder || !savedOrder.items || !orderPanel) return false;
    orderPanel.currentOrder = {
      items: JSON.parse(JSON.stringify(savedOrder.items)),
      summary: savedOrder.summary || 'Loaded saved order: ' + savedOrder.name
    };
    orderPanel.render(orderPanel.currentOrder.summary);
    return true;
  }
};
