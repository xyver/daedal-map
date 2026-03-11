/**
 * MessagePack fetch utilities
 * All API calls should use these instead of raw fetch()
 *
 * MessagePack library loaded via CDN, available as window.MessagePack
 */

import { getAccessToken, getStorageNamespace } from '../auth.js';

// Get MessagePack from global scope (loaded via CDN)
const msgpack = window.MessagePack || {};

// localStorage key for tracking API calls for session recovery
const API_CALLS_KEY = 'countymap_api_calls';

// localStorage key for tracking executed orders for session recovery
const ORDERS_KEY = 'countymap_executed_orders';

// API paths that should be tracked for recovery (data endpoints)
const TRACKED_API_PATTERNS = [
  '/api/earthquakes/',
  '/api/storms/',
  '/api/volcanoes/',
  '/api/wildfires/',
  '/api/tornadoes/',
  '/api/tsunamis/',
  '/api/floods/',
  '/api/climate/'
];

function namespacedKey(baseKey) {
  return `${baseKey}:${getStorageNamespace()}`;
}

function buildAuthHeaders() {
  const token = getAccessToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Check if a URL should be tracked for session recovery.
 */
function shouldTrackCall(url) {
  return TRACKED_API_PATTERNS.some(pattern => url.includes(pattern));
}

/**
 * Log an API call to localStorage for session recovery.
 */
function logApiCall(url) {
  try {
    const calls = JSON.parse(localStorage.getItem(namespacedKey(API_CALLS_KEY)) || '[]');
    // Avoid duplicates
    if (!calls.includes(url)) {
      calls.push(url);
      localStorage.setItem(namespacedKey(API_CALLS_KEY), JSON.stringify(calls));
    }
  } catch (e) {
    // localStorage not available
  }
}

/**
 * Get all logged API calls for session recovery.
 */
export function getApiCallsForRecovery() {
  try {
    return JSON.parse(localStorage.getItem(namespacedKey(API_CALLS_KEY)) || '[]');
  } catch (e) {
    return [];
  }
}

/**
 * Clear logged API calls (called by New Chat).
 */
export function clearApiCalls() {
  try {
    localStorage.removeItem(namespacedKey(API_CALLS_KEY));
  } catch (e) {
    // Ignore
  }
}

/**
 * Log an executed order for session recovery.
 * Stores only the order (request) data, not the response.
 * @param {Object} order - The order that was executed
 */
export function logExecutedOrder(order) {
  try {
    const orders = JSON.parse(localStorage.getItem(namespacedKey(ORDERS_KEY)) || '[]');
    // Store with timestamp and summary for display
    const record = {
      order: order,
      summary: order.summary || 'Data order',
      timestamp: Date.now()
    };
    orders.push(record);
    // Keep only last 10 orders to avoid storage bloat
    if (orders.length > 10) {
      orders.shift();
    }
    localStorage.setItem(namespacedKey(ORDERS_KEY), JSON.stringify(orders));
  } catch (e) {
    console.warn('Failed to log executed order:', e);
  }
}

/**
 * Get all logged executed orders for session recovery.
 * @returns {Array} Array of {order, summary, timestamp} records
 */
export function getExecutedOrdersForRecovery() {
  try {
    return JSON.parse(localStorage.getItem(namespacedKey(ORDERS_KEY)) || '[]');
  } catch (e) {
    return [];
  }
}

/**
 * Clear logged executed orders (called by New Chat).
 */
export function clearExecutedOrders() {
  try {
    localStorage.removeItem(namespacedKey(ORDERS_KEY));
  } catch (e) {
    // Ignore
  }
}

/**
 * Fetch data from API endpoint with MessagePack decoding.
 * @param {string} url - API endpoint
 * @param {object} options - fetch options (optional)
 * @returns {Promise<any>} Decoded response data
 */
export async function fetchMsgpack(url, options = {}) {
  // Log data API calls for session recovery
  if (shouldTrackCall(url)) {
    logApiCall(url);
  }

  const response = await fetch(url, {
    ...options,
    headers: {
      'Accept': 'application/msgpack',
      ...buildAuthHeaders(),
      ...options.headers,
    }
  });

  if (!response.ok) {
    let errorMsg = 'Request failed';
    try {
      const buffer = await response.arrayBuffer();
      const decoded = msgpack.decode(new Uint8Array(buffer));
      errorMsg = decoded.error || errorMsg;
    } catch (e) {
      errorMsg = response.statusText;
    }
    throw new Error(errorMsg);
  }

  const buffer = await response.arrayBuffer();
  return msgpack.decode(new Uint8Array(buffer));
}

/**
 * POST data to API endpoint with MessagePack encoding/decoding.
 * @param {string} url - API endpoint
 * @param {object} data - Data to send
 * @returns {Promise<any>} Decoded response data
 */
export async function postMsgpack(url, data) {
  return fetchMsgpack(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/msgpack',
      ...buildAuthHeaders(),
    },
    body: msgpack.encode(data)
  });
}

/**
 * GET request with query params and MessagePack response.
 * @param {string} url - Base URL
 * @param {object} params - Query parameters
 * @returns {Promise<any>} Decoded response data
 */
export async function getMsgpack(url, params = {}) {
  const queryString = new URLSearchParams(params).toString();
  const fullUrl = queryString ? `${url}?${queryString}` : url;
  return fetchMsgpack(fullUrl);
}
