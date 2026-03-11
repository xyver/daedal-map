/**
 * Chat API Communication
 * Handles all HTTP communication with the chat backend.
 * Supports both msgpack and streaming SSE endpoints.
 * Reusable across map app and admin dashboard.
 */

import { postMsgpack } from '../utils/fetch.js';
import { getAccessToken } from '../auth.js';

/**
 * Get the appropriate API URL, respecting API_BASE_URL if set.
 * @param {string} path - API path (e.g., '/chat', '/chat/stream')
 * @returns {string} Full API URL
 */
export function getApiUrl(path) {
  if (typeof API_BASE_URL !== 'undefined' && API_BASE_URL) {
    return `${API_BASE_URL}${path}`;
  }
  return path;
}

/**
 * Send a chat request via msgpack.
 * @param {Object} payload - Full request payload (query, viewport, history, etc.)
 * @returns {Promise<Object>} Parsed response
 */
export async function sendChatRequest(payload) {
  const url = getApiUrl('/chat');
  return await postMsgpack(url, payload);
}

/**
 * Send a streaming chat request via SSE.
 * Parses Server-Sent Events and calls onProgress for each stage.
 * @param {Object} payload - Full request payload
 * @param {Function} onProgress - Callback: (stage, message) => void
 * @returns {Promise<Object|null>} Final result object, or null if no result
 */
export async function sendStreamingRequest(payload, onProgress) {
  const url = getApiUrl('/chat/stream');
  const token = getAccessToken();

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let result = null;
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    // Process complete SSE events
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';  // Keep incomplete line in buffer

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        try {
          const data = JSON.parse(line.slice(6));

          if (data.stage === 'complete') {
            result = data.result;
          } else if (onProgress) {
            onProgress(data.stage, data.message);
          }
        } catch (e) {
          console.warn('Failed to parse SSE data:', line);
        }
      }
    }
  }

  return result;
}

/**
 * Queue an order for background processing.
 * @param {Array} items - Order items
 * @param {Object} hints - Order hints (summary, etc.)
 * @param {string} sessionId - Current session ID
 * @returns {Promise<Object>} { queue_id, position }
 */
export async function queueOrder(items, hints, sessionId) {
  const url = getApiUrl('/api/orders/queue');
  return await postMsgpack(url, {
    items,
    hints,
    session_id: sessionId
  });
}

/**
 * Check status of queued orders.
 * @param {Array<string>} queueIds - Queue IDs to check
 * @returns {Promise<Object>} Map of queue_id -> status
 */
export async function checkOrderStatus(queueIds) {
  const url = getApiUrl('/api/orders/status');
  return await postMsgpack(url, { queue_ids: queueIds });
}

/**
 * Cancel a queued order.
 * @param {string} queueId - Queue ID to cancel
 * @returns {Promise<Object>} Response
 */
export async function cancelOrder(queueId) {
  const url = getApiUrl('/api/orders/cancel');
  return await postMsgpack(url, { queue_id: queueId });
}

/**
 * Clear a session on the backend (fire and forget).
 * @param {string} sessionId - Session ID to clear
 */
export async function clearBackendSession(sessionId) {
  try {
    await postMsgpack('/api/session/clear', { sessionId });
  } catch (e) {
    console.log('[Session] Backend clear skipped:', e.message);
  }
}
