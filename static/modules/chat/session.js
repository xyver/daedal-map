/**
 * Chat Session Manager
 * Handles session ID lifecycle and chat state persistence via localStorage.
 * Reusable across map app and admin dashboard.
 */

import { getSessionMaxAgeMs, getStorageNamespace, isAuthenticated } from '../auth.js';

// Storage keys
const SESSION_ID_KEY = 'countymap_session_id';
const SESSION_TIMESTAMP_KEY = 'countymap_session_timestamp';
const CHAT_HISTORY_KEY = 'countymap_chat_history';
const CHAT_MESSAGES_KEY = 'countymap_chat_messages';

function namespacedKey(baseKey) {
  return `${baseKey}:${getStorageNamespace()}`;
}

/**
 * Get existing session ID from localStorage or create a new one.
 * Session persists across tab close/refresh for recovery.
 * @returns {string} Session ID
 */
export function getOrCreateSessionId() {
  const sessionKey = namespacedKey(SESSION_ID_KEY);
  const timestampKey = namespacedKey(SESSION_TIMESTAMP_KEY);
  let sessionId = localStorage.getItem(sessionKey);
  const timestamp = localStorage.getItem(timestampKey);

  // Check if session is expired
  const isExpired = timestamp && (Date.now() - parseInt(timestamp, 10)) > getSessionMaxAgeMs();

  if (sessionId && !isExpired) {
    // Update timestamp on reuse
    localStorage.setItem(timestampKey, Date.now().toString());
    console.log('[Session] Restored session:', sessionId);
    return sessionId;
  }

  // Create new session - also clear stale chat storage so old conversation
  // doesn't bleed into the new session on page load.
  clearChatStorage();
  const prefix = isAuthenticated() ? 'authsess_' : 'sess_';
  sessionId = prefix + Date.now() + '_' + Math.random().toString(36).substring(2, 11);
  localStorage.setItem(sessionKey, sessionId);
  localStorage.setItem(timestampKey, Date.now().toString());
  console.log('[Session] Created new session:', sessionId);
  return sessionId;
}

/**
 * Clear the current session ID from localStorage.
 * @returns {string} New session ID (auto-created)
 */
export function resetSessionId() {
  localStorage.removeItem(namespacedKey(SESSION_ID_KEY));
  localStorage.removeItem(namespacedKey(SESSION_TIMESTAMP_KEY));
  return getOrCreateSessionId();
}

/**
 * Save chat state to localStorage for persistence across browser close.
 * @param {Array} history - Chat history array (role/content pairs)
 * @param {string} messagesHtml - Rendered messages HTML string
 */
export function saveChatState(history, messagesHtml) {
  try {
    localStorage.setItem(namespacedKey(CHAT_HISTORY_KEY), JSON.stringify(history));
    if (messagesHtml) {
      localStorage.setItem(namespacedKey(CHAT_MESSAGES_KEY), messagesHtml);
    }
  } catch (e) {
    console.warn('[Session] Could not save chat state:', e.message);
  }
}

/**
 * Restore chat state from localStorage.
 * @returns {Object|null} { history: Array, messagesHtml: string } or null if nothing saved
 */
export function restoreChatState() {
  try {
    const historyJson = localStorage.getItem(namespacedKey(CHAT_HISTORY_KEY));
    const messagesHtml = localStorage.getItem(namespacedKey(CHAT_MESSAGES_KEY));

    if (historyJson || messagesHtml) {
      const history = historyJson ? JSON.parse(historyJson) : [];
      console.log('[Session] Restored chat history:', history.length, 'messages');
      return { history, messagesHtml: messagesHtml || '' };
    }
  } catch (e) {
    console.warn('[Session] Could not restore chat state:', e.message);
  }
  return null;
}

/**
 * Clear all chat state from localStorage.
 */
export function clearChatStorage() {
  localStorage.removeItem(namespacedKey(CHAT_HISTORY_KEY));
  localStorage.removeItem(namespacedKey(CHAT_MESSAGES_KEY));
}
