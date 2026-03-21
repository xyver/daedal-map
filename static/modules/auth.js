/**
 * Frontend auth manager for token-aware runtime behavior.
 *
 * Login and account UX live on daedalmap.com.
 * The public app only reads session context and routes users to the private site.
 */

const AUTH_EVENT = 'countymap-auth-changed';
const LOGGED_IN_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
const GUEST_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const SITE_BASE = 'https://www.daedalmap.com';
const LEGACY_SHARED_COOKIE_DOMAIN = '.daedalmap.com';
const LEGACY_SHARED_ACCESS_COOKIE = 'dm_access_token';
const LEGACY_SHARED_REFRESH_COOKIE = 'dm_refresh_token';

let authClient = null;
let authConfig = null;
let currentSession = null;
let currentProfile = null;
let initialized = false;
let _lastAuthUserId = null;

async function fetchProfile() {
  try {
    const token = currentSession?.access_token;
    if (!token) { currentProfile = null; return; }
    const resp = await fetch('/api/auth/me', {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!resp.ok) { currentProfile = null; return; }
    const buf = await resp.arrayBuffer();
    const mp = window.MessagePack || {};
    currentProfile = mp.decode ? mp.decode(new Uint8Array(buf)) : null;
  } catch (e) {
    currentProfile = null;
  }
}

function readHashSessionTokens() {
  const raw = String(window.location.hash || '').replace(/^#/, '');
  if (!raw || !raw.includes('access_token=')) return null;
  const params = new URLSearchParams(raw);
  const accessToken = params.get('access_token');
  const refreshToken = params.get('refresh_token');
  if (!accessToken || !refreshToken) return null;
  return {
    access_token: accessToken,
    refresh_token: refreshToken
  };
}

function readHashHandoffCode() {
  const raw = String(window.location.hash || '').replace(/^#/, '');
  if (!raw || !raw.includes('handoff_code=')) return null;
  const params = new URLSearchParams(raw);
  const code = params.get('handoff_code');
  return code ? String(code).trim() : null;
}

function readWindowNameHandoffCode() {
  const raw = String(window.name || '').trim();
  if (!raw.startsWith('dm_handoff:')) return null;
  const code = raw.slice('dm_handoff:'.length).trim();
  return code || null;
}

function clearWindowNameHandoffCode() {
  const raw = String(window.name || '').trim();
  if (raw.startsWith('dm_handoff:')) {
    window.name = '';
  }
}

function readHashLogoutSignal() {
  const raw = String(window.location.hash || '').replace(/^#/, '');
  if (!raw || !raw.includes('logout=')) return null;
  const params = new URLSearchParams(raw);
  if (params.get('logout') !== '1') return null;
  return {
    returnTo: params.get('return_to') || ''
  };
}

function clearLegacySharedCookie(name) {
  document.cookie = `${name}=; domain=${LEGACY_SHARED_COOKIE_DOMAIN}; path=/; max-age=0; samesite=lax; secure`;
}

function clearLegacySharedCookies() {
  clearLegacySharedCookie(LEGACY_SHARED_ACCESS_COOKIE);
  clearLegacySharedCookie(LEGACY_SHARED_REFRESH_COOKIE);
}

async function importHashSession(client) {
  const tokens = readHashSessionTokens();
  if (!tokens) return null;
  try {
    const { data, error } = await client.auth.setSession(tokens);
    if (error) {
      console.warn('[Auth] Session handoff failed:', error.message);
      return null;
    }
    return data?.session || null;
  } finally {
    window.history.replaceState(null, '', window.location.pathname + window.location.search);
  }
}

async function importHandoffCodeSession(client) {
  const code = readWindowNameHandoffCode() || readHashHandoffCode();
  if (!code) return null;
  try {
    const response = await fetch(`${SITE_BASE}/api/auth/handoff/exchange`, {
      method: 'POST',
      mode: 'cors',
      credentials: 'omit',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code })
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    const accessToken = payload?.access_token;
    const refreshToken = payload?.refresh_token;
    if (!accessToken || !refreshToken) {
      throw new Error('Incomplete handoff payload');
    }
    const { data, error } = await client.auth.setSession({
      access_token: accessToken,
      refresh_token: refreshToken
    });
    if (error) {
      throw error;
    }
    return data?.session || null;
  } catch (error) {
    console.warn('[Auth] Handoff exchange failed:', error?.message || error);
    return null;
  } finally {
    clearWindowNameHandoffCode();
    window.history.replaceState(null, '', window.location.pathname + window.location.search);
  }
}

async function consumeLogoutSignal(client) {
  const signal = readHashLogoutSignal();
  if (!signal) return false;
  try {
    await client.auth.signOut();
  } catch (error) {
    console.warn('[Auth] Logout bridge failed:', error?.message || error);
  } finally {
    currentSession = null;
    currentProfile = null;
    _lastAuthUserId = null;
    window.history.replaceState(null, '', window.location.pathname + window.location.search);
  }

  const returnTo = String(signal.returnTo || '').trim();
  if (returnTo) {
    try {
      const dest = new URL(returnTo, window.location.origin);
      const allowedOrigins = new Set([window.location.origin, SITE_BASE]);
      if (allowedOrigins.has(dest.origin)) {
        window.location.replace(dest.toString());
        return true;
      }
    } catch (_) {
      // Fall through to default signed-out state.
    }
  }
  return false;
}

function emitAuthChanged() {
  window.dispatchEvent(new CustomEvent(AUTH_EVENT, {
    detail: {
      isAuthenticated: isAuthenticated(),
      user: getCurrentUser()
    }
  }));
}

async function loadConfig() {
  const response = await fetch('/api/auth/config');
  if (!response.ok) {
    throw new Error(`Failed to load auth config: ${response.status}`);
  }
  return response.json();
}

function getBrowserSupabase() {
  if (!window.supabase?.createClient) {
    throw new Error('Supabase browser client not loaded');
  }
  return window.supabase;
}

function updateDom() {
  const btn = document.getElementById('authBtn');
  const status = document.getElementById('authStatusText');
  if (!btn || !status) return;

  if (!authConfig?.enabled) {
    btn.textContent = 'Account';
    btn.disabled = true;
    btn.classList.remove('logged-in');
    status.textContent = 'Guest mode: auth not configured.';
    return;
  }

  if (isAuthenticated()) {
    const email = getCurrentUser()?.email || 'Signed in';
    const accountUrl = currentProfile?.account_url || `${SITE_BASE}/account`;
    btn.textContent = 'Account';
    btn.disabled = false;
    btn.classList.add('logged-in');
    status.innerHTML = `${email}: authenticated runtime access enabled. <a href="${accountUrl}" target="_blank" rel="noopener">Manage account on daedalmap.com</a>`;
  } else {
    btn.textContent = 'Sign In';
    btn.disabled = false;
    btn.classList.remove('logged-in');
    status.innerHTML = `Guest mode: local-only workspace and cache. <a href="${SITE_BASE}/login" target="_blank" rel="noopener">Create account</a>`;
  }
}

async function handleAuthClick() {
  if (!authConfig?.enabled) return;
  if (isAuthenticated()) {
    window.location.href = `${SITE_BASE}/account`;
    return;
  }
  const returnTo = encodeURIComponent(window.location.href);
  // Signed-out users enter through the private account route so .com can
  // drive login and then hand the session back to the app.
  window.location.href = `${SITE_BASE}/account?return=${returnTo}`;
}

export const AuthManager = {
  async init() {
    if (initialized) {
      updateDom();
      return;
    }

    try {
      authConfig = await loadConfig();
      if (authConfig.enabled) {
        const supabase = getBrowserSupabase();
        authClient = supabase.createClient(authConfig.supabase_url, authConfig.supabase_anon_key, {
          auth: {
            persistSession: true,
            autoRefreshToken: true,
            storageKey: 'countymap-auth',
            storage: window.localStorage
          }
        });
        const logoutRedirected = await consumeLogoutSignal(authClient);
        if (logoutRedirected) {
          return;
        }
        // Handle cross-domain session handoff from daedalmap.com explicitly.
        // The private site redirects back here with a short-lived one-time code.
        // Exchange it for a session, then clean the hash. Direct Supabase hash
        // imports remain supported for provider/magic-link flows landing here.
        const handoffSession = await importHandoffCodeSession(authClient) || await importHashSession(authClient);
        const { data, error } = await authClient.auth.getSession();
        if (!error) {
          currentSession = handoffSession || data.session;
          _lastAuthUserId = currentSession?.user?.id ?? null;
          clearLegacySharedCookies();
          await fetchProfile();
        }
        authClient.auth.onAuthStateChange(async (_event, session) => {
          const newUserId = session?.user?.id ?? null;
          const userChanged = newUserId !== _lastAuthUserId;
          _lastAuthUserId = newUserId;
          currentSession = session;
          clearLegacySharedCookies();
          await fetchProfile();
          updateDom();
          if (userChanged && (_event === 'SIGNED_IN' || _event === 'SIGNED_OUT')) {
            emitAuthChanged();
          }
        });
      }
    } catch (error) {
      console.warn('[Auth] Disabled:', error.message);
      authConfig = { enabled: false, supabase_url: '', supabase_anon_key: '' };
    }

    const btn = document.getElementById('authBtn');
    if (btn) {
      btn.addEventListener('click', handleAuthClick);
    }

    initialized = true;
    updateDom();
  }
};

export function onAuthChanged(callback) {
  window.addEventListener(AUTH_EVENT, callback);
}

export function isAuthenticated() {
  return Boolean(currentSession?.user);
}

export function getCurrentUser() {
  return currentSession?.user || null;
}

export function getAccessToken() {
  return currentSession?.access_token || null;
}

export function getStorageNamespace() {
  const user = getCurrentUser();
  return user?.id ? `user:${user.id}` : 'guest';
}

export function getSessionMaxAgeMs() {
  return isAuthenticated() ? LOGGED_IN_MAX_AGE_MS : GUEST_MAX_AGE_MS;
}

export function getCurrentProfile() {
  return currentProfile;
}
