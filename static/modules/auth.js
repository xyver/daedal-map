/**
 * Frontend auth manager for optional Supabase login.
 *
 * Logged-out users keep the standard lightweight local cache.
 * Logged-in users get user-scoped persistence with longer retention.
 */

const AUTH_EVENT = 'countymap-auth-changed';
const LOGGED_IN_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
const GUEST_MAX_AGE_MS = 24 * 60 * 60 * 1000;

let authClient = null;
let authConfig = null;
let currentSession = null;
let currentProfile = null;
let initialized = false;
let currentTab = 'signin';
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

function getElements() {
  return {
    modal: document.getElementById('authModal'),
    backdrop: document.getElementById('authModalBackdrop'),
    close: document.getElementById('authModalClose'),
    message: document.getElementById('authMessage'),
    signInTab: document.getElementById('authTabSignIn'),
    signUpTab: document.getElementById('authTabSignUp'),
    signInForm: document.getElementById('signInForm'),
    signUpForm: document.getElementById('signUpForm'),
    signInEmail: document.getElementById('signInEmail'),
    signInPassword: document.getElementById('signInPassword'),
    signUpEmail: document.getElementById('signUpEmail'),
    signUpPassword: document.getElementById('signUpPassword'),
    signUpPasswordConfirm: document.getElementById('signUpPasswordConfirm'),
    magicLinkBtn: document.getElementById('magicLinkBtn')
  };
}

function setMessage(text = '', tone = '') {
  const { message } = getElements();
  if (!message) return;
  message.textContent = text;
  message.className = 'auth-message';
  if (tone) {
    message.classList.add(tone);
  }
}

function switchTab(tabName) {
  currentTab = tabName;
  const { signInTab, signUpTab, signInForm, signUpForm } = getElements();
  if (!signInTab || !signUpTab || !signInForm || !signUpForm) return;
  const signInActive = tabName === 'signin';
  signInTab.classList.toggle('active', signInActive);
  signUpTab.classList.toggle('active', !signInActive);
  signInForm.classList.toggle('active', signInActive);
  signUpForm.classList.toggle('active', !signInActive);
  setMessage('');
}

function openModal(tabName = 'signin') {
  const { modal } = getElements();
  if (!modal) return;
  switchTab(tabName);
  modal.classList.remove('hidden');
  modal.setAttribute('aria-hidden', 'false');
}

function closeModal() {
  const { modal } = getElements();
  if (!modal) return;
  modal.classList.add('hidden');
  modal.setAttribute('aria-hidden', 'true');
  setMessage('');
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
    btn.textContent = 'Login';
    btn.disabled = true;
    btn.classList.remove('logged-in');
    status.textContent = 'Guest mode: auth not configured.';
    return;
  }

  if (isAuthenticated()) {
    const email = getCurrentUser()?.email || 'Signed in';
    const planId = getUserPlanId();
    const maxPacks = currentProfile?.max_packs;
    const credits = currentProfile?.credits_balance;
    const shells = getEnabledShells();
    const packText = maxPacks == null ? 'unlimited packs' : `${maxPacks} pack${maxPacks === 1 ? '' : 's'}`;
    const creditsText = credits != null ? ` | ${credits} credits` : '';
    const accountUrl = currentProfile?.account_url || 'https://daedalmap.com/account';
    btn.textContent = 'Logout';
    btn.disabled = false;
    btn.classList.add('logged-in');
    status.innerHTML = `${email}: ${planId} plan, ${packText}${creditsText}, shells ${shells.join(', ')}. <a href="${accountUrl}" target="_blank" rel="noopener">Manage account</a>`;
  } else {
    btn.textContent = 'Login';
    btn.disabled = false;
    btn.classList.remove('logged-in');
    status.innerHTML = 'Guest mode: local-only workspace and cache. <a href="https://daedalmap.com/account" target="_blank" rel="noopener">Create account</a>';
  }
}

async function sendMagicLink() {
  const { signInEmail, signUpEmail } = getElements();
  const email = (currentTab === 'signup' ? signUpEmail?.value : signInEmail?.value) || signInEmail?.value || signUpEmail?.value;
  if (!email) return;
  const redirectTo = `${window.location.origin}/`;
  const { error } = await authClient.auth.signInWithOtp({
    email,
    options: { emailRedirectTo: redirectTo }
  });
  if (error) {
    setMessage(`Magic link failed: ${error.message}`, 'error');
    return;
  }
  setMessage(`Magic link sent to ${email}`, 'success');
}

async function handleSignInSubmit(event) {
  event.preventDefault();
  const { signInEmail, signInPassword } = getElements();
  const email = signInEmail?.value?.trim() || '';
  const password = signInPassword?.value || '';
  if (!email || !password) {
    setMessage('Email and password are required.', 'error');
    return;
  }
  const { error } = await authClient.auth.signInWithPassword({ email, password });
  if (error) {
    setMessage(`Sign in failed: ${error.message}`, 'error');
    return;
  }
  closeModal();
}

async function handleSignUpSubmit(event) {
  event.preventDefault();
  const { signUpEmail, signUpPassword, signUpPasswordConfirm } = getElements();
  const email = signUpEmail?.value?.trim() || '';
  const password = signUpPassword?.value || '';
  const confirm = signUpPasswordConfirm?.value || '';

  if (!email || !password || !confirm) {
    setMessage('Email and both password fields are required.', 'error');
    return;
  }
  if (password !== confirm) {
    setMessage('Passwords do not match.', 'error');
    return;
  }

  const { error } = await authClient.auth.signUp({
    email,
    password,
    options: {
      emailRedirectTo: `${window.location.origin}/`
    }
  });
  if (error) {
    setMessage(`Account creation failed: ${error.message}`, 'error');
    return;
  }

  setMessage('Account created. Check your email if confirmation is enabled, then sign in.', 'success');
  switchTab('signin');
  const { signInEmail, signInPassword } = getElements();
  if (signInEmail) signInEmail.value = email;
  if (signInPassword) signInPassword.value = '';
}

function bindModalEvents() {
  const {
    backdrop,
    close,
    signInTab,
    signUpTab,
    signInForm,
    signUpForm,
    magicLinkBtn
  } = getElements();

  backdrop?.addEventListener('click', closeModal);
  close?.addEventListener('click', closeModal);
  signInTab?.addEventListener('click', () => switchTab('signin'));
  signUpTab?.addEventListener('click', () => switchTab('signup'));
  signInForm?.addEventListener('submit', handleSignInSubmit);
  signUpForm?.addEventListener('submit', handleSignUpSubmit);
  magicLinkBtn?.addEventListener('click', async () => {
    await sendMagicLink();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeModal();
    }
  });

  document.querySelectorAll('.auth-pw-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const target = document.getElementById(btn.dataset.target);
      if (!target) return;
      const hidden = target.type === 'password';
      target.type = hidden ? 'text' : 'password';
      btn.textContent = hidden ? 'hide' : 'show';
    });
  });
}

async function handleAuthClick() {
  if (!authConfig?.enabled) return;
  if (isAuthenticated()) {
    const { error } = await authClient.auth.signOut();
    if (error) {
      window.alert(`Logout failed: ${error.message}`);
      return;
    }
    window.location.reload();
    return;
  }
  openModal('signin');
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
        // Handle cross-domain session handoff from daedalmap.com.
        // After login on .com the user is redirected here with tokens in the
        // URL hash. Supabase detects this automatically via detectSessionInUrl.
        // We then clean the hash so tokens are not left in browser history.
        const hash = window.location.hash;
        if (hash && hash.includes('access_token=')) {
          window.history.replaceState(null, '', window.location.pathname + window.location.search);
        }

        const { data, error } = await authClient.auth.getSession();
        if (!error) {
          currentSession = data.session;
          _lastAuthUserId = data.session?.user?.id ?? null;
          await fetchProfile();
        }
        authClient.auth.onAuthStateChange(async (_event, session) => {
          const newUserId = session?.user?.id ?? null;
          const userChanged = newUserId !== _lastAuthUserId;
          _lastAuthUserId = newUserId;
          currentSession = session;
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
    bindModalEvents();

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

export function getUserPlanId() {
  return currentProfile?.plan_id || 'free';
}

export function getEnabledShells() {
  return currentProfile?.enabled_shells || ['simple'];
}

export function getCreditsBalance() {
  return currentProfile?.credits_balance ?? null;
}

export async function updateUser(updates) {
  if (!authClient) throw new Error('Not authenticated');
  return authClient.auth.updateUser(updates);
}
