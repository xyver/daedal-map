import { AuthManager, getCurrentProfile, getCurrentUser, isAuthenticated, onAuthChanged } from './auth.js';
import { fetchMsgpack, postMsgpack } from './utils/fetch.js';

const els = {
  locked: document.getElementById('settingsLocked'),
  unlocked: document.getElementById('settingsUnlocked'),
  loginBtn: document.getElementById('settingsLoginBtn'),
  summaryPlan: document.getElementById('summaryPlan'),
  summaryShells: document.getElementById('summaryShells'),
  summaryPackLimit: document.getElementById('summaryPackLimit'),
  summaryPackCount: document.getElementById('summaryPackCount'),
  backupPathInput: document.getElementById('backupPathInput'),
  saveBtn: document.getElementById('saveSettingsBtn'),
  initFoldersBtn: document.getElementById('initFoldersBtn'),
  status: document.getElementById('settingsStatus'),
  currentConfig: document.getElementById('currentConfig'),
  timezoneSelect: document.getElementById('timezoneSelect')
};

function showStatus(message, type = 'success') {
  if (!els.status) return;
  els.status.textContent = message;
  els.status.className = `settings-status ${type}`;
  window.setTimeout(() => {
    els.status.className = 'settings-status';
  }, 5000);
}

function updateConfigDisplay(settings) {
  if (!els.currentConfig) return;

  if (settings.error) {
    els.currentConfig.innerHTML = `<span style="color:#ff7e8f;">${settings.error}</span>`;
    return;
  }

  if (!settings.backup_path) {
    els.currentConfig.innerHTML = '<em>No backup path configured</em>';
    return;
  }

  let html = `<strong>Backup Path:</strong> ${settings.backup_path}<br>`;
  if (settings.folders_exist) {
    html += '<br><strong>Folder Status:</strong><br>';
    for (const [folder, exists] of Object.entries(settings.folders_exist)) {
      const color = exists ? '#48c774' : '#ff7e8f';
      const label = exists ? '[OK]' : '[Missing]';
      html += `<span style="color:${color};">${label}</span> ${folder}<br>`;
    }
  }
  els.currentConfig.innerHTML = html;
}

async function loadSettings() {
  try {
    const settings = await fetchMsgpack('/api/settings');
    if (els.backupPathInput) {
      els.backupPathInput.value = settings.backup_path || '';
    }
    updateConfigDisplay(settings);
  } catch (error) {
    updateConfigDisplay({ error: error.message || 'Could not load settings' });
  }
}

async function saveSettings() {
  const backupPath = els.backupPathInput?.value?.trim() || '';
  try {
    const result = await postMsgpack('/api/settings', { backup_path: backupPath });
    if (result.success) {
      showStatus('Settings saved.', 'success');
      updateConfigDisplay(result.settings || { backup_path: backupPath });
      return;
    }
    showStatus(result.error || 'Failed to save settings.', 'error');
  } catch (error) {
    showStatus(error.message || 'Failed to save settings.', 'error');
  }
}

async function initializeFolders() {
  const backupPath = els.backupPathInput?.value?.trim() || '';
  if (!backupPath) {
    showStatus('Enter a backup path first.', 'error');
    return;
  }

  try {
    const result = await postMsgpack('/api/settings/init-folders', { backup_path: backupPath });
    if (result.success) {
      showStatus('Folders initialized.', 'success');
      await loadSettings();
      return;
    }
    showStatus(result.error || 'Failed to initialize folders.', 'error');
  } catch (error) {
    showStatus(error.message || 'Failed to initialize folders.', 'error');
  }
}

function loadTimezoneSettings() {
  if (!els.timezoneSelect) return;
  try {
    const saved = localStorage.getItem('liveTimezone');
    if (saved) {
      els.timezoneSelect.value = saved;
    }
  } catch (error) {
    // Ignore unavailable localStorage
  }

  els.timezoneSelect.addEventListener('change', (event) => {
    try {
      localStorage.setItem('liveTimezone', event.target.value);
    } catch (error) {
      // Ignore unavailable localStorage
    }
  });
}

function renderAccountSummary() {
  const profile = getCurrentProfile() || {};
  const user = getCurrentUser() || {};
  const userPacks = profile.user_packs || [];
  const orgPacks = profile.org_packs || [];
  const uniquePacks = new Set([...userPacks, ...orgPacks]);

  if (els.summaryPlan) els.summaryPlan.textContent = profile.plan_id || 'free';
  if (els.summaryShells) els.summaryShells.textContent = (profile.enabled_shells || ['simple']).join(', ');
  if (els.summaryPackLimit) {
    els.summaryPackLimit.textContent = profile.max_packs == null ? 'unlimited' : String(profile.max_packs);
  }
  if (els.summaryPackCount) els.summaryPackCount.textContent = String(uniquePacks.size);

  const authStatus = document.getElementById('authStatusText');
  if (authStatus && user.email) {
    authStatus.textContent = `${user.email}: ${profile.plan_id || 'free'} plan, ${uniquePacks.size} entitled pack${uniquePacks.size === 1 ? '' : 's'}.`;
  }
}

async function renderPage() {
  const signedIn = isAuthenticated();
  els.locked?.classList.toggle('hidden', signedIn);
  els.unlocked?.classList.toggle('hidden', !signedIn);

  if (!signedIn) {
    return;
  }

  renderAccountSummary();
  await loadSettings();
}

async function init() {
  await AuthManager.init();
  loadTimezoneSettings();

  els.loginBtn?.addEventListener('click', () => {
    document.getElementById('authBtn')?.click();
  });
  els.saveBtn?.addEventListener('click', saveSettings);
  els.initFoldersBtn?.addEventListener('click', initializeFolders);

  onAuthChanged(() => {
    window.location.reload();
  });

  await renderPage();
}

init();
