import { AuthManager, getCurrentProfile, getCurrentUser, isAuthenticated, onAuthChanged, updateUser } from './auth.js';
import { fetchMsgpack, postMsgpack } from './utils/fetch.js';

// ============================================================================
// PACK REGISTRY
// All known packs grouped by bundle. entitled_by_default = true until
// real entitlement enforcement is wired from Supabase.
// ============================================================================

const PACK_REGISTRY = [
  {
    bundle: 'Disasters',
    packs: [
      { id: 'fires',       label: 'Fires',       description: 'Global wildfire events, US and Canada perimeters and risk scores' },
      { id: 'earthquakes', label: 'Earthquakes',  description: 'USGS global events, PAGER historical catalog, NRCan Canada' },
      { id: 'hurricanes',  label: 'Hurricanes',   description: 'IBTrACS global hurricane and tropical storm tracks' },
      { id: 'tornadoes',   label: 'Tornadoes',    description: 'Global tornado events' },
      { id: 'floods',      label: 'Floods',       description: 'Global flood events (data through 2019)' },
      { id: 'volcanoes',   label: 'Volcanoes',    description: 'Global volcanic activity' },
      { id: 'tsunamis',    label: 'Tsunamis',     description: 'Global tsunami events' },
    ]
  },
  {
    bundle: 'Development',
    packs: [
      { id: 'un_sdg',     label: 'UN SDG',     description: 'All 14 Sustainable Development Goal indicators by country' },
      { id: 'currencies', label: 'Currencies', description: 'Global currency lifecycle and reference data' },
    ]
  },
  {
    bundle: 'Geometry',
    packs: [
      { id: 'geometry_global', label: 'Global Geometry',      description: 'All countries and territories to admin level 2', always_on: true },
      { id: 'geometry_usa',    label: 'US Detailed Geometry', description: 'Census tracts, block groups, ZIP codes, and tribal lands' },
    ]
  }
];

const PACKS_STORAGE_KEY = 'active_pack_ids';

function loadActivePacks() {
  try {
    const saved = localStorage.getItem(PACKS_STORAGE_KEY);
    if (saved) return new Set(JSON.parse(saved));
  } catch (_) {}
  // Default: all packs on
  const all = new Set();
  PACK_REGISTRY.forEach(b => b.packs.forEach(p => all.add(p.id)));
  return all;
}

function saveActivePacks(activeSet) {
  try {
    localStorage.setItem(PACKS_STORAGE_KEY, JSON.stringify([...activeSet]));
  } catch (_) {}
}

function renderPacksSection(entitledPackIds) {
  const container = document.getElementById('packsList');
  if (!container) return;

  // entitledPackIds: Set of pack IDs the user is allowed to use.
  // For now pass null to mean "all entitled" until Supabase is wired.
  const entitled = entitledPackIds || new Set(PACK_REGISTRY.flatMap(b => b.packs.map(p => p.id)));
  const active = loadActivePacks();

  const html = PACK_REGISTRY.map(bundle => `
    <div class="packs-bundle">
      <h3 class="packs-bundle-label">${bundle.bundle}</h3>
      <div class="packs-grid">
        ${bundle.packs.map(pack => {
          const isEntitled = pack.always_on || entitled.has(pack.id);
          const isActive = active.has(pack.id);
          const disabledAttr = pack.always_on ? 'disabled' : (!isEntitled ? 'disabled' : '');
          const checkedAttr = (pack.always_on || isActive) ? 'checked' : '';
          const lockedClass = pack.always_on ? 'pack-always-on' : (!isEntitled ? 'pack-locked' : '');
          return `
            <label class="pack-card ${lockedClass}" for="pack_${pack.id}">
              <input type="checkbox" id="pack_${pack.id}" class="pack-checkbox" data-pack-id="${pack.id}"
                     ${checkedAttr} ${disabledAttr}>
              <div class="pack-card-body">
                <span class="pack-label">${pack.label}</span>
                <span class="pack-desc">${pack.description}</span>
                ${pack.always_on ? '<span class="pack-tag">always on</span>' : ''}
                ${!isEntitled && !pack.always_on ? '<span class="pack-tag locked">upgrade</span>' : ''}
              </div>
            </label>`;
        }).join('')}
      </div>
    </div>
  `).join('');

  container.innerHTML = html;
}

function collectPackSelection() {
  const active = new Set();
  // always-on packs are always included
  PACK_REGISTRY.forEach(b => b.packs.forEach(p => { if (p.always_on) active.add(p.id); }));
  document.querySelectorAll('.pack-checkbox:not([disabled]):checked').forEach(cb => {
    active.add(cb.dataset.packId);
  });
  return active;
}

function initPacksSection(entitledPackIds) {
  renderPacksSection(entitledPackIds);

  const applyBtn = document.getElementById('packsApplyBtn');
  const status = document.getElementById('packsSaveStatus');
  if (!applyBtn) return;

  applyBtn.addEventListener('click', () => {
    const selection = collectPackSelection();
    saveActivePacks(selection);
    if (status) {
      status.textContent = 'Catalog updated.';
      window.setTimeout(() => { status.textContent = ''; }, 3000);
    }
  });
}

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

// ============================================================================
// TABS
// ============================================================================

function initTabs() {
  const tabs = document.querySelectorAll('.settings-tab');
  const panels = document.querySelectorAll('.settings-tab-panel');

  function activateTab(tabId) {
    tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
    panels.forEach(p => p.classList.toggle('active', p.id === `tab-${tabId}`));
    try { localStorage.setItem('settings_tab', tabId); } catch (_) {}
  }

  tabs.forEach(tab => {
    tab.addEventListener('click', () => activateTab(tab.dataset.tab));
  });

  // Restore last active tab
  try {
    const saved = localStorage.getItem('settings_tab');
    if (saved && document.getElementById(`tab-${saved}`)) {
      activateTab(saved);
    }
  } catch (_) {}
}

// ============================================================================
// PROFILE UPDATES
// ============================================================================

function setFieldStatus(elId, message, isError) {
  const el = document.getElementById(elId);
  if (!el) return;
  el.textContent = message;
  el.className = `profile-field-status ${isError ? 'error' : 'success'}`;
  window.setTimeout(() => { el.textContent = ''; el.className = 'profile-field-status'; }, 4000);
}

async function initProfileSection() {
  const user = getCurrentUser();
  const emailInput = document.getElementById('profileEmail');
  if (emailInput && user?.email) emailInput.value = user.email;

  document.getElementById('updateEmailBtn')?.addEventListener('click', async () => {
    const newEmail = document.getElementById('profileEmail')?.value?.trim();
    if (!newEmail) return;
    try {
      const { error } = await updateUser({ email: newEmail });
      if (error) throw error;
      setFieldStatus('profileEmailStatus', 'Confirmation sent to new email.', false);
    } catch (err) {
      setFieldStatus('profileEmailStatus', err.message || 'Could not update email.', true);
    }
  });

  document.getElementById('updatePasswordBtn')?.addEventListener('click', async () => {
    const newPw = document.getElementById('profileNewPassword')?.value;
    if (!newPw || newPw.length < 8) {
      setFieldStatus('profilePasswordStatus', 'Password must be at least 8 characters.', true);
      return;
    }
    try {
      const { error } = await updateUser({ password: newPw });
      if (error) throw error;
      document.getElementById('profileNewPassword').value = '';
      setFieldStatus('profilePasswordStatus', 'Password updated.', false);
    } catch (err) {
      setFieldStatus('profilePasswordStatus', err.message || 'Could not update password.', true);
    }
  });
}

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

function initAdminSection() {
  const profile = getCurrentProfile() || {};
  const isMaster = profile.plan_id === 'master' || profile.is_admin === true;

  const section = document.getElementById('adminSection');
  if (section) section.classList.toggle('hidden', !isMaster);
  if (!isMaster) return;

  const refreshBtn = document.getElementById('adminCatalogRefreshBtn');
  const status = document.getElementById('adminCatalogRefreshStatus');
  if (!refreshBtn) return;

  refreshBtn.addEventListener('click', async () => {
    refreshBtn.disabled = true;
    if (status) status.textContent = 'Refreshing...';
    try {
      const result = await postMsgpack('/api/admin/catalog/refresh', {});
      if (status) status.textContent = `Done: ${result.source_count} sources loaded.`;
    } catch (e) {
      if (status) status.textContent = `Error: ${e.message}`;
    } finally {
      refreshBtn.disabled = false;
    }
  });
}

async function renderPage() {
  const signedIn = isAuthenticated();
  els.locked?.classList.toggle('hidden', signedIn);
  els.unlocked?.classList.toggle('hidden', !signedIn);

  if (!signedIn) {
    return;
  }

  initTabs();
  renderAccountSummary();
  await initProfileSection();
  initPacksSection(null);
  initAdminSection();
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
