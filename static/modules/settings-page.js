import { AuthManager, getCurrentProfile, getCurrentUser, isAuthenticated, onAuthChanged, updateUser } from './auth.js';
import { fetchMsgpack, postMsgpack } from './utils/fetch.js';

// ============================================================================
// PACKS - driven entirely by catalog API (pack_id is the publish gate)
// ============================================================================

const PACKS_STORAGE_KEY = 'active_pack_ids';

const ACRONYMS = new Set(['sdg', 'un', 'fx', 'co2', 'imf', 'bop', 'us', 'usa', 'epa', 'cia', 'nasa', 'who', 'bom', 'zcta', 'nrcan', 'abs']);

function prettifyId(id) {
  return id.replace(/_/g, ' ').split(' ').map(word =>
    ACRONYMS.has(word.toLowerCase()) ? word.toUpperCase() : word.charAt(0).toUpperCase() + word.slice(1)
  ).join(' ');
}

function loadActivePacks(defaultIds) {
  try {
    const saved = localStorage.getItem(PACKS_STORAGE_KEY);
    if (saved) return new Set(JSON.parse(saved));
  } catch (_) {}
  return new Set(defaultIds || []);
}

function saveActivePacks(activeSet) {
  try {
    localStorage.setItem(PACKS_STORAGE_KEY, JSON.stringify([...activeSet]));
  } catch (_) {}
}

function buildPacksFromSources(sources) {
  // Published: one card per unique pack_id.
  // Prefer the source whose source_id matches its pack_id as the representative.
  const packMap = new Map();
  for (const src of sources) {
    if (!src.pack_id) continue;
    if (!packMap.has(src.pack_id) || src.source_id === src.pack_id) {
      packMap.set(src.pack_id, src);
    }
  }
  // Count sources per pack to detect multi-source packs
  const packSourceCount = new Map();
  for (const src of sources) {
    if (!src.pack_id) continue;
    packSourceCount.set(src.pack_id, (packSourceCount.get(src.pack_id) || 0) + 1);
  }

  const published = [...packMap.entries()].map(([pack_id, src]) => {
    const count = packSourceCount.get(pack_id) || 1;
    const desc = count > 1
      ? (src.topic_tags || []).slice(0, 4).join(', ')
      : (src.source_name || '');
    return {
      id: pack_id,
      label: prettifyId(pack_id),
      description: desc,
      category: src.category || 'other',
      source_count: count,
      pack_page: `https://daedalmap.com/packs/${pack_id}`,
    };
  });

  // Internal: sources with no pack_id
  const internal = sources
    .filter(s => !s.pack_id)
    .map(src => ({
      id: src.source_id,
      label: src.source_name || prettifyId(src.source_id),
      description: (src.topic_tags || []).slice(0, 3).join(', '),
      category: src.category || 'other',
    }));

  return { published, internal };
}

function groupByCategory(items) {
  const groups = new Map();
  for (const item of items) {
    const cat = item.category || 'other';
    if (!groups.has(cat)) groups.set(cat, []);
    groups.get(cat).push(item);
  }
  return groups;
}

function renderPackCards(items, active, entitled) {
  return items.map(item => {
    const isEntitled = entitled ? entitled.has(item.id) : true;
    const isActive = active.has(item.id);
    const disabledAttr = isEntitled ? '' : 'disabled';
    const checkedAttr = isActive ? 'checked' : '';
    const lockedClass = isEntitled ? '' : 'pack-locked';
    return `
      <label class="pack-card ${lockedClass}" for="pack_${item.id}">
        <input type="checkbox" id="pack_${item.id}" class="pack-checkbox" data-pack-id="${item.id}"
               ${checkedAttr} ${disabledAttr}>
        <div class="pack-card-body">
          <span class="pack-label">${item.label}</span>
          <span class="pack-desc">${item.description}</span>
          <div class="pack-card-footer">
            ${item.source_count > 1 ? `<span class="pack-tag">${item.source_count} sources</span>` : ''}
            ${!isEntitled ? '<span class="pack-tag locked">upgrade</span>' : ''}
            ${item.pack_page ? `<a class="pack-more-info" href="${item.pack_page}" target="_blank" rel="noopener">More info</a>` : ''}
          </div>
        </div>
      </label>`;
  }).join('');
}

function renderCategoryGroups(groups, active, entitled) {
  return [...groups.entries()].map(([cat, items]) => `
    <div class="packs-bundle">
      <h3 class="packs-bundle-label">${prettifyId(cat)}</h3>
      <div class="packs-grid">
        ${renderPackCards(items, active, entitled)}
      </div>
    </div>
  `).join('');
}

function renderPacksSection(sources, entitledPackIds, isMaster) {
  const container = document.getElementById('packsList');
  if (!container) return;

  if (!sources || sources.length === 0) {
    container.innerHTML = '<p class="packs-loading">Loading catalog...</p>';
    return;
  }

  const { published, internal } = buildPacksFromSources(sources);
  const defaultActive = published.map(p => p.id);
  const active = loadActivePacks(defaultActive);
  const entitled = entitledPackIds; // null = all entitled (master)

  const publishedGroups = groupByCategory(published);
  let html = renderCategoryGroups(publishedGroups, active, entitled);

  if (isMaster && internal.length > 0) {
    const internalGroups = groupByCategory(internal);
    html += `
      <div class="packs-section-header packs-section-internal">
        <h3>Internal / Unreleased</h3>
        <span class="packs-section-note">Master account only</span>
      </div>
      ${renderCategoryGroups(internalGroups, active, null)}
    `;
  }

  container.innerHTML = html;
}

function collectPackSelection() {
  const active = new Set();
  document.querySelectorAll('.pack-checkbox:not([disabled]):checked').forEach(cb => {
    active.add(cb.dataset.packId);
  });
  return active;
}

async function initPacksSection(entitledPackIds, isMaster) {
  const container = document.getElementById('packsList');
  if (container) container.innerHTML = '<p class="packs-loading">Loading catalog...</p>';

  let sources = [];
  try {
    const data = await fetchMsgpack('/api/catalog/sources');
    sources = data.sources || [];
  } catch (_) {}

  renderPacksSection(sources, entitledPackIds, isMaster);

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
  const profile = getCurrentProfile() || {};
  const isMaster = profile.plan_id === 'master' || profile.is_admin === true;
  await initPacksSection(null, isMaster);
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
