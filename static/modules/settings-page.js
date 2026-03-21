import { AuthManager, getCurrentProfile, onAuthChanged } from './auth.js';
import { fetchMsgpack, postMsgpack } from './utils/fetch.js';

// ============================================================================
// PACKS - driven entirely by catalog API (pack_id is the publish gate)
// ============================================================================

const ACRONYMS = new Set(['sdg', 'un', 'fx', 'co2', 'imf', 'bop', 'us', 'usa', 'epa', 'cia', 'nasa', 'who', 'bom', 'zcta', 'nrcan', 'abs']);

function prettifyId(id) {
  return id.replace(/_/g, ' ').split(' ').map(word =>
    ACRONYMS.has(word.toLowerCase()) ? word.toUpperCase() : word.charAt(0).toUpperCase() + word.slice(1)
  ).join(' ');
}

function getPackSiteBase() {
  const configured = String(window.__SITE_URL__ || '').replace(/\/$/, '');
  if (!configured) return 'https://daedalmap.com';
  try {
    const url = new URL(configured);
    const host = url.hostname.toLowerCase();
    if (host === 'localhost' || host === '127.0.0.1') {
      return 'https://daedalmap.com';
    }
    return configured;
  } catch (_) {
    return 'https://daedalmap.com';
  }
}

let runtimePackState = null;

function apiUrl(path) {
  if (!path) return path;
  if (/^https?:\/\//i.test(path)) return path;
  const base = String(window.__API_BASE__ || '').replace(/\/$/, '');
  return base ? `${base}${path}` : path;
}

async function loadRuntimePackState() {
  try {
    const state = await fetchMsgpack(apiUrl('/api/runtime/packs/state'));
    runtimePackState = state || null;
    return runtimePackState;
  } catch (_) {
    runtimePackState = null;
    return null;
  }
}

async function saveRuntimePackState(activeSet, catalogMode = 'managed_packs') {
  const result = await postMsgpack(apiUrl('/api/runtime/packs/active'), {
    active_pack_ids: [...activeSet],
    catalog_mode: catalogMode,
  });
  runtimePackState = result?.state ? {
    ...(runtimePackState || {}),
    ...result.state,
  } : runtimePackState;
  return result;
}

async function installRuntimePack(packId, activate = false) {
  const result = await postMsgpack(apiUrl('/api/runtime/packs/install-local'), {
    pack_id: packId,
    activate,
    replace_existing: true,
  });
  runtimePackState = result?.state ? {
    ...(runtimePackState || {}),
    ...result.state,
  } : runtimePackState;
  return result;
}

async function uninstallRuntimePack(packId) {
  const result = await postMsgpack(apiUrl('/api/runtime/packs/uninstall'), {
    pack_id: packId,
  });
  runtimePackState = result?.state ? {
    ...(runtimePackState || {}),
    ...result.state,
  } : runtimePackState;
  return result;
}

function buildPacksFromSources(sources) {
  const siteOrigin = getPackSiteBase();
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
      pack_page: `${siteOrigin}/packs/${pack_id}`,
    };
  });
  return { published };
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

function describePackState(item, runtimeState, active, installed) {
  const isInstalled = installed.has(item.id);
  const isActive = active.has(item.id);
  const runtimeMode = runtimeState?.runtime_mode || 'local';
  const catalogMode = runtimeState?.catalog_mode || 'unmanaged_data_root';

  if (isInstalled) {
    return {
      tagClass: 'pack-tag',
      tagText: 'installed locally',
      detailText: isActive ? 'Using local installed copy in the active runtime catalog.' : 'Installed locally and ready to activate.',
      actionLabel: 'Remove',
      actionClass: 'pack-remove-btn',
      actionDisabled: false,
      actionMode: 'remove',
      checkboxEnabled: true,
    };
  }

  if (runtimeMode === 'cloud' && catalogMode === 'unmanaged_data_root') {
    return {
      tagClass: 'pack-tag',
      tagText: 'active from cloud',
      detailText: 'Available directly from the current cloud catalog. Local pack install is a separate step.',
      actionLabel: 'Install locally soon',
      actionClass: 'pack-install-btn',
      actionDisabled: true,
      actionMode: 'install',
      checkboxEnabled: false,
    };
  }

  if (runtimeMode === 'cloud') {
    return {
      tagClass: 'pack-tag',
      tagText: 'available in cloud',
      detailText: 'Visible in the current cloud lane. Published versus staging control now lives on daedalmap.com.',
      actionLabel: 'Install locally soon',
      actionClass: 'pack-install-btn',
      actionDisabled: true,
      actionMode: 'install',
      checkboxEnabled: false,
    };
  }

  return {
    tagClass: 'pack-tag locked',
    tagText: 'ready for local install',
    detailText: 'Available from the current local data tree and can be packaged into local managed packs.',
    actionLabel: 'Install',
    actionClass: 'pack-install-btn',
    actionDisabled: false,
    actionMode: 'install',
    checkboxEnabled: false,
  };
}

function renderPackCards(items, active, entitled, installed, runtimeState, isMaster) {
  return items.map(item => {
    const isEntitled = entitled ? entitled.has(item.id) : true;
    const packState = describePackState(item, runtimeState, active, installed);
    const isActive = installed.has(item.id) && active.has(item.id);
    const disabledAttr = (isEntitled && packState.checkboxEnabled) ? '' : 'disabled';
    const checkedAttr = isActive && packState.checkboxEnabled ? 'checked' : '';
    const lockedClass = isEntitled ? '' : 'pack-locked';
    const installStatusTag = `<span class="${packState.tagClass}">${packState.tagText}</span>`;
    const actionDisabledAttr = packState.actionDisabled || !isEntitled ? 'disabled' : '';
    const actionButton = `<button type="button" class="pack-manage-btn ${packState.actionClass}" data-pack-id="${item.id}" ${actionDisabledAttr}>${packState.actionLabel}</button>`;
    const infoAction = item.pack_page ? `<a class="pack-more-info" href="${item.pack_page}" target="_blank" rel="noopener">More info</a>` : '';
    return `
      <label class="pack-card ${lockedClass}" for="pack_${item.id}">
        <input type="checkbox" id="pack_${item.id}" class="pack-checkbox" data-pack-id="${item.id}"
               ${checkedAttr} ${disabledAttr}>
        <div class="pack-card-body">
          <span class="pack-label">${item.label}</span>
          <span class="pack-desc">${item.description}</span>
          <span class="pack-desc">${packState.detailText}</span>
          <div class="pack-card-footer">
            ${item.source_count > 1 ? `<span class="pack-tag">${item.source_count} sources</span>` : ''}
            ${installStatusTag}
            ${!isEntitled ? '<span class="pack-tag locked">upgrade</span>' : ''}
            ${infoAction}
            ${actionButton}
          </div>
        </div>
      </label>`;
  }).join('');
}

function renderCategoryGroups(groups, active, entitled, installed, runtimeState, isMaster) {
  return [...groups.entries()].map(([cat, items]) => `
    <div class="packs-bundle">
      <h3 class="packs-bundle-label">${prettifyId(cat)}</h3>
      <div class="packs-grid">
        ${renderPackCards(items, active, entitled, installed, runtimeState, isMaster)}
      </div>
    </div>
  `).join('');
}

function renderPacksSection(sources, entitledPackIds, isMaster, runtimeState) {
  const container = document.getElementById('packsList');
  if (!container) return;

  const { published } = buildPacksFromSources(sources || []);
  const defaultActive = published.map(p => p.id);
  const activeIds = runtimeState?.active_pack_ids?.length ? runtimeState.active_pack_ids : defaultActive;
  const active = new Set(activeIds);
  const installed = new Set(runtimeState?.installed_pack_ids || []);
  const entitled = entitledPackIds; // null = all entitled (master)

  let html = '';
  if (runtimeState) {
    const installedCount = runtimeState.installed_pack_ids?.length || 0;
    const activeCount = runtimeState.active_pack_ids?.length || active.size;
    html += `
      <div class="packs-section-header">
        <span class="packs-section-note">
          Install mode: ${runtimeState.install_mode || 'local'}.
          Runtime mode: ${runtimeState.runtime_mode || 'local'}.
          Catalog mode: ${runtimeState.catalog_mode || 'unmanaged_data_root'}.
          Installed packs: ${installedCount}. Active packs: ${activeCount}.
        </span>
      </div>
    `;
    if (runtimeState.runtime_mode === 'cloud') {
      html += `
        <div class="packs-section-header">
          <span class="packs-section-note">
            Cloud-visible packs are already available from the current cloud catalog. Local installs, when used, go to ${runtimeState.packs_root}.
          </span>
        </div>
      `;
    }
  }

  if (published.length > 0) {
    const publishedGroups = groupByCategory(published);
    html += `
      <div class="packs-section-header">
        <h3>Available Runtime Packs</h3>
        <span class="packs-section-note">Packs currently visible to this runtime from the active catalog lane.</span>
      </div>
      ${renderCategoryGroups(publishedGroups, active, entitled, installed, runtimeState, isMaster)}
    `;
  }

  if (!html.trim()) {
    const cloudPrefix = String(runtimeState?.cloud_prefix || '').trim();
    const laneLabel = cloudPrefix || 'current runtime lane';
    html = `
      <div class="packs-section-header">
        <h3>No Packs Available</h3>
        <span class="packs-section-note">
          No packs are available from ${laneLabel}. This is expected when the published lane is empty.
        </span>
      </div>
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

  const applyBtn = document.getElementById('packsApplyBtn');
  const status = document.getElementById('packsSaveStatus');

  async function refreshPacksSection() {
    let sources = [];
    const [runtimeState] = await Promise.all([
      loadRuntimePackState(),
    ]);
    let catalogError = null;
    try {
      const data = await fetchMsgpack(apiUrl('/api/catalog/sources'));
      sources = data.sources || [];
    } catch (error) {
      catalogError = error;
    }
    if (catalogError && (!sources || sources.length === 0) && container) {
      container.innerHTML = `<p class="packs-loading">Could not load catalog: ${catalogError.message || 'request failed'}.</p>`;
      return;
    }
    renderPacksSection(sources, entitledPackIds, isMaster, runtimeState);

    document.querySelectorAll('.pack-install-btn').forEach(btn => {
      btn.addEventListener('click', async (event) => {
        event.preventDefault();
        event.stopPropagation();
        const packId = btn.dataset.packId;
        btn.disabled = true;
        try {
          await installRuntimePack(packId, false);
          if (status) status.textContent = `Installed ${packId}.`;
          await refreshPacksSection();
        } catch (error) {
          if (status) status.textContent = error?.message || `Could not install ${packId}.`;
        } finally {
          window.setTimeout(() => { if (status) status.textContent = ''; }, 4000);
        }
      });
    });

    document.querySelectorAll('.pack-remove-btn').forEach(btn => {
      btn.addEventListener('click', async (event) => {
        event.preventDefault();
        event.stopPropagation();
        const packId = btn.dataset.packId;
        btn.disabled = true;
        try {
          await uninstallRuntimePack(packId);
          if (status) status.textContent = `Removed ${packId}.`;
          await refreshPacksSection();
        } catch (error) {
          if (status) status.textContent = error?.message || `Could not remove ${packId}.`;
        } finally {
          window.setTimeout(() => { if (status) status.textContent = ''; }, 4000);
        }
      });
    });

  }

  await refreshPacksSection();

  if (!applyBtn) return;

  applyBtn.addEventListener('click', async () => {
    const selection = collectPackSelection();
    try {
      await saveRuntimePackState(selection, 'managed_packs');
      if (status) {
        status.textContent = 'Runtime catalog updated.';
        window.setTimeout(() => { status.textContent = ''; }, 3000);
      }
      await refreshPacksSection();
    } catch (error) {
      if (status) {
        status.textContent = error?.message || 'Could not update runtime catalog.';
        window.setTimeout(() => { status.textContent = ''; }, 4000);
      }
    }
  });
}

const els = {
  unlocked: document.getElementById('settingsUnlocked'),
  loginBtn: document.getElementById('settingsLoginBtn'),
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
    const settings = await fetchMsgpack(apiUrl('/api/settings'));
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
    const result = await postMsgpack(apiUrl('/api/settings'), { backup_path: backupPath });
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
    const result = await postMsgpack(apiUrl('/api/settings/init-folders'), { backup_path: backupPath });
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

function initAdminSection() {
  return;
}

async function renderPage() {
  initTabs();
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
    const siteBase = String(window.__SITE_URL__ || 'https://daedalmap.com').replace(/\/$/, '');
    window.location.href = `${siteBase}/settings/account?return=${encodeURIComponent(window.location.href)}`;
  });
  els.saveBtn?.addEventListener('click', saveSettings);
  els.initFoldersBtn?.addEventListener('click', initializeFolders);

  onAuthChanged(() => {
    window.location.reload();
  });

  await renderPage();
}

init();
