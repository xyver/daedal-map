import { AuthManager, getCurrentProfile, getCurrentUser, isAuthenticated, onAuthChanged, updateUser } from './auth.js';
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
let releaseMarkerMap = new Map();
let currentPackLookup = new Map();

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

async function loadReleaseMarkers() {
  try {
    const data = await fetchMsgpack(apiUrl('/api/runtime/packs/release-markers'));
    const packs = Array.isArray(data?.packs) ? data.packs : [];
    releaseMarkerMap = new Map(packs.map(pack => [pack.pack_id, pack]));
  } catch (_) {
    releaseMarkerMap = new Map();
  }
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
    const marker = releaseMarkerMap.get(pack_id) || null;
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
      release_marker: marker,
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

function openPackStatusModal(item) {
  const modal = document.getElementById('packStatusModal');
  if (!modal || !item) return;

  const marker = item.release_marker || {};
  const title = document.getElementById('packStatusTitle');
  const subtitle = document.getElementById('packStatusSubtitle');
  const stage = document.getElementById('packStatusStage');
  const copy = document.getElementById('packStatusCopy');
  const tracker = document.getElementById('packStatusTracker');
  const next = document.getElementById('packStatusNext');
  const reasons = document.getElementById('packStatusReasons');
  const meta = document.getElementById('packStatusMeta');

  const stageClass = marker.already_published
    ? 'stage-published'
    : marker.ready_for_publish
      ? 'stage-ready'
      : 'stage-blocked';
  const stageLabel = marker.already_published
    ? 'Published'
    : marker.ready_for_publish
      ? 'Ready For Publish'
      : 'In Review';
  const stageCopy = marker.already_published
    ? 'This pack has been promoted into the published lane and is ready for normal runtime use.'
    : marker.ready_for_publish
      ? 'This pack is staged, has a QA suite, and is ready for publish promotion.'
      : 'This pack is still moving through staging and QA before it can be promoted.';

  const steps = [
    { label: 'Catalog', done: true, copy: 'Pack is defined in the runtime catalog.' },
    { label: 'Staging', done: Boolean(marker.staged_in_s3), copy: 'Files are present in the staging lane.' },
    { label: 'QA Suite', done: Boolean(marker.qa_suite_exists), copy: marker.qa_suite || 'Needs dedicated QA suite.' },
    { label: 'Ready', done: Boolean(marker.ready_for_publish || marker.already_published), copy: 'Release gates cleared for publish.' },
    { label: 'Published', done: Boolean(marker.already_published), copy: 'Pack is promoted into the published lane.' },
  ];
  const currentIndex = steps.findIndex(step => !step.done);

  if (title) title.textContent = `${item.label} Release Status`;
  if (subtitle) subtitle.textContent = item.description || 'Pack release progress through staging, QA, and publication.';
  if (stage) {
    stage.className = `pack-status-pill ${stageClass}`;
    stage.textContent = stageLabel;
  }
  if (copy) copy.textContent = stageCopy;
  if (tracker) {
    tracker.innerHTML = steps.map((step, index) => {
      const classes = ['pack-status-step'];
      if (step.done) classes.push('is-done');
      if (!step.done && index === currentIndex) classes.push('is-current');
      return `
        <div class="${classes.join(' ')}">
          <div class="pack-status-step-num">${index + 1}</div>
          <div class="pack-status-step-label">${step.label}</div>
          <div class="pack-status-step-copy">${step.copy}</div>
        </div>
      `;
    }).join('');
  }
  if (next) {
    next.textContent = marker.next_step || 'No next step recorded yet.';
  }
  if (reasons) {
    const items = (marker.reasons || []).length
      ? marker.reasons
      : ['No blocking reasons recorded.'];
    reasons.innerHTML = items.map(entry => `<li>${entry}</li>`).join('');
  }
  if (meta) {
    const metaItems = [
      `Pack ID: ${item.id}`,
      `Sources: ${item.source_count || 1}`,
      `QA suite: ${marker.qa_suite || 'not assigned'}`,
      `Staged in S3: ${marker.staged_in_s3 ? 'yes' : 'no'}`,
      `Published in S3: ${marker.published_to_s3 ? 'yes' : 'no'}`,
    ];
    if (item.pack_page) {
      const pageLabel = marker.already_published ? 'Public page' : 'Future public page';
      metaItems.push(`${pageLabel}: ${item.pack_page}`);
    }
    meta.innerHTML = metaItems.map(entry => `<li>${entry}</li>`).join('');
  }

  modal.classList.remove('hidden');
  modal.setAttribute('aria-hidden', 'false');
}

function closePackStatusModal() {
  const modal = document.getElementById('packStatusModal');
  if (!modal) return;
  modal.classList.add('hidden');
  modal.setAttribute('aria-hidden', 'true');
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
  const marker = item.release_marker || null;

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
      tagText: marker?.already_published ? 'active from published' : 'active from staging',
      detailText: marker?.already_published
        ? 'Available directly from the released cloud catalog. Local pack install is a separate step.'
        : 'Visible from the staging cloud lane for admin/review use. Local pack install is a separate step.',
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
      tagText: marker?.already_published ? 'published in cloud' : 'private in staging',
      detailText: marker?.already_published
        ? 'Released in the published cloud lane and ready for normal runtime use.'
        : 'Visible only in the staging lane until it is promoted to published.',
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
    const infoAction = isMaster
      ? `<button type="button" class="pack-more-info pack-more-info-btn" data-pack-info="${item.id}">More info</button>`
      : (item.pack_page ? `<a class="pack-more-info" href="${item.pack_page}" target="_blank" rel="noopener">More info</a>` : '');
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

  const { published, internal } = buildPacksFromSources(sources);
  currentPackLookup = new Map([...published, ...internal].map(item => [item.id, item]));
  const hasReleaseMarkers = releaseMarkerMap.size > 0;
  const cloudPrefix = String(runtimeState?.cloud_prefix || '').trim();
  const stagingPrefix = String(runtimeState?.staging_prefix || 'staging').trim();
  const publishedPrefix = String(runtimeState?.published_prefix || 'published').trim();
  const usingStagingLane = runtimeState?.runtime_mode === 'cloud' && cloudPrefix === stagingPrefix;
  const usingPublishedLane = runtimeState?.runtime_mode === 'cloud' && cloudPrefix === publishedPrefix;

  const releasePublished = hasReleaseMarkers
    ? published.filter(item => item.release_marker?.already_published)
    : (usingPublishedLane ? published : []);
  const releasePrivate = hasReleaseMarkers
    ? published.filter(item => !item.release_marker?.already_published)
    : (usingStagingLane ? published : []);
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

  if (releasePublished.length > 0) {
    const publishedGroups = groupByCategory(releasePublished);
    html += `
      <div class="packs-section-header">
        <h3>Published</h3>
        <span class="packs-section-note">Visible to normal users and released from the published lane.</span>
      </div>
      ${renderCategoryGroups(publishedGroups, active, entitled, installed, runtimeState, isMaster)}
    `;
  }

  if (isMaster && releasePrivate.length > 0) {
    const privateGroups = groupByCategory(releasePrivate);
    html += `
      <div class="packs-section-header packs-section-internal">
        <h3>Private / Admin Review</h3>
        <span class="packs-section-note">Visible only to admin/master users. These packs are still in staging or not yet released.</span>
      </div>
      ${renderCategoryGroups(privateGroups, active, null, installed, runtimeState, isMaster)}
    `;
  }

  if (isMaster && internal.length > 0) {
    const internalGroups = groupByCategory(internal);
    html += `
      <div class="packs-section-header packs-section-internal">
        <h3>Internal / Unreleased</h3>
        <span class="packs-section-note">Master account only</span>
      </div>
      ${renderCategoryGroups(internalGroups, active, null, installed, runtimeState, isMaster)}
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
      loadReleaseMarkers(),
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

    document.querySelectorAll('[data-pack-info]').forEach(btn => {
      btn.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        const item = currentPackLookup.get(btn.dataset.packInfo);
        openPackStatusModal(item);
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

  document.getElementById('profileEmailForm')?.addEventListener('submit', async (event) => {
    event.preventDefault();
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

  document.getElementById('profilePasswordForm')?.addEventListener('submit', async (event) => {
    event.preventDefault();
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
      const result = await postMsgpack(apiUrl('/api/admin/catalog/refresh'), {});
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

  document.getElementById('packStatusClose')?.addEventListener('click', closePackStatusModal);
  document.getElementById('packStatusBackdrop')?.addEventListener('click', closePackStatusModal);

  await renderPage();
}

init();
