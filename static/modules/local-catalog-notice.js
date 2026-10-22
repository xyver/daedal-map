/** First-open guidance and review of source folders placed in local DATA_ROOT. */

let checking = false;
let localUnavailable = false;

async function checkLocalCatalog() {
  // Show guidance only over the map, never over a settings handoff page.
  if (!document.getElementById('sidebarModeTitle')) return;
  if (checking || localUnavailable || document.querySelector('dialog[aria-label="Local data catalog"]')) return;
  checking = true;
  let payload;
  try {
    const response = await fetch('/api/local-wrapper/catalog/discovered', { credentials: 'same-origin' });
    if (!response.ok) {
      if (response.status === 403 || response.status === 404) localUnavailable = true;
      return;
    }
    payload = await response.json();
  } catch (_) {
    return;
  } finally {
    checking = false;
  }
  const sources = Array.isArray(payload.sources) ? payload.sources : [];
  const archives = Array.isArray(payload.archives) ? payload.archives : [];
  const hasData = sources.length || archives.length;
  if (!hasData && !payload.first_open) return;
  const signature = hasData
    ? [...sources.map(source => `${source.pack_id}:${source.source_id}`),
       ...archives.map(archive => `${archive.path}:${archive.version}`)].join('|')
    : 'first-open';
  try {
    if (sessionStorage.getItem('daedal-local-catalog-dismissed') === signature) return;
  } catch (_) { /* Session storage can be disabled. */ }

  const dialog = document.createElement('dialog');
  dialog.setAttribute('aria-label', 'Local data catalog');
  dialog.style.cssText = 'max-width:440px;width:calc(100% - 32px);padding:24px;border:1px solid #4b6778;border-radius:14px;background:#10202b;color:#f4f8fa;box-shadow:0 20px 70px #0009;line-height:1.5';
  const title = document.createElement('h2');
  title.textContent = hasData ? 'New local data detected' : 'Your local map is ready';
  title.style.margin = '0 0 8px';
  dialog.append(title);
  const description = document.createElement('p');
  description.textContent = hasData
    ? 'These sources or pack ZIPs are in your data folder but are not in the local catalog. Add them to make them available in the map?'
    : 'Visit the Launcher Store in the Data tab to download your first data pack.';
  dialog.append(description);
  if (hasData) {
    const list = document.createElement('ul');
    for (const source of sources) {
      const item = document.createElement('li');
      item.textContent = `${source.source_name || source.source_id} (${source.pack_id})`;
      list.append(item);
    }
    for (const archive of archives) {
      const item = document.createElement('li');
      item.textContent = `${archive.filename} (${archive.pack_id}, ${archive.version})`;
      list.append(item);
    }
    dialog.append(list);
  }
  const status = document.createElement('p');
  status.setAttribute('role', 'status');
  dialog.append(status);
  const actions = document.createElement('div');
  actions.style.cssText = 'display:flex;gap:10px;flex-wrap:wrap;margin-top:16px';
  const primary = document.createElement('button');
  primary.type = 'button';
  primary.textContent = hasData ? 'Add to catalog' : 'Visit Launcher Store';
  primary.style.cssText = 'padding:9px 16px;border:0;border-radius:8px;background:#27c9da;color:#071c25;cursor:pointer;font-weight:700';
  const later = document.createElement('button');
  later.type = 'button';
  later.textContent = 'Later';
  later.style.cssText = 'padding:9px 16px;border:1px solid #7894a4;border-radius:8px;background:transparent;color:#f4f8fa;cursor:pointer';
  const dismiss = () => {
    try { sessionStorage.setItem('daedal-local-catalog-dismissed', signature); } catch (_) { /* Continue. */ }
    dialog.close();
    dialog.remove();
  };
  later.addEventListener('click', dismiss);
  primary.addEventListener('click', async () => {
    if (!hasData) {
      primary.disabled = true;
      status.textContent = 'Opening the launcher Data tab...';
      try {
        const response = await fetch('/settings?tab=library', { credentials: 'same-origin' });
        if (!response.ok) throw new Error('Could not reach the launcher. Open its Data tab to download packs.');
        dismiss();
      } catch (error) {
        status.textContent = error.message || 'Open the launcher Data tab to download packs.';
        primary.disabled = false;
      }
      return;
    }
    primary.disabled = true;
    later.disabled = true;
    status.textContent = 'Adding local data...';
    try {
      for (const archive of archives) {
        const response = await fetch('/api/local-wrapper/catalog/add-archive', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: archive.path }),
        });
        const result = await response.json();
        if (!response.ok || result.ok === false) throw new Error(result.error || `Could not add ${archive.filename}`);
      }
      const refreshed = await fetch('/api/local-wrapper/catalog/discovered', { credentials: 'same-origin' });
      if (!refreshed.ok) throw new Error('Could not refresh local data discovery.');
      const remaining = (await refreshed.json()).sources || [];
      for (const packId of [...new Set(remaining.map(source => source.pack_id))]) {
        const response = await fetch('/api/local-wrapper/catalog/add', {
          method: 'POST', credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pack_id: packId }),
        });
        const result = await response.json();
        if (!response.ok || result.ok === false) throw new Error(result.error || `Could not add ${packId}`);
      }
      window.location.reload();
    } catch (error) {
      status.textContent = error.message || 'Could not add local data.';
      primary.disabled = false;
      later.disabled = false;
    }
  });
  actions.append(primary, later);
  dialog.append(actions);
  document.body.append(dialog);
  dialog.showModal();
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', checkLocalCatalog, { once: true });
} else {
  checkLocalCatalog();
}
window.addEventListener('focus', checkLocalCatalog);
