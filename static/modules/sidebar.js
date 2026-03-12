/**
 * Sidebar - UI components for sidebar resizing and settings.
 * Combines ResizeManager, SidebarResizer, and SettingsManager.
 */

import { fetchMsgpack, postMsgpack } from './utils/fetch.js';

// ============================================================================
// RESIZE MANAGER - Draggable resize handles for sidebar sections
// ============================================================================

export const ResizeManager = {
  activeHandle: null,
  startY: 0,
  startHeights: {},

  /**
   * Initialize resize manager
   */
  init() {
    this.setupResizeHandle('resizeInput', 'chatMessages', 'chatInputArea', true);
    this.setupResizeHandle('resizeOrder', 'chatInputArea', 'orderPanel', false);
  },

  /**
   * Setup a resize handle
   * @param {string} handleId - ID of the resize handle element
   * @param {string} aboveId - ID of the element above the handle
   * @param {string} belowId - ID of the element below the handle
   * @param {boolean} aboveFlexible - If true, above element uses flex, else fixed height
   */
  setupResizeHandle(handleId, aboveId, belowId, aboveFlexible) {
    const handle = document.getElementById(handleId);
    const above = document.getElementById(aboveId);
    const below = document.getElementById(belowId);

    if (!handle || !above || !below) return;

    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      this.activeHandle = { handle, above, below, aboveFlexible };
      this.startY = e.clientY;
      this.startHeights = {
        above: above.offsetHeight,
        below: below.offsetHeight
      };
      handle.classList.add('active');
      document.body.style.cursor = 'ns-resize';
      document.body.style.userSelect = 'none';
    });

    // Touch support
    handle.addEventListener('touchstart', (e) => {
      e.preventDefault();
      const touch = e.touches[0];
      this.activeHandle = { handle, above, below, aboveFlexible };
      this.startY = touch.clientY;
      this.startHeights = {
        above: above.offsetHeight,
        below: below.offsetHeight
      };
      handle.classList.add('active');
    }, { passive: false });

    // Global mouse/touch move and up handlers
    document.addEventListener('mousemove', (e) => this.handleMove(e.clientY));
    document.addEventListener('mouseup', () => this.handleEnd());
    document.addEventListener('touchmove', (e) => {
      if (this.activeHandle) {
        e.preventDefault();
        this.handleMove(e.touches[0].clientY);
      }
    }, { passive: false });
    document.addEventListener('touchend', () => this.handleEnd());
  },

  /**
   * Handle drag movement
   */
  handleMove(clientY) {
    if (!this.activeHandle) return;

    const { above, below, aboveFlexible } = this.activeHandle;
    const deltaY = clientY - this.startY;

    // Calculate new heights
    let newAboveHeight = this.startHeights.above + deltaY;
    let newBelowHeight = this.startHeights.below - deltaY;

    // Get min heights from CSS
    const aboveMinHeight = parseInt(getComputedStyle(above).minHeight) || 60;
    const belowMinHeight = parseInt(getComputedStyle(below).minHeight) || 60;

    // Enforce minimums
    if (newAboveHeight < aboveMinHeight) {
      newAboveHeight = aboveMinHeight;
      newBelowHeight = this.startHeights.above + this.startHeights.below - aboveMinHeight;
    }
    if (newBelowHeight < belowMinHeight) {
      newBelowHeight = belowMinHeight;
      newAboveHeight = this.startHeights.above + this.startHeights.below - belowMinHeight;
    }

    // Apply heights
    if (aboveFlexible) {
      // For chat messages, set flex-basis instead of height
      above.style.flex = `0 0 ${newAboveHeight}px`;
    } else {
      above.style.height = `${newAboveHeight}px`;
    }
    below.style.height = `${newBelowHeight}px`;
  },

  /**
   * Handle drag end
   */
  handleEnd() {
    if (!this.activeHandle) return;

    this.activeHandle.handle.classList.remove('active');
    this.activeHandle = null;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }
};

// ============================================================================
// SIDEBAR RESIZER - Handles sidebar width resizing
// ============================================================================

export const SidebarResizer = {
  isResizing: false,
  startX: 0,
  startWidth: 0,
  minWidth: 300,
  maxWidth: 800,

  /**
   * Initialize sidebar resizer
   */
  init() {
    const handle = document.getElementById('sidebarResizeHandle');
    const sidebar = document.getElementById('sidebar');

    if (!handle || !sidebar) return;

    // Mouse events
    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      this.startResize(e.clientX, sidebar, handle);
    });

    // Touch events
    handle.addEventListener('touchstart', (e) => {
      e.preventDefault();
      const touch = e.touches[0];
      this.startResize(touch.clientX, sidebar, handle);
    }, { passive: false });

    // Global move and end handlers
    document.addEventListener('mousemove', (e) => this.handleMove(e.clientX));
    document.addEventListener('mouseup', () => this.handleEnd());
    document.addEventListener('touchmove', (e) => {
      if (this.isResizing) {
        e.preventDefault();
        this.handleMove(e.touches[0].clientX);
      }
    }, { passive: false });
    document.addEventListener('touchend', () => this.handleEnd());

    // Store sidebar reference
    this.sidebar = sidebar;
    this.handle = handle;
  },

  /**
   * Start resizing
   */
  startResize(clientX, sidebar, handle) {
    this.isResizing = true;
    this.startX = clientX;
    this.startWidth = sidebar.offsetWidth;
    handle.classList.add('active');
    document.body.style.cursor = 'ew-resize';
    document.body.style.userSelect = 'none';
  },

  /**
   * Handle drag movement
   */
  handleMove(clientX) {
    if (!this.isResizing) return;

    const deltaX = clientX - this.startX;
    let newWidth = this.startWidth + deltaX;

    // Enforce min/max
    newWidth = Math.max(this.minWidth, Math.min(this.maxWidth, newWidth));

    // Apply new width
    this.sidebar.style.width = newWidth + 'px';
  },

  /**
   * Handle drag end
   */
  handleEnd() {
    if (!this.isResizing) return;

    this.isResizing = false;
    this.handle.classList.remove('active');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  }
};

// ============================================================================
// SETTINGS MANAGER - Settings view and configuration
// ============================================================================

export const SettingsManager = {
  elements: {},
  isVisible: false,

  /**
   * Initialize settings manager
   */
  init() {
    this.elements = {
      chatContainer: document.getElementById('chatContainer'),
      settingsView: document.getElementById('settingsView'),
      settingsLink: document.getElementById('settingsLink'),
      backToChat: document.getElementById('backToChat'),
      backupPathInput: document.getElementById('backupPathInput'),
      saveBtn: document.getElementById('saveSettingsBtn'),
      initFoldersBtn: document.getElementById('initFoldersBtn'),
      status: document.getElementById('settingsStatus'),
      currentConfig: document.getElementById('currentConfig'),
      sidebarFooter: document.getElementById('sidebarFooter'),
      timezoneSelect: document.getElementById('timezoneSelect')
    };

    if (this.elements.settingsLink) {
      this.elements.settingsLink.setAttribute('href', '/settings');
    }

    this.setupEventListeners();
    this.loadSettings();
    this.loadTimezoneSettings();
  },

  /**
   * Setup event listeners
   */
  setupEventListeners() {
    const { settingsLink, backToChat, saveBtn, initFoldersBtn } = this.elements;

    // Toggle to settings view
    settingsLink?.addEventListener('click', () => {});

    // Back to chat
    backToChat?.addEventListener('click', (e) => {
      e.preventDefault();
      this.hideSettings();
    });

    // Save settings
    saveBtn?.addEventListener('click', () => {
      this.saveSettings();
    });

    // Initialize folders
    initFoldersBtn?.addEventListener('click', () => {
      this.initializeFolders();
    });
  },

  /**
   * Show settings view
   */
  showSettings() {
    const { chatContainer, settingsView, sidebarFooter } = this.elements;
    chatContainer.classList.add('hidden');
    settingsView.classList.add('active');
    sidebarFooter.style.display = 'none';
    this.isVisible = true;
    this.loadSettings();
  },

  /**
   * Hide settings view
   */
  hideSettings() {
    const { chatContainer, settingsView, sidebarFooter } = this.elements;
    chatContainer.classList.remove('hidden');
    settingsView.classList.remove('active');
    sidebarFooter.style.display = 'block';
    this.isVisible = false;
  },

  /**
   * Load current settings from server
   */
  async loadSettings() {
    try {
      const settings = await fetchMsgpack('/api/settings');
      this.elements.backupPathInput.value = settings.backup_path || '';
      this.updateConfigDisplay(settings);
    } catch (error) {
      console.log('Could not load settings:', error.message);
      this.updateConfigDisplay({ error: 'Could not connect to server' });
    }
  },

  /**
   * Save settings to server
   */
  async saveSettings() {
    const backupPath = this.elements.backupPathInput.value.trim();

    try {
      const result = await postMsgpack('/api/settings', { backup_path: backupPath });
      if (result.success) {
        this.showStatus('Settings saved successfully!', 'success');
        this.updateConfigDisplay(result.settings || { backup_path: backupPath });
      } else {
        this.showStatus(result.error || 'Failed to save settings', 'error');
      }
    } catch (error) {
      this.showStatus('Error: ' + error.message, 'error');
    }
  },

  /**
   * Initialize folder structure at backup path
   */
  async initializeFolders() {
    const backupPath = this.elements.backupPathInput.value.trim();

    if (!backupPath) {
      this.showStatus('Please enter a backup path first', 'error');
      return;
    }

    try {
      const result = await postMsgpack('/api/settings/init-folders', { backup_path: backupPath });
      if (result.success) {
        this.showStatus('Folders initialized: ' + result.folders.join(', '), 'success');
        this.loadSettings();
      } else {
        this.showStatus(result.error || 'Failed to initialize folders', 'error');
      }
    } catch (error) {
      this.showStatus('Error: ' + error.message, 'error');
    }
  },

  /**
   * Show status message
   */
  showStatus(message, type) {
    const status = this.elements.status;
    status.textContent = message;
    status.className = 'settings-status ' + type;

    // Auto-hide after 5 seconds
    setTimeout(() => {
      status.className = 'settings-status';
    }, 5000);
  },

  /**
   * Update the current configuration display
   */
  updateConfigDisplay(settings) {
    const { currentConfig } = this.elements;

    if (settings.error) {
      currentConfig.innerHTML = `<span style="color: #dc3545;">${settings.error}</span>`;
      return;
    }

    let html = '';

    if (settings.backup_path) {
      html += `<strong>Backup Path:</strong> ${settings.backup_path}<br>`;

      if (settings.folders_exist) {
        html += '<br><strong>Folder Status:</strong><br>';
        for (const [folder, exists] of Object.entries(settings.folders_exist)) {
          const icon = exists ? '[OK]' : '[Missing]';
          const color = exists ? '#28a745' : '#dc3545';
          html += `<span style="color: ${color};">${icon}</span> ${folder}<br>`;
        }
      }
    } else {
      html = '<em>No backup path configured</em>';
    }

    currentConfig.innerHTML = html;
  },

  /**
   * Load timezone setting from localStorage and set up listener
   */
  loadTimezoneSettings() {
    const { timezoneSelect } = this.elements;
    if (!timezoneSelect) return;

    // Load saved timezone
    try {
      const saved = localStorage.getItem('liveTimezone');
      if (saved) {
        timezoneSelect.value = saved;
      }
    } catch (e) {
      // localStorage not available
    }

    // Listen for changes
    timezoneSelect.addEventListener('change', (e) => {
      const tz = e.target.value;
      try {
        localStorage.setItem('liveTimezone', tz);
      } catch (e) {
        // localStorage not available
      }

      // Update TimeSlider if available
      if (window.TimeSlider) {
        window.TimeSlider.setLiveTimezone(tz);
      }
    });
  }
};
