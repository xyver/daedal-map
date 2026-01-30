/**
 * Popup Builder - Generate popup HTML content for map features.
 * Handles formatting and display of feature properties.
 */

import { fetchMsgpack } from './utils/fetch.js';

// Dependencies set via setDependencies to avoid circular imports
let App = null;
let ChoroplethManager = null;

export function setDependencies(deps) {
  App = deps.App;
  ChoroplethManager = deps.ChoroplethManager;
}

// ============================================================================
// POPUP BUILDER - Generate popup HTML content
// ============================================================================

export const PopupBuilder = {
  // Units to hide in popup display (redundant/obvious)
  hideUnits: ['count', 'number', 'people', 'persons', 'units'],

  // Admin level data loaded from server (populated by loadAdminLevels)
  adminLevels: null,
  adminLevelsLoading: false,

  /**
   * Load admin level names from server reference data
   */
  async loadAdminLevels() {
    if (this.adminLevels || this.adminLevelsLoading) return;
    this.adminLevelsLoading = true;

    try {
      this.adminLevels = await fetchMsgpack('/reference/admin-levels');
    } catch (e) {
      console.warn('Failed to load admin levels:', e);
    }
    this.adminLevelsLoading = false;
  },

  /**
   * Get singular suffix for an admin level (e.g., "counties" -> "County")
   * @param {string} iso3 - Country ISO3 code
   * @param {number} level - Admin level number
   * @returns {string|null} Singular suffix or null
   */
  getAdminSuffix(iso3, level) {
    if (!this.adminLevels) return null;

    const countryLevels = this.adminLevels[iso3] || this.adminLevels["_default"];
    if (!countryLevels) return null;

    const levelNames = countryLevels[String(level)];
    if (!levelNames || !levelNames[0]) return null;

    // Get first name (display name) and convert to singular title case
    let name = levelNames[0];

    // Common plural -> singular conversions
    if (name.endsWith('ies')) {
      name = name.slice(0, -3) + 'y';  // counties -> county
    } else if (name.endsWith('es') && !name.endsWith('ches') && !name.endsWith('shes')) {
      name = name.slice(0, -2);  // places -> place (but not matches)
    } else if (name.endsWith('s') && !name.endsWith('ss')) {
      name = name.slice(0, -1);  // states -> state
    }

    // Title case
    return name.charAt(0).toUpperCase() + name.slice(1);
  },

  // Fields to skip in popup display (technical/internal fields)
  skipFields: [
    // Identity fields
    'geometry', 'coordinates', 'loc_id', 'parent_id', 'level', 'code', 'abbrev',
    'name', 'Name', 'Location', 'name_long', 'name_sort', 'formal_en', 'name_local',
    // Country/region name variants
    'country', 'country_name', 'country_code', 'iso_code', 'iso_a3', 'iso_3166_2',
    'Admin Country Name', 'Sov Country Name', 'Admin Country Abbr', 'Sov Country Abbr',
    'stusab', 'state', 'postal', 'continent',
    // Admin/type fields
    'Admin Type', 'type', 'admin_level',
    // Geometry metadata
    'centroid_lon', 'centroid_lat', 'Longitude', 'Latitude',
    'bbox_min_lon', 'bbox_min_lat', 'bbox_max_lon', 'bbox_max_lat',
    'has_polygon', 'timezone',
    // Children counts (shown via enriched info)
    'children_count', 'children_by_level', 'descendants_count', 'descendants_by_level',
    // Categorization
    'population_year', 'gdp_year', 'economy type', 'income_group',
    'UN Region', 'subregion', 'region_wb',
    // Year shown separately
    'data_year',
    // Internal timestamp fields (from time slider)
    'Time', 'time', 'Data Time', 'data_time'
  ],

  /**
   * Build popup HTML from feature properties
   * @param {Object} properties - Feature properties
   * @param {Object} sourceData - Optional source metadata (from chat query)
   * @param {Object} locationInfo - Optional enriched location info from API
   * @returns {string} HTML content
   */
  build(properties, sourceData = null, locationInfo = null) {
    const lines = [];

    // Title - add admin level suffix if appropriate (e.g., "Clackamas" -> "Clackamas County")
    let name = properties.name || properties.country_name ||
               properties.country || properties.Name || 'Unknown';
    const stateAbbr = properties.stusab || properties.abbrev || '';

    // Add admin level suffix if name doesn't already include it
    const adminLevel = properties.admin_level;
    const locId = properties.loc_id || '';
    const iso3 = locId.split('-')[0] || '';
    if (adminLevel && iso3) {
      const suffix = this.getAdminSuffix(iso3, adminLevel);
      if (suffix && !name.toLowerCase().includes(suffix.toLowerCase())) {
        name = `${name} ${suffix}`;
      }
    }

    lines.push(`<strong>${name}${stateAbbr ? ', ' + stateAbbr : ''}</strong>`);

    // Check if we have actual data fields (from a chat query)
    // Pass fromQuery=true when sourceData is present to include ALL numeric fields
    const hasSourceData = sourceData !== null;
    const dataFields = this.getRelevantFields(properties, hasSourceData);
    const hasQueryData = dataFields.length > 0;

    // Debug mode: show coverage info
    if (App?.debugMode && properties.coverage !== undefined) {
      lines.push(this.buildHierarchyInfo(properties));
    } else if (hasQueryData) {
      // DATA MODE: Show data fields from chat query
      const displayYear = properties.data_year || properties.year || '';
      const yearSuffix = displayYear ? ` (${displayYear})` : '';

      // Determine if we should use tabbed mode (click/locked popup with multiple categories)
      const useTabbed = locationInfo && sourceData && sourceData.metric_sources
        && sourceData.sources && sourceData.sources.length > 1 && dataFields.length > 5;

      if (useTabbed) {
        // TABBED MODE: Group metrics by source category
        lines.push(this.buildTabbedContent(dataFields, properties, sourceData, yearSuffix));
      } else if (!locationInfo && dataFields.length > 3) {
        // HOVER MODE (condensed): Show top 3 fields only
        for (const key of dataFields.slice(0, 3)) {
          const value = properties[key];
          if (value == null || value === '') continue;
          const fieldName = this.cleanFieldName(key);
          const formattedValue = this.formatValue(key, value);
          lines.push(`${fieldName}: ${formattedValue}${yearSuffix}`);
        }
        if (dataFields.length > 3) {
          lines.push(`<span style="font-size: 10px; color: #999;">+${dataFields.length - 3} more (click to expand)</span>`);
        }
      } else {
        // FLAT MODE: Show all fields (<=5 or single category)
        // If available_metrics provided, show all of them (with N/A for missing)
        const metricsToShow = sourceData?.available_metrics || dataFields;
        const activeMetric = ChoroplethManager?.metric;
        for (const key of metricsToShow.slice(0, 10)) {
          const value = properties[key];
          const fieldName = this.cleanFieldName(key);
          const isActive = activeMetric && key === activeMetric;
          if (value == null || value === '') {
            // Show N/A for missing metrics
            const style = isActive ? 'color: #999; font-weight: 600;' : 'color: #999;';
            lines.push(`<span style="${style}">${fieldName}: N/A</span>`);
          } else {
            const formattedValue = this.formatValue(key, value);
            if (isActive) {
              lines.push(`<span style="font-weight: bold;">${fieldName}: ${formattedValue}${yearSuffix}</span>`);
            } else {
              lines.push(`${fieldName}: ${formattedValue}${yearSuffix}`);
            }
          }
        }
      }

      // Source info (from chat query) - compact with clickable links
      if (sourceData && !useTabbed) {
        if (sourceData.sources && sourceData.sources.length > 0) {
          const sourceLinks = sourceData.sources.slice(0, 2).map(s => {
            if (s.url && s.url !== 'Unknown') {
              return `<a href="${s.url}" target="_blank" style="color: #5dade2;">${s.name}</a>`;
            }
            return s.name;
          }).join(', ');
          lines.push(`<span style="font-size: 10px; color: #888;">Source: ${sourceLinks}</span>`);
        } else if (sourceData.source_name) {
          const url = sourceData.source_url || sourceData.url;
          if (url && url !== 'Unknown') {
            lines.push(`<span style="font-size: 10px; color: #888;">Source: <a href="${url}" target="_blank" style="color: #5dade2;">${sourceData.source_name}</a></span>`);
          } else {
            lines.push(`<span style="font-size: 10px; color: #888;">Source: ${sourceData.source_name}</span>`);
          }
        }
      }
    } else if (locationInfo && !locationInfo.error) {
      // EXPLORATION MODE: Show location info from API (no query data present)
      lines.push(this.buildLocationInfo(locationInfo));
    } else {
      // Fallback: show any remaining non-skip fields
      const fieldsToShow = Object.keys(properties).filter(k =>
        !this.skipFields.includes(k) &&
        k.toLowerCase() !== 'year' &&
        properties[k] != null &&
        properties[k] !== ''
      );

      for (const key of fieldsToShow.slice(0, 10)) {
        const value = properties[key];
        if (value == null || value === '') continue;

        const fieldName = this.cleanFieldName(key);
        const formattedValue = this.formatValue(key, value);
        // Prefer data_year (actual year of data) over year (slider position)
        const displayYear = properties.data_year || properties.year || '';
        const yearSuffix = displayYear ? ` (${displayYear})` : '';

        lines.push(`${fieldName}: ${formattedValue}${yearSuffix}`);
      }
    }

    // Compact hint for zoom navigation (no leading break)
    lines.push('<em style="font-size: 10px; color: #999;">Zoom for sub-layers</em>');

    return lines.join('<br>');
  },

  /**
   * Build tabbed popup content grouping metrics by source category.
   * @param {Array} dataFields - All data field keys
   * @param {Object} properties - Feature properties with values
   * @param {Object} sourceData - Source metadata with metric_sources and sources
   * @param {string} yearSuffix - Year display suffix
   * @returns {string} HTML for tabbed content
   */
  buildTabbedContent(dataFields, properties, sourceData, yearSuffix) {
    const metricSources = sourceData.metric_sources || {};
    const sources = sourceData.sources || [];

    // Build source lookup by id
    const sourceLookup = {};
    for (const s of sources) {
      sourceLookup[s.id] = s;
    }

    // Group fields by category
    const groups = {};
    for (const key of dataFields) {
      const sourceId = metricSources[key];
      const source = sourceId ? sourceLookup[sourceId] : null;
      const category = source ? (source.category || 'general') : 'general';
      if (!groups[category]) {
        groups[category] = [];
      }
      groups[category].push(key);
    }

    const categoryNames = Object.keys(groups);

    // Build tab bar
    let html = '<div class="popup-tabs">';
    for (let i = 0; i < categoryNames.length; i++) {
      const cat = categoryNames[i];
      const label = cat.charAt(0).toUpperCase() + cat.slice(1);
      html += `<button class="popup-tab${i === 0 ? ' active' : ''}" data-tab="${cat}">${label}</button>`;
    }
    html += '</div>';

    // Build tab content panels
    for (let i = 0; i < categoryNames.length; i++) {
      const cat = categoryNames[i];
      const fields = groups[cat];
      html += `<div class="popup-tab-content${i === 0 ? ' active' : ''}" data-tab="${cat}">`;
      for (const key of fields) {
        const value = properties[key];
        if (value == null || value === '') continue;
        const fieldName = this.cleanFieldName(key);
        const formattedValue = this.formatValue(key, value);
        html += `${fieldName}: ${formattedValue}${yearSuffix}<br>`;
      }
      // Source link for this category
      const catSources = sources.filter(s => (s.category || 'general') === cat);
      if (catSources.length > 0) {
        const link = catSources[0].url
          ? `<a href="${catSources[0].url}" target="_blank" style="color: #5dade2;">${catSources[0].name}</a>`
          : catSources[0].name;
        html += `<span style="font-size: 10px; color: #888;">Source: ${link}</span>`;
      }
      html += '</div>';
    }

    return html;
  },

  /**
   * Build location info section from enriched API data
   * @param {Object} info - Location info from /geometry/{loc_id}/info
   * @returns {string} HTML content
   */
  buildLocationInfo(info) {
    const parts = [];

    // Memberships first (G20, BRICS, EU for countries; "Part of: X" for sub-nationals)
    if (info.memberships && info.memberships.length > 0) {
      const first = info.memberships[0];
      if (first.startsWith('Part of:')) {
        parts.push(`<span style="color: #888; font-size: 11px;">${first}</span>`);
      } else {
        const memberships = info.memberships.slice(0, 3).join(', ');
        parts.push(`<span style="color: #888; font-size: 11px;">${memberships}</span>`);
      }
    }

    // Country-level datasets (shown at country level)
    const datasetCounts = info.dataset_counts || {};
    const countryDatasets = datasetCounts.country || 0;
    if (info.admin_level === 0 && countryDatasets > 0) {
      parts.push(`<span style="color: #888; font-size: 11px;">${countryDatasets} datasets</span>`);
    }

    // Subdivisions - compact, one line with dataset counts
    if (info.children_count > 0 || info.descendants_count > 0) {
      const subdivisionLines = this.formatSubdivisions(info);
      for (const line of subdivisionLines) {
        parts.push(`<span style="color: #888; font-size: 11px;">${line}</span>`);
      }
    }

    return parts.join('<br>');
  },

  /**
   * Format subdivision counts into array of lines with dataset counts
   * @param {Object} info - Location info with children/descendants counts, level_names, dataset_counts
   * @returns {string[]} Array of formatted lines like ["52 states (20 datasets)", "3,144 counties (3 datasets)"]
   */
  formatSubdivisions(info) {
    // Parse children_by_level and descendants_by_level
    let childrenByLevel = {};
    let descendantsByLevel = {};

    try {
      if (typeof info.children_by_level === 'string') {
        childrenByLevel = JSON.parse(info.children_by_level);
      } else if (info.children_by_level) {
        childrenByLevel = info.children_by_level;
      }
    } catch (e) {}

    try {
      if (typeof info.descendants_by_level === 'string') {
        descendantsByLevel = JSON.parse(info.descendants_by_level);
      } else if (info.descendants_by_level) {
        descendantsByLevel = info.descendants_by_level;
      }
    } catch (e) {}

    // Use country-specific level names from API, or fall back to defaults
    const countryLevelNames = info.level_names || {};
    const defaultNames = { 1: 'states/provinces', 2: 'districts', 3: 'subdivisions', 4: 'localities' };

    // Dataset counts by geographic level (e.g., {"country": 20, "county": 3})
    const datasetCounts = info.dataset_counts || {};

    // Map admin levels to catalog geographic levels
    const levelToGeoLevel = { 0: 'country', 1: 'state', 2: 'county', 3: 'place' };

    const lines = [];

    // Format each level on its own line
    const allLevels = { ...childrenByLevel, ...descendantsByLevel };
    const sortedLevels = Object.keys(allLevels).map(Number).sort((a, b) => a - b);

    for (const level of sortedLevels) {
      const count = descendantsByLevel[level] || childrenByLevel[level] || 0;
      if (count > 0) {
        const levelName = countryLevelNames[level] || defaultNames[level] || `level ${level}`;

        // Get dataset count for this level
        const geoLevel = levelToGeoLevel[level];
        const dsCount = datasetCounts[geoLevel] || 0;

        if (dsCount > 0) {
          lines.push(`${count.toLocaleString()} ${levelName} (${dsCount} datasets)`);
        } else {
          lines.push(`${count.toLocaleString()} ${levelName}`);
        }
      }
    }

    // Limit to first 3 levels
    return lines.slice(0, 3);
  },

  /**
   * Build coverage info for debug mode popup
   * @param {Object} properties - Feature properties with coverage data
   * @returns {string} HTML content for coverage info
   */
  buildHierarchyInfo(properties) {
    const currentLevel = properties.current_admin_level || 0;
    const actualDepth = properties.actual_depth || 0;
    const coverage = properties.coverage || 0;
    const drillableDepth = properties.drillable_depth || 0;
    let levelCounts = properties.level_counts || {};
    let geometryCounts = properties.geometry_counts || {};

    // Parse if it's a JSON string (GeoJSON stringifies nested objects)
    if (typeof levelCounts === 'string') {
      try {
        levelCounts = JSON.parse(levelCounts);
      } catch (e) {
        levelCounts = {};
      }
    }
    if (typeof geometryCounts === 'string') {
      try {
        geometryCounts = JSON.parse(geometryCounts);
      } catch (e) {
        geometryCounts = {};
      }
    }

    const lines = [];
    const levelNames = ['country', 'state', 'county', 'place', 'locality', 'neighborhood'];
    const currentLevelName = levelNames[currentLevel] || `level ${currentLevel}`;

    // Show current admin level
    lines.push(`<br><strong>Admin Level: ${currentLevel} (${currentLevelName})</strong>`);

    // Show coverage percentage
    const coveragePct = Math.round(coverage * 100);
    const coverageColor = coverage >= 1 ? '#44aa44' : coverage >= 0.5 ? '#ff9900' : '#ff4444';
    lines.push(`<strong style="color: ${coverageColor};">Geometry: ${coveragePct}%</strong>`);

    // Show depth info
    lines.push(`Depth: ${actualDepth} levels (drill to level ${drillableDepth})`);

    // Show level counts with geometry availability
    // Iterate over actual keys in levelCounts (may start at admin_level > 0)
    const levels = Object.keys(levelCounts).map(Number).sort((a, b) => a - b);

    for (const level of levels) {
      const count = levelCounts[String(level)] || 0;
      const geomCount = geometryCounts[String(level)] || 0;
      if (count > 0 && level < levelNames.length) {
        const hasGeom = geomCount > 0;
        const color = hasGeom ? '#44aa44' : '#ff9900';
        const geomNote = hasGeom ? '' : ' (no geometry)';
        lines.push(`<span style="color: ${color};">${levelNames[level]}: ${count.toLocaleString()}${geomNote}</span>`);
      }
    }

    return lines.join('<br>');
  },

  /**
   * Get relevant data fields (numeric, interesting values)
   * @param {Object} properties - Feature properties
   * @param {boolean} fromQuery - If true, include ALL numeric fields (from chat query)
   */
  getRelevantFields(properties, fromQuery = false) {
    const relevant = [];
    // Keywords for exploration mode (when no specific query)
    const keywords = ['co2', 'gdp', 'population', 'emission', 'capita', 'total',
                      'methane', 'temperature', 'energy', 'oil', 'gas', 'coal',
                      'balance', 'account', 'trade', 'export', 'import', 'income',
                      'life', 'mortality', 'birth', 'death', 'health', 'age', 'median',
                      // ABS population data keywords
                      'natural', 'increase', 'internal', 'overseas', 'arrivals',
                      'departures', 'net', 'migration', 'area', 'density'];

    for (const [key, value] of Object.entries(properties)) {
      if (this.skipFields.includes(key) || value == null || value === '') continue;
      if (key.toLowerCase() === 'year') continue;

      const keyLower = key.toLowerCase();
      const isNumeric = !isNaN(parseFloat(value));

      // From chat query: include ALL numeric non-skip fields
      if (fromQuery && isNumeric) {
        relevant.push(key);
        continue;
      }

      // Exploration mode: filter by keywords
      const isRelevant = keywords.some(kw => keyLower.includes(kw));
      // Also include fields with our metric_label format (contain parentheses)
      const isLabeledMetric = key.includes('(') && key.includes(')');

      if (isNumeric && (isRelevant || isLabeledMetric)) {
        relevant.push(key);
      }
    }
    return relevant;
  },

  /**
   * Clean field name for display - removes redundant units like (count)
   * @param {string} key - Raw field name like "Population (count)"
   * @returns {string} Cleaned name like "Population"
   */
  cleanFieldName(key) {
    // Check for unit suffix pattern: "Name (unit)"
    const match = key.match(/^(.+?)\s*\(([^)]+)\)$/);
    if (match) {
      const name = match[1];
      const unit = match[2].toLowerCase();
      // Hide redundant units
      if (this.hideUnits.includes(unit)) {
        return name;
      }
    }
    // No unit suffix or not in hide list - format normally
    return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  },

  /**
   * Format a value for display
   */
  formatValue(key, value) {
    const keyLower = key.toLowerCase();
    const numValue = parseFloat(value);

    if (!isNaN(numValue)) {
      if (keyLower.includes('gdp') && !keyLower.includes('per')) {
        if (numValue > 1e9) return `$${(numValue / 1e9).toFixed(2)} billion`;
        if (numValue > 1e6) return `$${(numValue / 1e6).toFixed(2)} million`;
        return `$${numValue.toLocaleString()}`;
      }
      if (keyLower.includes('co2')) {
        if (keyLower.includes('per_capita') || keyLower.includes('percapita')) {
          return `${numValue.toFixed(2)} tonnes/person`;
        }
        return `${numValue.toFixed(2)} million tonnes`;
      }
      if (keyLower.includes('population') || keyLower.includes('pop')) {
        return numValue.toLocaleString();
      }
      if (keyLower.includes('percent') || keyLower.includes('rate')) {
        return `${numValue.toFixed(1)}%`;
      }
      if (numValue > 1000) return numValue.toLocaleString();
      return numValue.toFixed(2);
    }
    return value;
  }
};
