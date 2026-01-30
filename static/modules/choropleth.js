/**
 * Choropleth Manager - Color scaling and legend for data visualization.
 * Handles color interpolation and legend display.
 */

import { CONFIG } from './config.js';

// Dependencies set via setDependencies to avoid circular imports
let MapAdapter = null;

export function setDependencies(deps) {
  MapAdapter = deps.MapAdapter;
}

// ============================================================================
// CHOROPLETH MANAGER - Color scaling and legend for data visualization
// ============================================================================

export const ChoroplethManager = {
  metric: null,
  minValue: null,
  maxValue: null,
  colorScale: null,

  // DOM elements
  legend: null,
  legendTitle: null,
  legendGradient: null,
  legendMin: null,
  legendMax: null,

  /**
   * Convert timestamp to year (for looking up data keyed by year)
   */
  timestampToYear(ts) {
    if (Math.abs(ts) < 50000) return ts;  // Already a year
    return new Date(ts).getUTCFullYear();
  },

  /**
   * Check if metric name indicates a percentage value.
   * Used to force 0-100 scale for percentage metrics.
   */
  isPercentageMetric(metric) {
    if (!metric) return false;
    const lower = metric.toLowerCase();
    // Match common percentage indicators
    return lower.includes('percent') ||
           lower.includes('proportion') ||
           lower.includes('share') ||
           lower.includes('rate') ||
           lower.endsWith('_pct') ||
           lower.endsWith('_percent');
  },

  /**
   * Initialize choropleth with data range
   */
  init(metric, yearData, availableYears) {
    this.metric = metric;

    // Cache DOM elements
    this.legend = document.getElementById('choroplethLegend');
    this.legendTitle = document.getElementById('legendTitle');
    this.legendGradient = document.getElementById('legendGradient');
    this.legendMin = document.getElementById('legendMin');
    this.legendMax = document.getElementById('legendMax');

    // Detect if yearData keys are years or timestamps
    // Check first key to determine format
    const dataKeys = Object.keys(yearData);
    const firstKey = dataKeys.length > 0 ? dataKeys[0] : null;
    const dataUsesYears = firstKey && Math.abs(Number(firstKey)) < 50000;

    // Calculate global min/max across ALL years for consistent scaling
    let allValues = [];
    for (const time of availableYears) {
      // Convert timestamp to year if data uses year keys
      const key = dataUsesYears ? this.timestampToYear(time) : time;
      const yearValues = yearData[key] || {};
      for (const locId in yearValues) {
        const val = yearValues[locId][metric];
        if (val != null && !isNaN(val)) {
          allValues.push(val);
        }
      }
    }

    if (allValues.length > 0) {
      this.minValue = Math.min(...allValues);
      this.maxValue = Math.max(...allValues);
    } else {
      this.minValue = 0;
      this.maxValue = 100;
    }

    // PERCENTAGE SCALE OVERRIDE: Force 0-100 scale for percentage metrics
    // Comment out the next 3 lines to use dynamic min/max for percentages instead
    if (this.isPercentageMetric(metric)) {
      this.minValue = 0;
      this.maxValue = 100;
    }

    // Create color scale function (for legend only)
    this.colorScale = this.createScale(this.minValue, this.maxValue);

    // Update legend
    this.createLegend(metric);

    // Show legend
    if (this.legend) {
      this.legend.classList.add('visible');
      console.log('ChoroplethManager.init: Legend shown, metric:', metric, 'range:', this.minValue, '-', this.maxValue);
    } else {
      console.warn('ChoroplethManager.init: Legend element not found!');
    }
  },

  /**
   * Create color scale function (value -> color)
   * Uses turbo-inspired palette (good contrast on dark backgrounds)
   */
  createScale(min, max) {
    return (value) => {
      if (value == null || isNaN(value)) return '#cccccc';  // Gray for no data

      // Normalize to 0-1
      let t = (value - min) / (max - min);
      t = Math.max(0, Math.min(1, t));  // Clamp to 0-1

      // Turbo-inspired color stops (blue -> cyan -> green -> yellow -> orange -> red)
      // High contrast on dark backgrounds, intuitive (cool to warm)
      const colors = [
        { t: 0.0, r: 48, g: 18, b: 59 },     // Deep blue-purple
        { t: 0.2, r: 70, g: 131, b: 193 },   // Light blue
        { t: 0.4, r: 86, g: 199, b: 165 },   // Cyan-teal
        { t: 0.6, r: 190, g: 220, b: 60 },   // Yellow-green
        { t: 0.8, r: 249, g: 140, b: 42 },   // Orange
        { t: 1.0, r: 217, g: 33, b: 32 }     // Red
      ];

      // Find the two colors to interpolate between
      let c1 = colors[0], c2 = colors[1];
      for (let i = 0; i < colors.length - 1; i++) {
        if (t >= colors[i].t && t <= colors[i + 1].t) {
          c1 = colors[i];
          c2 = colors[i + 1];
          break;
        }
      }

      // Interpolate between c1 and c2
      const localT = (t - c1.t) / (c2.t - c1.t);
      const r = Math.round(c1.r + (c2.r - c1.r) * localT);
      const g = Math.round(c1.g + (c2.g - c1.g) * localT);
      const b = Math.round(c1.b + (c2.b - c1.b) * localT);

      return `rgb(${r}, ${g}, ${b})`;
    };
  },

  /**
   * Create legend display
   */
  createLegend(metric) {
    // Truncate long metric names
    const displayName = metric.length > 25 ? metric.substring(0, 22) + '...' : metric;
    this.legendTitle.textContent = displayName;

    // Create gradient background (turbo palette)
    this.legendGradient.style.background =
      'linear-gradient(to right, rgb(48,18,59), rgb(70,131,193), rgb(86,199,165), rgb(190,220,60), rgb(249,140,42), rgb(217,33,32))';

    // Format min/max values
    this.legendMin.textContent = this.formatValue(this.minValue);
    this.legendMax.textContent = this.formatValue(this.maxValue);
  },

  /**
   * Format value for display
   */
  formatValue(value) {
    if (value == null) return 'N/A';
    if (Math.abs(value) >= 1e9) return (value / 1e9).toFixed(1) + 'B';
    if (Math.abs(value) >= 1e6) return (value / 1e6).toFixed(1) + 'M';
    if (Math.abs(value) >= 1e3) return (value / 1e3).toFixed(1) + 'K';
    if (Number.isInteger(value)) return value.toString();
    return value.toFixed(2);
  },

  /**
   * Update map colors for current data
   * Uses efficient interpolate expression that reads directly from feature properties
   */
  update(geojson, metric) {
    if (!MapAdapter?.map?.getLayer(CONFIG.layers.fill)) return;

    // Build interpolate expression that reads metric value from properties
    // This is much faster than case expressions with 200+ conditions
    const colorExpression = this.buildInterpolateExpression(metric);
    MapAdapter.map.setPaintProperty(CONFIG.layers.fill, 'fill-color', colorExpression);
  },

  /**
   * Update color scale based on a subset of values (e.g., for admin level filtering).
   * Recalculates min/max from provided values and updates the legend and map colors.
   * @param {number[]} values - Array of numeric values to calculate scale from
   * @param {string} metric - Current metric name (for map paint property)
   */
  updateScaleForValues(values, metric) {
    if (!values || values.length === 0) return;

    // Filter out null/NaN values
    const validValues = values.filter(v => v != null && !isNaN(v));
    if (validValues.length === 0) return;

    // Ensure DOM elements are cached
    if (!this.legend) {
      this.legend = document.getElementById('choroplethLegend');
      this.legendTitle = document.getElementById('legendTitle');
      this.legendGradient = document.getElementById('legendGradient');
      this.legendMin = document.getElementById('legendMin');
      this.legendMax = document.getElementById('legendMax');
    }

    this.minValue = Math.min(...validValues);
    this.maxValue = Math.max(...validValues);
    this.metric = metric || this.metric;

    // Update color scale function
    this.colorScale = this.createScale(this.minValue, this.maxValue);

    // Update legend with new range and title
    if (this.legendTitle && this.metric) {
      const displayName = this.metric.length > 25 ? this.metric.substring(0, 22) + '...' : this.metric;
      this.legendTitle.textContent = displayName;
    }
    if (this.legendGradient) {
      this.legendGradient.style.background =
        'linear-gradient(to right, rgb(48,18,59), rgb(70,131,193), rgb(86,199,165), rgb(190,220,60), rgb(249,140,42), rgb(217,33,32))';
    }
    if (this.legendMin) this.legendMin.textContent = this.formatValue(this.minValue);
    if (this.legendMax) this.legendMax.textContent = this.formatValue(this.maxValue);

    // Show legend
    if (this.legend) {
      this.legend.classList.add('visible');
      console.log('ChoroplethManager: Legend shown');
    }

    // Update map paint property with new scale
    if (MapAdapter?.map?.getLayer(CONFIG.layers.fill)) {
      const colorExpression = this.buildInterpolateExpression(metric || this.metric);
      MapAdapter.map.setPaintProperty(CONFIG.layers.fill, 'fill-color', colorExpression);
    }

    console.log(`ChoroplethManager: Updated scale to ${this.formatValue(this.minValue)} - ${this.formatValue(this.maxValue)}`);
  },

  /**
   * Build MapLibre interpolate expression for data-driven colors
   * Uses turbo color stops and reads value directly from feature properties
   */
  buildInterpolateExpression(metric) {
    const min = this.minValue;
    const max = this.maxValue;

    // Handle edge case where min === max
    if (min === max) {
      return [
        'case',
        ['boolean', ['feature-state', 'hover'], false],
        '#ffffff',
        ['has', metric],
        'rgb(86, 199, 165)',  // Cyan-teal for all values
        '#cccccc'  // Gray for no data
      ];
    }

    // Turbo color stops at normalized positions (0 to 1)
    // We interpolate in the actual value domain [min, max]
    const stops = [
      [min, 'rgb(48, 18, 59)'],                                   // 0.0 - Deep blue-purple
      [min + (max - min) * 0.2, 'rgb(70, 131, 193)'],             // 0.2 - Light blue
      [min + (max - min) * 0.4, 'rgb(86, 199, 165)'],             // 0.4 - Cyan-teal
      [min + (max - min) * 0.6, 'rgb(190, 220, 60)'],             // 0.6 - Yellow-green
      [min + (max - min) * 0.8, 'rgb(249, 140, 42)'],             // 0.8 - Orange
      [max, 'rgb(217, 33, 32)']                                    // 1.0 - Red
    ];

    // Build interpolate expression
    const interpolateExpr = ['interpolate', ['linear'], ['get', metric]];
    for (const [value, color] of stops) {
      interpolateExpr.push(value, color);
    }

    // Wrap in case expression to handle hover and missing data
    return [
      'case',
      ['boolean', ['feature-state', 'hover'], false],
      '#ffffff',  // White on hover
      ['has', metric],
      interpolateExpr,
      '#cccccc'  // Gray for no data
    ];
  },

  /**
   * Hide the legend
   */
  hide() {
    if (this.legend) {
      this.legend.classList.remove('visible');
    }
  },

  /**
   * Reset choropleth manager
   */
  reset() {
    this.hide();
    this.metric = null;
    this.minValue = null;
    this.maxValue = null;
  }
};
