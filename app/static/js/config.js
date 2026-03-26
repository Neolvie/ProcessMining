'use strict';

// ── Chart.js global defaults ──────────────────────────────────────────────
Chart.defaults.color = '#625F6A';
Chart.defaults.borderColor = '#E0E0E0';
Chart.defaults.font.family = "Inter, 'Segoe UI', system-ui, sans-serif";
Chart.defaults.font.size = 12;

const COLORS = {
  orange: '#FF7A00', blue:   '#3C65CC', green:  '#3AC436',
  red:    '#D32F2F', amber:  '#F5A623', purple: '#7C3AED',
  cyan:   '#0891B2', teal:   '#0D9488', pink:   '#DB2777',
  indigo: '#4F46E5',
};
const PALETTE = Object.values(COLORS);
