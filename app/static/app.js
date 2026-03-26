'use strict';

// ── Chart.js global defaults ─────────────────────────────────────────────────
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

// ── State ────────────────────────────────────────────────────────────────────
const state = {
  currentView: 'dashboard',
  charts: {},
  processesData: [],
  blocksData: [],
  bottlenecksData: null,
  issuesData: null,
  filterProcessId: '',
};

// ── Utilities ─────────────────────────────────────────────────────────────────
function fmtNum(n, decimals = 0) {
  if (n == null) return '—';
  return Number(n).toLocaleString('ru-RU', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtDur(hours) {
  if (hours == null || hours === 0) return '—';
  if (hours < 1/60) return '<1 мин';
  if (hours < 1)    return Math.round(hours * 60) + ' мин';
  const h = Math.floor(hours);
  const m = Math.round((hours - h) * 60);
  return m ? `${h}ч ${m}м` : `${h}ч`;
}

function fmtSec(sec) {
  if (sec == null) return '—';
  if (sec < 60)   return sec + ' с';
  if (sec < 3600) return Math.round(sec / 60) + ' мин';
  return fmtDur(sec / 3600);
}

function fmtPct(v) {
  if (v == null) return '—';
  return fmtNum(v, 1) + '%';
}

function fmtTs(ts) {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    return d.toLocaleString('ru-RU', {
      month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit',
    });
  } catch { return ts; }
}

function badge(text, color = 'muted') {
  return `<span class="badge badge-${color}">${text}</span>`;
}

function statusBadge(status) {
  const map = {
    completed:   ['Завершён',  'green'],
    aborted:     ['Прерван',   'red'],
    in_progress: ['В работе',  'blue'],
  };
  const [label, color] = map[status] || [status, 'muted'];
  return badge(label, color);
}

function bar(pct, color = 'blue') {
  const w = Math.min(100, Math.max(0, pct || 0));
  return `<div class="bar-track"><div class="bar-fill bar-${color}" style="width:${w}%"></div></div>`;
}

function loading(text = 'Загрузка…') {
  return `<div class="loading-overlay">
    <div class="spinner"></div>
    <div class="loading-text">${text}</div>
  </div>`;
}

function empty(text = 'Нет данных') {
  return `<div class="empty">
    <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
      <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/>
      <line x1="12" y1="16" x2="12.01" y2="16"/>
    </svg>
    <div>${text}</div>
  </div>`;
}

// ── API ───────────────────────────────────────────────────────────────────────
async function api(path, params = {}) {
  const qs = new URLSearchParams(Object.entries(params).filter(([,v]) => v != null && v !== '')).toString();
  const url = '/api' + path + (qs ? '?' + qs : '');
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`);
  return res.json();
}

// ── Chart helpers ─────────────────────────────────────────────────────────────
function destroyChart(id) {
  if (state.charts[id]) { state.charts[id].destroy(); delete state.charts[id]; }
}

function mkChart(id, config) {
  destroyChart(id);
  const canvas = document.getElementById(id);
  if (!canvas) return null;
  const ch = new Chart(canvas, config);
  state.charts[id] = ch;
  return ch;
}

function barChartCfg(labels, datasets, opts = {}) {
  return {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: opts.legend ?? false } },
      scales: {
        x: { stacked: opts.stacked, grid: { display: false },
             ticks: { maxRotation: opts.rotateX ?? 0, font: { size: 11 } } },
        y: { stacked: opts.stacked, beginAtZero: true,
             ticks: { font: { size: 11 } },
             title: opts.yTitle ? { display: true, text: opts.yTitle } : undefined },
      },
      ...opts.extra,
    },
  };
}

function lineChartCfg(labels, datasets, opts = {}) {
  return {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: opts.legend ?? true } },
      scales: {
        x: { grid: { display: false },
             ticks: { maxRotation: opts.rotateX ?? 45, font: { size: 10 },
                      maxTicksLimit: opts.maxTicks ?? 24 } },
        y: { beginAtZero: true, ticks: { font: { size: 11 } } },
      },
      elements: { point: { radius: 2 }, line: { tension: 0.3 } },
      ...opts.extra,
    },
  };
}

function ds(label, data, color, opts = {}) {
  return {
    label, data,
    backgroundColor: opts.fill ? color + '33' : color,
    borderColor: color,
    borderWidth: opts.border ?? 2,
    fill: opts.fill ?? false,
    ...opts,
  };
}

// ── Table builder ─────────────────────────────────────────────────────────────
let _sortState = {};

function buildTable(containerId, columns, rows, opts = {}) {
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!rows || rows.length === 0) { el.innerHTML = empty(); return; }

  const sortKey = _sortState[containerId]?.key ?? opts.sortKey ?? columns[0]?.key;
  const sortDir = _sortState[containerId]?.dir ?? (opts.sortDir || 'desc');

  const sorted = [...rows].sort((a, b) => {
    const va = a[sortKey] ?? -Infinity;
    const vb = b[sortKey] ?? -Infinity;
    const cmp = typeof va === 'string' ? va.localeCompare(vb, 'ru') : (va - vb);
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const thCells = columns.map(c => {
    const isSorted = c.key === sortKey;
    const cls = isSorted ? (sortDir === 'asc' ? 'sort-asc' : 'sort-desc') : '';
    return `<th class="${cls}" data-key="${c.key}">${c.label}</th>`;
  }).join('');

  const tbRows = sorted.map(row => {
    const tds = columns.map(c => {
      const val = row[c.key];
      const rendered = c.render ? c.render(val, row) : (val ?? '—');
      return `<td>${rendered}</td>`;
    }).join('');
    const rowCls = opts.clickable ? 'clickable-row' : '';
    const rowAttr = opts.onRowClick ? `onclick="${opts.onRowClick}(this)"` : '';
    const dataAttrs = opts.rowData ? opts.rowData.map(k => `data-${k}="${row[k] ?? ''}"`).join(' ') : '';
    return `<tr class="${rowCls}" ${rowAttr} ${dataAttrs}>${tds}</tr>`;
  }).join('');

  el.innerHTML = `<table>
    <thead><tr>${thCells}</tr></thead>
    <tbody>${tbRows}</tbody>
  </table>`;

  // Sortable headers
  el.querySelectorAll('thead th[data-key]').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.dataset.key;
      const cur = _sortState[containerId] || {};
      const dir = cur.key === key && cur.dir === 'desc' ? 'asc' : 'desc';
      _sortState[containerId] = { key, dir };
      buildTable(containerId, columns, rows, opts);
    });
  });
}

// ── Navigation ─────────────────────────────────────────────────────────────────
function navigate(viewName) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const view = document.getElementById('view-' + viewName);
  if (view) view.classList.add('active');
  const nav = document.querySelector(`.nav-item[data-view="${viewName}"]`);
  if (nav) nav.classList.add('active');
  state.currentView = viewName;

  const loaders = {
    dashboard:    loadDashboard,
    timeline:     loadTimeline,
    processes:    loadProcesses,
    blocks:       loadBlocks,
    flow:         loadFlow,
    bottlenecks:  loadBottlenecks,
    issues:       loadIssues,
    ai:           loadAI,
  };
  loaders[viewName]?.();
}

document.querySelectorAll('.nav-item[data-view]').forEach(el => {
  el.addEventListener('click', () => navigate(el.dataset.view));
});

// ── Tabs ──────────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    const target = tab.dataset.tab;
    const parent = tab.closest('.view') || tab.parentElement.parentElement;
    parent.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    parent.querySelectorAll('.tab-content').forEach(c => { c.style.display = 'none'; });
    tab.classList.add('active');
    const content = document.getElementById(target);
    if (content) content.style.display = '';
  });
});

// ── Status poller ─────────────────────────────────────────────────────────────
async function pollStatus() {
  try {
    const s = await api('/status');
    const dot = document.getElementById('status-dot');
    const txt = document.getElementById('status-text');
    if (s.status === 'ready') {
      dot.classList.remove('loading');
      const meta = s.meta || {};
      txt.innerHTML =
        `Данные загружены за ${s.total_seconds}с &nbsp;·&nbsp; ` +
        `${fmtNum(meta.total_lines)} строк &nbsp;·&nbsp; ` +
        `${fmtNum(meta.parsed_events)} событий &nbsp;·&nbsp; ` +
        `${(meta.files || []).length} файлов`;
      loadDashboard();
      return;
    }
    txt.textContent = 'Загрузка данных…';
    setTimeout(pollStatus, 1000);
  } catch {
    setTimeout(pollStatus, 2000);
  }
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
let _dashLoaded = false;

async function loadDashboard() {
  if (_dashLoaded) return;

  const [overview, procs, tl] = await Promise.all([
    api('/overview'),
    api('/processes'),
    api('/timeline', { granularity: 'hour' }),
  ]);

  _dashLoaded = true;

  // Period badge
  const p = overview.period || {};
  const from = p.period_from ? fmtTs(p.period_from) : '';
  const to   = p.period_to   ? fmtTs(p.period_to)   : '';
  const badge_el = document.getElementById('dash-period');
  if (badge_el && from) badge_el.textContent = `${from} – ${to}`;

  // KPI row 1 — counts
  const I = overview.instances;
  document.getElementById('dash-kpi-row1').innerHTML = `
    <div class="kpi accent-blue">
      <div class="kpi-label">Всего экземпляров</div>
      <div class="kpi-value blue">${fmtNum(I.total)}</div>
      <div class="kpi-sub">${overview.unique_process_types} типов процессов</div>
    </div>
    <div class="kpi accent-green">
      <div class="kpi-label">Завершено</div>
      <div class="kpi-value green">${fmtNum(I.completed)}</div>
      <div class="kpi-sub">${fmtPct(I.completion_rate)} от всех</div>
    </div>
    <div class="kpi accent-red">
      <div class="kpi-label">Прервано</div>
      <div class="kpi-value red">${fmtNum(I.aborted)}</div>
      <div class="kpi-sub">${fmtPct(I.abort_rate)} от всех</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">В работе</div>
      <div class="kpi-value">${fmtNum(I.in_progress)}</div>
      <div class="kpi-sub">Без завершения в окне</div>
    </div>
  `;

  // KPI row 2 — performance
  const P = overview.performance;
  const B = overview.blocks;
  const E = overview.errors;
  document.getElementById('dash-kpi-row2').innerHTML = `
    <div class="kpi">
      <div class="kpi-label">Ср. время процесса</div>
      <div class="kpi-value">${fmtSec(P.avg_duration_sec)}</div>
      <div class="kpi-sub">Медиана: ${fmtSec(P.median_duration_sec)}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">P95 времени процесса</div>
      <div class="kpi-value">${fmtSec(P.p95_duration_sec)}</div>
      <div class="kpi-sub">95-й перцентиль</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Активаций блоков</div>
      <div class="kpi-value">${fmtNum(B.activations)}</div>
      <div class="kpi-sub">Прервано: ${fmtNum(B.abortions)} (${fmtPct(B.abort_rate)})</div>
    </div>
    <div class="kpi ${E.lock_contentions > 0 ? 'accent-amber' : ''}">
      <div class="kpi-label">Блокировки / Сбои</div>
      <div class="kpi-value ${E.lock_contentions > 0 ? 'amber' : ''}">${fmtNum(E.lock_contentions)}</div>
      <div class="kpi-sub">Failed spans: ${fmtNum(E.failed_spans)}</div>
    </div>
  `;

  // Hourly chart – bucket is already in local time 'YYYY-MM-DD HH:MM'
  const buckets = tl.map(r => {
    const [date, time] = r.bucket.split(' ');
    const [y, m, d] = date.split('-');
    return `${d}.${m} ${time.slice(0,2)}h`;
  });
  mkChart('chart-hourly', lineChartCfg(buckets, [
    ds('Старты',      tl.map(r => r.process_starts),      COLORS.blue,   { fill: true }),
    ds('Завершения',  tl.map(r => r.process_completions), COLORS.green),
    ds('Прерывания',  tl.map(r => r.process_abortions),   COLORS.red),
  ], { legend: true, maxTicks: 48, rotateX: 45 }));

  // Process pie chart
  const top10 = procs.slice(0, 10);
  const rest  = procs.slice(10).reduce((s, p) => s + p.total, 0);
  const pieLabels = top10.map(p => p.display_name || p.name || p.process_id);
  const pieData   = top10.map(p => p.total);
  if (rest > 0) { pieLabels.push('Прочие'); pieData.push(rest); }
  mkChart('chart-process-pie', {
    type: 'doughnut',
    data: {
      labels: pieLabels,
      datasets: [{ data: pieData, backgroundColor: PALETTE, borderWidth: 2,
                   borderColor: '#FFFFFF' }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } },
        tooltip: { callbacks: {
          label: ctx => ` ${ctx.label}: ${fmtNum(ctx.raw)} (${fmtPct(ctx.raw/I.total*100)})`
        }},
      },
      cutout: '60%',
    },
  });

  // Top processes table
  buildTable('dash-top-processes',
    [
      { key: 'display_name', label: 'Процесс',
        render: (v, r) => `<span title="${r.process_id}">${v || r.name || r.process_id}</span>` },
      { key: 'total',           label: 'Экз-ры',   render: v => fmtNum(v) },
      { key: 'completion_rate', label: '% завершения',
        render: (v, r) => `${fmtPct(v)}<br>${bar(v, 'green')}` },
      { key: 'abort_rate',      label: '% прерваний',
        render: (v, r) => `${fmtPct(v)}<br>${bar(v, 'red')}` },
      { key: 'avg_duration_hours', label: 'Ср. длит.',
        render: v => fmtDur(v) },
      { key: 'median_duration_hours', label: 'Медиана',
        render: v => fmtDur(v) },
      { key: 'avg_blocks', label: 'Блоков/экз', render: v => fmtNum(v, 1) },
    ],
    procs.slice(0, 15),
    { sortKey: 'total', sortDir: 'desc',
      clickable: true, onRowClick: 'openProcessModal',
      rowData: ['process_id', 'display_name'] }
  );
}

// ── Timeline ──────────────────────────────────────────────────────────────────
async function loadTimeline() {
  const gran = document.getElementById('tl-granularity')?.value || 'hour';
  const tl = await api('/timeline', { granularity: gran });

  // bucket is already in local time (UTC+4), format: 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD'
  const fmt = gran === 'hour'
    ? r => { const [date, time] = r.bucket.split(' '); const [y,m,d] = date.split('-'); return `${d}.${m} ${time.slice(0,2)}h`; }
    : r => { const [y,m,d] = r.bucket.split('-'); return `${d}.${m}`; };

  const labels = tl.map(fmt);

  mkChart('chart-tl-processes', lineChartCfg(labels, [
    ds('Старты процессов',    tl.map(r => r.process_starts),      COLORS.blue,   { fill: true }),
    ds('Завершения',          tl.map(r => r.process_completions), COLORS.green),
    ds('Прерывания',          tl.map(r => r.process_abortions),   COLORS.red),
  ], { legend: true, maxTicks: gran === 'hour' ? 48 : 10, rotateX: 45 }));

  mkChart('chart-tl-blocks', {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { ...ds('Активации блоков', tl.map(r => r.block_activations), COLORS.blue + '99'),
          type: 'bar', yAxisID: 'y', order: 2 },
        { ...ds('Ср. время обраб. (мс)', tl.map(r => Math.round(r.avg_span_ms || 0)), COLORS.amber),
          type: 'line', yAxisID: 'y2', order: 1, fill: false },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: true } },
      scales: {
        x: { grid: { display: false }, ticks: { maxRotation: 45, maxTicksLimit: gran === 'hour' ? 48 : 10, font: { size: 10 } } },
        y:  { beginAtZero: true, position: 'left',  title: { display: true, text: 'Активации' } },
        y2: { beginAtZero: true, position: 'right', title: { display: true, text: 'мс' },
               grid: { drawOnChartArea: false } },
      },
    },
  });
}

// ── Processes ─────────────────────────────────────────────────────────────────
async function loadProcesses() {
  if (state.processesData.length) { renderProcessTable(); return; }
  document.getElementById('proc-table').innerHTML = loading();
  state.processesData = await api('/processes');

  // Populate block & flow filters
  ['block-filter-process', 'flow-filter-process'].forEach(id => {
    const sel = document.getElementById(id);
    if (!sel) return;
    sel.innerHTML = '<option value="">Все процессы</option>' +
      state.processesData.map(p =>
        `<option value="${p.process_id}">${p.display_name || p.name || p.process_id}</option>`
      ).join('');
  });

  renderProcessTable();
}

function filterProcessTable() {
  renderProcessTable(document.getElementById('proc-search')?.value || '');
}

function renderProcessTable(search = '') {
  const rows = search
    ? state.processesData.filter(p =>
        (p.display_name || p.name || '').toLowerCase().includes(search.toLowerCase()))
    : state.processesData;

  buildTable('proc-table', [
    { key: 'display_name', label: 'Процесс',
      render: (v, r) => `<span style="cursor:pointer;color:var(--blue)" onclick="openProcessModalById('${r.process_id}','${(v||'').replace(/'/g,"\\'")}')"><strong>${v || r.name || r.process_id}</strong></span>` },
    { key: 'total',           label: 'Экз-ры',   render: v => fmtNum(v) },
    { key: 'completed',       label: 'Завершено', render: v => fmtNum(v) },
    { key: 'aborted',         label: 'Прервано',  render: v => fmtNum(v) },
    { key: 'in_progress',     label: 'В работе',  render: v => fmtNum(v) },
    { key: 'completion_rate', label: '% завершения',
      render: v => `${fmtPct(v)} ${bar(v, 'green')}` },
    { key: 'abort_rate', label: '% прерваний',
      render: v => `${fmtPct(v)} ${bar(v, 'red')}` },
    { key: 'avg_duration_hours',    label: 'Ср. длит.',   render: v => fmtDur(v) },
    { key: 'median_duration_hours', label: 'Медиана',     render: v => fmtDur(v) },
    { key: 'p95_duration_hours',    label: 'P95',         render: v => fmtDur(v) },
    { key: 'avg_blocks',            label: 'Блоков/экз',  render: v => fmtNum(v, 1) },
    { key: 'total_blocks',          label: 'Всего блоков',render: v => fmtNum(v) },
  ], rows, { sortKey: 'total', sortDir: 'desc' });
}

// ── Blocks ────────────────────────────────────────────────────────────────────
async function loadBlocks() {
  document.getElementById('blocks-table').innerHTML = loading();
  const procId = document.getElementById('block-filter-process')?.value || '';
  state.blocksData = await api('/blocks', { process_id: procId || undefined, limit: 200 });
  renderBlocksTable();
}

function renderBlocksTable() {
  const sortKey = document.getElementById('block-sort')?.value || 'avg_duration_sec';
  _sortState['blocks-table'] = { key: sortKey, dir: 'desc' };

  buildTable('blocks-table', [
    { key: 'block_id',      label: 'Блок',     render: v => `<code style="color:#94a3b8">${v}</code>` },
    { key: 'scheme_id',     label: 'Схема',    render: v => fmtNum(v) },
    { key: 'process_name',  label: 'Процесс',
      render: v => `<span style="color:#cbd5e1">${v || '—'}</span>` },
    { key: 'activations',   label: 'Активаций',  render: v => fmtNum(v) },
    { key: 'completions',   label: 'Завершено',  render: v => fmtNum(v) },
    { key: 'abortions',     label: 'Прервано',
      render: (v, r) => v > 0 ? badge(v, 'red') : '0' },
    { key: 'abort_rate',    label: '% прерваний',
      render: v => v > 10 ? badge(fmtPct(v), 'red') : v > 5 ? badge(fmtPct(v), 'amber') : fmtPct(v) },
    { key: 'avg_duration_sec', label: 'Ср. длит.',
      render: (v, r) => {
        if (!v) return '—';
        const color = v > 86400 ? 'red' : v > 3600 ? 'amber' : 'muted';
        return `<span style="color:var(--${color === 'muted' ? 'text' : color})">${fmtSec(v)}</span>`;
      }},
    { key: 'median_duration_sec', label: 'Медиана', render: v => fmtSec(v) },
    { key: 'p95_duration_sec',    label: 'P95',     render: v => fmtSec(v) },
    { key: 'max_duration_sec',    label: 'Макс',    render: v => fmtSec(v) },
  ], state.blocksData, { sortKey, sortDir: 'desc' });
}

// ── Flow ──────────────────────────────────────────────────────────────────────
async function loadFlow() {
  document.getElementById('flow-table').innerHTML = loading();
  const procId = document.getElementById('flow-filter-process')?.value || '';
  const data = await api('/flow', { process_id: procId || undefined, top_n: 40 });

  buildTable('flow-table', [
    { key: 'from_block',        label: 'Из блока',   render: v => `<code>${v}</code>` },
    { key: 'to_block',          label: 'В блок',     render: v => `<code>${v}</code>` },
    { key: 'scheme_id',         label: 'Схема',      render: v => fmtNum(v) },
    { key: 'transition_count',  label: 'Переходов',  render: v => fmtNum(v) },
    { key: 'avg_gap_sec',       label: 'Ср. интервал', render: v => fmtSec(v) },
  ], data, { sortKey: 'transition_count', sortDir: 'desc' });

  // Bar chart of top 20 transitions
  const top20 = data.slice(0, 20);
  mkChart('chart-flow', barChartCfg(
    top20.map(r => `${r.from_block}→${r.to_block}`),
    [{ label: 'Переходов', data: top20.map(r => r.transition_count),
       backgroundColor: COLORS.blue + 'cc', borderColor: COLORS.blue, borderWidth: 1 }],
    { rotateX: 45, yTitle: 'Кол-во' }
  ));
}

// ── Bottlenecks ───────────────────────────────────────────────────────────────
async function loadBottlenecks() {
  if (state.bottlenecksData) { renderBottlenecks(); return; }
  state.bottlenecksData = await api('/bottlenecks');
  renderBottlenecks();
}

function renderBottlenecks() {
  const d = state.bottlenecksData;

  // Slow blocks
  const sb = d.slow_blocks || [];
  mkChart('chart-slow-blocks', barChartCfg(
    sb.map(r => `[${r.scheme_id}] ${r.block_id}`),
    [
      { label: 'Ср. (ч)', data: sb.map(r => r.avg_hours || 0),
        backgroundColor: COLORS.amber + 'cc', borderColor: COLORS.amber, borderWidth: 1 },
      { label: 'P95 (ч)', data: sb.map(r => r.p95_hours || 0),
        backgroundColor: COLORS.red + '88',   borderColor: COLORS.red,   borderWidth: 1 },
    ],
    { legend: true, rotateX: 45, yTitle: 'Часов', stacked: false }
  ));
  buildTable('tbl-slow-blocks', [
    { key: 'block_id',     label: 'Блок',    render: v => `<code>${v}</code>` },
    { key: 'scheme_id',    label: 'Схема',   render: v => fmtNum(v) },
    { key: 'process_name', label: 'Процесс' },
    { key: 'activations',  label: 'Актив-й', render: v => fmtNum(v) },
    { key: 'avg_hours',    label: 'Ср. длит.',
      render: v => `<strong style="color:var(--amber)">${fmtDur(v)}</strong>` },
    { key: 'p95_hours',    label: 'P95',     render: v => fmtDur(v) },
    { key: 'max_hours',    label: 'Макс',
      render: v => `<span style="color:var(--red)">${fmtDur(v)}</span>` },
  ], sb, { sortKey: 'avg_hours', sortDir: 'desc' });

  // Slow processes
  const sp = d.slow_processes || [];
  mkChart('chart-slow-processes', barChartCfg(
    sp.map(r => r.display_name || r.process_id),
    [{ label: 'Ср. (ч)', data: sp.map(r => r.avg_hours || 0),
       backgroundColor: PALETTE }],
    { rotateX: 45, yTitle: 'Часов' }
  ));
  buildTable('tbl-slow-processes', [
    { key: 'display_name', label: 'Процесс' },
    { key: 'total',        label: 'Экз-ры',  render: v => fmtNum(v) },
    { key: 'avg_hours',    label: 'Ср. длит.',
      render: v => `<strong style="color:var(--amber)">${fmtDur(v)}</strong>` },
    { key: 'p95_hours',    label: 'P95',     render: v => fmtDur(v) },
    { key: 'max_hours',    label: 'Макс',
      render: v => `<span style="color:var(--red)">${fmtDur(v)}</span>` },
  ], sp, { sortKey: 'avg_hours', sortDir: 'desc' });

  // High abort blocks
  const ab = d.high_abort_blocks || [];
  mkChart('chart-abort-blocks', barChartCfg(
    ab.map(r => `[${r.scheme_id}] ${r.block_id}`),
    [{ label: '% прерваний', data: ab.map(r => r.abort_rate || 0),
       backgroundColor: ab.map(r => r.abort_rate > 30 ? COLORS.red + 'cc' : COLORS.amber + 'cc'),
       borderColor: ab.map(r => r.abort_rate > 30 ? COLORS.red : COLORS.amber),
       borderWidth: 1 }],
    { rotateX: 45, yTitle: '%' }
  ));
  buildTable('tbl-abort-blocks', [
    { key: 'block_id',     label: 'Блок',      render: v => `<code>${v}</code>` },
    { key: 'scheme_id',    label: 'Схема',     render: v => fmtNum(v) },
    { key: 'process_name', label: 'Процесс' },
    { key: 'activations',  label: 'Активаций', render: v => fmtNum(v) },
    { key: 'abortions',    label: 'Прервано',  render: v => fmtNum(v) },
    { key: 'abort_rate',   label: '% прерваний',
      render: v => v > 30 ? badge(fmtPct(v), 'red') : badge(fmtPct(v), 'amber') },
  ], ab, { sortKey: 'abort_rate', sortDir: 'desc' });
}

// ── Issues ────────────────────────────────────────────────────────────────────
async function loadIssues() {
  if (state.issuesData) { renderIssues(); return; }
  state.issuesData = await api('/issues');
  renderIssues();
}

function renderIssues() {
  const d = state.issuesData;

  // Abort by process
  const abp = d.abort_by_process || [];
  mkChart('chart-abort-proc', barChartCfg(
    abp.map(r => r.display_name || r.process_id),
    [
      { label: 'Всего',    data: abp.map(r => r.total),   backgroundColor: COLORS.blue + '55',
        borderColor: COLORS.blue, borderWidth: 1 },
      { label: 'Прервано', data: abp.map(r => r.aborted), backgroundColor: COLORS.red + 'aa',
        borderColor: COLORS.red, borderWidth: 1 },
    ],
    { legend: true, stacked: false, rotateX: 45 }
  ));
  buildTable('tbl-abort-proc', [
    { key: 'display_name', label: 'Процесс' },
    { key: 'total',        label: 'Всего',   render: v => fmtNum(v) },
    { key: 'aborted',      label: 'Прервано', render: v => fmtNum(v) },
    { key: 'abort_rate',   label: '% прерваний',
      render: v => v > 20 ? badge(fmtPct(v), 'red') : v > 5 ? badge(fmtPct(v), 'amber') : fmtPct(v) },
  ], abp, { sortKey: 'abort_rate', sortDir: 'desc' });

  // Lock contentions
  const lk = d.lock_contentions_by_hour || [];
  if (lk.length) {
    mkChart('chart-locks', lineChartCfg(
      lk.map(r => fmtTs(r.hour)),
      [ds('Блокировки', lk.map(r => r.count), COLORS.amber, { fill: true })],
      { legend: false, maxTicks: 24, rotateX: 45 }
    ));
  } else {
    const c = document.getElementById('chart-locks');
    if (c) c.parentElement.innerHTML = empty('Блокировок не зафиксировано');
  }

  // Long running instances
  buildTable('tbl-long', [
    { key: 'instance_id',    label: 'ID',      render: v => `<code>${v}</code>` },
    { key: 'process_name',   label: 'Процесс' },
    { key: 'start_time',     label: 'Старт',   render: v => fmtTs(v) },
    { key: 'status',         label: 'Статус',  render: v => statusBadge(v) },
    { key: 'duration_hours', label: 'Длит.',
      render: v => v > 24 ? `<span style="color:var(--red)">${fmtDur(v)}</span>` : fmtDur(v) },
    { key: 'block_count',    label: 'Блоков',  render: v => fmtNum(v) },
  ], d.long_running_instances || [], { sortKey: 'duration_hours', sortDir: 'desc' });

  // Failed spans
  buildTable('tbl-fails', [
    { key: 'timestamp',    label: 'Время',   render: v => fmtTs(v) },
    { key: 'instance_id',  label: 'Экз-р',  render: v => `<code>${v}</code>` },
    { key: 'block_id',     label: 'Блок',   render: v => `<code>${v}</code>` },
    { key: 'message_type', label: 'Тип сообщения' },
    { key: 'host',         label: 'Хост' },
  ], d.failed_spans || [], { sortKey: 'timestamp', sortDir: 'desc' });
}

// ── Process detail modal ──────────────────────────────────────────────────────
function openProcessModal(row) {
  openProcessModalById(row.dataset.processId, row.dataset.displayName);
}

async function openProcessModalById(processId, displayName) {
  const modal = document.getElementById('modal-backdrop');
  const body  = document.getElementById('modal-body');
  if (!modal || !body) return;
  modal.classList.add('open');
  body.innerHTML = `<div class="modal-title">${displayName || processId}</div>` + loading('Загрузка деталей…');

  const [detail, tl] = await Promise.all([
    api(`/process/${processId}`),
    api(`/process/${processId}/timeline`),
  ]);

  const inst = detail.instances || [];
  const bs   = detail.block_stats || [];

  const completed = inst.filter(i => i.status === 'completed').length;
  const aborted   = inst.filter(i => i.status === 'aborted').length;
  const durations = inst.filter(i => i.duration_hours != null).map(i => i.duration_hours);
  const avg = durations.length ? durations.reduce((a,b)=>a+b,0)/durations.length : null;

  // hour is already in local time 'YYYY-MM-DD HH:MM'
  const tlLabels = tl.map(r => {
    const [date, time] = r.hour.split(' ');
    const [y, m, d] = date.split('-');
    return `${d}.${m} ${time.slice(0,2)}h`;
  });

  body.innerHTML = `
    <div class="modal-title">${displayName || processId}</div>
    <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:16px">
      <div class="kpi"><div class="kpi-label">Экземпляры</div>
        <div class="kpi-value blue">${fmtNum(inst.length)}</div></div>
      <div class="kpi accent-green"><div class="kpi-label">Завершено</div>
        <div class="kpi-value green">${fmtNum(completed)}</div>
        <div class="kpi-sub">${fmtPct(completed/inst.length*100)}</div></div>
      <div class="kpi accent-red"><div class="kpi-label">Прервано</div>
        <div class="kpi-value red">${fmtNum(aborted)}</div>
        <div class="kpi-sub">${fmtPct(aborted/inst.length*100)}</div></div>
      <div class="kpi"><div class="kpi-label">Ср. длительность</div>
        <div class="kpi-value">${fmtDur(avg)}</div></div>
    </div>
    <div style="margin-bottom:16px">
      <div class="card-title">Активность по часам</div>
      <div class="chart-wrap short"><canvas id="modal-chart-tl"></canvas></div>
    </div>
    <div class="card-title" style="margin-bottom:8px">Топ блоков по длительности</div>
    <div id="modal-block-tbl"></div>
    <div class="card-title" style="margin-bottom:8px;margin-top:16px">Последние экземпляры</div>
    <div id="modal-inst-tbl"></div>
  `;

  // Timeline mini chart
  mkChart('modal-chart-tl', lineChartCfg(tlLabels, [
    ds('Старты',     tl.map(r => r.started),   COLORS.blue, { fill: true }),
    ds('Завершения', tl.map(r => r.completed), COLORS.green),
  ], { legend: true, maxTicks: 24, rotateX: 45 }));

  // Block stats
  buildTable('modal-block-tbl', [
    { key: 'block_id',    label: 'Блок',  render: v => `<code>${v}</code>` },
    { key: 'scheme_id',   label: 'Схема', render: v => fmtNum(v) },
    { key: 'activations', label: 'Актив-й', render: v => fmtNum(v) },
    { key: 'abortions',   label: 'Прервано',
      render: v => v > 0 ? badge(v, 'red') : '0' },
    { key: 'avg_min',     label: 'Ср. (мин)',
      render: v => v > 60 ? `<span style="color:var(--amber)">${fmtNum(v,0)} мин</span>`
                          : `${fmtNum(v,0)} мин` },
    { key: 'p95_min',     label: 'P95 (мин)', render: v => `${fmtNum(v,0)} мин` },
  ], bs, { sortKey: 'avg_min', sortDir: 'desc' });

  // Instance list
  buildTable('modal-inst-tbl', [
    { key: 'instance_id',    label: 'ID',     render: v => `<code>${v}</code>` },
    { key: 'start_time',     label: 'Старт',  render: v => fmtTs(v) },
    { key: 'status',         label: 'Статус', render: v => statusBadge(v) },
    { key: 'duration_hours', label: 'Длит.',  render: v => fmtDur(v) },
    { key: 'block_count',    label: 'Блоков', render: v => fmtNum(v) },
    { key: 'scheme_id',      label: 'Схема',  render: v => fmtNum(v) },
  ], inst.slice(0, 50), { sortKey: 'start_time', sortDir: 'desc' });
}

function closeModal() {
  document.getElementById('modal-backdrop')?.classList.remove('open');
}
document.getElementById('modal-backdrop')?.addEventListener('click', e => {
  if (e.target.id === 'modal-backdrop') closeModal();
});

// ── Init ──────────────────────────────────────────────────────────────────────
pollStatus();
