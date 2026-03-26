'use strict';

async function loadDashboard() {
  if (_dashLoaded) return;

  const [overview, procs, tl, heatmap, hist] = await Promise.all([
    api('/overview'),
    api('/processes'),
    api('/timeline', { granularity: 'hour' }),
    api('/heatmap'),
    api('/histogram'),
  ]);

  _dashLoaded = true;

  // Period badge
  const p = overview.period || {};
  const from = p.period_from ? fmtTs(p.period_from) : '';
  const to   = p.period_to   ? fmtTs(p.period_to)   : '';
  const badge_el = document.getElementById('dash-period');
  if (badge_el && from) badge_el.textContent = `${from} – ${to}`;

  // KPI row 1
  const I = overview.instances;
  document.getElementById('kpi-row1').innerHTML = `
    <div class="kpi accent-orange">
      <div class="kpi-label">Всего экземпляров</div>
      <div class="kpi-value" id="kpi-total"></div>
      <div class="kpi-sub">${overview.unique_process_types} типов процессов</div>
    </div>
    <div class="kpi accent-green">
      <div class="kpi-label">Завершено</div>
      <div class="kpi-value green" id="kpi-done"></div>
      <div class="kpi-sub">${fmtPct(I.completion_rate)} от всех</div>
    </div>
    <div class="kpi accent-red">
      <div class="kpi-label">Прервано</div>
      <div class="kpi-value red" id="kpi-aborted"></div>
      <div class="kpi-sub">${fmtPct(I.abort_rate)} от всех</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">В работе</div>
      <div class="kpi-value" id="kpi-active"></div>
      <div class="kpi-sub">Без завершения в окне</div>
    </div>
  `;
  countUp(document.getElementById('kpi-total'),   I.total,       0);
  countUp(document.getElementById('kpi-done'),    I.completed,   0);
  countUp(document.getElementById('kpi-aborted'), I.aborted,     0);
  countUp(document.getElementById('kpi-active'),  I.in_progress, 0);

  // KPI row 2
  const P = overview.performance;
  const B = overview.blocks;
  const E = overview.errors;
  document.getElementById('kpi-row2').innerHTML = `
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

  // Insight chips
  renderInsights(overview, procs);

  // Hourly line chart
  const buckets = tl.map(r => {
    const [date, time] = r.bucket.split(' ');
    const [, m, d] = date.split('-');
    return `${d}.${m} ${time.slice(0, 2)}h`;
  });
  mkChart('chart-hourly', lineChartCfg(buckets, [
    ds('Старты',      tl.map(r => r.process_starts),      COLORS.orange, { fill: true }),
    ds('Завершения',  tl.map(r => r.process_completions), COLORS.green),
    ds('Прерывания',  tl.map(r => r.process_abortions),   COLORS.red),
  ], { legend: true, maxTicks: 48, rotateX: 45 }));

  // Heatmap
  renderHeatmap(heatmap);

  // Doughnut
  const top10 = procs.slice(0, 10);
  const rest  = procs.slice(10).reduce((s, p) => s + p.total, 0);
  const pieLabels = top10.map(p => p.display_name || p.name || p.process_id);
  const pieData   = top10.map(p => p.total);
  if (rest > 0) { pieLabels.push('Прочие'); pieData.push(rest); }
  mkChart('chart-pie', {
    type: 'doughnut',
    data: {
      labels: pieLabels,
      datasets: [{ data: pieData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#FFFFFF' }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } },
        tooltip: { callbacks: {
          label: ctx => ` ${ctx.label}: ${fmtNum(ctx.raw)} (${fmtPct(ctx.raw / I.total * 100)})`,
        }},
      },
      cutout: '60%',
    },
  });

  // Histogram
  renderHistogram(hist);

  // Top processes table
  buildTable('dash-top-proc', [
    { key: 'display_name', label: 'Процесс',
      render: (v, r) => `<span style="cursor:pointer;color:var(--blue)"
        onclick="openProcessModalById('${r.process_id}','${(v || '').replace(/'/g, "\\'")}')">
        <strong>${v || r.name || r.process_id}</strong></span>` },
    { key: 'total',           label: 'Экз-ры',      render: v => fmtNum(v) },
    { key: 'completion_rate', label: '% завершения',
      render: v => `${fmtPct(v)}<br>${bar(v, 'green')}` },
    { key: 'abort_rate',      label: '% прерваний',
      render: v => `${fmtPct(v)}<br>${bar(v, 'red')}` },
    { key: 'avg_duration_hours',    label: 'Ср. длит.',  render: v => fmtDur(v) },
    { key: 'median_duration_hours', label: 'Медиана',    render: v => fmtDur(v) },
    { key: 'avg_blocks',            label: 'Блоков/экз', render: v => fmtNum(v, 1) },
  ], procs.slice(0, 15), { sortKey: 'total', sortDir: 'desc',
    clickable: true, onRowClick: 'openProcessModal',
    rowData: ['process_id', 'display_name'] });
}

// ── Heatmap (dow × hour CSS grid) ────────────────────────────────────────
function renderHeatmap(data) {
  const container = document.getElementById('heatmap-container');
  if (!container) return;
  if (!data || data.length === 0) { container.innerHTML = empty('Нет данных тепловой карты'); return; }

  // DuckDB EXTRACT('dow'): 0=Sun, 1=Mon, ..., 6=Sat
  const DOW_LABELS = ['Вс', 'Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб'];
  const maxVal = Math.max(...data.map(r => (r.starts || 0) + (r.activations || 0)), 1);

  // Build lookup: dow → hour → value
  const lookup = {};
  data.forEach(r => {
    if (!lookup[r.dow]) lookup[r.dow] = {};
    lookup[r.dow][r.hour] = r.starts || 0;
  });

  // Hours label row
  let html = '<div class="heatmap-grid">';
  html += '<div class="heatmap-label"></div>';
  for (let h = 0; h < 24; h++) {
    html += `<div class="heatmap-label" style="font-size:10px;justify-content:center">${h}</div>`;
  }

  // Data rows (1=Mon…6=Sat, then 0=Sun)
  const dowOrder = [1, 2, 3, 4, 5, 6, 0];
  for (const dow of dowOrder) {
    html += `<div class="heatmap-label">${DOW_LABELS[dow]}</div>`;
    for (let h = 0; h < 24; h++) {
      const val = (lookup[dow] || {})[h] || 0;
      const alpha = val > 0 ? 0.15 + 0.85 * (val / maxVal) : 0;
      const bg = val > 0 ? `rgba(255,122,0,${alpha.toFixed(2)})` : '#F4F4F4';
      html += `<div class="heatmap-cell" style="background:${bg}" title="${DOW_LABELS[dow]} ${h}:00 — ${val} стартов"></div>`;
    }
  }
  html += '</div>';
  container.innerHTML = html;
}

// ── Histogram ─────────────────────────────────────────────────────────────
function renderHistogram(hist) {
  if (!hist || hist.length === 0) return;
  mkChart('chart-histogram', barChartCfg(
    hist.map(r => r.bucket),
    [{
      label: 'Экземпляры',
      data: hist.map(r => r.count),
      backgroundColor: COLORS.blue + 'cc',
      borderColor: COLORS.blue,
      borderWidth: 1,
    }],
    { rotateX: 45, yTitle: 'Кол-во' }
  ));
}

// ── Insight chips ──────────────────────────────────────────────────────────
function renderInsights(overview, procs) {
  const row = document.getElementById('insight-row');
  if (!row) return;

  const chips = [];
  const I = overview.instances;
  const E = overview.errors;
  const P = overview.performance;

  if (I.completion_rate < 70) {
    chips.push({ icon: '⚠️', text: `Завершаемость низкая: ${fmtPct(I.completion_rate)}`, color: 'red' });
  } else if (I.completion_rate > 90) {
    chips.push({ icon: '✅', text: `Отличная завершаемость: ${fmtPct(I.completion_rate)}`, color: 'green' });
  }

  if (E.lock_contentions > 50) {
    chips.push({ icon: '🔒', text: `Много блокировок: ${fmtNum(E.lock_contentions)}`, color: 'amber' });
  }

  if (P.p95_duration_sec > 86400) {
    chips.push({ icon: '🐢', text: `P95 > 24ч: ${fmtSec(P.p95_duration_sec)}`, color: 'amber' });
  }

  const topAbort = procs.reduce((a, b) => (b.abort_rate > (a?.abort_rate || 0) ? b : a), null);
  if (topAbort && topAbort.abort_rate > 20) {
    chips.push({ icon: '🔴', text: `${topAbort.display_name || topAbort.process_id}: ${fmtPct(topAbort.abort_rate)} прерываний`, color: 'red' });
  }

  if (chips.length === 0) {
    chips.push({ icon: '💚', text: 'Всё в норме — критических проблем не выявлено', color: 'green' });
  }

  row.innerHTML = chips.map(c =>
    `<div class="insight-chip chip-${c.color}">${c.icon} ${c.text}</div>`
  ).join('');
}
