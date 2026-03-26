'use strict';

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
    { legend: true, rotateX: 45, yTitle: 'Часов' }
  ));
  buildTable('tbl-slow-blocks', [
    { key: 'block_id',     label: 'Блок',     render: v => `<code>${v}</code>` },
    { key: 'scheme_id',    label: 'Схема',    render: v => fmtNum(v) },
    { key: 'process_name', label: 'Процесс' },
    { key: 'activations',  label: 'Актив-й',  render: v => fmtNum(v) },
    { key: 'avg_hours',    label: 'Ср. длит.',
      render: v => `<strong style="color:var(--amber)">${fmtDur(v)}</strong>` },
    { key: 'p95_hours',    label: 'P95',      render: v => fmtDur(v) },
    { key: 'max_hours',    label: 'Макс',
      render: v => `<span style="color:var(--red)">${fmtDur(v)}</span>` },
  ], sb, { sortKey: 'avg_hours', sortDir: 'desc' });

  // Slow processes
  const sp = d.slow_processes || [];
  mkChart('chart-slow-proc', barChartCfg(
    sp.map(r => r.display_name || r.process_id),
    [{ label: 'Ср. (ч)', data: sp.map(r => r.avg_hours || 0), backgroundColor: PALETTE }],
    { rotateX: 45, yTitle: 'Часов' }
  ));
  buildTable('tbl-slow-proc', [
    { key: 'display_name', label: 'Процесс' },
    { key: 'total',        label: 'Экз-ры',   render: v => fmtNum(v) },
    { key: 'avg_hours',    label: 'Ср. длит.',
      render: v => `<strong style="color:var(--amber)">${fmtDur(v)}</strong>` },
    { key: 'p95_hours',    label: 'P95',      render: v => fmtDur(v) },
    { key: 'max_hours',    label: 'Макс',
      render: v => `<span style="color:var(--red)">${fmtDur(v)}</span>` },
  ], sp, { sortKey: 'avg_hours', sortDir: 'desc' });

  // High abort blocks
  const ab = d.high_abort_blocks || [];
  mkChart('chart-high-abort', barChartCfg(
    ab.map(r => `[${r.scheme_id}] ${r.block_id}`),
    [{ label: '% прерваний', data: ab.map(r => r.abort_rate || 0),
       backgroundColor: ab.map(r => r.abort_rate > 30 ? COLORS.red + 'cc' : COLORS.amber + 'cc'),
       borderColor: ab.map(r => r.abort_rate > 30 ? COLORS.red : COLORS.amber),
       borderWidth: 1 }],
    { rotateX: 45, yTitle: '%' }
  ));
  buildTable('tbl-high-abort', [
    { key: 'block_id',     label: 'Блок',      render: v => `<code>${v}</code>` },
    { key: 'scheme_id',    label: 'Схема',     render: v => fmtNum(v) },
    { key: 'process_name', label: 'Процесс' },
    { key: 'activations',  label: 'Активаций', render: v => fmtNum(v) },
    { key: 'abortions',    label: 'Прервано',  render: v => fmtNum(v) },
    { key: 'abort_rate',   label: '% прерваний',
      render: v => v > 30 ? badge(fmtPct(v), 'red') : badge(fmtPct(v), 'amber') },
  ], ab, { sortKey: 'abort_rate', sortDir: 'desc' });
}
