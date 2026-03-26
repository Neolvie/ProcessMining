'use strict';

async function loadBlocks() {
  document.getElementById('blocks-table').innerHTML = loading();
  const procId = document.getElementById('block-proc-filter')?.value || '';
  state.blocksData = await api('/blocks', { process_id: procId || undefined, limit: 200 });
  renderBlocksTable();
}

function renderBlocksTable() {
  const sortKey = document.getElementById('block-sort')?.value || 'avg_duration_sec';
  _sortState['blocks-table'] = { key: sortKey, dir: 'desc' };

  buildTable('blocks-table', [
    { key: 'block_id',     label: 'Блок',      render: v => `<code>${v}</code>` },
    { key: 'scheme_id',    label: 'Схема',      render: v => fmtNum(v) },
    { key: 'process_name', label: 'Процесс',    render: v => v || '—' },
    { key: 'activations',  label: 'Активаций',  render: v => fmtNum(v) },
    { key: 'completions',  label: 'Завершено',  render: v => fmtNum(v) },
    { key: 'abortions',    label: 'Прервано',
      render: v => v > 0 ? badge(v, 'red') : '0' },
    { key: 'abort_rate',   label: '% прерваний',
      render: v => v > 10 ? badge(fmtPct(v), 'red') : v > 5 ? badge(fmtPct(v), 'amber') : fmtPct(v) },
    { key: 'avg_duration_sec', label: 'Ср. длит.',
      render: v => {
        if (!v) return '—';
        const color = v > 86400 ? 'red' : v > 3600 ? 'amber' : 'muted';
        const cssVar = color === 'muted' ? 'var(--muted)' : `var(--${color})`;
        return `<span style="color:${cssVar}">${fmtSec(v)}</span>`;
      }},
    { key: 'median_duration_sec', label: 'Медиана', render: v => fmtSec(v) },
    { key: 'p95_duration_sec',    label: 'P95',      render: v => fmtSec(v) },
    { key: 'max_duration_sec',    label: 'Макс',     render: v => fmtSec(v) },
  ], state.blocksData, { sortKey, sortDir: 'desc' });
}
