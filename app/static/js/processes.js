'use strict';

async function loadProcesses() {
  if (state.processesData.length) { renderProcessTable(); return; }
  document.getElementById('proc-table').innerHTML = loading();
  state.processesData = await api('/processes');

  // Populate process filters in Blocks and Flow views
  ['block-proc-filter', 'flow-proc-filter'].forEach(id => {
    const sel = document.getElementById(id);
    if (!sel) return;
    sel.innerHTML = '<option value="">Все процессы</option>' +
      state.processesData.map(p =>
        `<option value="${p.process_id}">${p.display_name || p.name || p.process_id}</option>`
      ).join('');
  });

  renderProcessTable();
}

function filterProcesses() {
  renderProcessTable(document.getElementById('proc-search')?.value || '');
}

function renderProcessTable(search = '') {
  const rows = search
    ? state.processesData.filter(p =>
        (p.display_name || p.name || '').toLowerCase().includes(search.toLowerCase()))
    : state.processesData;

  buildTable('proc-table', [
    { key: 'display_name', label: 'Процесс',
      render: (v, r) => `<span class="proc-link" data-pid="${r.process_id}" style="cursor:pointer;color:var(--blue)">
        <strong>${v || r.name || r.process_id}</strong></span>` },
    { key: 'total',               label: 'Экз-ры',       render: v => fmtNum(v) },
    { key: 'completed',           label: 'Завершено',     render: v => fmtNum(v) },
    { key: 'aborted',             label: 'Прервано',      render: v => fmtNum(v) },
    { key: 'in_progress',         label: 'В работе',      render: v => fmtNum(v) },
    { key: 'completion_rate',     label: '% завершения',
      render: v => `${fmtPct(v)} ${bar(v, 'green')}` },
    { key: 'abort_rate',          label: '% прерваний',
      render: v => `${fmtPct(v)} ${bar(v, 'red')}` },
    { key: 'avg_duration_hours',    label: 'Ср. длит.',   render: v => fmtDur(v) },
    { key: 'median_duration_hours', label: 'Медиана',     render: v => fmtDur(v) },
    { key: 'p95_duration_hours',    label: 'P95',         render: v => fmtDur(v) },
    { key: 'avg_blocks',            label: 'Блоков/экз',  render: v => fmtNum(v, 1) },
    { key: 'total_blocks',          label: 'Всего блоков',render: v => fmtNum(v) },
  ], rows, { sortKey: 'total', sortDir: 'desc' });
}
