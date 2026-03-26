'use strict';

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
    { legend: true, rotateX: 45 }
  ));
  buildTable('tbl-abort-proc', [
    { key: 'display_name', label: 'Процесс' },
    { key: 'total',        label: 'Всего',    render: v => fmtNum(v) },
    { key: 'aborted',      label: 'Прервано', render: v => fmtNum(v) },
    { key: 'abort_rate',   label: '% прерваний',
      render: v => v > 20 ? badge(fmtPct(v), 'red') : v > 5 ? badge(fmtPct(v), 'amber') : fmtPct(v) },
  ], abp, { sortKey: 'abort_rate', sortDir: 'desc' });

  // Lock contentions by hour
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
    { key: 'timestamp',    label: 'Время',         render: v => fmtTs(v) },
    { key: 'instance_id',  label: 'Экз-р',        render: v => `<code>${v}</code>` },
    { key: 'block_id',     label: 'Блок',          render: v => `<code>${v}</code>` },
    { key: 'message_type', label: 'Тип сообщения' },
    { key: 'host',         label: 'Хост' },
  ], d.failed_spans || [], { sortKey: 'timestamp', sortDir: 'desc' });
}
