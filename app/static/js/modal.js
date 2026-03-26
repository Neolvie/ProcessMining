'use strict';

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
  const avg = durations.length ? durations.reduce((a, b) => a + b, 0) / durations.length : null;

  const tlLabels = tl.map(r => {
    const [date, time] = r.hour.split(' ');
    const [, m, d] = date.split('-');
    return `${d}.${m} ${time.slice(0, 2)}h`;
  });

  body.innerHTML = `
    <div class="modal-title">${displayName || processId}</div>
    <div class="kpi-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:16px">
      <div class="kpi"><div class="kpi-label">Экземпляры</div>
        <div class="kpi-value">${fmtNum(inst.length)}</div></div>
      <div class="kpi accent-green"><div class="kpi-label">Завершено</div>
        <div class="kpi-value green">${fmtNum(completed)}</div>
        <div class="kpi-sub">${fmtPct(completed / inst.length * 100)}</div></div>
      <div class="kpi accent-red"><div class="kpi-label">Прервано</div>
        <div class="kpi-value red">${fmtNum(aborted)}</div>
        <div class="kpi-sub">${fmtPct(aborted / inst.length * 100)}</div></div>
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

  mkChart('modal-chart-tl', lineChartCfg(tlLabels, [
    ds('Старты',     tl.map(r => r.started),   COLORS.orange, { fill: true }),
    ds('Завершения', tl.map(r => r.completed), COLORS.green),
  ], { legend: true, maxTicks: 24, rotateX: 45 }));

  buildTable('modal-block-tbl', [
    { key: 'block_id',    label: 'Блок',       render: v => `<code>${v}</code>` },
    { key: 'scheme_id',   label: 'Схема',      render: v => fmtNum(v) },
    { key: 'activations', label: 'Актив-й',    render: v => fmtNum(v) },
    { key: 'abortions',   label: 'Прервано',
      render: v => v > 0 ? badge(v, 'red') : '0' },
    { key: 'avg_min',     label: 'Ср. (мин)',
      render: v => v > 60
        ? `<span style="color:var(--amber)">${fmtNum(v, 0)} мин</span>`
        : `${fmtNum(v, 0)} мин` },
    { key: 'p95_min',     label: 'P95 (мин)',  render: v => `${fmtNum(v, 0)} мин` },
  ], bs, { sortKey: 'avg_min', sortDir: 'desc' });

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
