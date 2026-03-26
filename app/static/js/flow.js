'use strict';

async function loadFlow() {
  document.getElementById('flow-table').innerHTML = loading();
  const procId = document.getElementById('flow-proc-filter')?.value || '';
  const data = await api('/flow', { process_id: procId || undefined, top_n: 40 });

  buildTable('flow-table', [
    { key: 'from_block',       label: 'Из блока',     render: v => `<code>${v}</code>` },
    { key: 'to_block',         label: 'В блок',       render: v => `<code>${v}</code>` },
    { key: 'scheme_id',        label: 'Схема',        render: v => fmtNum(v) },
    { key: 'transition_count', label: 'Переходов',    render: v => fmtNum(v) },
    { key: 'avg_gap_sec',      label: 'Ср. интервал', render: v => fmtSec(v) },
  ], data, { sortKey: 'transition_count', sortDir: 'desc' });

  const top20 = data.slice(0, 20);
  mkChart('chart-flow', barChartCfg(
    top20.map(r => `${r.from_block}→${r.to_block}`),
    [{ label: 'Переходов', data: top20.map(r => r.transition_count),
       backgroundColor: COLORS.blue + 'cc', borderColor: COLORS.blue, borderWidth: 1 }],
    { rotateX: 45, yTitle: 'Кол-во' }
  ));
}
