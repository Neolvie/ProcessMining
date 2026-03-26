'use strict';

async function loadTimeline() {
  const gran = document.getElementById('tl-gran')?.value || 'hour';
  const tl = await api('/timeline', { granularity: gran });

  const fmt = gran === 'hour'
    ? r => { const [date, time] = r.bucket.split(' '); const [, m, d] = date.split('-'); return `${d}.${m} ${time.slice(0, 2)}h`; }
    : r => { const [, m, d] = r.bucket.split('-'); return `${d}.${m}`; };

  const labels = tl.map(fmt);

  mkChart('chart-tl-proc', lineChartCfg(labels, [
    ds('Старты процессов',  tl.map(r => r.process_starts),      COLORS.orange, { fill: true }),
    ds('Завершения',        tl.map(r => r.process_completions), COLORS.green),
    ds('Прерывания',        tl.map(r => r.process_abortions),   COLORS.red),
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
