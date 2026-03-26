'use strict';

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
        x: {
          stacked: opts.stacked,
          grid: { display: false },
          ticks: { maxRotation: opts.rotateX ?? 0, font: { size: 11 } },
        },
        y: {
          stacked: opts.stacked,
          beginAtZero: true,
          ticks: { font: { size: 11 } },
          title: opts.yTitle ? { display: true, text: opts.yTitle } : undefined,
        },
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
        x: {
          grid: { display: false },
          ticks: { maxRotation: opts.rotateX ?? 45, font: { size: 10 },
                   maxTicksLimit: opts.maxTicks ?? 24 },
        },
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
