'use strict';

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

// Animated count-up for KPI numbers
function countUp(el, target, decimals = 0, duration = 700) {
  const start = performance.now();
  const num = parseFloat(target) || 0;
  function tick(now) {
    const t = Math.min((now - start) / duration, 1);
    const ease = 1 - Math.pow(1 - t, 3);
    el.textContent = fmtNum(num * ease, decimals);
    if (t < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}
