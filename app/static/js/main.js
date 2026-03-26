'use strict';

// ── Navigation ────────────────────────────────────────────────────────────
function navigate(viewName) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const view = document.getElementById('view-' + viewName);
  if (view) view.classList.add('active');
  const nav = document.querySelector(`.nav-item[data-view="${viewName}"]`);
  if (nav) nav.classList.add('active');
  state.currentView = viewName;

  const loaders = {
    dashboard:   loadDashboard,
    timeline:    loadTimeline,
    processes:   loadProcesses,
    blocks:      loadBlocks,
    flow:        loadFlow,
    bottlenecks: loadBottlenecks,
    issues:      loadIssues,
    ai:          loadAI,
  };
  loaders[viewName]?.();
}

document.querySelectorAll('.nav-item[data-view]').forEach(el => {
  el.addEventListener('click', () => navigate(el.dataset.view));
});

// ── Tabs ──────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    const target = tab.dataset.tab;
    const parent = tab.closest('.view') || tab.parentElement.parentElement;
    parent.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    parent.querySelectorAll('.tab-content').forEach(c => { c.style.display = 'none'; });
    tab.classList.add('active');
    const content = document.getElementById(target);
    if (content) content.style.display = '';
  });
});

// ── Status poller ─────────────────────────────────────────────────────────
async function pollStatus() {
  try {
    const s = await api('/status');
    const dot = document.getElementById('status-dot');
    const txt = document.getElementById('status-text');
    if (s.status === 'ready') {
      if (dot) dot.classList.remove('loading');
      const meta = s.meta || {};
      if (txt) txt.innerHTML =
        `Данные загружены за ${s.total_seconds}с &nbsp;·&nbsp; ` +
        `${fmtNum(meta.total_lines)} строк &nbsp;·&nbsp; ` +
        `${fmtNum(meta.parsed_events)} событий &nbsp;·&nbsp; ` +
        `${(meta.files || []).length} файлов`;
      loadDashboard();
      return;
    }
    if (txt) txt.textContent = 'Загрузка данных…';
    setTimeout(pollStatus, 1000);
  } catch {
    setTimeout(pollStatus, 2000);
  }
}

// ── Delegated click: .proc-link → открыть модал процесса ─────────────────
document.addEventListener('click', e => {
  const link = e.target.closest('.proc-link');
  if (!link) return;
  const pid = link.dataset.pid;
  if (!pid) return;
  const proc = state.processesData.find(p => p.process_id === pid);
  openProcessModalById(pid, proc?.display_name || proc?.name || pid);
});

// ── Init ──────────────────────────────────────────────────────────────────
pollStatus();
