'use strict';

let _uploadFiles = [];  // staged files

async function loadUpload() {
  await refreshLogList();
}

// ── File list (current logs on server) ───────────────────────────────────────
async function refreshLogList() {
  const el = document.getElementById('log-file-list');
  if (!el) return;
  el.innerHTML = loading('Загрузка списка файлов…');
  try {
    const files = await api('/logs');
    renderLogList(files);
  } catch {
    el.innerHTML = empty('Не удалось загрузить список файлов');
  }
}

function renderLogList(files) {
  const el = document.getElementById('log-file-list');
  const summary = document.getElementById('log-files-summary');
  if (!el) return;

  if (!files || files.length === 0) {
    el.innerHTML = empty('Файлов логов нет. Загрузите первый архив.');
    if (summary) summary.textContent = '0 файлов';
    return;
  }

  const totalMb = files.reduce((s, f) => s + f.size_mb, 0);
  if (summary) summary.textContent = `${files.length} файл${files.length === 1 ? '' : files.length < 5 ? 'а' : 'ов'} · ${totalMb.toFixed(1)} МБ`;

  el.innerHTML = files.map(f => `
    <div class="log-file-row">
      <div class="log-file-icon">📄</div>
      <div class="log-file-name" title="${f.name}">${f.name}</div>
      <div class="log-file-size">${f.size_mb.toFixed(1)} МБ</div>
      <button class="log-file-del" onclick="deleteLogFile('${f.name.replace(/'/g, "\\'")}')"
              title="Удалить файл и пересчитать">×</button>
    </div>
  `).join('');
}

async function deleteLogFile(filename) {
  if (!confirm(`Удалить файл «${filename}»?\n\nБаза данных будет пересчитана.`)) return;

  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '…';

  try {
    await fetch(`/api/logs/${encodeURIComponent(filename)}`, { method: 'DELETE' });
    showUploadStatus('reloading', `Файл «${filename}» удалён. Пересчёт…`);
    pollAfterChange();
  } catch (e) {
    btn.disabled = false;
    btn.textContent = '×';
    showUploadStatus('error', `Ошибка удаления: ${e.message}`);
  }
}

// ── Drop zone ─────────────────────────────────────────────────────────────────
function initDropZone() {
  const zone = document.getElementById('drop-zone');
  const input = document.getElementById('file-input');
  if (!zone || !input) return;

  zone.addEventListener('click', () => input.click());

  zone.addEventListener('dragover', e => {
    e.preventDefault();
    zone.classList.add('drag-over');
  });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('drag-over');
    stageFiles([...e.dataTransfer.files]);
  });

  input.addEventListener('change', () => {
    stageFiles([...input.files]);
    input.value = '';
  });
}

function stageFiles(newFiles) {
  for (const f of newFiles) {
    if (!_uploadFiles.find(x => x.name === f.name && x.size === f.size)) {
      _uploadFiles.push(f);
    }
  }
  renderStagedFiles();
}

function removeStagedFile(idx) {
  _uploadFiles.splice(idx, 1);
  renderStagedFiles();
}

function renderStagedFiles() {
  const el = document.getElementById('staged-files');
  const btn = document.getElementById('btn-upload');
  if (!el) return;

  if (_uploadFiles.length === 0) {
    el.innerHTML = '';
    if (btn) btn.disabled = true;
    return;
  }

  if (btn) btn.disabled = false;
  const totalMb = _uploadFiles.reduce((s, f) => s + f.size, 0) / 1024 / 1024;

  el.innerHTML = `
    <div class="staged-header">
      Выбрано: ${_uploadFiles.length} файл${_uploadFiles.length < 2 ? '' : _uploadFiles.length < 5 ? 'а' : 'ов'}
      · ${totalMb.toFixed(1)} МБ
      <button class="btn-ghost btn-sm" onclick="_uploadFiles=[];renderStagedFiles()">Очистить</button>
    </div>
    ${_uploadFiles.map((f, i) => `
      <div class="staged-file">
        <span class="staged-icon">${iconForFile(f.name)}</span>
        <span class="staged-name">${f.name}</span>
        <span class="staged-size">${(f.size / 1024 / 1024).toFixed(1)} МБ</span>
        <button class="log-file-del" onclick="removeStagedFile(${i})">×</button>
      </div>
    `).join('')}
  `;
}

function iconForFile(name) {
  const low = name.toLowerCase();
  if (low.endsWith('.zip') || low.endsWith('.tar.gz') || low.endsWith('.tgz')) return '🗜️';
  if (low.endsWith('.gz')) return '📦';
  return '📄';
}

// ── Upload ────────────────────────────────────────────────────────────────────
function startUpload() {
  if (_uploadFiles.length === 0) return;

  const btn = document.getElementById('btn-upload');
  const bar = document.getElementById('upload-progress-wrap');
  const prog = document.getElementById('upload-progress-bar');
  const progText = document.getElementById('upload-progress-text');

  if (btn) btn.disabled = true;
  if (bar) bar.style.display = '';

  const formData = new FormData();
  for (const f of _uploadFiles) formData.append('files', f);

  const xhr = new XMLHttpRequest();

  xhr.upload.addEventListener('progress', e => {
    if (!e.lengthComputable) return;
    const pct = Math.round(e.loaded / e.total * 100);
    if (prog) prog.style.width = pct + '%';
    if (progText) progText.textContent = `Передача: ${pct}%`;
  });

  xhr.addEventListener('load', () => {
    if (prog) prog.style.width = '100%';
    if (progText) { progText.style.display = ''; progText.textContent = 'Анализ данных…'; }

    if (xhr.status >= 200 && xhr.status < 300) {
      const res = JSON.parse(xhr.responseText);
      showUploadStatus('reloading',
        `Загружено ${res.saved.length} файл(ов). Пересчёт базы данных…`);
      _uploadFiles = [];
      renderStagedFiles();
      pollAfterChange();
    } else {
      let msg = 'Ошибка загрузки';
      try { msg = JSON.parse(xhr.responseText).detail || msg; } catch {}
      showUploadStatus('error', msg);
      if (btn) btn.disabled = false;
      if (bar) bar.style.display = 'none';
    }
  });

  xhr.addEventListener('error', () => {
    showUploadStatus('error', 'Сетевая ошибка при передаче файла');
    if (btn) btn.disabled = false;
    if (bar) bar.style.display = 'none';
  });

  xhr.open('POST', '/api/upload');
  xhr.send(formData);
}

// ── Poll after upload/delete ──────────────────────────────────────────────────
function pollAfterChange() {
  const progText = document.getElementById('upload-progress-text');

  const interval = setInterval(async () => {
    try {
      const s = await api('/status');
      if (s.status === 'ready') {
        clearInterval(interval);

        // Update header status
        const dot = document.getElementById('status-dot');
        const txt = document.getElementById('status-text');
        if (dot) dot.classList.remove('loading');
        const meta = s.meta || {};
        if (txt) txt.innerHTML =
          `Данные загружены за ${s.total_seconds}с &nbsp;·&nbsp; ` +
          `${fmtNum(meta.total_lines)} строк &nbsp;·&nbsp; ` +
          `${fmtNum(meta.parsed_events)} событий &nbsp;·&nbsp; ` +
          `${(meta.files || []).length} файлов`;

        showUploadStatus('ok',
          `Готово! Разобрано ${fmtNum(meta.parsed_events)} событий из ${(meta.files || []).length} файлов за ${s.parse_seconds}с`);

        const bar = document.getElementById('upload-progress-wrap');
        if (bar) bar.style.display = 'none';

        const btn = document.getElementById('btn-upload');
        if (btn) btn.disabled = false;

        // Refresh file list and reset dashboard cache
        await refreshLogList();
        _dashLoaded = false;
      } else if (s.status === 'reloading') {
        if (progText) progText.textContent = `Пересчёт данных… (${fmtNum(s.meta?.parsed_events)} событий обработано)`;
      }
    } catch {}
  }, 1000);
}

function showUploadStatus(type, text) {
  const el = document.getElementById('upload-status');
  if (!el) return;
  const icons = { ok: '✅', error: '❌', reloading: '⏳' };
  const colors = { ok: 'var(--green)', error: 'var(--red)', reloading: 'var(--amber)' };
  el.style.display = '';
  el.innerHTML = `<span style="color:${colors[type]}">${icons[type]} ${text}</span>`;
}

// Initialise drop zone when view becomes active
const _origNavigate = window._origNavigate;
document.addEventListener('DOMContentLoaded', () => {
  initDropZone();
});
