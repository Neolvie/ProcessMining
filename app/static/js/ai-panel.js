'use strict';

async function loadAI() {
  // Load config info
  try {
    const s = await api('/status');
    const cfg = document.getElementById('ai-config-info');
    if (cfg) {
      if (s.ai_enabled) {
        cfg.innerHTML = `
          <div style="color:var(--green);font-weight:600;margin-bottom:6px">✅ AI включён</div>
          <div>Ключ: настроен</div>
          <div>Модель: <code style="background:var(--surface2);padding:1px 4px;border-radius:3px">${s.ai_model || 'не задана'}</code></div>
        `;
        const badge = document.getElementById('ai-model-badge');
        if (badge) {
          badge.textContent = s.ai_model || 'AI';
          badge.style.display = '';
        }
      } else {
        cfg.innerHTML = `
          <div style="color:var(--red);font-weight:600;margin-bottom:6px">⚠️ AI не настроен</div>
          <div>Задайте <code>OPENROUTER_API_KEY</code> в файле <code>.env</code></div>
          <div style="margin-top:4px;color:var(--subtle)">Скопируйте .env.example → .env и заполните ключ</div>
        `;
      }
    }
  } catch {}
}

async function runAI() {
  const output = document.getElementById('ai-output');
  const btn    = document.getElementById('btn-run-ai');
  const clearBtn = document.getElementById('btn-clear-ai');
  if (!output) return;

  output.classList.remove('empty');
  output.innerHTML = `<div class="loading-overlay" style="position:relative;height:60px">
    <div class="spinner"></div>
    <div class="loading-text">Анализирую данные…</div>
  </div>`;
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Анализирую…'; }
  if (clearBtn) clearBtn.style.display = 'none';

  let fullText = '';

  try {
    const res = await fetch('/api/ai-insights');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    output.innerHTML = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop(); // keep incomplete line

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const payload = line.slice(6).trim();
        if (payload === '[DONE]') break;
        try {
          const { text } = JSON.parse(payload);
          if (text) {
            fullText += text;
            output.innerHTML = markdownToHtml(fullText);
            output.scrollTop = output.scrollHeight;
          }
        } catch {}
      }
    }

    if (!fullText) {
      output.innerHTML = '<p style="color:var(--muted)">Ответ не получен.</p>';
    }
  } catch (err) {
    output.innerHTML = `<p style="color:var(--red)">Ошибка: ${err.message}</p>`;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '✨ Запустить анализ'; }
    if (clearBtn && fullText) clearBtn.style.display = '';
  }
}

function clearAI() {
  const output = document.getElementById('ai-output');
  const clearBtn = document.getElementById('btn-clear-ai');
  if (output) {
    output.classList.add('empty');
    output.innerHTML = 'Нажмите «Запустить анализ» — ИИ изучит данные и выдаст рекомендации на русском языке';
  }
  if (clearBtn) clearBtn.style.display = 'none';
}

// Minimal markdown → HTML renderer
function markdownToHtml(md) {
  return md
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/^#### (.+)$/gm, '<h4>$1</h4>')
    .replace(/^### (.+)$/gm, '<h4>$1</h4>')
    .replace(/^## (.+)$/gm, '<h4>$1</h4>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/`(.+?)`/g, '<code style="background:var(--surface2);padding:1px 4px;border-radius:3px">$1</code>')
    .replace(/^[-*] (.+)$/gm, '<li>$1</li>')
    .replace(/(<li>[\s\S]*?<\/li>)/g, '<ul>$1</ul>')
    .replace(/<\/ul>\s*<ul>/g, '')
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/^(?!<[hup])(.+)$/gm, (_, line) => line ? line : '')
    .replace(/^<\/p><p>/, '')
    .replace(/(<h4>|<ul>)(<\/p><p>|<p>)/g, '$1')
    .replace(/(<\/p>)(<h4>|<ul>)/g, '$2');
}
