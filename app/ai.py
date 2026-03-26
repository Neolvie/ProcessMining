"""
AI insights module — calls OpenRouter (OpenAI-compatible) to analyse
Process Mining data and stream recommendations in Russian.
"""

from __future__ import annotations
import os
import json
import logging
from typing import Iterator

logger = logging.getLogger("uvicorn.error")

_ENABLED: bool | None = None   # lazy-init


def is_enabled() -> bool:
    global _ENABLED
    if _ENABLED is None:
        _ENABLED = bool(os.getenv("OPENROUTER_API_KEY", "").strip())
    return _ENABLED


def _client():
    from openai import OpenAI
    return OpenAI(
        api_key=os.getenv("OPENROUTER_API_KEY", ""),
        base_url=os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
    )


def _fmt_dur(hours: float | None) -> str:
    if hours is None:
        return "н/д"
    if hours < 1:
        return f"{round(hours*60)} мин"
    h = int(hours)
    m = round((hours - h) * 60)
    return f"{h}ч {m}м" if m else f"{h}ч"


def _fmt_sec(sec: float | None) -> str:
    if sec is None:
        return "н/д"
    return _fmt_dur(sec / 3600)


def build_prompt(data: dict) -> str:
    ov   = data["overview"]
    inst = ov["instances"]
    perf = ov["performance"]
    blks = ov["blocks"]
    err  = ov["errors"]
    p    = ov["period"]

    lines: list[str] = []
    lines.append(
        f"Ты эксперт по Process Mining и оптимизации бизнес-процессов. "
        f"Проанализируй данные системы документооборота Directum RX за период "
        f"{p.get('period_from','?')} – {p.get('period_to','?')} и дай конкретные "
        f"рекомендации на РУССКОМ языке. Будь лаконичен и конкретен.\n"
    )

    # ── Overview ────────────────────────────────────────────────────────────
    lines.append("## ОБЩАЯ СТАТИСТИКА")
    lines.append(f"- Всего экземпляров процессов: **{inst['total']}** ({ov['unique_process_types']} типов)")
    lines.append(f"- Завершено: **{inst['completed']}** ({inst['completion_rate']}%)")
    lines.append(f"- Прервано: **{inst['aborted']}** ({inst['abort_rate']}%)")
    lines.append(f"- В работе (нет завершения в окне): **{inst['in_progress']}**")
    lines.append(f"- Среднее время процесса: **{_fmt_sec(perf['avg_duration_sec'])}**")
    lines.append(f"- Медиана: **{_fmt_sec(perf['median_duration_sec'])}**, P95: **{_fmt_sec(perf['p95_duration_sec'])}**")
    lines.append(f"- Блоков активировано: **{blks['activations']}**, прервано блоков: **{blks['abortions']}** ({blks['abort_rate']}%)")
    lines.append(f"- Блокировки объектов: **{err['lock_contentions']}**, сбои обработки: **{err['failed_spans']}**\n")

    # ── Top processes ────────────────────────────────────────────────────────
    lines.append("## ТОП ПРОЦЕССОВ (по количеству экземпляров)")
    for p2 in data["top_processes"][:10]:
        lines.append(
            f"- **{p2['display_name']}**: {p2['total']} экз., "
            f"завершено {p2['completion_rate']}%, прервано {p2['abort_rate']}%, "
            f"ср. {_fmt_dur(p2['avg_duration_hours'])}, P95 {_fmt_dur(p2['p95_duration_hours'])}"
        )
    lines.append("")

    # ── Slow blocks ──────────────────────────────────────────────────────────
    if data["slow_blocks"]:
        lines.append("## МЕДЛЕННЫЕ БЛОКИ (узкие места по длительности)")
        for b in data["slow_blocks"][:7]:
            lines.append(
                f"- Блок **{b['block_id']}** схема {b['scheme_id']} "
                f"({b.get('process_name','?')}): "
                f"ср. {_fmt_dur(b['avg_hours'])}, P95 {_fmt_dur(b['p95_hours'])}, "
                f"макс {_fmt_dur(b['max_hours'])}, активаций {b['activations']}"
            )
        lines.append("")

    # ── High abort blocks ────────────────────────────────────────────────────
    if data["high_abort_blocks"]:
        lines.append("## БЛОКИ С ВЫСОКИМ % ПРЕРЫВАНИЙ")
        for b in data["high_abort_blocks"][:7]:
            lines.append(
                f"- Блок **{b['block_id']}** схема {b['scheme_id']} "
                f"({b.get('process_name','?')}): "
                f"{b['abort_rate']}% прерываний ({b['abortions']}/{b['activations']})"
            )
        lines.append("")

    # ── Abort by process ─────────────────────────────────────────────────────
    if data["abort_by_process"]:
        lines.append("## ПРЕРЫВАНИЯ ПО ТИПАМ ПРОЦЕССОВ")
        for p3 in data["abort_by_process"][:7]:
            lines.append(
                f"- **{p3['display_name']}**: {p3['abort_rate']}% прерываний "
                f"({p3['aborted']} из {p3['total']})"
            )
        lines.append("")

    # ── Anomalies ────────────────────────────────────────────────────────────
    if data["lock_events_total"] > 0 or data["failed_spans_total"] > 0:
        lines.append("## ТЕХНИЧЕСКИЕ АНОМАЛИИ")
        if data["lock_events_total"] > 0:
            lines.append(f"- Событий блокировки объектов: **{data['lock_events_total']}**")
        if data["failed_spans_total"] > 0:
            lines.append(f"- Сбоев обработки сообщений: **{data['failed_spans_total']}**")
        if data["long_running_count"] > 0:
            lines.append(f"- Долгих/зависших экземпляров: **{data['long_running_count']}**")
        lines.append("")

    lines.append(
        "---\n"
        "На основе этих данных дай ответ строго в следующем формате:\n\n"
        "## 🔴 Ключевые проблемы (3–5 пунктов с конкретными цифрами)\n\n"
        "## 🟡 Рекомендации по оптимизации (3–5 конкретных действий)\n\n"
        "## ✅ Приоритетные шаги (что сделать в первую очередь, нумерованный список)\n\n"
        "## 📊 Общая оценка\n"
        "Одна фраза: общее состояние процессов и главный вывод."
    )

    return "\n".join(lines)


def stream_analysis(data: dict) -> Iterator[str]:
    """Yield text chunks from the AI streaming response."""
    if not is_enabled():
        yield "⚠️ AI-анализ недоступен: не задан `OPENROUTER_API_KEY` в `.env`."
        return

    model = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
    prompt = build_prompt(data)
    logger.info(f"AI analysis: model={model}, prompt_len={len(prompt)}")

    try:
        client = _client()
        stream = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
            max_tokens=2000,
            temperature=0.3,
        )
        for chunk in stream:
            content = chunk.choices[0].delta.content or ""
            if content:
                yield content
    except Exception as e:
        logger.error(f"AI analysis error: {e}")
        yield f"\n\n❌ Ошибка при обращении к AI: {e}"
