"""
Log parser for Directum RX WorkflowProcessService logs.
Parses JSON-lines log files and extracts structured events.
"""

import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

try:
    import orjson as json_lib
    def _loads(s): return json_lib.loads(s)
except ImportError:
    import json as json_lib
    def _loads(s): return json_lib.loads(s)

# Known message templates → event types
_MT_MAP = {
    "Process started, InstanceId = {instanceId}, ProcessId = {processId}, Version = {version}, StartId = {startId}": "process_started",
    "Process completed. InstanceId = {instanceId}, SchemeId = {schemeId}": "process_completed",
    "Process aborted. InstanceId = {instanceId}, SchemeId = {schemeId}": "process_aborted",
    "Block activated, Id = {blockId}, IterationId = {iterationId}, InstanceId = {instanceId}, SchemeId = {instanceSchemeId}, SchemeVersionId = {schemeVersionId}": "block_activated",
    "Block completed. Id = {blockId}, Result = {blockResult}, InstanceId = {instanceId}, SchemeId = {instanceSchemeId}": "block_completed",
    "Block aborted, Id = {blockId}, InstanceId = {instanceId}, SchemeId = {instanceSchemeId}": "block_aborted",
}

_LOCK_RE = re.compile(r'Object = (\d+) already locked')


def _get_host(filepath: str) -> str:
    """Extract short host name from filename like aurahost1-workflowprocess..."""
    name = Path(filepath).name
    # e.g. aurahost1-workflowprocessservice.WorkflowProcessService.2026-03-23.log
    return name.split('-')[0]


def _parse_ts(ts_str: str) -> Optional[datetime]:
    """Parse '2026-03-23 00:00:04.443+04:00' → UTC datetime."""
    try:
        return datetime.fromisoformat(ts_str.replace(' ', 'T')).astimezone(timezone.utc)
    except Exception:
        return None


def _initiator(trace_id: str) -> str:
    """Map trace ID prefix to human-readable initiator type."""
    prefix = trace_id.split('-')[0] if trace_id else ''
    return {
        'cl': 'client',
        'js': 'scheduler',
        'it': 'integration',
        'wp': 'workflow',
        'do': 'document',
    }.get(prefix, 'other')


def _extract_event(obj: dict, host: str) -> Optional[dict]:
    """Extract a structured event dict from a parsed log line."""
    mt = obj.get('mt', '')
    args = obj.get('args') or {}
    span = obj.get('span') or {}
    lg = obj.get('lg', '')
    level = obj.get('l', '')
    tr = obj.get('tr', '')

    ts = _parse_ts(obj.get('t', ''))
    if ts is None:
        return None

    base = {
        'timestamp': ts,
        'host': host,
        'trace_id': tr,
        'initiator': _initiator(tr),
        'event_type': None,
        'instance_id': None,
        'process_id': None,
        'scheme_id': None,
        'scheme_version_id': None,
        'block_id': None,
        'block_result': None,
        'iteration_id': None,
        'duration_ms': None,
        'message_type': None,
        'span_status': None,
    }

    # ── Core process / block events ──────────────────────────────────────────
    event_type = _MT_MAP.get(mt)
    if event_type == 'process_started':
        base.update(event_type='process_started',
                    instance_id=args.get('instanceId'),
                    process_id=str(args.get('processId', '')))

    elif event_type == 'process_completed':
        base.update(event_type='process_completed',
                    instance_id=args.get('instanceId'),
                    scheme_id=args.get('schemeId'))

    elif event_type == 'process_aborted':
        base.update(event_type='process_aborted',
                    instance_id=args.get('instanceId'),
                    scheme_id=args.get('schemeId'))

    elif event_type == 'block_activated':
        base.update(event_type='block_activated',
                    instance_id=args.get('instanceId'),
                    scheme_id=args.get('instanceSchemeId'),
                    block_id=str(args.get('blockId', '')),
                    iteration_id=args.get('iterationId'),
                    scheme_version_id=str(args.get('schemeVersionId', '')))

    elif event_type == 'block_completed':
        br = args.get('blockResult')
        base.update(event_type='block_completed',
                    instance_id=args.get('instanceId'),
                    scheme_id=args.get('instanceSchemeId'),
                    block_id=str(args.get('blockId', '')),
                    block_result=str(br) if br is not None and br != '' else None)

    elif event_type == 'block_aborted':
        base.update(event_type='block_aborted',
                    instance_id=args.get('instanceId'),
                    scheme_id=args.get('instanceSchemeId'),
                    block_id=str(args.get('blockId', '')))

    # ── Telemetry spans ───────────────────────────────────────────────────────
    elif lg == 'WorkflowProcessServiceSpan' and span:
        status = span.get('status')
        if status in ('Ok', 'Failed'):
            base.update(event_type='span',
                        instance_id=span.get('instanceId'),
                        block_id=str(span['blockId']) if span.get('blockId') else None,
                        duration_ms=span.get('durationMs'),
                        message_type=span.get('messageType'),
                        span_status=status)

    # ── Lock contention warnings ──────────────────────────────────────────────
    elif level == 'Warn':
        m = _LOCK_RE.search(mt)
        if m:
            base.update(event_type='lock_contention',
                        instance_id=int(m.group(1)))

    else:
        return None

    if base['event_type'] is None:
        return None

    return base


def parse_logs(logs_dir: str) -> tuple[list[dict], dict]:
    """
    Parse all *.log files in logs_dir.

    Returns:
        events   - list of event dicts
        meta     - parsing statistics
    """
    events: list[dict] = []
    meta = {
        'files': [],
        'total_lines': 0,
        'parsed_events': 0,
        'parse_errors': 0,
    }

    log_files = sorted(Path(logs_dir).glob('*.log'))
    if not log_files:
        return events, meta

    for filepath in log_files:
        host = _get_host(str(filepath))
        file_events = 0
        file_errors = 0
        file_lines = 0

        with open(filepath, encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                file_lines += 1
                try:
                    obj = _loads(line)
                    event = _extract_event(obj, host)
                    if event:
                        events.append(event)
                        file_events += 1
                except Exception:
                    file_errors += 1

        meta['files'].append({
            'name': Path(filepath).name,
            'host': host,
            'lines': file_lines,
            'events': file_events,
            'errors': file_errors,
        })
        meta['total_lines'] += file_lines
        meta['parsed_events'] += file_events
        meta['parse_errors'] += file_errors

    return events, meta
