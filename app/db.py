"""
DuckDB analytics layer for Process Mining.
Builds in-memory tables from parsed events and provides query functions.
"""

from __future__ import annotations

import os
import duckdb
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Optional

# Log timestamps are stored as UTC; this offset is added for display grouping
# Set TZ_OFFSET_HOURS=4 for UTC+4 (Moscow)
_TZ_H = int(os.getenv('TZ_OFFSET_HOURS', '4'))
_TZ_INTERVAL = f"INTERVAL '{_TZ_H} hours'"


# ── Global connection ─────────────────────────────────────────────────────────
_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("Database not initialized. Call build_db() first.")
    return _conn


# ── Build ─────────────────────────────────────────────────────────────────────
def build_db(events: list[dict], process_names: dict[str, dict]) -> None:
    """
    Load parsed events and process-name mapping into DuckDB.
    Creates all derived tables needed by the analytics API.
    """
    global _conn
    _conn = duckdb.connect(':memory:')
    conn = _conn

    # ── 1. Raw events table ───────────────────────────────────────────────────
    df = pd.DataFrame(events)
    if df.empty:
        df = pd.DataFrame(columns=[
            'timestamp', 'host', 'trace_id', 'initiator', 'event_type',
            'instance_id', 'process_id', 'scheme_id', 'scheme_version_id',
            'block_id', 'block_result', 'iteration_id',
            'duration_ms', 'message_type', 'span_status',
        ])

    # Ensure types
    df['instance_id'] = pd.to_numeric(df['instance_id'], errors='coerce').astype('Int64')
    df['scheme_id']   = pd.to_numeric(df['scheme_id'],   errors='coerce').astype('Int64')
    df['duration_ms'] = pd.to_numeric(df['duration_ms'], errors='coerce')
    df['iteration_id']= pd.to_numeric(df['iteration_id'],errors='coerce').astype('Int64')

    conn.execute("CREATE TABLE events AS SELECT * FROM df")

    # ── 2. Process name lookup ────────────────────────────────────────────────
    pn_rows = [
        {'process_id': pid, 'name': v['name'], 'display_name': v['display_name']}
        for pid, v in process_names.items()
    ]
    pn_df = pd.DataFrame(pn_rows) if pn_rows else pd.DataFrame(
        columns=['process_id', 'name', 'display_name'])
    conn.execute("CREATE TABLE process_names AS SELECT * FROM pn_df")

    # ── 3. scheme → process_id mapping ───────────────────────────────────────
    conn.execute("""
        CREATE TABLE scheme_process_map AS
        WITH ranked AS (
            SELECT
                bs.scheme_id,
                ps.process_id,
                COUNT(*) AS cnt,
                ROW_NUMBER() OVER (
                    PARTITION BY bs.scheme_id
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM events bs
            JOIN events ps
                ON bs.instance_id = ps.instance_id
                AND ps.event_type = 'process_started'
                AND ps.process_id IS NOT NULL
                AND ps.process_id != ''
            WHERE bs.scheme_id IS NOT NULL
            GROUP BY bs.scheme_id, ps.process_id
        )
        SELECT scheme_id, process_id
        FROM ranked
        WHERE rn = 1
    """)

    # ── 4. Instance lifecycle table ───────────────────────────────────────────
    conn.execute("""
        CREATE TABLE instance_lifecycle AS
        WITH
        starts AS (
            SELECT instance_id,
                   MIN(timestamp)   AS start_time,
                   MAX(process_id)  AS process_id
            FROM events
            WHERE event_type = 'process_started'
              AND instance_id IS NOT NULL
            GROUP BY instance_id
        ),
        ends AS (
            SELECT instance_id,
                   MIN(CASE WHEN event_type = 'process_completed' THEN timestamp END) AS completed_time,
                   MIN(CASE WHEN event_type = 'process_aborted'   THEN timestamp END) AS aborted_time,
                   MAX(CASE WHEN event_type IN ('process_completed','process_aborted')
                            THEN scheme_id END) AS scheme_id
            FROM events
            WHERE event_type IN ('process_completed','process_aborted')
              AND instance_id IS NOT NULL
            GROUP BY instance_id
        ),
        block_info AS (
            SELECT instance_id,
                   MAX(scheme_id) AS scheme_id,
                   MIN(timestamp) AS first_block_time,
                   MAX(timestamp) AS last_block_time,
                   COUNT(*)       AS block_count
            FROM events
            WHERE event_type = 'block_activated'
              AND instance_id IS NOT NULL
              AND scheme_id IS NOT NULL
            GROUP BY instance_id
        ),
        all_ids AS (
            SELECT DISTINCT instance_id FROM events WHERE instance_id IS NOT NULL
        )
        SELECT
            a.instance_id,
            COALESCE(s.process_id, spm.process_id)         AS process_id,
            COALESCE(e.scheme_id, bi.scheme_id)             AS scheme_id,
            s.start_time,
            bi.first_block_time,
            bi.last_block_time,
            e.completed_time,
            e.aborted_time,
            bi.block_count,
            CASE
                WHEN e.completed_time IS NOT NULL THEN 'completed'
                WHEN e.aborted_time   IS NOT NULL THEN 'aborted'
                ELSE 'in_progress'
            END AS status,
            CASE
                WHEN e.completed_time IS NOT NULL AND s.start_time IS NOT NULL
                    THEN DATEDIFF('second', s.start_time, e.completed_time)
                WHEN e.aborted_time IS NOT NULL AND s.start_time IS NOT NULL
                    THEN DATEDIFF('second', s.start_time, e.aborted_time)
                ELSE NULL
            END AS duration_seconds
        FROM all_ids a
        LEFT JOIN starts      s   ON a.instance_id = s.instance_id
        LEFT JOIN ends        e   ON a.instance_id = e.instance_id
        LEFT JOIN block_info  bi  ON a.instance_id = bi.instance_id
        LEFT JOIN scheme_process_map spm ON COALESCE(e.scheme_id, bi.scheme_id) = spm.scheme_id
    """)

    # ── 5. Block duration table ───────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE block_durations AS
        WITH
        activated AS (
            SELECT instance_id, scheme_id, block_id, timestamp AS activated_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY instance_id, block_id
                       ORDER BY timestamp
                   ) AS rn
            FROM events
            WHERE event_type = 'block_activated'
              AND instance_id IS NOT NULL
        ),
        completed AS (
            SELECT instance_id, scheme_id, block_id, timestamp AS completed_at, block_result,
                   ROW_NUMBER() OVER (
                       PARTITION BY instance_id, block_id
                       ORDER BY timestamp
                   ) AS rn
            FROM events
            WHERE event_type = 'block_completed'
              AND instance_id IS NOT NULL
        ),
        aborted AS (
            SELECT instance_id, scheme_id, block_id, timestamp AS aborted_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY instance_id, block_id
                       ORDER BY timestamp
                   ) AS rn
            FROM events
            WHERE event_type = 'block_aborted'
              AND instance_id IS NOT NULL
        )
        SELECT
            a.instance_id,
            COALESCE(a.scheme_id, c.scheme_id)              AS scheme_id,
            a.block_id,
            a.activated_at,
            c.completed_at,
            ab.aborted_at,
            c.block_result,
            CASE
                WHEN c.completed_at IS NOT NULL  THEN 'completed'
                WHEN ab.aborted_at  IS NOT NULL  THEN 'aborted'
                ELSE 'in_progress'
            END AS block_status,
            CASE
                WHEN c.completed_at IS NOT NULL
                    THEN DATEDIFF('second', a.activated_at, c.completed_at)
                WHEN ab.aborted_at IS NOT NULL
                    THEN DATEDIFF('second', a.activated_at, ab.aborted_at)
                ELSE NULL
            END AS duration_seconds
        FROM activated a
        LEFT JOIN completed c
            ON a.instance_id = c.instance_id
           AND a.block_id    = c.block_id
           AND a.rn          = c.rn
        LEFT JOIN aborted ab
            ON a.instance_id = ab.instance_id
           AND a.block_id    = ab.block_id
           AND a.rn          = ab.rn
    """)

    # ── 6. Directly-follows graph (block transitions) ─────────────────────────
    conn.execute("""
        CREATE TABLE block_transitions AS
        WITH ordered AS (
            SELECT
                instance_id,
                COALESCE(scheme_id, 0)   AS scheme_id,
                block_id,
                activated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY instance_id
                    ORDER BY activated_at
                ) AS seq
            FROM block_durations
            WHERE activated_at IS NOT NULL
        )
        SELECT
            a.scheme_id,
            a.block_id         AS from_block,
            b.block_id         AS to_block,
            COUNT(*)           AS cnt,
            AVG(DATEDIFF('second', a.activated_at, b.activated_at)) AS avg_gap_sec
        FROM ordered a
        JOIN ordered b
            ON a.instance_id = b.instance_id
           AND a.seq + 1      = b.seq
        GROUP BY a.scheme_id, a.block_id, b.block_id
        ORDER BY cnt DESC
    """)


# ── Query helpers ─────────────────────────────────────────────────────────────
def _q(sql: str, params: dict | None = None) -> list[dict]:
    """Execute SQL and return list of row dicts."""
    conn = get_conn()
    if params:
        rel = conn.execute(sql, params)
    else:
        rel = conn.execute(sql)
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _scalar(sql: str) -> Any:
    conn = get_conn()
    return conn.execute(sql).fetchone()[0]


def _period_filter(date_from: str | None, date_to: str | None, host: str | None,
                   table: str = 'events', ts_col: str = 'timestamp') -> str:
    clauses = []
    if date_from:
        clauses.append(f"{table}.{ts_col} >= '{date_from}'::TIMESTAMPTZ")
    if date_to:
        clauses.append(f"{table}.{ts_col} <  ('{date_to}'::DATE + INTERVAL 1 DAY)::TIMESTAMPTZ")
    if host and host != 'all':
        clauses.append(f"{table}.host = '{host}'")
    return ('WHERE ' + ' AND '.join(clauses)) if clauses else ''


# ── API query functions ───────────────────────────────────────────────────────
def query_overview() -> dict:
    conn = get_conn()

    inst = _q("""
        SELECT
            COUNT(*)                                         AS total,
            COUNT(*) FILTER (WHERE status = 'completed')    AS completed,
            COUNT(*) FILTER (WHERE status = 'aborted')      AS aborted,
            COUNT(*) FILTER (WHERE status = 'in_progress')  AS in_progress,
            AVG(duration_seconds)                            AS avg_duration_sec,
            PERCENTILE_CONT(0.5) WITHIN GROUP
                (ORDER BY duration_seconds)                  AS median_duration_sec,
            PERCENTILE_CONT(0.95) WITHIN GROUP
                (ORDER BY duration_seconds)                  AS p95_duration_sec
        FROM instance_lifecycle
    """)[0]

    blocks = _q("""
        SELECT
            COUNT(*)                                          AS activations,
            COUNT(*) FILTER (WHERE block_status = 'completed') AS completions,
            COUNT(*) FILTER (WHERE block_status = 'aborted')   AS abortions,
            COUNT(*) FILTER (WHERE block_status = 'in_progress') AS in_progress
        FROM block_durations
    """)[0]

    errors = _q("""
        SELECT
            COUNT(*) FILTER (WHERE event_type = 'lock_contention') AS lock_contentions,
            COUNT(*) FILTER (WHERE event_type = 'span' AND span_status = 'Failed') AS failed_spans
        FROM events
    """)[0]

    period = _q(f"""
        SELECT
            STRFTIME(MIN(timestamp + {_TZ_INTERVAL}), '%Y-%m-%d %H:%M') AS period_from,
            STRFTIME(MAX(timestamp + {_TZ_INTERVAL}), '%Y-%m-%d %H:%M') AS period_to
        FROM events
    """)[0]

    unique_process_types = _scalar("""
        SELECT COUNT(DISTINCT process_id) FROM instance_lifecycle
        WHERE process_id IS NOT NULL AND process_id != ''
    """)

    return {
        'period': period,
        'instances': {
            'total': int(inst['total']),
            'completed': int(inst['completed']),
            'aborted': int(inst['aborted']),
            'in_progress': int(inst['in_progress']),
            'completion_rate': round(inst['completed'] / inst['total'] * 100, 1) if inst['total'] else 0,
            'abort_rate': round(inst['aborted'] / inst['total'] * 100, 1) if inst['total'] else 0,
        },
        'performance': {
            'avg_duration_sec': round(inst['avg_duration_sec'] or 0),
            'median_duration_sec': round(inst['median_duration_sec'] or 0),
            'p95_duration_sec': round(inst['p95_duration_sec'] or 0),
        },
        'blocks': {
            'activations': int(blocks['activations']),
            'completions': int(blocks['completions']),
            'abortions': int(blocks['abortions']),
            'in_progress': int(blocks['in_progress']),
            'abort_rate': round(blocks['abortions'] / blocks['activations'] * 100, 1) if blocks['activations'] else 0,
        },
        'errors': {
            'lock_contentions': int(errors['lock_contentions']),
            'failed_spans': int(errors['failed_spans']),
        },
        'unique_process_types': int(unique_process_types),
    }


def query_processes() -> list[dict]:
    return _q("""
        SELECT
            il.process_id,
            COALESCE(pn.display_name, pn.name, il.process_id) AS display_name,
            COALESCE(pn.name, il.process_id)                   AS name,
            COUNT(*)                                           AS total,
            COUNT(*) FILTER (WHERE il.status = 'completed')   AS completed,
            COUNT(*) FILTER (WHERE il.status = 'aborted')     AS aborted,
            COUNT(*) FILTER (WHERE il.status = 'in_progress') AS in_progress,
            ROUND(COUNT(*) FILTER (WHERE il.status = 'completed') * 100.0
                  / NULLIF(COUNT(*), 0), 1)                    AS completion_rate,
            ROUND(COUNT(*) FILTER (WHERE il.status = 'aborted') * 100.0
                  / NULLIF(COUNT(*), 0), 1)                    AS abort_rate,
            ROUND(AVG(il.duration_seconds) / 3600.0, 2)       AS avg_duration_hours,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
                  (ORDER BY il.duration_seconds) / 3600.0, 2) AS median_duration_hours,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                  (ORDER BY il.duration_seconds) / 3600.0, 2) AS p95_duration_hours,
            ROUND(AVG(il.block_count), 1)                      AS avg_blocks,
            SUM(il.block_count)                                AS total_blocks
        FROM instance_lifecycle il
        LEFT JOIN process_names pn ON il.process_id = pn.process_id
        WHERE il.process_id IS NOT NULL AND il.process_id != ''
        GROUP BY il.process_id, pn.display_name, pn.name
        ORDER BY total DESC
    """)


def query_process_timeline(process_id: str | None = None) -> list[dict]:
    where = f"AND il.process_id = '{process_id}'" if process_id else ''
    return _q(f"""
        SELECT
            STRFTIME(DATE_TRUNC('hour', il.start_time + {_TZ_INTERVAL}), '%Y-%m-%d %H:%M') AS hour,
            COUNT(*)                           AS started,
            COUNT(*) FILTER (WHERE il.status = 'completed') AS completed,
            COUNT(*) FILTER (WHERE il.status = 'aborted')   AS aborted
        FROM instance_lifecycle il
        WHERE il.start_time IS NOT NULL {where}
        GROUP BY 1
        ORDER BY 1
    """)


def query_blocks(process_id: str | None = None, scheme_id: int | None = None,
                 limit: int = 100) -> list[dict]:
    conditions = []
    if process_id:
        conditions.append(f"spm.process_id = '{process_id}'")
    if scheme_id:
        conditions.append(f"bd.scheme_id = {scheme_id}")
    where = ('AND ' + ' AND '.join(conditions)) if conditions else ''

    return _q(f"""
        SELECT
            bd.scheme_id,
            bd.block_id,
            COALESCE(pn.display_name, pn.name, spm.process_id) AS process_name,
            COUNT(*)                                             AS activations,
            COUNT(*) FILTER (WHERE bd.block_status = 'completed') AS completions,
            COUNT(*) FILTER (WHERE bd.block_status = 'aborted')   AS abortions,
            ROUND(COUNT(*) FILTER (WHERE bd.block_status = 'aborted') * 100.0
                  / NULLIF(COUNT(*), 0), 1)                      AS abort_rate,
            ROUND(AVG(bd.duration_seconds), 0)                   AS avg_duration_sec,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
                  (ORDER BY bd.duration_seconds), 0)             AS median_duration_sec,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                  (ORDER BY bd.duration_seconds), 0)             AS p95_duration_sec,
            MAX(bd.duration_seconds)                             AS max_duration_sec
        FROM block_durations bd
        LEFT JOIN scheme_process_map spm ON bd.scheme_id = spm.scheme_id
        LEFT JOIN process_names pn       ON spm.process_id = pn.process_id
        WHERE bd.scheme_id IS NOT NULL {where}
        GROUP BY bd.scheme_id, bd.block_id, pn.display_name, pn.name, spm.process_id
        HAVING COUNT(*) >= 3
        ORDER BY avg_duration_sec DESC NULLS LAST
        LIMIT {limit}
    """)


def query_block_results(scheme_id: int, block_id: str) -> list[dict]:
    return _q(f"""
        SELECT
            COALESCE(block_result, '(null)') AS result,
            COUNT(*) AS count
        FROM block_durations
        WHERE scheme_id = {scheme_id}
          AND block_id = '{block_id}'
          AND block_status = 'completed'
        GROUP BY block_result
        ORDER BY count DESC
    """)


def query_timeline(granularity: str = 'hour') -> list[dict]:
    trunc = 'hour' if granularity == 'hour' else 'day'
    fmt = '%Y-%m-%d %H:%M' if trunc == 'hour' else '%Y-%m-%d'
    return _q(f"""
        SELECT
            STRFTIME(DATE_TRUNC('{trunc}', timestamp + {_TZ_INTERVAL}), '{fmt}') AS bucket,
            COUNT(*) FILTER (WHERE event_type = 'process_started')   AS process_starts,
            COUNT(*) FILTER (WHERE event_type = 'process_completed') AS process_completions,
            COUNT(*) FILTER (WHERE event_type = 'process_aborted')   AS process_abortions,
            COUNT(*) FILTER (WHERE event_type = 'block_activated')   AS block_activations,
            COUNT(*) FILTER (WHERE event_type = 'span' AND message_type LIKE '%BlockMessage')
                                                                      AS messages_handled,
            AVG(CASE WHEN event_type = 'span' THEN duration_ms END)  AS avg_span_ms
        FROM events
        WHERE timestamp IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """)


def query_flow(scheme_id: int | None = None, process_id: str | None = None,
               top_n: int = 30) -> list[dict]:
    conditions = []
    if scheme_id:
        conditions.append(f"bt.scheme_id = {scheme_id}")
    elif process_id:
        conditions.append(f"spm.process_id = '{process_id}'")

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    return _q(f"""
        SELECT
            bt.scheme_id,
            bt.from_block,
            bt.to_block,
            bt.cnt              AS transition_count,
            ROUND(bt.avg_gap_sec, 0) AS avg_gap_sec
        FROM block_transitions bt
        LEFT JOIN scheme_process_map spm ON bt.scheme_id = spm.scheme_id
        {where}
        ORDER BY bt.cnt DESC
        LIMIT {top_n}
    """)


def query_bottlenecks() -> dict:
    slow_blocks = _q("""
        SELECT
            bd.scheme_id,
            bd.block_id,
            COALESCE(pn.display_name, pn.name, spm.process_id) AS process_name,
            COUNT(*) AS activations,
            ROUND(AVG(bd.duration_seconds) / 3600.0, 2)         AS avg_hours,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                  (ORDER BY bd.duration_seconds) / 3600.0, 2)  AS p95_hours,
            ROUND(MAX(bd.duration_seconds) / 3600.0, 2)         AS max_hours
        FROM block_durations bd
        LEFT JOIN scheme_process_map spm ON bd.scheme_id = spm.scheme_id
        LEFT JOIN process_names pn       ON spm.process_id = pn.process_id
        WHERE bd.duration_seconds IS NOT NULL
          AND bd.duration_seconds > 60
          AND bd.block_status = 'completed'
        GROUP BY bd.scheme_id, bd.block_id, pn.display_name, pn.name, spm.process_id
        HAVING COUNT(*) >= 5
        ORDER BY avg_hours DESC
        LIMIT 20
    """)

    slow_processes = _q("""
        SELECT
            il.process_id,
            COALESCE(pn.display_name, pn.name, il.process_id) AS display_name,
            COUNT(*) AS total,
            ROUND(AVG(il.duration_seconds) / 3600.0, 2)        AS avg_hours,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                  (ORDER BY il.duration_seconds) / 3600.0, 2) AS p95_hours,
            ROUND(MAX(il.duration_seconds) / 3600.0, 2)        AS max_hours
        FROM instance_lifecycle il
        LEFT JOIN process_names pn ON il.process_id = pn.process_id
        WHERE il.duration_seconds IS NOT NULL
          AND il.status = 'completed'
          AND il.process_id IS NOT NULL
        GROUP BY il.process_id, pn.display_name, pn.name
        HAVING COUNT(*) >= 3
        ORDER BY avg_hours DESC
        LIMIT 15
    """)

    high_abort_blocks = _q("""
        SELECT
            bd.scheme_id,
            bd.block_id,
            COALESCE(pn.display_name, pn.name, spm.process_id) AS process_name,
            COUNT(*) AS activations,
            COUNT(*) FILTER (WHERE bd.block_status = 'aborted') AS abortions,
            ROUND(COUNT(*) FILTER (WHERE bd.block_status = 'aborted') * 100.0
                  / NULLIF(COUNT(*), 0), 1) AS abort_rate
        FROM block_durations bd
        LEFT JOIN scheme_process_map spm ON bd.scheme_id = spm.scheme_id
        LEFT JOIN process_names pn       ON spm.process_id = pn.process_id
        WHERE bd.scheme_id IS NOT NULL
        GROUP BY bd.scheme_id, bd.block_id, pn.display_name, pn.name, spm.process_id
        HAVING COUNT(*) >= 5 AND abort_rate > 5
        ORDER BY abort_rate DESC, abortions DESC
        LIMIT 15
    """)

    return {
        'slow_blocks': slow_blocks,
        'slow_processes': slow_processes,
        'high_abort_blocks': high_abort_blocks,
    }


def query_issues() -> dict:
    lock_contentions = _q(f"""
        SELECT
            STRFTIME(DATE_TRUNC('hour', timestamp + {_TZ_INTERVAL}), '%Y-%m-%d %H:%M') AS hour,
            COUNT(*) AS count
        FROM events
        WHERE event_type = 'lock_contention'
        GROUP BY 1
        ORDER BY 1
    """)

    failed_spans = _q("""
        SELECT
            instance_id,
            block_id,
            timestamp::VARCHAR AS timestamp,
            message_type,
            host
        FROM events
        WHERE event_type = 'span' AND span_status = 'Failed'
        ORDER BY timestamp DESC
        LIMIT 50
    """)

    long_running = _q("""
        SELECT
            il.instance_id,
            COALESCE(pn.display_name, pn.name, il.process_id) AS process_name,
            il.start_time::VARCHAR      AS start_time,
            il.status,
            ROUND(il.duration_seconds / 3600.0, 1) AS duration_hours,
            il.block_count
        FROM instance_lifecycle il
        LEFT JOIN process_names pn ON il.process_id = pn.process_id
        WHERE il.duration_seconds > 86400   -- > 24 hours
           OR (il.status = 'in_progress' AND il.start_time IS NOT NULL)
        ORDER BY il.duration_seconds DESC NULLS LAST
        LIMIT 50
    """)

    abort_by_process = _q("""
        SELECT
            il.process_id,
            COALESCE(pn.display_name, pn.name, il.process_id) AS display_name,
            COUNT(*)                                            AS total,
            COUNT(*) FILTER (WHERE il.status = 'aborted')     AS aborted,
            ROUND(COUNT(*) FILTER (WHERE il.status = 'aborted') * 100.0
                  / NULLIF(COUNT(*), 0), 1) AS abort_rate
        FROM instance_lifecycle il
        LEFT JOIN process_names pn ON il.process_id = pn.process_id
        WHERE il.process_id IS NOT NULL
        GROUP BY il.process_id, pn.display_name, pn.name
        HAVING COUNT(*) >= 5 AND abort_rate > 0
        ORDER BY abort_rate DESC
        LIMIT 15
    """)

    return {
        'lock_contentions_by_hour': lock_contentions,
        'failed_spans': failed_spans,
        'long_running_instances': long_running,
        'abort_by_process': abort_by_process,
    }


def query_filters() -> dict:
    period = _q("""
        SELECT
            MIN(timestamp)::DATE::VARCHAR AS date_from,
            MAX(timestamp)::DATE::VARCHAR AS date_to
        FROM events
    """)[0]

    hosts = [r['host'] for r in _q("SELECT DISTINCT host FROM events ORDER BY host")]

    schemes = _q("""
        SELECT
            il.scheme_id,
            COALESCE(pn.display_name, pn.name, spm.process_id, il.scheme_id::VARCHAR) AS label
        FROM (SELECT DISTINCT scheme_id FROM instance_lifecycle WHERE scheme_id IS NOT NULL) il
        LEFT JOIN scheme_process_map spm ON il.scheme_id = spm.scheme_id
        LEFT JOIN process_names pn       ON spm.process_id = pn.process_id
        ORDER BY label
    """)

    return {
        'period': period,
        'hosts': hosts,
        'schemes': schemes,
    }


def query_process_detail(process_id: str) -> dict:
    instances = _q(f"""
        SELECT
            il.instance_id,
            il.scheme_id,
            il.start_time::VARCHAR       AS start_time,
            il.completed_time::VARCHAR   AS completed_time,
            il.aborted_time::VARCHAR     AS aborted_time,
            il.status,
            ROUND(il.duration_seconds / 3600.0, 2) AS duration_hours,
            il.block_count
        FROM instance_lifecycle il
        WHERE il.process_id = '{process_id}'
        ORDER BY il.start_time DESC NULLS LAST
        LIMIT 200
    """)

    block_stats = _q(f"""
        SELECT
            bd.scheme_id,
            bd.block_id,
            COUNT(*) AS activations,
            COUNT(*) FILTER (WHERE bd.block_status = 'completed') AS completions,
            COUNT(*) FILTER (WHERE bd.block_status = 'aborted')   AS abortions,
            ROUND(AVG(bd.duration_seconds) / 60.0, 1)             AS avg_min,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                  (ORDER BY bd.duration_seconds) / 60.0, 1)      AS p95_min
        FROM block_durations bd
        JOIN scheme_process_map spm ON bd.scheme_id = spm.scheme_id
        WHERE spm.process_id = '{process_id}'
        GROUP BY bd.scheme_id, bd.block_id
        HAVING COUNT(*) >= 2
        ORDER BY avg_min DESC NULLS LAST
        LIMIT 50
    """)

    return {'instances': instances, 'block_stats': block_stats}


def query_heatmap() -> list[dict]:
    """Activity heatmap: hour-of-day × day-of-week for process starts."""
    return _q(f"""
        SELECT
            EXTRACT('dow'  FROM timestamp + {_TZ_INTERVAL})::INTEGER AS dow,
            EXTRACT('hour' FROM timestamp + {_TZ_INTERVAL})::INTEGER AS hour,
            COUNT(*) FILTER (WHERE event_type = 'process_started')   AS starts,
            COUNT(*) FILTER (WHERE event_type = 'block_activated')   AS activations
        FROM events
        WHERE timestamp IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)


def query_duration_histogram(process_id: str | None = None) -> list[dict]:
    """Histogram of process completion durations."""
    where = f"AND il.process_id = '{process_id}'" if process_id else ''
    return _q(f"""
        WITH buckets AS (
            SELECT duration_seconds,
                CASE
                    WHEN duration_seconds <    300 THEN '< 5 мин'
                    WHEN duration_seconds <   1800 THEN '5–30 мин'
                    WHEN duration_seconds <   3600 THEN '30–60 мин'
                    WHEN duration_seconds <  14400 THEN '1–4 ч'
                    WHEN duration_seconds <  86400 THEN '4–24 ч'
                    ELSE '> 24 ч'
                END AS bucket,
                CASE
                    WHEN duration_seconds <    300 THEN 1
                    WHEN duration_seconds <   1800 THEN 2
                    WHEN duration_seconds <   3600 THEN 3
                    WHEN duration_seconds <  14400 THEN 4
                    WHEN duration_seconds <  86400 THEN 5
                    ELSE 6
                END AS sort_order
            FROM instance_lifecycle il
            WHERE il.status = 'completed'
              AND il.duration_seconds IS NOT NULL {where}
        )
        SELECT bucket, sort_order, COUNT(*) AS count
        FROM buckets
        GROUP BY bucket, sort_order
        ORDER BY sort_order
    """)


def query_summary_for_ai() -> dict:
    """Compact data bundle for AI analysis prompt."""
    overview = query_overview()
    procs = query_processes()[:20]
    bottlenecks = query_bottlenecks()
    issues = query_issues()

    return {
        'overview': overview,
        'top_processes': [
            {k: v for k, v in p.items()
             if k in ('display_name', 'total', 'completed', 'aborted',
                      'completion_rate', 'abort_rate', 'avg_duration_hours',
                      'median_duration_hours', 'p95_duration_hours')}
            for p in procs
        ],
        'slow_blocks':      bottlenecks['slow_blocks'][:10],
        'slow_processes':   bottlenecks['slow_processes'][:10],
        'high_abort_blocks': bottlenecks['high_abort_blocks'][:10],
        'abort_by_process': issues['abort_by_process'][:10],
        'lock_events_total': sum(r['count'] for r in issues['lock_contentions_by_hour']),
        'failed_spans_total': len(issues['failed_spans']),
        'long_running_count': len(issues['long_running_instances']),
    }
