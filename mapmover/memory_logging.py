"""Periodic memory evidence in service logs; no external monitor or history store."""
from datetime import datetime, timezone
from collections import deque
from contextlib import contextmanager
from itertools import count
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import shutil
import sys
import threading
import time
import uuid

logger = logging.getLogger(__name__)
# The application logger also writes an unrotated local file. Send these
# samples directly to stdout for Railway capture, without duplicating to disk.
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    logger.addHandler(logging.StreamHandler(sys.stdout))
_EPOCH = uuid.uuid4().hex
_START = time.monotonic()
_FILE_HANDLER = None
_MEMORY_LOG_BACKUPS = 90  # About 900 MiB; headroom for a month as sample size grows.
_STAGE_LOCK = threading.Lock()
_STAGE_IDS = count(1)
_ACTIVE_STAGES: dict[int, dict] = {}
_RECENT_STAGE_EVENTS = deque(maxlen=64)


def _stage_snapshot() -> dict:
    """Read process counters without walking caches or executing SQL."""
    from mapmover.memory_diagnostics import process_memory_snapshot

    process = process_memory_snapshot()
    status = process.get('status_kib') or {}
    allocator = process.get('glibc_allocator') or {}
    return {
        'rss_kib': status.get('VmRSS'),
        'anonymous_rss_kib': status.get('RssAnon'),
        'glibc_allocated_bytes': allocator.get('allocated_heap_bytes'),
        'glibc_free_bytes': allocator.get('free_heap_bytes'),
        'cpu_seconds': round(time.process_time(), 4),
    }


@contextmanager
def memory_stage(name: str):
    """Mark bounded background work in the process memory timeline.

    Deltas are process-wide and only directional when work overlaps. Native
    allocation stacks still require a profiler in an isolated process.
    """
    enabled = os.environ.get('MEMORY_STAGE_ENABLED',
                             '1' if os.environ.get('DEPLOYMENT', '').lower() == 'railway' else '0')
    if enabled.strip().lower() not in {'1', 'true', 'yes', 'on'}:
        yield
        return
    label = str(name)[:80]
    stage_id = next(_STAGE_IDS)
    try:
        before = _stage_snapshot()
    except Exception:
        before = {}
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    with _STAGE_LOCK:
        overlap = bool(_ACTIVE_STAGES)
        for active in _ACTIVE_STAGES.values():
            active['overlap'] = True
        _ACTIVE_STAGES[stage_id] = {'name': label, 'overlap': overlap}
    status = 'ok'
    try:
        yield
    except BaseException:
        status = 'error'
        raise
    finally:
        try:
            after = _stage_snapshot()
        except Exception:
            after = {}
        event = {
            'process_epoch': _EPOCH, 'pid': os.getpid(),
            'name': label, 'started_at': started_at,
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'duration_seconds': round(time.monotonic() - started, 3),
            'status': status, 'before': before, 'after': after,
        }
        with _STAGE_LOCK:
            event['overlapped_other_stage'] = _ACTIVE_STAGES.pop(stage_id)['overlap']
            _RECENT_STAGE_EVENTS.append(event)
        logger.info('memory_stage %s', json.dumps(event, separators=(',', ':')))


def _ensure_file_log():
    """Attach a bounded persistent-volume log in Railway; never fail startup."""
    global _FILE_HANDLER
    if _FILE_HANDLER is not None:
        return str(_FILE_HANDLER.baseFilename)
    default = '/mnt/artifact-cache/memory_samples.jsonl' if os.environ.get('DEPLOYMENT', '').lower() == 'railway' else ''
    configured = os.environ.get('MEMORY_SAMPLE_LOG_PATH', default).strip()
    if not configured:
        return None
    try:
        path = Path(configured)
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024,
                                      backupCount=_MEMORY_LOG_BACKUPS, encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(handler)
        _FILE_HANDLER = handler
        return str(path)
    except OSError as exc:
        logger.warning('Persistent memory sample log unavailable: %s', exc)
        return None


def _storage_snapshot(artifacts=None) -> dict:
    """Sample the cache filesystem and loaded cache counters without walking files."""
    result = {}
    configured = os.environ.get('PUBLISHED_ARTIFACT_CACHE_DIR', '').strip()
    if artifacts is not None:
        try:
            status = artifacts.artifact_cache_status()
            result['artifact_cache'] = {
                key: status[key] for key in (
                    'enabled', 'bytes', 'quota_bytes', 'entry_count',
                    'disk_hits', 'disk_misses', 'remote_reads', 'hydrations',
                    'hydrated_bytes', 'evictions', 'evicted_bytes',
                ) if key in status
            }
            configured = str(status.get('root') or configured)
        except (OSError, ValueError, TypeError) as exc:
            result['artifact_cache_error'] = type(exc).__name__
    if not configured and os.environ.get('DEPLOYMENT', '').lower() == 'railway':
        configured = '/mnt/artifact-cache/published-artifacts'
    if configured:
        root = Path(configured)
        probe = root if root.exists() else root.parent
        if probe.exists():
            try:
                usage = shutil.disk_usage(probe)
                result['filesystem'] = {
                    'total_bytes': usage.total,
                    'used_bytes': usage.used,
                    'free_bytes': usage.free,
                }
            except OSError as exc:
                result['filesystem_error'] = type(exc).__name__
        else:
            result['filesystem_error'] = 'path_unavailable'
    return result


def sample_memory():
    """Inspect loaded objects only; no SQL, cache loads, GC or profiling."""
    from mapmover.memory_diagnostics import (
        loaded_dataframe_cache_memory, loaded_owner_memory, process_memory_snapshot,
    )
    import sys

    started = time.monotonic()
    row = {
        'schema_version': 2, 'generated_at': datetime.now(timezone.utc).isoformat(),
        'process_epoch': _EPOCH, 'pid': os.getpid(),
        'uptime_seconds': round(started - _START, 3),
    }
    process = process_memory_snapshot()
    row['process'] = process.get('status_kib', {})
    row['smaps_rollup'] = process.get('smaps_rollup_kib', {})
    row['glibc_allocator'] = process.get('glibc_allocator', {})
    row['python_runtime'] = process.get('python_runtime', {})
    row['python_tracemalloc'] = process.get('python_tracemalloc', {'enabled': False})
    row['arrow_memory_pool'] = process.get('arrow_memory_pool', {'loaded': False})
    row['cgroup_current_bytes'] = process.get('cgroup_current_bytes')
    row['cgroup_peak_bytes'] = process.get('cgroup_peak_bytes')
    row['cgroup'] = process.get('cgroup_stat', {})
    row['cgroup_events'] = process.get('cgroup_events', {})
    with _STAGE_LOCK:
        row['active_memory_stages'] = sorted({stage['name'] for stage in _ACTIVE_STAGES.values()})
        row['recent_memory_stage_events'] = list(_RECENT_STAGE_EVENTS)
        _RECENT_STAGE_EVENTS.clear()
    for key in ('status_error', 'smaps_rollup_error', 'cgroup_stat_error', 'cgroup_events_error'):
        if process.get(key):
            row[key] = process[key]
    owners = loaded_owner_memory(include_entries=False)['owners']
    # Keep log entries small; per-key detail remains available on demand.
    row['owners'] = {key: {k: v for k, v in owner.items() if k != 'entry_estimates'}
                     for key, owner in owners.items()}
    row['dataframe_caches'] = loaded_dataframe_cache_memory()
    pool = sys.modules.get('mapmover.duckdb_helpers')
    if pool is not None and hasattr(pool, '_QUERY_POOL'):
        # Exact lease activity plus non-blocking pool occupancy; no SQL is run.
        if hasattr(pool, 'query_pool_activity_status'):
            row['query_pool'] = pool.query_pool_activity_status()
        else:
            row['query_pool'] = {
                'created': pool._QUERY_POOL_CREATED, 'idle': pool._QUERY_POOL.qsize(),
                'capacity': pool._QUERY_POOL_SIZE, 'generation': pool._QUERY_POOL_GENERATION,
                'approximate': True,
            }
    artifacts = sys.modules.get('mapmover.runtime.published_artifacts')
    if artifacts is not None:
        row['artifact_activity'] = dict(artifacts._CACHE_STATS)
    row['storage'] = _storage_snapshot(artifacts)
    security = sys.modules.get('mapmover.security')
    limiter = getattr(security, 'rate_limiter', None) if security else None
    if limiter is not None and hasattr(limiter, 'stats'):
        row['rate_limiter'] = limiter.stats()
    execution = sys.modules.get('mapmover.mcp_execution')
    if execution is not None and hasattr(execution, 'execution_status'):
        row['mcp_execution'] = execution.execution_status()
    row['sample_seconds'] = round(time.monotonic() - started, 4)
    row['measurement_note'] = 'Process fields are KiB except Threads. Python estimates overlap and exclude native buffers. Cgroup includes other processes and file cache.'
    return row


def start_memory_logging():
    """One-minute cloud default; MEMORY_SAMPLE_SECONDS=0 disables collection."""
    default = '60' if os.environ.get('DEPLOYMENT', '').lower() == 'railway' else '0'
    try:
        interval = float(os.environ.get('MEMORY_SAMPLE_SECONDS', default))
        if not 0 <= interval <= 86400:
            raise ValueError('interval outside 0..86400')
    except ValueError:
        logger.warning('Invalid MEMORY_SAMPLE_SECONDS; using deployment default')
        interval = float(default)
    stop = threading.Event()
    if interval == 0:
        return lambda: None
    file_path = _ensure_file_log()
    logger.info('memory_logging_started interval_seconds=%s persistent_log=%s', interval, file_path or 'disabled')
    interval = max(60, interval)

    def run():
        next_due = time.monotonic()
        while not stop.is_set():
            if stop.wait(max(0, next_due - time.monotonic())):
                return
            try:
                observed = time.monotonic()
                lag = max(0.0, observed - next_due)
                missed = int(lag // interval)
                row = sample_memory()
                row['configured_interval_seconds'] = interval
                row['scheduled_lag_seconds'] = round(lag, 3)
                row['missed_intervals'] = missed
                logger.info('memory_sample %s', json.dumps(row, separators=(',', ':')))
            except Exception:
                logger.exception('Memory sample failed')
            next_due += interval * (missed + 1)

    thread = threading.Thread(target=run, name='memory-logging', daemon=True)
    thread.start()

    def close():
        stop.set()
        thread.join(timeout=2)

    return close
