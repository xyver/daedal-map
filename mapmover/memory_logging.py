"""Periodic memory evidence in service logs; no external monitor or history store."""
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
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


def _read_number(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def sample_memory():
    """Inspect loaded objects only; no SQL, cache loads, GC or profiling."""
    from mapmover.memory_diagnostics import loaded_owner_memory
    import sys

    started = time.monotonic()
    row = {
        'schema_version': 1, 'generated_at': datetime.now(timezone.utc).isoformat(),
        'process_epoch': _EPOCH, 'pid': os.getpid(),
        'uptime_seconds': round(started - _START, 3),
    }
    try:
        status = Path('/proc/self/status').read_text().splitlines()
        row['process'] = {
            line.split(':', 1)[0]: int(line.split()[1])
            for line in status
            if line.startswith(('VmRSS:', 'VmHWM:', 'RssAnon:', 'RssFile:', 'Threads:'))
        }
    except (OSError, ValueError) as exc:
        row['process_error'] = str(exc)
    row['cgroup_current_bytes'] = _read_number('/sys/fs/cgroup/memory.current')
    try:
        row['cgroup'] = {
            key: int(value) for key, value in
            (line.split() for line in Path('/sys/fs/cgroup/memory.stat').read_text().splitlines())
            if key in {'anon', 'file', 'kernel', 'sock', 'shmem'}
        }
    except (OSError, ValueError) as exc:
        row['cgroup_error'] = str(exc)
    owners = loaded_owner_memory(include_entries=False)['owners']
    # Keep log entries small; per-key detail remains available on demand.
    row['owners'] = {key: {k: v for k, v in owner.items() if k != 'entry_estimates'}
                     for key, owner in owners.items()}
    pool = sys.modules.get('mapmover.duckdb_helpers')
    if pool is not None and hasattr(pool, '_QUERY_POOL'):
        # Approximate occupancy; do not block behind connection initialization.
        row['query_pool'] = {
            'created': pool._QUERY_POOL_CREATED, 'idle': pool._QUERY_POOL.qsize(),
            'capacity': pool._QUERY_POOL_SIZE, 'generation': pool._QUERY_POOL_GENERATION,
            'approximate': True,
        }
    artifacts = sys.modules.get('mapmover.runtime.published_artifacts')
    if artifacts is not None:
        row['artifact_activity'] = dict(artifacts._CACHE_STATS)
    row['sample_seconds'] = round(time.monotonic() - started, 4)
    row['measurement_note'] = 'Process fields are KiB except Threads. Python estimates overlap and exclude native buffers. Cgroup includes other processes and file cache.'
    return row


def start_memory_logging():
    """Five-minute cloud default; MEMORY_SAMPLE_SECONDS=0 disables collection."""
    default = '300' if os.environ.get('DEPLOYMENT', '').lower() == 'railway' else '0'
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
