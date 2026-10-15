"""Bounded worker isolation for synchronous MCP tool work.

Most geography helpers are intentionally synchronous because they are also used
by the REST and local runtimes.  Calling them directly from an ``async`` MCP
route blocks the ASGI event loop while DuckDB, pandas, or object storage is
working.  This module provides one small, shared boundary for those calls.

The worker semaphore is retained until the underlying future actually finishes,
including after the HTTP-facing timeout.  Python cannot safely kill an
arbitrary thread, so this prevents timed-out work from turning into an
unbounded background pile while keeping health and unrelated app routes alive.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from functools import partial
from typing import Any, Callable, TypeVar


T = TypeVar("T")


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(str(os.environ.get(name, default)).strip()))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return max(0.0, float(str(os.environ.get(name, default)).strip()))
    except (TypeError, ValueError):
        return default


class MCPExecutionCapacityError(RuntimeError):
    """Raised when every isolated MCP worker is still occupied."""


class MCPExecutionTimeoutError(TimeoutError):
    """Raised when a tool exceeds its hosted response budget."""

    def __init__(self, message: str, *, timeout_seconds: float | None = None) -> None:
        super().__init__(message)
        self.timeout_seconds = timeout_seconds


_MAX_WORKERS = _env_int("MCP_EXECUTION_MAX_WORKERS", 2)
# A slow object-store read must produce a tool error before interactive clients
# abandon the request. Timed-out work still holds its slot until it exits.
_DEFAULT_TIMEOUT_SECONDS = _env_int("MCP_EXECUTION_TIMEOUT_SECONDS", 30)
_JOB_TIMEOUT_SECONDS = _env_int("MCP_EXECUTION_JOB_TIMEOUT_SECONDS", 120)
_LONG_JOB_TOOLS = frozenset({"create_geometry_export", "create_conversion_job"})
_CAPACITY_WAIT_SECONDS = _env_float("MCP_EXECUTION_CAPACITY_WAIT_SECONDS", 2.0)
_EXECUTOR = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="mcp-tool")
_CAPACITY = threading.BoundedSemaphore(_MAX_WORKERS)
_STATE_LOCK = threading.Lock()
_ACTIVE_WORKERS = 0
_TRACE_LOCAL = threading.local()
_LOGGER = logging.getLogger(__name__)


class _ExecutionTrace:
    def __init__(self, tool_name: str, work_name: str, capacity_wait_ms: int, diagnostic_id: str | None) -> None:
        self.tool_name = tool_name
        self.work_name = work_name
        self.capacity_wait_ms = capacity_wait_ms
        self.request_tag = hashlib.sha256(str(diagnostic_id).encode("utf-8")).hexdigest()[:12] if diagnostic_id else None
        self.started = time.monotonic()
        self.stage = "worker_started"
        self.stage_started = self.started
        self.stage_durations_ms: dict[str, int] = {}
        self.files: set[str] = set()
        self.io: dict[str, int] = {}
        self.last_artifact_error: str | None = None
        self.timed_out = False
        self.lock = threading.Lock()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            now = time.monotonic()
            stage_ms = round((now - self.stage_started) * 1000)
            durations = dict(self.stage_durations_ms)
            durations[self.stage] = durations.get(self.stage, 0) + stage_ms
            return {
                "tool": self.tool_name,
                "work": self.work_name,
                "request_tag": self.request_tag,
                "capacity_wait_ms": self.capacity_wait_ms,
                "stage": self.stage,
                "stage_ms": stage_ms,
                "stage_durations_ms": durations,
                "worker_ms": round((now - self.started) * 1000),
                "selected_files": len(self.files),
                "artifact_io": dict(self.io),
                "last_artifact_error": self.last_artifact_error,
            }


def mark_mcp_stage(stage: str) -> None:
    """Record a bounded, non-sensitive worker stage for timeout diagnostics."""
    trace = getattr(_TRACE_LOCAL, "trace", None)
    if trace is not None:
        with trace.lock:
            now = time.monotonic()
            trace.stage_durations_ms[trace.stage] = trace.stage_durations_ms.get(trace.stage, 0) + round(
                (now - trace.stage_started) * 1000
            )
            trace.stage = stage
            trace.stage_started = now


def record_mcp_file(path: Any) -> None:
    """Count unique files selected by the current MCP worker, without logging paths."""
    trace = getattr(_TRACE_LOCAL, "trace", None)
    if trace is not None:
        with trace.lock:
            trace.files.add(str(path))


def record_mcp_artifact_io(kind: str, amount: int = 1) -> None:
    trace = getattr(_TRACE_LOCAL, "trace", None)
    if trace is not None:
        with trace.lock:
            trace.io[kind] = trace.io.get(kind, 0) + amount


def record_mcp_artifact_error(error: BaseException) -> None:
    trace = getattr(_TRACE_LOCAL, "trace", None)
    if trace is not None:
        with trace.lock:
            trace.last_artifact_error = type(error).__name__


@contextmanager
def mcp_stage(stage: str):
    trace = getattr(_TRACE_LOCAL, "trace", None)
    if trace is None:
        yield
        return
    with trace.lock:
        previous = trace.stage
    mark_mcp_stage(stage)
    try:
        yield
    finally:
        mark_mcp_stage(previous)


def _execute_traced(trace: _ExecutionTrace, function: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    _TRACE_LOCAL.trace = trace
    try:
        return function(*args, **kwargs)
    finally:
        snapshot = trace.snapshot()
        if trace.timed_out:
            _LOGGER.warning("MCP timed-out worker finished: %s", snapshot)
        elif snapshot["worker_ms"] >= 5000:
            _LOGGER.warning("MCP slow worker finished: %s", snapshot)
        _TRACE_LOCAL.trace = None


def _increment_active_workers() -> None:
    global _ACTIVE_WORKERS
    with _STATE_LOCK:
        _ACTIVE_WORKERS += 1


def _decrement_active_workers() -> None:
    global _ACTIVE_WORKERS
    with _STATE_LOCK:
        _ACTIVE_WORKERS = max(0, _ACTIVE_WORKERS - 1)


def _release_capacity(_future: Future[Any]) -> None:
    _decrement_active_workers()
    _CAPACITY.release()


async def run_mcp_blocking(
    tool_name: str,
    function: Callable[..., T],
    /,
    *args: Any,
    timeout_seconds: float | None = None,
    capacity_wait_seconds: float | None = None,
    cancellation_event: threading.Event | None = None,
    diagnostic_id: str | None = None,
    **kwargs: Any,
) -> T:
    """Run synchronous tool work off-loop with fail-fast bounded capacity.

    The timeout bounds the caller's wait, not the underlying thread.  Capacity
    remains charged until that thread exits so retries cannot multiply work
    that the first caller abandoned.
    """

    wait_budget = max(
        0.0,
        float(_CAPACITY_WAIT_SECONDS if capacity_wait_seconds is None else capacity_wait_seconds),
    )
    wait_started = asyncio.get_running_loop().time()
    deadline = wait_started + wait_budget
    while not _CAPACITY.acquire(blocking=False):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise MCPExecutionCapacityError(
                f"MCP execution capacity is busy; retry {tool_name} shortly"
            )
        await asyncio.sleep(min(0.025, remaining))
    _increment_active_workers()

    try:
        trace = _ExecutionTrace(
            tool_name, getattr(function, "__name__", type(function).__name__),
            round((asyncio.get_running_loop().time() - wait_started) * 1000),
            diagnostic_id,
        )
        future = _EXECUTOR.submit(partial(_execute_traced, trace, function, *args, **kwargs))
    except BaseException:
        _decrement_active_workers()
        _CAPACITY.release()
        raise
    future.add_done_callback(_release_capacity)

    wrapped = asyncio.wrap_future(future)
    default_budget = (
        _JOB_TIMEOUT_SECONDS if tool_name in _LONG_JOB_TOOLS else _DEFAULT_TIMEOUT_SECONDS
    )
    budget = max(0.001, float(timeout_seconds if timeout_seconds is not None else default_budget))
    try:
        return await asyncio.wait_for(asyncio.shield(wrapped), timeout=budget)
    except asyncio.TimeoutError as exc:
        trace.timed_out = True
        _LOGGER.warning("MCP worker timed out: %s", trace.snapshot())
        if cancellation_event is not None:
            cancellation_event.set()
        raise MCPExecutionTimeoutError(
            f"{tool_name} exceeded the hosted execution budget of {budget:g} seconds",
            timeout_seconds=budget,
        ) from exc


def execution_status() -> dict[str, int]:
    """Return non-sensitive configuration for diagnostics and readiness logs."""

    with _STATE_LOCK:
        active_workers = _ACTIVE_WORKERS
    return {
        "max_workers": _MAX_WORKERS,
        "active_workers": active_workers,
        "default_timeout_seconds": _DEFAULT_TIMEOUT_SECONDS,
    }
