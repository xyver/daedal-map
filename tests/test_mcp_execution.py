import asyncio
import threading
import unittest
from unittest import mock

import duckdb

from mapmover import mcp_execution
from mapmover.duckdb_helpers import run_df


class MCPExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_interrupts_active_duckdb_query_and_releases_worker(self):
        started = threading.Event()
        finished = threading.Event()
        interrupted = threading.Event()
        connections = []

        class StartedConnection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, *args, **kwargs):
                started.set()
                return self.connection.execute(*args, **kwargs)

            def interrupt(self):
                self.connection.interrupt()

        def worker():
            connection = duckdb.connect()
            connections.append(connection)
            try:
                with (
                    mock.patch("mapmover.duckdb_helpers.is_cloud_mode", return_value=False),
                    mock.patch("mapmover.duckdb_helpers._get_thread_connection", return_value=StartedConnection(connection)),
                ):
                    try:
                        run_df("SELECT sum(i) FROM range(1000000000000) t(i)", [])
                    except duckdb.InterruptException:
                        interrupted.set()
            finally:
                finished.set()
                connection.close()

        task = asyncio.create_task(mcp_execution.run_mcp_blocking(
            "resolve_deep_point", worker, timeout_seconds=0.1,
        ))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            with self.assertRaises(mcp_execution.MCPExecutionTimeoutError):
                await task
            self.assertTrue(await asyncio.to_thread(finished.wait, 2))
            self.assertTrue(interrupted.is_set())
        finally:
            if connections and not finished.is_set():
                connections[0].interrupt()
                await asyncio.to_thread(finished.wait, 2)

    async def test_artifact_client_error_trace_records_only_code(self):
        class ClientError(Exception):
            response = {"Error": {"Code": "NoSuchKey", "Message": "private object path"}}

        trace = mcp_execution._ExecutionTrace("test_tool", "worker", 0, None)
        mcp_execution._TRACE_LOCAL.trace = trace
        try:
            mcp_execution.record_mcp_artifact_error(ClientError("private object path"))
            self.assertEqual(trace.snapshot()["last_artifact_error"], "ClientError:NoSuchKey")
            self.assertNotIn("private object path", str(trace.snapshot()))
        finally:
            del mcp_execution._TRACE_LOCAL.trace

    async def test_create_job_keeps_its_longer_execution_budget(self):
        with mock.patch.object(mcp_execution, "_JOB_TIMEOUT_SECONDS", 7):
            with mock.patch.object(mcp_execution.asyncio, "wait_for", wraps=asyncio.wait_for) as wait:
                self.assertEqual(
                    await mcp_execution.run_mcp_blocking("create_geometry_export", lambda: "done"),
                    "done",
                )
        self.assertEqual(wait.call_args.kwargs["timeout"], 7)

    async def test_blocking_work_does_not_block_event_loop(self):
        gate = threading.Event()
        started = threading.Event()

        def worker():
            started.set()
            gate.wait(timeout=2)
            return "done"

        task = asyncio.create_task(
            mcp_execution.run_mcp_blocking("test_tool", worker, timeout_seconds=2)
        )
        await asyncio.to_thread(started.wait, 1)
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        gate.set()
        self.assertEqual(await task, "done")

    async def test_timeout_keeps_capacity_charged_until_worker_finishes(self):
        gate = threading.Event()
        test_capacity = threading.BoundedSemaphore(1)
        test_executor = mcp_execution.ThreadPoolExecutor(max_workers=1)
        with (
            mock.patch.object(mcp_execution, "_CAPACITY", test_capacity),
            mock.patch.object(mcp_execution, "_EXECUTOR", test_executor),
            mock.patch.object(mcp_execution, "_MAX_WORKERS", 1),
            mock.patch.object(mcp_execution, "_ACTIVE_WORKERS", 0),
        ):
            with self.assertRaises(mcp_execution.MCPExecutionTimeoutError):
                await mcp_execution.run_mcp_blocking(
                    "slow_tool", lambda: gate.wait(timeout=2), timeout_seconds=0.01
                )
            self.assertEqual(
                mcp_execution.execution_status(),
                {"max_workers": 1, "active_workers": 1, "default_timeout_seconds": 30},
            )
            with self.assertRaises(mcp_execution.MCPExecutionCapacityError):
                await mcp_execution.run_mcp_blocking(
                    "second_tool", lambda: None, capacity_wait_seconds=0.01,
                )
            gate.set()
            await asyncio.sleep(0.05)
            self.assertIsNone(await mcp_execution.run_mcp_blocking("third_tool", lambda: None))
            await asyncio.sleep(0.01)
            self.assertEqual(mcp_execution.execution_status()["active_workers"], 0)
        test_executor.shutdown(wait=True)

    async def test_short_capacity_wait_absorbs_small_parallel_burst(self):
        gate = threading.Event()
        started = threading.Event()
        test_capacity = threading.BoundedSemaphore(1)
        test_executor = mcp_execution.ThreadPoolExecutor(max_workers=1)

        def first_worker():
            started.set()
            gate.wait(timeout=1)

        with (
            mock.patch.object(mcp_execution, "_CAPACITY", test_capacity),
            mock.patch.object(mcp_execution, "_EXECUTOR", test_executor),
            mock.patch.object(mcp_execution, "_MAX_WORKERS", 1),
            mock.patch.object(mcp_execution, "_ACTIVE_WORKERS", 0),
        ):
            first = asyncio.create_task(
                mcp_execution.run_mcp_blocking("first_tool", first_worker)
            )
            await asyncio.to_thread(started.wait, 1)
            second = asyncio.create_task(
                mcp_execution.run_mcp_blocking(
                    "second_tool", lambda: "done", capacity_wait_seconds=0.5,
                )
            )
            await asyncio.sleep(0.05)
            gate.set()
            await first
            self.assertEqual(await second, "done")
        test_executor.shutdown(wait=True)

    async def test_timeout_signals_cooperative_cancellation(self):
        cancellation = threading.Event()

        def worker():
            cancellation.wait(timeout=2)

        with self.assertRaises(mcp_execution.MCPExecutionTimeoutError):
            await mcp_execution.run_mcp_blocking(
                "cancellable_tool",
                worker,
                timeout_seconds=0.01,
                cancellation_event=cancellation,
            )
        self.assertTrue(cancellation.wait(timeout=1))

    async def test_timeout_reports_worker_stage_and_selected_file_count(self):
        gate = threading.Event()
        started = threading.Event()

        def worker():
            mcp_execution.mark_mcp_stage("family_exact_shapes")
            mcp_execution.record_mcp_file("private/shape.parquet")
            with mcp_execution.mcp_stage("artifact_cache_resolution"):
                mcp_execution.record_mcp_artifact_io("cache_misses")
                mcp_execution.record_mcp_artifact_error(OSError("secret path"))
                started.set()
                gate.wait(timeout=1)

        with mock.patch.object(mcp_execution, "_LOGGER") as logger:
            try:
                task = asyncio.create_task(
                    mcp_execution.run_mcp_blocking(
                        "resolve_deep_point", worker, timeout_seconds=0.05,
                        diagnostic_id="caller-private-id",
                    )
                )
                await asyncio.to_thread(started.wait, 1)
                with self.assertRaises(mcp_execution.MCPExecutionTimeoutError):
                    await task
                snapshot = logger.warning.call_args.args[1]
                self.assertEqual(snapshot["stage"], "artifact_cache_resolution")
                self.assertEqual(snapshot["selected_files"], 1)
                self.assertEqual(snapshot["artifact_io"], {"cache_misses": 1})
                self.assertEqual(snapshot["last_artifact_error"], "OSError")
                self.assertNotIn("secret path", str(snapshot))
                self.assertIn("family_exact_shapes", snapshot["stage_durations_ms"])
                self.assertEqual(len(snapshot["request_tag"]), 12)
                self.assertNotIn("caller-private-id", str(snapshot))
                self.assertNotIn("private/shape.parquet", str(snapshot))
            finally:
                gate.set()


if __name__ == "__main__":
    unittest.main()
