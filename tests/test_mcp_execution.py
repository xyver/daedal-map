import asyncio
import threading
import unittest
from unittest import mock

from mapmover import mcp_execution


class MCPExecutionTests(unittest.IsolatedAsyncioTestCase):
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
