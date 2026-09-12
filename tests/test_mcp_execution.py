import asyncio
import threading
import unittest
from unittest import mock

from mapmover import mcp_execution


class MCPExecutionTests(unittest.IsolatedAsyncioTestCase):
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
        with mock.patch.object(mcp_execution, "_CAPACITY", test_capacity), mock.patch.object(
            mcp_execution, "_EXECUTOR", test_executor
        ):
            with self.assertRaises(mcp_execution.MCPExecutionTimeoutError):
                await mcp_execution.run_mcp_blocking(
                    "slow_tool", lambda: gate.wait(timeout=2), timeout_seconds=0.01
                )
            with self.assertRaises(mcp_execution.MCPExecutionCapacityError):
                await mcp_execution.run_mcp_blocking("second_tool", lambda: None)
            gate.set()
            await asyncio.sleep(0.05)
            self.assertIsNone(await mcp_execution.run_mcp_blocking("third_tool", lambda: None))
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


if __name__ == "__main__":
    unittest.main()
