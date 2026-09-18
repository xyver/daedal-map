import threading
import unittest
from unittest.mock import patch

from mapmover import memory_logging as history


class MemoryHistoryTests(unittest.TestCase):
    def test_sample_preserves_unknown_memory_and_does_not_load_owners(self):
        import sys
        before = set(sys.modules)
        with patch.object(history.Path, 'read_text', side_effect=OSError('unavailable')):
            row = history.sample_memory()
        self.assertEqual(set(sys.modules), before)
        self.assertIsNone(row['cgroup_current_bytes'])
        self.assertIn('process_error', row)
        self.assertNotIn('process', row)
        self.assertTrue(all('entry_estimates' not in owner for owner in row['owners'].values()))

    def test_monitor_records_and_stops_without_waiting_for_interval(self):
        sampled = threading.Event()
        def log(*args):
            sampled.set()
        with patch.dict(history.os.environ, {'MEMORY_SAMPLE_SECONDS': '300'}), \
                patch.object(history, 'sample_memory', return_value={'process_epoch': history._EPOCH}), \
                patch.object(history.logger, 'info', side_effect=log):
            close = history.start_memory_logging()
            try:
                self.assertTrue(sampled.wait(2))
            finally:
                close()
        self.assertFalse(any(t.name == 'memory-logging' for t in threading.enumerate()))

    def test_disabled_monitor_does_not_sample(self):
        with patch.dict(history.os.environ, {'MEMORY_SAMPLE_SECONDS': '0'}), \
                patch.object(history, 'sample_memory') as sample:
            history.start_memory_logging()()
        sample.assert_not_called()


if __name__ == '__main__':
    unittest.main()
