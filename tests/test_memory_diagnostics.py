import unittest
from mapmover.memory_diagnostics import bounded_size, loaded_owner_memory


class MemoryDiagnosticsTests(unittest.TestCase):
    def test_cycles_are_counted_once(self):
        value = []
        value.append(value)
        report = bounded_size(value)
        self.assertEqual(report['visited_nodes'], 1)
        self.assertFalse(report['truncated'])

    def test_graph_is_bounded(self):
        report = bounded_size(list(range(1000)), max_nodes=10)
        self.assertLessEqual(report['visited_nodes'], 10)
        self.assertTrue(report['truncated'])

    def test_missing_owners_do_not_import_them(self):
        import sys
        before = set(sys.modules)
        report = loaded_owner_memory()
        self.assertIn('owners', report)
        self.assertEqual(set(sys.modules), before)
