import unittest
import sys
import threading
from types import SimpleNamespace
from unittest import mock

from mapmover.memory_diagnostics import (
    _proc_kib, bounded_size, loaded_dataframe_cache_memory, loaded_owner_memory,
)


class _Bytes:
    def __init__(self, value):
        self.value = value

    def sum(self):
        return self.value


class _Frame:
    def __init__(self, size):
        self.size = size

    def memory_usage(self, deep=False):
        assert deep
        return _Bytes(self.size)


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
        before = set(sys.modules)
        report = loaded_owner_memory()
        self.assertIn('owners', report)
        self.assertEqual(set(sys.modules), before)

    def test_proc_parser_keeps_selected_kib_fields(self):
        text = 'VmRSS:\t123 kB\nThreads:\t7\nIgnored:\t99 kB\n'
        with mock.patch('mapmover.memory_diagnostics.Path.read_text', return_value=text):
            self.assertEqual(_proc_kib('/proc/fake', ('VmRSS', 'Threads')), {'VmRSS': 123, 'Threads': 7})

    def test_dataframe_cache_split_exposes_reclaimable_expired_bytes(self):
        query = SimpleNamespace(
            _CACHE_LOCK=threading.Lock(),
            _CACHE={
                'active': (_Frame(100), 200.0),
                'expired': (_Frame(200), 50.0),
                'permanent': (_Frame(300), float('inf')),
            },
        )
        geometry = SimpleNamespace(
            _country_parquet_cache_lock=threading.Lock(),
            _country_parquet_cache={'USA': _Frame(400)},
        )
        with mock.patch.dict(sys.modules, {
            'mapmover.duckdb_helpers': query,
            'mapmover.geometry_handlers': geometry,
        }), mock.patch('mapmover.memory_diagnostics.time.monotonic', return_value=100.0):
            report = loaded_dataframe_cache_memory()
        self.assertEqual(report['query_results']['active_bytes'], 100)
        self.assertEqual(report['query_results']['expired_bytes'], 200)
        self.assertEqual(report['query_results']['permanent_bytes'], 300)
        self.assertEqual(report['geometry']['bytes'], 400)
