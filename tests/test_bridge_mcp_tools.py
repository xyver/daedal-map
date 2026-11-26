"""Focused contract checks for the dedicated Bridge MCP facade."""
from __future__ import annotations

import unittest
from unittest import mock
from jsonschema import Draft202012Validator
from mcp_surface_shared import build_tool_definitions
from fastapi import FastAPI
from fastapi.testclient import TestClient
from mapmover.routes.mcp import router
from bridge_tools_shared import bridge_query_result


class BridgeMcpToolsTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def call(self, method, params=None):
        response = self.client.post('/mcp/bridges', json={
            'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or {},
        })
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_facade_lists_only_bridge_workflow(self):
        names = {item['name'] for item in self.call('tools/list')['result']['tools']}
        self.assertEqual(names, {'get_tool_help', 'list_bridges', 'get_bridge', 'query_bridge'})
        help_result = self.call('tools/call', {'name': 'get_tool_help',
            'arguments': {'tool_name': 'query_bridge'}})['result']['structuredContent']
        self.assertTrue(help_result['ok'])
        self.assertIn('equivalent_identity', help_result['purpose'])

    def test_dispatch_and_unavailable_error(self):
        with mock.patch('mapmover.runtime.bridge_mcp.list_bridges', return_value={
            'ok': True, 'bridges': [{'system': 'overture_gers'}]}):
            result = self.call('tools/call', {'name': 'list_bridges', 'arguments': {}})
        self.assertEqual(result['result']['structuredContent']['bridges'][0]['system'], 'overture_gers')
        with mock.patch('mapmover.runtime.bridge_mcp.query_bridge', return_value={
            'ok': False, 'error': {'code': 'bridge_artifact_unavailable', 'message': 'not readable'}}):
            result = self.call('tools/call', {'name': 'query_bridge', 'arguments': {
                'system': 'overture_gers', 'country_scope': 'CAN',
                'external_id': 'd5654a87-5adf-42b3-9ff7-c656797a1776'}})
        self.assertEqual(result['result']['structuredContent']['error']['code'], 'bridge_artifact_unavailable')

    def test_published_bridge_output_contracts(self):
        definitions = {item['name']: item for item in build_tool_definitions()}
        samples = {
            'list_bridges': {'ok': True, 'bridges': [{'system': 'overture_gers', 'execution_status': 'not_yet_verified'}]},
            'get_bridge': {'ok': True, 'bridge': {'system': 'overture_gers', 'execution_status': 'artifact_verified'}},
            'query_bridge': bridge_query_result(system='overture_gers', country_scope='CAN',
                direction='external_to_loc_id', value='sample',
                rows=[{'external_id': 'sample', 'loc_id': 'CAN', 'relationship_type': 'equivalent_identity'}],
                limit=25),
        }
        for name, payload in samples.items():
            schema = definitions[name]['outputSchema']
            Draft202012Validator.check_schema(schema)
            Draft202012Validator(schema).validate(payload)
            Draft202012Validator(schema).validate({'ok': False,
                'error': {'code': 'bridge_artifact_unavailable', 'message': 'missing'}})

    def test_bridge_discovery_metadata_and_help_handoff(self):
        from mapmover.runtime import reference_exchange
        from mcp_tool_help_shared import topic_help_payload

        catalog = {"schema_version": "test", "external_reference_bridges": [{
            "bridge_id": "overture_gers_loc_id_bridge", "external_system": "overture_gers",
            "external_release": "2026-07-22.0", "row_count": 243162, "status": "admitted",
        }]}
        with mock.patch.object(reference_exchange, 'load_geometry_catalog', return_value=catalog), \
             mock.patch.object(reference_exchange, '_published_geometry_family_index', return_value={}), \
             mock.patch.object(reference_exchange, '_public_catalog_records',
                               side_effect=lambda source, key: source.get(key, [])):
            directory = reference_exchange.geography_directory()
        self.assertEqual(directory['external_bridges'][0]['system'], 'overture_gers')
        self.assertEqual(directory['external_bridges'][0]['source_release'], '2026-07-22.0')
        overview = topic_help_payload('overview', question='How do I use Overture GERS bridges?',
                                      available_tool_names=['get_tool_help', 'get_catalog'])
        self.assertEqual(overview['help_topic'], 'bridges')
        self.assertEqual(overview['start_here'][0]['mcp_endpoint'], 'https://app.daedalmap.com/mcp/bridges')
        self.assertEqual(overview['available_tools'], ['get_tool_help'])

    def test_cloud_country_coverage_reads_pinned_release_manifest(self):
        from types import SimpleNamespace
        from mapmover.runtime import bridge_mcp
        from mapmover.runtime.external_reference_adapters import _release_fingerprint
        manifest = {"profile": "external_reference_bridge", "external_system": "overture_gers",
                    "external_release": "2026-07-22.0", "adapter": {},
                    "partitions": [{"country_iso3": "CAN", "source_family": "region",
                                    "internal_spine_release": "old_canada", "row_count": 48}]}
        manifest['release_fingerprint'] = _release_fingerprint(manifest)
        adapter = SimpleNamespace(system='overture_gers')
        bridge = SimpleNamespace(release_fingerprint=manifest['release_fingerprint'],
                                 source_release='2026-07-22.0', partitions=())
        record = {'manifest_path': 'geometry/verified-release.json'}
        bridge_mcp._published_partition_metadata.cache_clear()
        with mock.patch.object(bridge_mcp, 'is_cloud_mode', return_value=True), \
             mock.patch.object(bridge_mcp, 'admitted_external_adapters', return_value=[adapter]), \
             mock.patch.object(bridge_mcp, 'admitted_bridge', return_value=bridge), \
             mock.patch.object(bridge_mcp, '_record', return_value=record), \
             mock.patch.object(bridge_mcp, 'read_artifact_json', return_value=manifest):
            coverage = bridge_mcp.country_bridge_coverage('CAN')
            self.assertEqual(coverage[0]['source_families'], ['region'])
            self.assertEqual(coverage[0]['row_count'], 48)
            self.assertEqual(bridge_mcp.country_bridge_coverage('USA'), [])
        bridge_mcp._published_partition_metadata.cache_clear()

    def test_country_crosswalk_alias_is_a_reverse_lookup_only(self):
        from types import SimpleNamespace
        from mapmover.runtime import external_reference_adapters as adapters

        edge = SimpleNamespace(loc_id='CAN-G182249235B24619009834807',
                               external_id='24bc6352-7e6d-4841-8b92-7611ff5000ce',
                               is_equivalence=True, geometry_confidence=0.99)
        crosswalk = {'mappings': {'CAN-BC': edge.loc_id}}
        calls = []

        def query(_bridge, _partitions, *, values, **_kwargs):
            calls.append(values)
            return [edge] if values == [edge.loc_id] else []

        with mock.patch('mapmover.foundation_helpers.load_country_crosswalk', return_value=crosswalk), \
             mock.patch.object(adapters, 'get_external_adapter', return_value=SimpleNamespace()), \
             mock.patch.object(adapters, 'admitted_bridge', return_value=SimpleNamespace()), \
             mock.patch.object(adapters, '_bridge_available', return_value=True), \
             mock.patch.object(adapters, '_selected_partitions', return_value=[]), \
             mock.patch.object(adapters, '_query_edges', side_effect=query):
            rows = adapters.lookup_loc_id_edges_batch('overture_gers', ['CAN-BC'], country_scope='CAN')
        self.assertEqual([row.external_id for row in rows['CAN-BC']], [edge.external_id])
        self.assertEqual(calls, [['CAN-BC'], [edge.loc_id]])
        with mock.patch('mapmover.foundation_helpers.load_country_crosswalk', return_value=crosswalk):
            self.assertEqual(adapters._country_geometry_alias('CAN-BC'), edge.loc_id)
            self.assertIsNone(adapters._country_geometry_alias('CAN-XX'))
        from mapmover.runtime import bridge_mcp
        with mock.patch('mapmover.foundation_helpers.load_country_crosswalk', return_value=crosswalk):
            self.assertEqual(
                bridge_mcp._country_aliases_for_bridge_ids('CAN', [edge.loc_id]),
                {edge.loc_id: 'CAN-BC'},
            )
        with mock.patch('mapmover.foundation_helpers.load_country_crosswalk',
                        return_value={'mappings': {'CAN-BC': edge.loc_id, 'CAN-XX': edge.loc_id}}):
            self.assertIsNone(adapters._country_geometry_alias('CAN-BC'))
            self.assertEqual(bridge_mcp._country_aliases_for_bridge_ids('CAN', [edge.loc_id]), {})

    def test_relationship_does_not_bind_identity(self):
        result = bridge_query_result(system='overture_gers', country_scope='CAN',
            direction='external_to_loc_id', value='example', rows=[
                {'external_id': 'example', 'loc_id': 'CAN', 'relationship_type': 'equivalent_identity'},
                {'external_id': 'example', 'loc_id': 'CAN-BC', 'relationship_type': 'overlaps'},
            ], limit=25)
        self.assertEqual(result['identity_bindings'], ['CAN'])
        self.assertEqual(result['identity_status'], 'unique')
        self.assertFalse(result['edges'][1]['identity_binding'])
        limited = bridge_query_result(system='overture_gers', country_scope='CAN',
            direction='external_to_loc_id', value='example', rows=[
                {'external_id': 'example', 'loc_id': 'CAN', 'relationship_type': 'equivalent_identity'},
                {'external_id': 'example', 'loc_id': 'CAN-BC', 'relationship_type': 'equivalent_identity'},
            ], limit=1)
        self.assertEqual(limited['identity_status'], 'ambiguous')
        self.assertTrue(limited['has_more'])


if __name__ == '__main__':
    unittest.main()
