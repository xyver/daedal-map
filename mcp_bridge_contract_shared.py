"""Published output and access contract for dedicated Bridge MCP tools."""
from __future__ import annotations
from copy import deepcopy
from typing import Any
from tool_access_shared import tool_capability_id, tool_family, tool_pricing

BRIDGE_TOOL_IDS = frozenset({"list_bridges", "get_bridge", "query_bridge"})
_ERROR = {"type": "object", "properties": {
    "code": {"type": "string"}, "message": {"type": "string"}},
    "required": ["code", "message"], "additionalProperties": True}
_BRIDGE = {"type": "object", "properties": {
    "bridge_id": {"type": ["string", "null"]},
    "system": {"type": "string"},
    "source_release": {"type": ["string", "null"]},
    "row_count": {"type": ["integer", "null"], "minimum": 0},
    "execution_status": {"type": "string"},
}, "required": ["system", "execution_status"], "additionalProperties": True}
_EDGE = {"type": "object", "properties": {
    "external_id": {"type": ["string", "null"]},
    "loc_id": {"type": ["string", "null"]},
    "relationship_type": {"type": "string"},
    "identity_binding": {"type": "boolean"},
}, "required": ["external_id", "loc_id", "relationship_type", "identity_binding"],
    "additionalProperties": True}

def bridge_tool_output_schema(name: str) -> dict[str, Any]:
    if name == "list_bridges":
        fields = {"bridges": {"type": "array", "items": _BRIDGE}}
        required = ["ok", "bridges"]
    elif name == "get_bridge":
        fields = {"bridge": _BRIDGE}
        required = ["ok", "bridge"]
    elif name == "query_bridge":
        fields = {
            "system": {"type": "string"}, "country_scope": {"type": "string"},
            "direction": {"enum": ["external_to_loc_id", "loc_id_to_external"]},
            "value": {"type": "string"}, "edges": {"type": "array", "items": _EDGE},
            "edge_count": {"type": "integer", "minimum": 0},
            "has_more": {"type": "boolean"},
            "identity_bindings": {"type": "array", "items": {"type": "string"}},
            "identity_binding_count": {"type": "integer", "minimum": 0},
            "identity_status": {"enum": ["none", "unique", "ambiguous"]},
        }
        required = ["ok", "system", "country_scope", "direction", "value", "edges",
                    "edge_count", "has_more", "identity_bindings", "identity_status"]
    else:
        raise ValueError(f"Unknown bridge tool: {name}")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object", "properties": {"ok": {"type": "boolean"},
            "error": _ERROR, **deepcopy(fields)},
        "anyOf": [{"required": required}, {"required": ["ok", "error"]}],
        "additionalProperties": True,
    }

def decorate_bridge_tool_definition(definition: dict[str, Any]) -> dict[str, Any]:
    name = str(definition.get("name") or "")
    if name not in BRIDGE_TOOL_IDS:
        return definition
    decorated = deepcopy(definition)
    decorated["outputSchema"] = bridge_tool_output_schema(name)
    decorated["annotations"] = {
        **decorated.get("annotations", {}), "readOnlyHint": True,
        "destructiveHint": False, "idempotentHint": True, "openWorldHint": False,
    }
    decorated["_meta"] = {**decorated.get("_meta", {}),
        "com.daedalmap/access": {
            "contract_version": "1.0.0",
            "capability_id": tool_capability_id(name),
            "family": tool_family(name),
            "pricing": tool_pricing(name),
            "help": {"tool": "get_tool_help", "arguments": {"tool_name": name}},
        },
        "com.daedalmap/bridge-contract": {
            "contract_version": "1.0.0",
            "result_family": "typed_bridge_edges" if name == "query_bridge" else "bridge_discovery",
            "identity_rule": "equivalent_identity_only",
        },
    }
    return decorated
