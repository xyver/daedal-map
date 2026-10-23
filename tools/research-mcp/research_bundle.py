from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


BUNDLE_DIR = Path(__file__).resolve().parent
if str(BUNDLE_DIR) not in sys.path:
    sys.path.insert(0, str(BUNDLE_DIR))

import research_mcp_module  # noqa: E402
from mcp_runtime_shared import jsonrpc_error_envelope, jsonrpc_result_envelope  # noqa: E402


PROVIDER_SLUG = "research"
MCP_PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {
    "name": "com.daedalmap/research-local",
    "title": "DaedalMap Local Research MCP",
    "version": "0.1.0",
}
INSTRUCTIONS = (
    "Local source-bound Research MCP. This server prepares explicit "
    "pack/source boundaries and deterministic evidence access; it does not "
    "run a hidden Research LLM. If the user asks how this MCP works, call "
    "how_research_mcp_works. Use get_tool_help with an exact name from tools/list for one tool's strict contract. Call where_is_research_data to identify the installed data source, then get_catalog with catalog='data' to scan available packs and sources. "
    "source is active. The calling client LLM owns natural-language interpretation: translate the user's request into each tool's strict JSON schema, except where a schema explicitly accepts a question or query string. On errors, inspect error, guidance, and clarification; ask the user only when clarification.required is true, otherwise repair the call directly. "
    "The local server reads the selected data folder. For "
    "source selection, start with "
    "resolve_research_corpus_intent or search_research_sources when the user "
    "does not know ids. Always call ask_research_sources with pack_ids or "
    "source_ids plus the question before any evidence query, even when "
    "resolve_research_corpus_intent has needs_confirmation=false; "
    "ask_research_sources is the binding step that returns the active "
    "source_ids corpus. Copy that exact full source_ids list into every "
    "query_research_source_data call and query one source_id inside it. Call "
    "resolve_research_geography before setting filters.region_ids from a human "
    "place name, regional group, or alias; copy its returned canonical loc_ids "
    "unchanged rather than guessing ids. Call "
    "get_research_pack for the selected packs before constructing queries. For "
    "multi-source packs, call it again with the relevant source_ids when "
    "source_selection_required is true. Use only the exact metrics, "
    "filterable_fields, and sortable_fields it returns (do not invent metric or "
    "filter names). Follow its research_query_contract for filter placement, "
    "time shape, and default/max row limits. If a query returns query_too_broad, "
    "narrow by region_ids, time, or aggregation before retrying; do not bypass "
    "the source boundary. Answer in the calling client model. Do not answer "
    "from the entire catalog when no source boundary is active, do not claim "
    "global/all-basin coverage unless the evidence calls actually covered it, "
    "and label any outside model knowledge as outside the returned data."
)
RESOURCE_MODE_URI = "daedalmap://research/mode"
RESOURCE_BOOTSTRAP_URI = "daedalmap://research/bootstrap"
RESOURCE_SETUP_URI = "daedalmap://research/setup"


def _research_tool_definitions() -> list[dict[str, Any]]:
    return [
        {
            "name": "get_tool_help",
            "title": "Get Research Tool Help",
            "description": "Return the strict schema, use/refusal guidance, example, outputs, and next calls for one exact Research MCP tool. Use how_research_mcp_works for the family workflow.",
            "inputSchema": {
                "type": "object",
                "properties": {"tool_name": {"type": "string", "description": "Exact Research MCP tool name from tools/list."}},
                "required": ["tool_name"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "how_research_mcp_works",
            "title": "How Research MCP Works",
            "description": "Explain the source-bound Research MCP workflow, when to search or resolve sources, and how to ask a question.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "Optional user wording, such as 'how do you work?'."},
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "where_is_research_data",
            "title": "Where Is Research Data",
            "description": "Scan the active installed catalog and report available Research packs and sources. A local server can also change its data folder.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "data_root": {
                        "type": "string",
                        "description": (
                            "Local data folder to read."
                        ),
                    },
                    "catalog_file": {
                        "type": "string",
                        "enum": ["catalog.json", "wip_catalog.json", "internal_catalog.json"],
                        "description": "Which catalog inside data_root to read. Default catalog.json.",
                    },
                    "reset": {
                        "type": "boolean",
                        "description": "Clear the local folder override and return to the default data source.",
                    },
                },
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": False},
        },
        {
            "name": "get_catalog",
            "title": "Get Research Catalog",
            "description": "List available data packs in this server's active installed catalog. Use get_research_pack for a selected pack's source and query details.",
            "inputSchema": {
                "type": "object",
                "properties": {"catalog": {"type": "string", "enum": ["data"], "description": "The Research data catalog."}},
                "required": ["catalog"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_research_geography",
            "title": "Resolve Research Geography",
            "description": "Resolve human place names, canonical loc_ids, and named regional groups through DaedalMap's shared geometry/crosswalk resolver. Use before setting query filters.region_ids; unresolved names return an explicit error rather than an empty data result.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "locations": {
                        "anyOf": [{"type": "array", "items": {"type": "string"}, "minItems": 1}, {"type": "string"}],
                        "description": "Human place names, canonical loc_ids, or named regional groups such as 'Mediterranean Sea'.",
                    },
                    "country_hint": {"type": "string", "description": "Optional country context when a place name is ambiguous."},
                },
                "required": ["locations"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "search_research_sources",
            "title": "Search Research Sources",
            "description": "Free discovery over DaedalMap pack/source metadata before creating a source-bound Research corpus.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural-language source need or topic."},
                    "geography": {"type": "string", "description": "Optional geographic scope hint."},
                    "time_window": {"type": "string", "description": "Optional time-window hint."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "resolve_research_corpus_intent",
            "title": "Resolve Research Corpus Intent",
            "description": "Map a natural-language source request to candidate DaedalMap pack/source ids.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Source-selection request, not the final analysis question."},
                    "allow_packs": {"type": "boolean", "description": "Allow pack-level matches. Default true."},
                    "include_sources": {"type": "boolean", "description": "Expand matched packs to source ids. Default true."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "get_research_pack",
            "title": "Get Research Pack",
            "description": "Return the existing DaedalMap Agent/API pack-detail contract plus a source-specific Research query contract for exact metrics, filter placement, sorts, time shape, coverage, and row limits.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pack_id": {
                        "type": "string",
                        "description": "One pack id selected for the Research corpus.",
                    },
                    "source_ids": {
                        "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}],
                        "description": "Optional source ids from this pack. Use this to load full exact metric definitions after the compact pack overview.",
                    },
                },
                "required": ["pack_id"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
        {
            "name": "ask_research_sources",
            "title": "Ask Research Sources",
            "description": "Bind a Research question to an explicit active corpus of DaedalMap source ids. Call this after search/resolve and before query_research_source_data. This tool never runs a hidden server-side LLM; the calling client model should reason from deterministic source/data results.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pack_ids": {
                        "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}],
                        "description": "Pack ids such as earthquakes, wildfires, and currency.",
                    },
                    "source_ids": {
                        "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}],
                        "description": "Concrete source ids when source-level control is needed.",
                    },
                    "question": {"type": "string"},
                    "execute": {
                        "type": "boolean",
                        "description": "Deprecated compatibility option. The source-list corpus is prepared without app-session hydration.",
                    },
                    "force_large_display": {
                        "type": "boolean",
                        "description": "Reserved for future deterministic display/data tools. Ignored by this source-bound preparation step.",
                    },
                },
                "required": ["question"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": False},
        },
        {
            "name": "query_research_source_data",
            "title": "Query Research Source Data",
            "description": "Run a deterministic query_dataset-style row query against one source_id inside an explicit Research source boundary. This returns evidence rows and never calls a server-side LLM.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_ids": {
                        "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}],
                        "description": "Full allowed source boundary returned by ask_research_sources. Copy it unchanged into each evidence query, even though query.source_id selects one source inside it.",
                    },
                    "pack_ids": {
                        "anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "string"}],
                        "description": "Optional pack boundary to expand to allowed source_ids.",
                    },
                    "query": {
                        "type": "object",
                        "description": "Canonical query_dataset payload for one source inside the supplied Research corpus.",
                        "properties": {
                            "request_id": {
                                "type": "string",
                                "description": "Optional caller-generated trace id.",
                            },
                            "source_id": {
                                "type": "string",
                                "description": "One exact source_id from the supplied source_ids boundary.",
                            },
                            "metrics": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 1,
                                "description": "Exact metric ids from get_research_pack; do not invent names.",
                            },
                            "filters": {
                                "type": "object",
                                "properties": {
                                    "region_ids": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Geographic loc_ids.",
                                    },
                                    "time": {
                                        "type": "object",
                                        "properties": {
                                            "value": {},
                                            "start": {},
                                            "end": {},
                                            "granularity": {"type": "string"},
                                        },
                                        "additionalProperties": False,
                                    },
                                    "equals": {
                                        "type": "object",
                                        "description": "Exact matches for published filterable_fields other than geography and time.",
                                    },
                                    "compare": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "field": {"type": "string"},
                                                "op": {
                                                    "type": "string",
                                                    "enum": ["=", "!=", ">", ">=", "<", "<="],
                                                },
                                                "value": {},
                                            },
                                            "required": ["field", "op", "value"],
                                            "additionalProperties": False,
                                        },
                                    },
                                },
                                "additionalProperties": False,
                            },
                            "sort": {
                                "anyOf": [
                                    {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "direction": {
                                                "type": "string",
                                                "enum": ["asc", "desc"],
                                            },
                                        },
                                        "required": ["field"],
                                        "additionalProperties": False,
                                    },
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "field": {"type": "string"},
                                                "direction": {
                                                    "type": "string",
                                                    "enum": ["asc", "desc"],
                                                },
                                            },
                                            "required": ["field"],
                                            "additionalProperties": False,
                                        },
                                    },
                                ],
                            },
                            "aggregation": {
                                "type": "object",
                                "description": "Optional source-approved event-count grouping. Read analysis_dimensions from get_research_pack first.",
                                "properties": {
                                    "dimensions": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "minItems": 1,
                                        "maxItems": 2,
                                    },
                                    "include_labels": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                                "required": ["dimensions"],
                                "additionalProperties": False,
                            },
                            "limit": {
                                "type": "integer",
                                "minimum": 1,
                                "description": "Must not exceed the source max_limit returned by get_research_pack.",
                            },
                        },
                        "required": ["source_id", "metrics"],
                        "additionalProperties": False,
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Optional limit applied when query.limit is absent.",
                    },
                },
                "required": ["source_ids", "query"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True},
        },
    ]


def _research_resource_definitions() -> list[dict[str, Any]]:
    return [
        {
            "uri": RESOURCE_SETUP_URI,
            "name": "Research MCP Setup",
            "title": "Research MCP Setup",
            "description": "Local Research MCP data and client setup.",
            "mimeType": "text/markdown",
        },
        {
            "uri": RESOURCE_BOOTSTRAP_URI,
            "name": "Research MCP Bootstrap",
            "title": "Research MCP Bootstrap",
            "description": "Short startup guidance for source-bound Research MCP use.",
            "mimeType": "text/markdown",
        },
        {
            "uri": RESOURCE_MODE_URI,
            "name": "Research MCP Mode",
            "title": "Research MCP Mode",
            "description": "Research MCP source-bound workflow and limits.",
            "mimeType": "text/markdown",
        },
    ]


def _research_prompt_definitions() -> list[dict[str, Any]]:
    return [
        {
            "name": "daedalmap_research_assistant",
            "title": "DaedalMap Research Assistant",
            "description": "Start a source-bound DaedalMap Research workflow.",
            "arguments": [],
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": (
                            "Use DaedalMap Research as a source-bound workflow. "
                            "If the user asks how this MCP works, call "
                            "how_research_mcp_works. "
                            "If the user names packs or topics, first call "
                            "resolve_research_corpus_intent. If the user needs "
                            "source discovery, call search_research_sources. "
                            "Then call ask_research_sources with explicit "
                            "pack_ids or source_ids. Call get_research_pack for "
                            "each selected pack before constructing data calls. "
                            "Use query_research_source_data "
                            "for deterministic rows inside that boundary, and keep "
                            "the source boundary visible in the response."
                        ),
                    },
                }
            ],
        }
    ]


def _jsonrpc_response(result: dict[str, Any], request_id: Any) -> JSONResponse:
    response = JSONResponse(jsonrpc_result_envelope(result, request_id))
    response.headers["MCP-Protocol-Version"] = MCP_PROTOCOL_VERSION
    response.headers["Cache-Control"] = "no-store"
    return response


def _jsonrpc_error(
    request_id: Any,
    code: int,
    message: str,
    *,
    data: dict[str, Any] | None = None,
    status_code: int = 200,
) -> JSONResponse:
    response = JSONResponse(
        jsonrpc_error_envelope(request_id, code, message, data=data),
        status_code=status_code,
    )
    response.headers["MCP-Protocol-Version"] = MCP_PROTOCOL_VERSION
    response.headers["Cache-Control"] = "no-store"
    return response


def _tool_result(payload: Any, *, is_error: bool = False) -> dict[str, Any]:
    text = json.dumps(payload, ensure_ascii=False, indent=2) if isinstance(payload, (dict, list)) else str(payload)
    result: dict[str, Any] = {
        "content": [{"type": "text", "text": text}],
        "structuredContent": payload if isinstance(payload, (dict, list)) else {"value": payload},
    }
    if is_error:
        result["isError"] = True
    return result


def _resource_text_result(uri: str, text: str, *, mime_type: str = "text/markdown") -> dict[str, Any]:
    return {"contents": [{"uri": uri, "mimeType": mime_type, "text": text}]}


def _read_resource(uri: str) -> dict[str, Any] | None:
    if uri == RESOURCE_SETUP_URI:
        return _resource_text_result(uri, _read_bundle_text("docs/research_mcp_setup.md"))
    if uri == RESOURCE_BOOTSTRAP_URI:
        return _resource_text_result(uri, _read_bundle_text("docs/research_mcp_bootstrap.md"))
    if uri == RESOURCE_MODE_URI:
        return _resource_text_result(uri, _read_bundle_text("docs/research_mcp_mode.md"))
    return None


def _read_bundle_text(relative_path: str) -> str:
    return (BUNDLE_DIR / relative_path).read_text(encoding="utf-8")


def _render_prompt(name: str) -> dict[str, Any] | None:
    prompt = next((item for item in _research_prompt_definitions() if str(item.get("name") or "") == name), None)
    if not prompt:
        return None
    return {
        "description": prompt.get("description"),
        "messages": prompt.get("messages") or [],
    }


async def handle_get_info(request: Request) -> JSONResponse:
    response = JSONResponse(info_payload())
    response.headers["MCP-Protocol-Version"] = MCP_PROTOCOL_VERSION
    response.headers["Cache-Control"] = "no-store"
    return response


def info_payload() -> dict[str, Any]:
    return {
        "serverInfo": SERVER_INFO,
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "transport": "streamable-http",
        "auth": "none (loopback local runtime)",
        "summary": "Local source-bound Research MCP for deterministic source discovery and source-list preparation.",
        "howToStart": [
            "Call how_research_mcp_works for startup instructions.",
            "Call resolve_research_corpus_intent or search_research_sources.",
            "Call ask_research_sources with explicit pack_ids or source_ids plus the question.",
            "Call get_research_pack for selected packs and follow their published query rules.",
            "Inspect source_ids and source_versions before making claims.",
            "Use query_research_source_data for deterministic rows and let the calling client LLM do the reasoning.",
        ],
        "tools": [tool["name"] for tool in _research_tool_definitions()],
        "prompts": [prompt["name"] for prompt in _research_prompt_definitions()],
        "resources": [resource["uri"] for resource in _research_resource_definitions()],
        "private": False,
    }


async def handle_post_request(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return _jsonrpc_error(None, -32700, "Parse error", status_code=400)

    if not isinstance(body, dict):
        return _jsonrpc_error(None, -32600, "Invalid Request", status_code=400)

    return await handle_jsonrpc_dict(body)


async def handle_jsonrpc_dict(body: dict[str, Any]) -> JSONResponse:
    request_id = body.get("id")
    method = str(body.get("method") or "").strip()
    params = body.get("params") or {}
    if params and not isinstance(params, dict):
        return _jsonrpc_error(request_id, -32602, "Invalid params")

    if method == "initialize":
        return _jsonrpc_response(
            {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"listChanged": False},
                    "prompts": {"listChanged": False},
                },
                "serverInfo": SERVER_INFO,
                "instructions": INSTRUCTIONS,
            },
            request_id,
        )

    if method in {"notifications/initialized", "notifications/cancelled"}:
        response = JSONResponse({}, status_code=202)
        response.headers["MCP-Protocol-Version"] = MCP_PROTOCOL_VERSION
        response.headers["Cache-Control"] = "no-store"
        return response

    if method == "ping":
        return _jsonrpc_response({}, request_id)

    if method == "tools/list":
        return _jsonrpc_response({"tools": _research_tool_definitions()}, request_id)

    if method == "resources/list":
        return _jsonrpc_response({"resources": _research_resource_definitions()}, request_id)

    if method == "resources/read":
        uri = str(params.get("uri") or "").strip()
        if not uri:
            return _jsonrpc_error(request_id, -32602, "Resource uri is required")
        payload = _read_resource(uri)
        if not payload:
            return _jsonrpc_error(request_id, -32602, f"Resource '{uri}' not found")
        return _jsonrpc_response(payload, request_id)

    if method == "prompts/list":
        return _jsonrpc_response({"prompts": _research_prompt_definitions()}, request_id)

    if method == "prompts/get":
        prompt_name = str(params.get("name") or "").strip()
        if not prompt_name:
            return _jsonrpc_error(request_id, -32602, "Prompt name is required")
        payload = _render_prompt(prompt_name)
        if not payload:
            return _jsonrpc_error(request_id, -32602, f"Prompt '{prompt_name}' not found")
        return _jsonrpc_response(payload, request_id)

    if method != "tools/call":
        return _jsonrpc_error(request_id, -32601, f"Method '{method}' not found")

    tool_name = str(params.get("name") or "").strip()
    arguments = params.get("arguments") or {}
    if not tool_name:
        return _jsonrpc_error(request_id, -32602, "Tool name is required")
    if arguments and not isinstance(arguments, dict):
        return _jsonrpc_error(request_id, -32602, "Tool arguments must be an object")
    if tool_name not in {tool["name"] for tool in _research_tool_definitions()}:
        return _jsonrpc_error(request_id, -32601, f"Tool '{tool_name}' not found")
    if tool_name == "get_catalog" and (set(arguments) != {"catalog"} or arguments.get("catalog") != "data"):
        return _jsonrpc_error(request_id, -32602, "get_catalog requires only catalog='data'")

    try:
        if tool_name == "get_tool_help":
            target_name = str(arguments.get("tool_name") or "").strip()
            target_definition = next((tool for tool in _research_tool_definitions() if tool["name"] == target_name), None)
            if target_definition is None:
                result = research_mcp_module._error_result(
                    "get_tool_help", "unknown_tool", f"Unknown Research MCP tool '{target_name}'."
                )
            else:
                result = research_mcp_module.research_tool_help_payload(target_name, target_definition)
        else:
            result = await research_mcp_module.call_tool_async(tool_name, arguments)
    except Exception as exc:
        print(
            f"[research_mcp_bundle] tool error tool={tool_name} exc={type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}"
        )
        return _jsonrpc_error(
            request_id,
            -32603,
            "Internal Research MCP tool error",
            data={"tool_name": tool_name, "exception_type": type(exc).__name__, "message": str(exc)},
            status_code=500,
        )

    is_error = result.get("outcome") == research_mcp_module.OUTCOME_ERROR
    return _jsonrpc_response(_tool_result(result, is_error=is_error), request_id)
