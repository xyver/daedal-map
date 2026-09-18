"""Bounded accounting of already-loaded owners; no cache loads or profiler resets."""
import sys
import time
import hashlib

OWNERS = (
    ('ops_state', 'mapmover.ops_orchestrator_runtime', '_LIVE_STATE_CACHE'),
    ('foundation_reference', 'mapmover.foundation_helpers', '_REFERENCE_JSON_CACHE'),
    ('foundation_assets', 'mapmover.foundation_helpers', '_COUNTRY_JSON_ASSET_CACHE'),
    ('foundation_crosswalks', 'mapmover.foundation_helpers', '_COUNTRY_CROSSWALK_CACHE'),
    ('foundation_exact', 'mapmover.foundation_helpers', '_GLOBAL_COUNTRIES_CACHE'),
    ('foundation_display', 'mapmover.foundation_helpers', '_GLOBAL_COUNTRY_DISPLAY_CACHE'),
    ('admin_hierarchy', 'mapmover.runtime.admin_hierarchy', '_BASE_GEOMETRY_CACHE'),
    ('admin_identity', 'mapmover.runtime.admin_spine_query', '_SHALLOW_IDENTITY_CACHE'),
    ('sessions', 'mapmover.session_cache', 'session_manager'),
    ('corpus', 'mapmover.corpus_registry', 'corpus_registry'),
)


def bounded_size(value, *, max_nodes=20000, seconds=0.03):
    """Approximate Python graph bytes, explicitly excluding unknown native buffers."""
    pending = [value]
    seen = set()
    total = 0
    deadline = time.monotonic() + seconds
    truncated = False
    while pending:
        if len(seen) >= max_nodes or time.monotonic() >= deadline:
            truncated = True
            break
        item = pending.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        total += sys.getsizeof(item)
        # Limit traversal frontier as well as visited nodes.
        remaining = max_nodes - len(seen) - len(pending)
        children = None
        if isinstance(item, dict):
            def pairs():
                for key, child in item.items():
                    yield key
                    yield child
            children = pairs()
        elif isinstance(item, (list, tuple, set, frozenset)):
            children = iter(item)
        elif type(item).__module__.startswith('mapmover.') and hasattr(item, '__dict__'):
            children = iter((vars(item),))
        if children is not None:
            try:
                for child in children:
                    if remaining <= 0:
                        truncated = True
                        break
                    pending.append(child)
                    remaining -= 1
            except RuntimeError:
                truncated = True  # A concurrent owner mutation is not a diagnostic failure.
    return {'estimated_python_bytes': total, 'visited_nodes': len(seen), 'truncated': truncated}


def loaded_owner_memory():
    result = {}
    for label, module_name, attribute in OWNERS:
        module = sys.modules.get(module_name)
        value = getattr(module, attribute, None) if module else None
        if value is None:
            result[label] = {'loaded': False}
            continue
        result[label] = {'loaded': True, **bounded_size(value)}
        if isinstance(value, (dict, list, tuple, set)):
            result[label]['entries'] = len(value)
        if isinstance(value, dict):
            details = []
            try:
                for key, child in value.items():
                    if len(details) >= 32:
                        break
                    key_text = str(key) if label == 'ops_state' else hashlib.sha256(str(key).encode()).hexdigest()[:12]
                    details.append({'key': key_text, **bounded_size(child, max_nodes=5000, seconds=0.005)})
            except RuntimeError:
                result[label]['changed_during_sample'] = True
            result[label]['entry_estimates'] = details
            result[label]['entries_sampled'] = len(details)
    return {'owners': result, 'note': 'Bounded Python estimates only; truncated graphs are partial. Owners may share objects; do not sum as unique RAM. Native buffers are not identified by this traversal.'}
