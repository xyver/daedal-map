"""Runtime reader for generated data/geometry material-policy envelopes.

Catalog generation owns legal interpretation. Hosted runtimes only consume
the generated decision, so they cannot independently reinterpret a licence
label. A missing ``material_policy`` fails closed.
"""

from __future__ import annotations

from typing import Any, Iterable


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def material_access_facts(record: dict[str, Any] | None) -> dict[str, Any]:
    """Return one normalized legal/access decision without mapping labels."""
    record = record if isinstance(record, dict) else {}
    policy = record.get("material_policy")
    if isinstance(policy, dict):
        hosted = policy.get("hosted_access") if isinstance(policy.get("hosted_access"), dict) else {}
        redistribution = policy.get("redistribution") if isinstance(policy.get("redistribution"), dict) else {}
        surfaces = policy.get("surface_access") if isinstance(policy.get("surface_access"), dict) else {}
        surface_access = {
            "hosted_results": bool(surfaces.get("hosted_results", hosted.get("free_allowed"))),
            "server_rendered_display": bool(surfaces.get("server_rendered_display", hosted.get("free_allowed"))),
            "client_geometry": bool(surfaces.get("client_geometry", redistribution.get("allowed"))),
            "download": bool(surfaces.get("download", redistribution.get("allowed"))),
        }
        return {
            "permission": _clean(policy.get("permission")) or None,
            "publication_cleared": bool(hosted.get("publication_ready")),
            "free_hosted_allowed": bool(hosted.get("free_allowed")),
            "paid_hosted_allowed": bool(hosted.get("paid_allowed")),
            "maximum_hosted_lane": _clean(hosted.get("maximum_lane")) or "blocked",
            "redistribution": _clean(redistribution.get("status")) or "unknown",
            "redistribution_allowed": bool(redistribution.get("allowed")),
            "surface_access": surface_access,
            "attribution": dict(policy.get("attribution") or {}),
            "citation": dict(policy.get("citation") or {}),
            "reason_codes": list(hosted.get("reason_codes") or []),
            "policy_fingerprint": policy.get("policy_fingerprint"),
            "source": "material_policy",
        }

    return {
        "permission": None,
        "publication_cleared": False,
        "free_hosted_allowed": False,
        "paid_hosted_allowed": False,
        "maximum_hosted_lane": "blocked",
        "redistribution": "unknown",
        "redistribution_allowed": False,
        "surface_access": {
            "hosted_results": False,
            "server_rendered_display": False,
            "client_geometry": False,
            "download": False,
        },
        "attribution": {},
        "citation": {},
        "reason_codes": ["material_policy_missing"],
        "policy_fingerprint": None,
        "source": "missing",
    }


def combine_material_access(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Take the strictest generated decision across contributing material."""
    facts = [material_access_facts(record) for record in records if isinstance(record, dict)]
    permissions = {fact["permission"] for fact in facts if fact.get("permission")}
    all_published = bool(facts) and all(fact["publication_cleared"] for fact in facts)
    all_free = bool(facts) and all(fact["free_hosted_allowed"] for fact in facts)
    all_paid = bool(facts) and all(fact["paid_hosted_allowed"] for fact in facts)
    all_redistributable = bool(facts) and all(fact["redistribution_allowed"] for fact in facts)
    surfaces = ("hosted_results", "server_rendered_display", "client_geometry", "download")
    return {
        "permissions": permissions,
        "publication_cleared": all_published,
        "free_hosted_allowed": all_free,
        "paid_hosted_allowed": all_paid,
        "maximum_hosted_lane": "paid" if all_paid else "free" if all_free else "blocked",
        "redistribution_allowed": all_redistributable,
        "surface_access": {
            surface: bool(facts) and all(fact["surface_access"].get(surface) for fact in facts)
            for surface in surfaces
        },
        "members": facts,
    }


SITE_ORIGIN = "https://www.daedalmap.com"
CITATION_GUIDANCE = (
    "Credit each attribution line. Cite DaedalMap for the release you used and each source entry; "
    "an entry's role is copied from the generated catalog."
)
_CITATION_PART_FIELDS = ("agency", "title", "vintage", "url", "license")


def _text(value: Any) -> str:
    return str(value or "").strip()


def citation_reference(author: str, title: str, url: str = "", *, year: str = "", accessed: str = "") -> str:
    """One reference-list entry: Author (Year). Title [Data set]. URL. Accessed date.

    Mirrors the website pack pages (live/site/detail_page.citation_reference) so
    hosted results and detail pages cite identically. Licences are deliberately
    absent; they belong to the attribution notice and each source's license field.
    """
    import re

    author, title, url = (_text(author).rstrip("."), _text(title).rstrip("."), _text(url).rstrip("."))
    year_match = re.search(r"\b(1[89]|20)\d{2}\b", _text(year))
    parts = [f"{author} ({year_match.group(0)})." if year_match else f"{author}."] if author else []
    if title:
        parts.append(f"{title} [Data set].")
    if url:
        parts.append(f"{url}.")
        if accessed:
            parts.append(f"Accessed {accessed}.")
    return " ".join(parts)


def daedalmap_release_reference(pack_id: str, pack_name: str, version: str, *,
                                maintainer: str = "DaedalMap", accessed: str = "") -> str:
    """The pack page's primary citation for the DaedalMap release."""
    title = f"{pack_name or pack_id} data pack" + (f", version {version}" if version else "")
    return citation_reference(maintainer or "DaedalMap", title, f"{SITE_ORIGIN}/packs/{pack_id}",
                              year=accessed, accessed=accessed)


def query_result_citation(
    source_record: dict[str, Any] | None,
    pack_record: dict[str, Any] | None = None,
    *,
    accessed: str = "",
) -> dict[str, Any] | None:
    """Attribution and citation for one query result, copied from generated catalog projections.

    The catalog builder owns attribution obligations and citation roles; this
    reader never maps licence labels or reclassifies roles.
    """
    source = source_record if isinstance(source_record, dict) else {}
    pack = pack_record if isinstance(pack_record, dict) else {}
    attribution = [_text(line) for line in source.get("attribution_lines") or [] if _text(line)]
    if not attribution and _text(source.get("attribution_text")):
        attribution = [_text(source.get("attribution_text"))]
    citation = source.get("citation") if isinstance(source.get("citation"), dict) else {}
    entries = []
    seen_references: set[str] = set()
    for entry in citation.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        parts = entry.get("parts") if isinstance(entry.get("parts"), dict) else {}
        # Same rule as the pack page: build from structured parts; fall back to
        # the generated text only when the entry has no agency or title.
        if _text(parts.get("agency")) or _text(parts.get("title")):
            reference = citation_reference(parts.get("agency"), parts.get("title"), parts.get("url"),
                                           year=parts.get("vintage"), accessed=accessed)
        else:
            reference = _text(entry.get("text"))
        if not reference or reference in seen_references:
            continue
        seen_references.add(reference)
        item: dict[str, Any] = {"text": reference}
        if _text(entry.get("role")):
            item["role"] = _text(entry.get("role"))
        item.update({field: _text(parts.get(field)) for field in _CITATION_PART_FIELDS if _text(parts.get(field))})
        entries.append(item)
    if not entries and _text(citation.get("text")):
        entries.append({"text": _text(citation.get("text"))})
    if not attribution and not entries:
        return None
    result: dict[str, Any] = {"guidance": CITATION_GUIDANCE, "attribution": attribution, "sources": entries}
    pack_id = _text(pack.get("pack_id") or source.get("pack_id"))
    if pack_id:
        version = _text(pack.get("pack_version") or source.get("pack_version"))
        pack_name = _text(pack.get("pack_name") or pack.get("title"))
        maintainer = _text(pack.get("pack_maintainer_name")) or "DaedalMap"
        result["daedalmap"] = {
            "text": daedalmap_release_reference(pack_id, pack_name, version, maintainer=maintainer, accessed=accessed),
            "pack_id": pack_id,
            "pack_name": pack_name or None,
            "version": version or None,
            "url": f"{SITE_ORIGIN}/packs/{pack_id}",
        }
    if accessed:
        result["accessed"] = accessed
    return result
