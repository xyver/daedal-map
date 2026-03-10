"""Candidate-scoring helpers extracted from preprocessor.py."""

from __future__ import annotations

import re
from typing import Callable


def detect_source_candidates(
    query: str,
    *,
    load_catalog: Callable[[], dict | None],
    score_source_full_match: float,
    score_source_id_match: float,
    score_source_partial_8: float,
    score_source_partial_4: float,
) -> dict:
    """Detect all possible source matches in query with confidence scores."""
    query_lower = query.lower()
    catalog = load_catalog()
    if not catalog:
        return {"candidates": [], "best": None}

    sources = catalog.get("sources", [])
    candidates = []

    def add_candidate(source_id: str, source_name: str, confidence: float, match_type: str, matched_text: str) -> None:
        candidates.append(
            {
                "source_id": source_id,
                "source_name": source_name or source_id,
                "confidence": min(1.0, confidence),
                "match_type": match_type,
                "matched_text": matched_text,
            }
        )

    data_keywords = ["data", "statistics", "dataset", "source", "metrics", "from the"]
    data_boost = 0.1 if any(kw in query_lower for kw in data_keywords) else 0.0

    sdg_pattern = re.search(r"\b(?:sdg|sustainable\s+development\s+goal)[\s\-_]*(\d{1,2})\b", query_lower)
    if sdg_pattern:
        goal_num = int(sdg_pattern.group(1))
        if 1 <= goal_num <= 17:
            goal_tag = f"goal{goal_num}"
            for source in sources:
                source_id = source.get("source_id", "")
                source_name = source.get("source_name", f"UN SDG Goal {goal_num}")
                topic_tags = source.get("topic_tags", [])
                source_name_lower = source_name.lower()
                if goal_tag in topic_tags or f"goal {goal_num}:" in source_name_lower or f"goal {goal_num} " in source_name_lower:
                    add_candidate(source_id, source_name, 1.0, "sdg_alias", sdg_pattern.group(0))

    for source in sources:
        source_id = source.get("source_id", "")
        source_name = source.get("source_name", "")
        source_name_lower = source_name.lower() if source_name else ""

        if source_name and source_name_lower in query_lower:
            add_candidate(source_id, source_name, score_source_full_match + data_boost, "full_name", source_name)
        elif source_name:
            name_parts = [p.strip() for p in source_name.replace(" - ", "|").replace(": ", "|").split("|")]
            for part in name_parts:
                part_lower = part.lower()
                if len(part) >= 4 and part_lower in query_lower:
                    base_score = score_source_partial_8 if len(part) >= 8 else score_source_partial_4
                    add_candidate(source_id, source_name, base_score + data_boost, "partial_name", part)
                    break

        if source_id and source_id.lower() in query_lower:
            add_candidate(source_id, source_name, score_source_id_match + data_boost, "source_id", source_id)

    candidates = sorted(candidates, key=lambda x: -x["confidence"])
    seen = set()
    unique_candidates = []
    for candidate in candidates:
        if candidate["source_id"] not in seen:
            seen.add(candidate["source_id"])
            unique_candidates.append(candidate)

    return {"candidates": unique_candidates, "best": unique_candidates[0] if unique_candidates else None}


def detect_intent_candidates(
    query: str,
    source_candidates: dict,
    location_candidates: dict,
    *,
    detect_navigation_intent: Callable[[str], dict],
    detect_show_borders_intent: Callable[[str], dict],
    detect_filter_intent: Callable[[str, dict], dict | None],
    score_data_keywords: float,
    score_data_from: float,
    score_source_mentioned: float,
    score_metric_keywords: float,
    score_nav_pattern: float,
    score_nav_penalty_data: float,
    score_nav_location_only: float,
) -> dict:
    """Detect possible user intents with confidence scores."""
    query_lower = query.lower().strip()
    candidates = []

    data_score = 0.0
    if any(kw in query_lower for kw in ["data", "statistics", "metrics", "show me data"]):
        data_score += score_data_keywords
    if any(kw in query_lower for kw in ["from the", "from", "dataset"]):
        data_score += score_data_from
    if source_candidates.get("best"):
        data_score += score_source_mentioned
    if any(kw in query_lower for kw in ["population", "gdp", "births", "deaths", "economy"]):
        data_score += score_metric_keywords
    if data_score > 0:
        candidates.append({"type": "data_request", "confidence": min(1.0, data_score), "signals": ["source_mentioned"] if source_candidates.get("best") else []})

    nav_score = 0.0
    nav_result = detect_navigation_intent(query)
    if nav_result.get("is_navigation"):
        nav_score += score_nav_pattern
        if "data" in query_lower or source_candidates.get("best"):
            nav_score += score_nav_penalty_data
    if location_candidates.get("best") and nav_score == 0:
        nav_score += score_nav_location_only
    if nav_score > 0:
        candidates.append({"type": "navigation", "confidence": max(0.0, min(1.0, nav_score)), "pattern": nav_result.get("pattern"), "location_text": nav_result.get("location_text")})

    ref_score = 0.0
    currency_analytics_terms = [
        "against usd", "vs usd", "drop", "depreciat", "appreciat", "volatility",
        "single year", "over the last", "trend", "time series", "change", "percent",
        "over time", "since ", "between ", "compare",
    ]
    is_currency_analytics = ("currency" in query_lower or "fx" in query_lower or "exchange rate" in query_lower) and any(term in query_lower for term in currency_analytics_terms)
    ref_patterns = [
        (r"capital of", 0.9),
        (r"what.+capital", 0.9),
        (r"currency", 0.8),
        (r"language.+spoken", 0.8),
        (r"what language", 0.8),
        (r"sdg\s*\d+", 0.9),
        (r"goal\s*\d+", 0.8),
    ]
    for pattern, score in ref_patterns:
        if pattern == r"currency" and is_currency_analytics:
            continue
        if re.search(pattern, query_lower):
            ref_score = max(ref_score, score)
    if ref_score > 0:
        candidates.append({"type": "reference_lookup", "confidence": ref_score, "signals": []})

    show_borders = detect_show_borders_intent(query)
    if show_borders.get("is_show_borders"):
        candidates.append({"type": "show_borders", "confidence": 0.9, "pattern": show_borders.get("pattern")})

    filter_result = detect_filter_intent(query, {}) or {}
    if filter_result.get("is_filter_intent"):
        candidates.append({"type": "filter_change", "confidence": 0.85, "filter_type": filter_result.get("filter_type"), "parsed_values": filter_result.get("parsed_values", {})})

    candidates = sorted(candidates, key=lambda x: -x["confidence"])
    return {"candidates": candidates, "best": candidates[0] if candidates else {"type": "data_request", "confidence": 0.5}}


def adjust_scores_with_context(
    source_candidates: dict,
    location_candidates: dict,
    intent_candidates: dict,
    *,
    penalty_location_in_source: float,
    penalty_nav_source_detected: float,
) -> dict:
    """Cross-reference candidates to adjust confidence scores."""
    adjusted_locations = []
    source_texts = set()
    for source_candidate in source_candidates.get("candidates", []):
        source_name = source_candidate.get("source_name", "").lower()
        source_texts.add(source_name)
        for word in source_name.split():
            if len(word) >= 4:
                source_texts.add(word)

    for location in location_candidates.get("candidates", []):
        matched_term = location.get("matched_term", "").lower()
        if any(matched_term in source_text for source_text in source_texts):
            location["confidence"] = max(0.0, location["confidence"] + penalty_location_in_source)
            location["penalized_reason"] = "term_in_source_name"
        adjusted_locations.append(location)

    adjusted_locations = sorted(adjusted_locations, key=lambda x: -x["confidence"])
    location_candidates["candidates"] = adjusted_locations
    location_candidates["best"] = adjusted_locations[0] if adjusted_locations else None

    adjusted_intents = []
    for intent in intent_candidates.get("candidates", []):
        if intent["type"] == "navigation" and source_candidates.get("best"):
            if source_candidates["best"]["confidence"] > 0.7:
                intent["confidence"] = max(0.0, intent["confidence"] + penalty_nav_source_detected)
                intent["adjusted_reason"] = "source_detected"
        adjusted_intents.append(intent)

    adjusted_intents = sorted(adjusted_intents, key=lambda x: -x["confidence"])
    intent_candidates["candidates"] = adjusted_intents
    intent_candidates["best"] = adjusted_intents[0] if adjusted_intents else None

    return {"sources": source_candidates, "locations": location_candidates, "intents": intent_candidates}
