"""
Session Cache Manager - Per-session data caching and tracking.

This module provides:
- SessionCache: Per-session tracking of what data has been sent to frontend
- session_manager: Global manager for all active sessions

The cache mirrors the frontend cache exactly, enabling:
- Deduplication: Don't send data cells/events already on the frontend
- Source clearing: Remove specific sources when user clicks "Clear" in Loaded tab

Key concept: the backend session cache = the frontend cache.
When we send data, we register it. When user clears, we remove it.

Dedup keys:
- Data: (loc_id, year, metric) - three-part key, one cell in the spreadsheet
- Events: event_id - whole event is all-or-nothing

Usage:
    from mapmover.session_cache import session_manager

    cache = session_manager.get_or_create(session_id)

    # Filter response before sending (post-fetch dedup)
    filtered_year_data = cache.filter_year_data(year_data)
    filtered_events = cache.filter_events(features)

    # Register what was actually sent
    cache.register_sent_year_data(filtered_year_data)  # auto-registers per metric
    cache.register_sent_events(filtered_events, source_id="earthquakes")

    # Clear a source (user clicked X in Loaded tab)
    cache.clear_source("earthquakes")
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from .cache_signature import CacheSignature, CacheInventory, DataPackage

logger = logging.getLogger(__name__)


class SessionCache:
    """
    Per-session cache tracking what data has been loaded.

    Stores:
    - inventory: What data signatures have been loaded
    - results: Cached execution results (GeoJSON, etc.)
    - metadata: Session info (created, last activity, etc.)
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.created_at = datetime.now()
        self.last_activity = datetime.now()

        # Cache inventory (tracks signatures of loaded data)
        self.inventory = CacheInventory(name=f"session_{session_id}")

        # Sent data keys - tracks exactly what cells/events were sent to frontend
        # For data: "{loc_id}:{year}:{metric}" (one cell in the spreadsheet)
        # For events: "{event_id}" (whole event is all-or-nothing)
        self._sent_all: set = set()  # flat set for O(1) dedup checks
        self._sent_by_source: Dict[str, set] = {}  # source_id -> keys (for clearing)

        # Cached results (request_key -> result)
        self._results: Dict[str, Dict] = {}

        # Chat history for session recovery
        self.chat_history: List[Dict] = []

        # Map state for recovery
        self.map_state: Dict = {}

    def touch(self):
        """Update last activity timestamp."""
        self.last_activity = datetime.now()

    def is_expired(self, ttl_hours: int = 4) -> bool:
        """Check if session has expired based on TTL."""
        return datetime.now() - self.last_activity > timedelta(hours=ttl_hours)

    def can_serve(self, requested: CacheSignature) -> bool:
        """Check if inventory can serve the requested data."""
        return self.inventory.can_serve(requested)

    def compute_delta(self, requested: CacheSignature) -> CacheSignature:
        """Compute what data needs to be fetched."""
        return self.inventory.compute_delta(requested)

    def store_result(
        self,
        request_key: str,
        result: Dict,
        signature: CacheSignature = None
    ):
        """
        Store execution result in cache.

        Args:
            request_key: Unique key for this request (hash of order items)
            result: The execution result (GeoJSON, etc.)
            signature: Optional signature of the data
        """
        self._results[request_key] = result
        if signature:
            self.inventory.add_signature(request_key, signature)
        self.touch()

    def get_cached_result(self, request_key: str) -> Optional[Dict]:
        """Get cached result by request key."""
        return self._results.get(request_key)

    def has_result(self, request_key: str) -> bool:
        """Check if result is cached."""
        return request_key in self._results

    def register_sent_events(self, features: list, source_id: str):
        """
        Register event features that were sent to the frontend.
        Tracks by event_id (whole event is all-or-nothing).
        """
        source_set = self._sent_by_source.setdefault(source_id, set())
        for f in features:
            props = f.get("properties", {})
            event_id = props.get("event_id") or props.get("loc_id") or f.get("id")
            if event_id:
                self._sent_all.add(event_id)
                source_set.add(event_id)

    def register_sent_year_data(self, year_data: dict):
        """
        Register year_data cells that were sent to the frontend.
        year_data format: {year: {loc_id: {metric: value, ...}, ...}, ...}
        Keys are three-part: "loc_id:year:metric" (one cell in the spreadsheet).
        Each metric is registered as its own source (clearing = deleting a column).
        """
        for year_str, loc_data in year_data.items():
            for loc_id, metrics in loc_data.items():
                for metric in metrics.keys():
                    key = f"{loc_id}:{year_str}:{metric}"
                    self._sent_all.add(key)
                    source_set = self._sent_by_source.setdefault(metric, set())
                    source_set.add(key)

    def is_event_sent(self, event_id: str) -> bool:
        """Check if an event was already sent."""
        return event_id in self._sent_all

    def is_cell_sent(self, loc_id: str, year, metric: str) -> bool:
        """Check if a specific (loc_id, year, metric) cell was already sent."""
        return f"{loc_id}:{year}:{metric}" in self._sent_all

    def filter_year_data(self, year_data: dict) -> dict:
        """
        Filter year_data to only include cells not yet sent.
        Returns filtered year_data with only new cells.
        """
        filtered = {}
        for year_str, loc_data in year_data.items():
            for loc_id, metrics in loc_data.items():
                new_metrics = {}
                for metric, value in metrics.items():
                    if f"{loc_id}:{year_str}:{metric}" not in self._sent_all:
                        new_metrics[metric] = value
                if new_metrics:
                    if year_str not in filtered:
                        filtered[year_str] = {}
                    filtered[year_str][loc_id] = new_metrics
        return filtered

    def filter_events(self, features: list) -> list:
        """
        Filter event features to only include events not yet sent.
        Multi-feature events (storm tracks) are all-or-nothing by event_id.
        """
        new_features = []
        for f in features:
            props = f.get("properties", {})
            event_id = props.get("event_id") or props.get("loc_id") or f.get("id")
            if not event_id or event_id not in self._sent_all:
                new_features.append(f)
        return new_features

    def clear_source(self, source_id: str) -> int:
        """
        Clear all sent keys for a specific source.
        Returns number of keys removed.
        """
        keys_to_remove = self._sent_by_source.pop(source_id, set())
        for key in keys_to_remove:
            self._sent_all.discard(key)
        return len(keys_to_remove)

    # -------------------------------------------------------------------------
    # Geometry tracking (dedup by loc_id, no year/metric dimension)
    # -------------------------------------------------------------------------

    def register_sent_geometry(self, features: list, source_id: str):
        """
        Register geometry features that were sent to the frontend.
        Tracks by loc_id (geometry features are all-or-nothing per loc_id).

        Args:
            features: List of GeoJSON features with loc_id in properties
            source_id: Source identifier (e.g., "geometry_zcta")
        """
        # Use "geom:{source_id}" as the key prefix to avoid collision with events
        geo_source_key = f"geom:{source_id}"
        source_set = self._sent_by_source.setdefault(geo_source_key, set())

        for f in features:
            props = f.get("properties", {})
            loc_id = props.get("loc_id")
            if loc_id:
                # Key format: "geom:{loc_id}" to distinguish from event/metric keys
                key = f"geom:{loc_id}"
                self._sent_all.add(key)
                source_set.add(key)

    def filter_geometry_features(self, features: list) -> list:
        """
        Filter geometry features to only include those not yet sent.
        Dedup by loc_id.

        Args:
            features: List of GeoJSON features

        Returns:
            List of features not yet sent
        """
        new_features = []
        for f in features:
            props = f.get("properties", {})
            loc_id = props.get("loc_id")
            if not loc_id or f"geom:{loc_id}" not in self._sent_all:
                new_features.append(f)
        return new_features

    def remove_geometry_by_loc_ids(self, source_id: str, loc_ids: list) -> int:
        """
        Remove specific geometry loc_ids from the cache.
        Used after frontend confirms removal.

        Args:
            source_id: Source identifier (e.g., "geometry_zcta")
            loc_ids: List of loc_ids to remove

        Returns:
            Number of loc_ids removed
        """
        geo_source_key = f"geom:{source_id}"
        source_set = self._sent_by_source.get(geo_source_key)

        if not source_set:
            return 0

        removed = 0
        for loc_id in loc_ids:
            key = f"geom:{loc_id}"
            if key in source_set:
                source_set.discard(key)
                self._sent_all.discard(key)
                removed += 1

        return removed

    @property
    def sent_count(self) -> int:
        """Number of data points tracked as sent."""
        return len(self._sent_all)

    def clear(self):
        """Clear all cached data."""
        self.inventory.clear()
        self._sent_all.clear()
        self._sent_by_source.clear()
        self._results.clear()
        self.chat_history.clear()
        self.map_state.clear()

    def get_status(self) -> Dict:
        """Get session status for recovery prompt."""
        stats = self.inventory.stats()
        return {
            "session_id": self.session_id,
            "has_data": stats["entry_count"] > 0,
            "cache_entries": stats["entry_count"],
            "total_locations": stats["total_locations"],
            "total_metrics": stats["total_metrics"],
            "total_years": stats["total_years"],
            "year_range": stats["year_range"],
            "chat_message_count": len(self.chat_history),
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
        }

    def stats(self) -> Dict:
        """Get detailed cache statistics."""
        inv_stats = self.inventory.stats()
        return {
            **inv_stats,
            "sent_count": len(self._sent_all),
            "sent_sources": list(self._sent_by_source.keys()),
            "result_count": len(self._results),
            "chat_history_count": len(self.chat_history),
            "age_seconds": (datetime.now() - self.created_at).total_seconds(),
            "idle_seconds": (datetime.now() - self.last_activity).total_seconds(),
        }


class SessionManager:
    """
    Global manager for all active session caches.

    Handles:
    - Session creation and retrieval
    - TTL-based cleanup
    - Cross-session statistics
    """

    # Default TTL (4 hours for deployed, could be configured)
    DEFAULT_TTL_HOURS = 4

    def __init__(self):
        self._sessions: Dict[str, SessionCache] = {}
        self._last_cleanup = datetime.now()
        self._cleanup_interval = timedelta(minutes=5)

    def get(self, session_id: str) -> Optional[SessionCache]:
        """Get session cache if it exists."""
        cache = self._sessions.get(session_id)
        if cache:
            cache.touch()
        return cache

    def get_or_create(self, session_id: str) -> SessionCache:
        """Get existing session cache or create new one."""
        if session_id not in self._sessions:
            self._sessions[session_id] = SessionCache(session_id)
            logger.info(f"Created new session cache: {session_id}")
        else:
            self._sessions[session_id].touch()

        # Periodically cleanup expired sessions
        self._maybe_cleanup()

        return self._sessions[session_id]

    def exists(self, session_id: str) -> bool:
        """Check if session exists."""
        return session_id in self._sessions

    def delete(self, session_id: str) -> bool:
        """Delete a session cache."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"Deleted session cache: {session_id}")
            return True
        return False

    def clear_session(self, session_id: str) -> bool:
        """Clear a session's cache but keep the session."""
        cache = self._sessions.get(session_id)
        if cache:
            cache.clear()
            logger.info(f"Cleared session cache: {session_id}")
            return True
        return False

    def _maybe_cleanup(self):
        """Cleanup expired sessions if interval has passed."""
        now = datetime.now()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = now
        expired = [
            sid for sid, cache in self._sessions.items()
            if cache.is_expired(self.DEFAULT_TTL_HOURS)
        ]

        for sid in expired:
            del self._sessions[sid]

        if expired:
            logger.info(f"Cleaned up {len(expired)} expired sessions")

    def stats(self) -> Dict:
        """Get overall statistics."""
        total_entries = 0
        total_results = 0

        for cache in self._sessions.values():
            stats = cache.stats()
            total_entries += stats.get("entry_count", 0)
            total_results += stats.get("result_count", 0)

        return {
            "active_sessions": len(self._sessions),
            "total_cache_entries": total_entries,
            "total_cached_results": total_results,
        }

    def list_sessions(self) -> List[Dict]:
        """List all active sessions with their status."""
        return [cache.get_status() for cache in self._sessions.values()]


# Global session manager instance
session_manager = SessionManager()
