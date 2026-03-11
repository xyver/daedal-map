"""
Supabase client for county-map project.
Handles cloud storage for query logs, error logs, and metadata sync.

Setup:
1. Add to your .env file:
   SUPABASE_URL=your_project_url
   SUPABASE_ANON_KEY=your_anon_key
   SUPABASE_SERVICE_KEY=your_service_role_key  (optional, for admin operations)

2. Create tables in Supabase SQL Editor:
   See create_tables() function for SQL or run it once to auto-create.
"""

import os
import json
from datetime import datetime
from typing import Optional, Dict, Any, List

try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

from dotenv import load_dotenv

load_dotenv()


class SupabaseClient:
    """Supabase client for logging and metadata sync."""

    def __init__(self):
        if not HAS_SUPABASE:
            raise ImportError(
                "Supabase not installed. Install with:\n"
                "  pip install supabase"
            )

        self.url = os.getenv("SUPABASE_URL")
        self.anon_key = os.getenv("SUPABASE_ANON_KEY")
        self.service_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not self.url or not self.anon_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env file"
            )

        # Use service key if available (for admin operations), otherwise anon key
        key = self.service_key if self.service_key else self.anon_key
        self.client: Client = create_client(self.url, key)

    # --- Session Logs (conversation sessions) ---

    def log_session_message(
        self,
        session_id: str,
        user_query: str,
        assistant_response: str,
        intent: Optional[str] = None,
        dataset_selected: Optional[str] = None,
        results_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict]:
        """
        Log a conversation message to a session.

        Uses upsert to create or update the session row.
        Each session stores the full conversation as a JSONB array.

        Args:
            session_id: Unique session identifier from frontend
            user_query: The user's message
            assistant_response: The AI's response
            intent: Query intent (chat, clarify, fetch_data, meta, modify_data)
            dataset_selected: Which dataset was used (if any)
            results_count: Number of results returned (if data query)
            metadata: Additional context

        Returns:
            The upserted row or None if failed
        """
        try:
            # First, try to get existing session
            existing = self.client.table("conversation_sessions").select("*").eq("session_id", session_id).execute()

            message_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "user": user_query,
                "assistant": assistant_response,
                "intent": intent,
                "dataset": dataset_selected,
                "results_count": results_count
            }

            if existing.data and len(existing.data) > 0:
                # Update existing session - append to messages array
                current_session = existing.data[0]
                messages = current_session.get("messages", [])
                messages.append(message_entry)

                # Update datasets used if new dataset
                datasets_used = current_session.get("datasets_used", [])
                if dataset_selected and dataset_selected not in datasets_used:
                    datasets_used.append(dataset_selected)

                # Update intents seen
                intents_seen = current_session.get("intents_seen", [])
                if intent and intent not in intents_seen:
                    intents_seen.append(intent)

                update_data = {
                    "messages": messages,
                    "message_count": len(messages),
                    "datasets_used": datasets_used,
                    "intents_seen": intents_seen,
                    "total_results": current_session.get("total_results", 0) + results_count,
                    "updated_at": datetime.utcnow().isoformat()
                }

                result = self.client.table("conversation_sessions").update(update_data).eq("session_id", session_id).execute()
            else:
                # Create new session
                data = {
                    "session_id": session_id,
                    "messages": [message_entry],
                    "message_count": 1,
                    "datasets_used": [dataset_selected] if dataset_selected else [],
                    "intents_seen": [intent] if intent else [],
                    "total_results": results_count,
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }

                result = self.client.table("conversation_sessions").insert(data).execute()

            return result.data[0] if result.data else None

        except Exception as e:
            print(f"Failed to log session message to Supabase: {e}")
            return None

    def get_session(self, session_id: str) -> Optional[Dict]:
        """Get a specific conversation session."""
        try:
            result = self.client.table("conversation_sessions").select("*").eq("session_id", session_id).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to get session: {e}")
            return None

    def get_recent_sessions(self, limit: int = 50) -> List[Dict]:
        """Get recent conversation sessions."""
        try:
            result = self.client.table("conversation_sessions").select("*").order("updated_at", desc=True).limit(limit).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Failed to get sessions: {e}")
            return []

    # --- Query Logs (legacy - keeping for backwards compatibility) ---

    def log_query(
        self,
        query: str,
        dataset_selected: Optional[str] = None,
        interest: Optional[str] = None,
        scale: Optional[str] = None,
        results_count: int = 0,
        response_time_ms: Optional[int] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict]:
        """
        Log a user query to the query_logs table.
        NOTE: This is legacy - prefer log_session_message for new code.

        Args:
            query: The user's natural language query
            dataset_selected: Which dataset was used to answer
            interest: Extracted interest/topic from query
            scale: Geographic scale (country, state, county, etc.)
            results_count: Number of results returned
            response_time_ms: How long the query took
            error: Error message if query failed
            metadata: Additional metadata (filter_column, sort_by, etc.)

        Returns:
            The inserted row or None if failed
        """
        try:
            data = {
                "query": query,
                "dataset_selected": dataset_selected,
                "interest": interest,
                "scale": scale,
                "results_count": results_count,
                "response_time_ms": response_time_ms,
                "error": error,
                "metadata": json.dumps(metadata) if metadata else None,
                "created_at": datetime.utcnow().isoformat()
            }

            result = self.client.table("query_logs").insert(data).execute()
            return result.data[0] if result.data else None

        except Exception as e:
            print(f"Failed to log query to Supabase: {e}")
            return None

    def get_query_logs(
        self,
        limit: int = 100,
        offset: int = 0,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        has_error: Optional[bool] = None
    ) -> List[Dict]:
        """
        Retrieve query logs with optional filtering.

        Args:
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            start_date: Filter queries after this date (ISO format)
            end_date: Filter queries before this date (ISO format)
            has_error: If True, only errors; if False, only successes

        Returns:
            List of query log records
        """
        try:
            query = self.client.table("query_logs").select("*")

            if start_date:
                query = query.gte("created_at", start_date)
            if end_date:
                query = query.lte("created_at", end_date)
            if has_error is True:
                query = query.not_.is_("error", "null")
            elif has_error is False:
                query = query.is_("error", "null")

            query = query.order("created_at", desc=True)
            query = query.range(offset, offset + limit - 1)

            result = query.execute()
            return result.data if result.data else []

        except Exception as e:
            print(f"Failed to get query logs from Supabase: {e}")
            return []

    def get_query_stats(self) -> Dict[str, Any]:
        """
        Get aggregate statistics about queries from conversation_sessions.

        Returns:
            Dict with stats like total_queries, error_rate, popular_queries, etc.
        """
        try:
            # Get session count
            session_result = self.client.table("conversation_sessions").select("id", count="exact").execute()
            total_sessions = session_result.count or 0

            # Get error count from error_logs table
            error_result = self.client.table("error_logs").select("id", count="exact").execute()
            error_count = error_result.count or 0

            # Get recent sessions
            recent = self.client.table("conversation_sessions").select("*").order("updated_at", desc=True).limit(10).execute()

            # Count total queries across all sessions (messages from users)
            total_queries = 0
            interest_counts = {}
            for session in (recent.data or []):
                messages = session.get("messages", [])
                for msg in messages:
                    if msg.get("role") == "user":
                        total_queries += 1
                    # Extract interests from message metadata if available
                    if msg.get("interest"):
                        interest = msg.get("interest")
                        interest_counts[interest] = interest_counts.get(interest, 0) + 1

            top_interests = sorted(interest_counts.items(), key=lambda x: x[1], reverse=True)[:10]

            return {
                "total_queries": total_sessions,  # Using sessions as main count
                "error_count": error_count,
                "error_rate": (error_count / total_sessions * 100) if total_sessions > 0 else 0,
                "recent_queries": recent.data or [],
                "top_interests": top_interests
            }

        except Exception as e:
            print(f"Failed to get query stats from Supabase: {e}")
            return {
                "total_queries": 0,
                "error_count": 0,
                "error_rate": 0,
                "recent_queries": [],
                "top_interests": []
            }

    # --- Error Logs ---

    def log_error(
        self,
        error_type: str,
        error_message: str,
        query: Optional[str] = None,
        traceback: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict]:
        """
        Log an error to the error_logs table.

        Args:
            error_type: Type of error (e.g., "AttributeError", "ValueError")
            error_message: The error message
            query: The query that caused the error (if applicable)
            traceback: Full traceback string
            metadata: Additional context

        Returns:
            The inserted row or None if failed
        """
        try:
            data = {
                "error_type": error_type,
                "error_message": error_message,
                "query": query,
                "traceback": traceback,
                "metadata": json.dumps(metadata) if metadata else None,
                "created_at": datetime.utcnow().isoformat()
            }

            result = self.client.table("error_logs").insert(data).execute()
            return result.data[0] if result.data else None

        except Exception as e:
            print(f"Failed to log error to Supabase: {e}")
            return None

    def get_error_logs(self, limit: int = 50) -> List[Dict]:
        """Get recent error logs."""
        try:
            result = self.client.table("error_logs").select("*").order("created_at", desc=True).limit(limit).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Failed to get error logs: {e}")
            return []

    # --- Data Quality Issues ---

    def log_data_quality_issue(
        self,
        issue_type: str,
        name: str,
        query: Optional[str] = None,
        dataset: Optional[str] = None,
        region: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict]:
        """
        Log data quality issues for tracking and fixing.

        Uses upsert to track unique issues without duplicates.
        Each issue is logged once with the first query that triggered it.

        Issue types:
        - 'missing_geometry': Country/place without map polygon
        - 'missing_region': Region name not in conversions.json
        - 'unknown_name': Name not matching any known entity
        - 'duplicate_data': Duplicate rows detected
        - 'missing_data': Expected data not found

        Args:
            issue_type: Type of issue (see above)
            name: The name/value that caused the issue
            query: The query that triggered this
            dataset: The dataset being queried
            region: The region filter used (if applicable)
            metadata: Additional context

        Returns:
            Success indicator or None if failed
        """
        if not name:
            return {"logged": 0}

        try:
            data = {
                "issue_type": issue_type,
                "name": name,
                "first_query": query[:500] if query else None,
                "dataset": dataset,
                "region": region,
                "metadata": json.dumps(metadata) if metadata else None,
                "occurrence_count": 1,
                "created_at": datetime.utcnow().isoformat()
            }

            # Upsert using composite key (issue_type + name)
            self.client.table("data_quality_issues").upsert(
                data,
                on_conflict="issue_type,name"
            ).execute()

            print(f"[LOG] Data quality issue: {issue_type} - '{name}'")
            return {"logged": 1}

        except Exception as e:
            print(f"Failed to log data quality issue to Supabase: {e}")
            return None

    def log_missing_geometry(
        self,
        country_names: List[str],
        query: Optional[str] = None,
        dataset: Optional[str] = None,
        region: Optional[str] = None
    ) -> Optional[Dict]:
        """Log countries/places missing map geometry. Wrapper for log_data_quality_issue."""
        if not country_names:
            return {"logged": 0}

        logged_count = 0
        for name in country_names:
            result = self.log_data_quality_issue(
                issue_type="missing_geometry",
                name=name,
                query=query,
                dataset=dataset,
                region=region
            )
            if result:
                logged_count += 1

        return {"logged": logged_count}

    def log_missing_region(
        self,
        region_name: str,
        query: Optional[str] = None,
        dataset: Optional[str] = None
    ) -> Optional[Dict]:
        """Log region names not found in conversions.json. Wrapper for log_data_quality_issue."""
        return self.log_data_quality_issue(
            issue_type="missing_region",
            name=region_name,
            query=query,
            dataset=dataset
        )

    def get_data_quality_issues(self, issue_type: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get data quality issues, optionally filtered by type."""
        try:
            query = self.client.table("data_quality_issues").select("*")
            if issue_type:
                query = query.eq("issue_type", issue_type)
            result = query.order("occurrence_count", desc=True).limit(limit).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Failed to get data quality issues: {e}")
            return []

    def get_missing_geometries(self, limit: int = 100) -> List[Dict]:
        """Get list of countries with missing geometry. Wrapper for backwards compatibility."""
        return self.get_data_quality_issues(issue_type="missing_geometry", limit=limit)

    def get_missing_regions(self, limit: int = 100) -> List[Dict]:
        """Get list of regions that failed lookup. Wrapper for backwards compatibility."""
        return self.get_data_quality_issues(issue_type="missing_region", limit=limit)

    # --- Control Plane: Plans ---

    def get_plan(self, plan_id: str) -> Optional[Dict]:
        """Get a plan record by id."""
        try:
            result = self.client.table("plans").select("*").eq("id", plan_id).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to get plan: {e}")
            return None

    def get_all_plans(self) -> List[Dict]:
        """Get all active plans."""
        try:
            result = self.client.table("plans").select("*").eq("is_active", True).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Failed to get plans: {e}")
            return []

    # --- Control Plane: Orgs ---

    def get_org(self, org_id: str) -> Optional[Dict]:
        """Get an org by id."""
        try:
            result = self.client.table("orgs").select("*").eq("id", org_id).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to get org: {e}")
            return None

    def create_org(self, name: str, slug: str, plan_id: str = "free") -> Optional[Dict]:
        """Create a new org."""
        try:
            result = self.client.table("orgs").insert({
                "name": name,
                "slug": slug,
                "plan_id": plan_id,
            }).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to create org: {e}")
            return None

    # --- Control Plane: Profiles ---

    def get_profile(self, user_id: str) -> Optional[Dict]:
        """Get a user profile by user_id."""
        try:
            result = self.client.table("profiles").select("*").eq("id", user_id).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to get profile: {e}")
            return None

    def upsert_profile(self, user_id: str, updates: Dict[str, Any]) -> Optional[Dict]:
        """Update a user profile. Only call with service key."""
        try:
            data = {"id": user_id, **updates, "updated_at": datetime.utcnow().isoformat()}
            result = self.client.table("profiles").upsert(data, on_conflict="id").execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to upsert profile: {e}")
            return None

    def set_user_plan(self, user_id: str, plan_id: str) -> Optional[Dict]:
        """Set the plan for a user. Requires service key."""
        return self.upsert_profile(user_id, {"plan_id": plan_id})

    # --- Control Plane: Memberships ---

    def get_user_memberships(self, user_id: str) -> List[Dict]:
        """Get all org memberships for a user."""
        try:
            result = self.client.table("memberships").select("*, orgs(*)").eq("user_id", user_id).execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Failed to get memberships: {e}")
            return []

    def add_membership(self, user_id: str, org_id: str, role: str = "member") -> Optional[Dict]:
        """Add a user to an org. Requires service key."""
        try:
            result = self.client.table("memberships").upsert({
                "user_id": user_id,
                "org_id": org_id,
                "role": role,
            }, on_conflict="user_id,org_id").execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to add membership: {e}")
            return None

    # --- Control Plane: Pack Entitlements ---

    def get_user_pack_entitlements(self, user_id: str) -> List[Dict]:
        """Get all active pack entitlements for a user (direct + org-level)."""
        try:
            # Direct user entitlements
            user_result = self.client.table("pack_entitlements").select("*").eq("user_id", user_id).execute()
            user_packs = user_result.data or []

            # Org-level entitlements via memberships
            memberships = self.get_user_memberships(user_id)
            org_packs = []
            for m in memberships:
                org_id = m.get("org_id")
                if org_id:
                    org_result = self.client.table("pack_entitlements").select("*").eq("org_id", org_id).execute()
                    org_packs.extend(org_result.data or [])

            return user_packs + org_packs
        except Exception as e:
            print(f"Failed to get pack entitlements: {e}")
            return []

    def grant_pack(self, pack_id: str, user_id: Optional[str] = None, org_id: Optional[str] = None,
                   granted_by: str = "manual", expires_at: Optional[str] = None) -> Optional[Dict]:
        """Grant a pack to a user or org. Requires service key."""
        if not user_id and not org_id:
            raise ValueError("user_id or org_id required")
        try:
            data: Dict[str, Any] = {
                "pack_id": pack_id,
                "granted_by": granted_by,
            }
            if user_id:
                data["user_id"] = user_id
            if org_id:
                data["org_id"] = org_id
            if expires_at:
                data["expires_at"] = expires_at

            conflict_col = "user_id,pack_id" if user_id else "org_id,pack_id"
            result = self.client.table("pack_entitlements").upsert(data, on_conflict=conflict_col).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            print(f"Failed to grant pack: {e}")
            return None

    def get_user_entitlement_context(self, user_id: str) -> Optional[Dict]:
        """
        Get full entitlement context for a user: plan, shells, packs.
        Uses the get_user_entitlement_context() Postgres function.
        Requires service key.

        Returns dict with:
            user_id, email, plan_id, is_admin, org_id, org_ids,
            enabled_shells, max_packs, user_packs, org_packs, org_plan_id
        """
        try:
            result = self.client.rpc("get_user_entitlement_context", {"p_user_id": user_id}).execute()
            return result.data if result.data else None
        except Exception as e:
            print(f"Failed to get entitlement context: {e}")
            return None

    # --- Dataset Metadata ---

    def sync_metadata(self, filename: str, metadata: Dict[str, Any]) -> Optional[Dict]:
        """
        Sync dataset metadata to Supabase.
        Uses upsert to update existing or insert new.

        Args:
            filename: The dataset filename (used as key)
            metadata: The full metadata dict

        Returns:
            The upserted row or None if failed
        """
        try:
            data = {
                "filename": filename,
                "description": metadata.get("description", ""),
                "source_name": metadata.get("source_name", ""),
                "source_url": metadata.get("source_url", ""),
                "license": metadata.get("license", "Unknown"),
                "geographic_level": metadata.get("geographic_level", "unknown"),
                "row_count": metadata.get("row_count", 0),
                "column_count": len(metadata.get("columns", {})),
                "topic_tags": metadata.get("topic_tags", []),
                "full_metadata": json.dumps(metadata),
                "updated_at": datetime.utcnow().isoformat()
            }

            result = self.client.table("dataset_metadata").upsert(
                data,
                on_conflict="filename"
            ).execute()

            return result.data[0] if result.data else None

        except Exception as e:
            print(f"Failed to sync metadata to Supabase: {e}")
            return None

    def get_all_metadata(self) -> List[Dict]:
        """Get all dataset metadata from Supabase."""
        try:
            result = self.client.table("dataset_metadata").select("*").order("filename").execute()
            return result.data if result.data else []
        except Exception as e:
            print(f"Failed to get metadata: {e}")
            return []

    # --- Connection Test ---

    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Supabase connection and return status.

        Returns:
            Dict with connection status and table info
        """
        try:
            # Try to query each table
            tables = {}

            for table in [
            "conversation_sessions", "error_logs", "dataset_metadata", "data_quality_issues",
            "plans", "orgs", "profiles", "memberships", "pack_entitlements",
        ]:
                try:
                    result = self.client.table(table).select("id", count="exact").limit(1).execute()
                    tables[table] = {
                        "exists": True,
                        "count": result.count or 0
                    }
                except Exception as e:
                    tables[table] = {
                        "exists": False,
                        "error": str(e)
                    }

            return {
                "connected": True,
                "url": self.url,
                "tables": tables
            }

        except Exception as e:
            return {
                "connected": False,
                "error": str(e)
            }


def get_supabase_client() -> Optional[SupabaseClient]:
    """
    Get a Supabase client instance, or None if not configured.

    Returns:
        SupabaseClient instance or None
    """
    try:
        return SupabaseClient()
    except (ImportError, ValueError) as e:
        print(f"Supabase not available: {e}")
        return None


# SQL to create tables (run once in Supabase SQL Editor)
# Full schema including control-plane tables is in:
#   county-map-private/build/supabase/control_plane_schema.sql
CREATE_TABLES_SQL = """
-- Conversation sessions table (primary logging - one row per user session)
CREATE TABLE IF NOT EXISTS conversation_sessions (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT UNIQUE NOT NULL,
    messages JSONB DEFAULT '[]',
    message_count INTEGER DEFAULT 0,
    datasets_used TEXT[] DEFAULT '{}',
    intents_seen TEXT[] DEFAULT '{}',
    total_results INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON conversation_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON conversation_sessions(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON conversation_sessions(created_at DESC);

-- Query logs table (legacy - kept for backwards compatibility)
CREATE TABLE IF NOT EXISTS query_logs (
    id BIGSERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    dataset_selected TEXT,
    interest TEXT,
    scale TEXT,
    results_count INTEGER DEFAULT 0,
    response_time_ms INTEGER,
    error TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_query_logs_created_at ON query_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_query_logs_interest ON query_logs(interest);
CREATE INDEX IF NOT EXISTS idx_query_logs_error ON query_logs(error) WHERE error IS NOT NULL;

-- Error logs table
CREATE TABLE IF NOT EXISTS error_logs (
    id BIGSERIAL PRIMARY KEY,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    query TEXT,
    traceback TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_error_logs_created_at ON error_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_error_logs_type ON error_logs(error_type);

-- Dataset metadata table
CREATE TABLE IF NOT EXISTS dataset_metadata (
    id BIGSERIAL PRIMARY KEY,
    filename TEXT UNIQUE NOT NULL,
    description TEXT,
    source_name TEXT,
    source_url TEXT,
    license TEXT,
    geographic_level TEXT,
    row_count INTEGER,
    column_count INTEGER,
    topic_tags TEXT[],
    full_metadata JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dataset_metadata_filename ON dataset_metadata(filename);

-- Data quality issues table (unified tracking for all data gaps)
-- Replaces separate missing_geometries and missing_regions tables
CREATE TABLE IF NOT EXISTS data_quality_issues (
    id BIGSERIAL PRIMARY KEY,
    issue_type TEXT NOT NULL,  -- 'missing_geometry', 'missing_region', 'unknown_name', etc.
    name TEXT NOT NULL,        -- The name/value that caused the issue
    first_query TEXT,
    dataset TEXT,
    region TEXT,
    metadata JSONB,
    occurrence_count INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(issue_type, name)   -- Composite unique constraint
);

CREATE INDEX IF NOT EXISTS idx_data_quality_type ON data_quality_issues(issue_type);
CREATE INDEX IF NOT EXISTS idx_data_quality_name ON data_quality_issues(name);
CREATE INDEX IF NOT EXISTS idx_data_quality_count ON data_quality_issues(occurrence_count DESC);

-- Enable Row Level Security (optional, for public access control)
-- ALTER TABLE query_logs ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE error_logs ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dataset_metadata ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE data_quality_issues ENABLE ROW LEVEL SECURITY;
"""


if __name__ == "__main__":
    # Test connection
    print("Testing Supabase connection...")

    if not HAS_SUPABASE:
        print("Supabase package not installed.")
        print("Install with: pip install supabase")
        exit(1)

    try:
        client = SupabaseClient()
        status = client.test_connection()

        if status["connected"]:
            print(f"Connected to: {status['url']}")
            print("\nTable status:")
            for table, info in status["tables"].items():
                if info["exists"]:
                    print(f"  {table}: {info['count']} rows")
                else:
                    print(f"  {table}: NOT FOUND - {info.get('error', 'unknown error')}")

            print("\nIf tables don't exist, run this SQL in Supabase SQL Editor:")
            print("-" * 50)
            print(CREATE_TABLES_SQL)
        else:
            print(f"Connection failed: {status['error']}")

    except Exception as e:
        print(f"Error: {e}")
