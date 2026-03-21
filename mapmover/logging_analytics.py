"""
Logging and analytics functions for query tracking and error monitoring.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from .paths import LOGS_DIR, ensure_dir

# Set up logging
try:
    logs_dir = ensure_dir(LOGS_DIR)
    _local_logs_enabled = True
except OSError:
    logs_dir = LOGS_DIR
    _local_logs_enabled = False

error_log_path = logs_dir / "errors.log"

# Create a custom logger with proper configuration
logger = logging.getLogger("mapmover")
logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates on reload
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Console handler (optional but useful for debugging)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# File handler - only when the runtime log dir is writable
if _local_logs_enabled:
    file_handler = logging.FileHandler(error_log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Prevent propagation to root logger (avoids duplicate logs)
logger.propagate = False

# Query analytics logger - tracks usage patterns
analytics_dir = logs_dir / "analytics"
if _local_logs_enabled:
    analytics_dir.mkdir(exist_ok=True)
analytics_log_path = analytics_dir / "query_analytics.jsonl"

# Initialize Supabase client (lazy loaded to avoid import issues)
_supabase_client = None


def get_supabase():
    """Get the Supabase client, initializing if needed."""
    global _supabase_client
    if _supabase_client is None:
        try:
            from supabase_client import get_supabase_client
            _supabase_client = get_supabase_client()
            if _supabase_client:
                logger.info("Supabase client initialized - cloud logging enabled")
            else:
                logger.info("Supabase not configured - using local logging only")
        except Exception as e:
            logger.warning(f"Could not initialize Supabase client: {e}")
            _supabase_client = False  # Mark as failed to avoid retrying
    return _supabase_client if _supabase_client else None


def log_conversation(session_id, query, response_text, intent=None,
                     dataset_selected=None, results_count=0, endpoint=None):
    """
    Log a conversation message to session-based storage.

    Each session (browser tab) gets one row in Supabase with all messages.
    Also logs locally to JSONL for backup.

    Args:
        session_id: Unique session identifier from frontend
        query: The user's query string
        response_text: The assistant's response
        intent: The query intent ('chat', 'clarify', 'fetch_data', 'modify_data', 'meta')
        dataset_selected: Which dataset was used (None for chat-only queries)
        results_count: Number of results returned (0 for chat-only queries)
        endpoint: Which endpoint the request came from ('chat', 'location', etc.)
    """
    # Build analytics data for local logging
    analytics_data = {
        "timestamp": datetime.now().isoformat(),
        "session_id": session_id,
        "query": query,
        "response": response_text[:500] if response_text else None,  # Truncate for local log
        "intent": intent,
        "dataset_selected": dataset_selected,
        "results_count": results_count,
        "endpoint": endpoint
    }

    # Always log locally first
    if _local_logs_enabled:
        try:
            with open(analytics_log_path, 'a', encoding='utf-8') as f:
                json.dump(analytics_data, f, ensure_ascii=False)
                f.write('\n')
        except Exception as e:
            logger.error(f"Failed to log analytics locally: {e}")

    # Log to Supabase session if configured
    supabase_client = get_supabase()
    if supabase_client and session_id:
        try:
            supabase_client.log_session_message(
                session_id=session_id,
                user_query=query,
                assistant_response=response_text or "",
                intent=intent,
                dataset_selected=dataset_selected,
                results_count=results_count
            )
        except Exception as e:
            logger.error(f"Failed to log session to Supabase: {e}")


def log_missing_geometry(country_names, query=None, dataset=None, region=None):
    """
    Log countries/places that are missing map geometry.

    This helps track which geometries need to be added to Countries.csv.

    Args:
        country_names: List of country/place names missing geometry
        query: The query that triggered this (optional)
        dataset: The dataset being queried (optional)
        region: The region filter used (optional)
    """
    if not country_names:
        return

    # Log locally
    missing_log_path = logs_dir / "analytics" / "missing_geometries.jsonl"
    missing_log_path.parent.mkdir(parents=True, exist_ok=True)

    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "missing_countries": country_names,
        "count": len(country_names),
        "query": query,
        "dataset": dataset,
        "region": region
    }

    if _local_logs_enabled:
        try:
            with open(missing_log_path, 'a', encoding='utf-8') as f:
                json.dump(log_entry, f, ensure_ascii=False)
                f.write('\n')
        except Exception as e:
            logger.error(f"Failed to log missing geometries locally: {e}")

    # Log to Supabase if configured
    supabase_client = get_supabase()
    if supabase_client:
        try:
            supabase_client.log_missing_geometry(
                country_names=country_names,
                query=query,
                dataset=dataset,
                region=region
            )
        except Exception as e:
            logger.error(f"Failed to log missing geometries to Supabase: {e}")


def log_error_to_cloud(error_type, error_message, query=None, tb=None, metadata=None):
    """
    Log errors to Supabase cloud for centralized error tracking.

    Args:
        error_type: Type of error (e.g., "JSONDecodeError", "ValueError")
        error_message: The error message
        query: The query that caused the error (if applicable)
        tb: Traceback string
        metadata: Additional context
    """
    supabase_client = get_supabase()
    if supabase_client:
        try:
            supabase_client.log_error(
                error_type=error_type,
                error_message=error_message,
                query=query,
                traceback=tb,
                metadata=metadata
            )
        except Exception as e:
            logger.error(f"Failed to log error to Supabase: {e}")


def log_missing_region_to_cloud(region_name, query=None, dataset=None):
    """
    Log missing region lookups to Supabase for tracking gaps in conversions.json.

    Args:
        region_name: The region name that failed lookup
        query: The query that triggered this
        dataset: The dataset being queried
    """
    supabase_client = get_supabase()
    if supabase_client:
        try:
            supabase_client.log_missing_region(
                region_name=region_name,
                query=query,
                dataset=dataset
            )
        except Exception as e:
            logger.error(f"Failed to log missing region to Supabase: {e}")

    # Also log locally for backup
    if _local_logs_enabled:
        try:
            log_dir = logs_dir / "analytics"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "missing_regions.jsonl"
            with open(log_file, 'a', encoding='utf-8') as f:
                entry = {
                    "timestamp": datetime.now().isoformat(),
                    "region_name": region_name,
                    "query": query,
                    "dataset": dataset
                }
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to log missing region locally: {e}")
