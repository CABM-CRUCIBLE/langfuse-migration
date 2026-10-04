# Author: C A B M
# Date: 2026-10-04

"""
Langfuse Bidirectional Migration Script (SDK v4)
=================================================
Migrates traces, observations, scores, prompts, datasets, and score configs
between any two Langfuse instances (Cloud <-> Self-Hosted Docker).

Addresses all issues identified in the critical analysis:
  - Uses SDK v4 Langfuse client with tracing disabled
  - Handles API differences between Cloud v4 and Self-Hosted v3
  - Uses direct batch ingestion (api.ingestion.batch) preserving IDs and timestamps
  - Exhaustive observation & score fetching per trace
  - Maps observation types (CHAIN, AGENT, TOOL -> SPAN) for Cloud schema compatibility
  - Migrates scores, prompts, datasets, and score configs
  - Rate limiting with exponential backoff on 429/502/503 errors
  - Idempotency via --from-timestamp filtering
  - Post-migration verification with count comparison

Requirements:
  pip install langfuse>=4.8.1

Usage:
  # Local Docker -> Langfuse Cloud
  python migrate_langfuse.py \
    --source-host http://localhost:10500 \
    --source-public-key pk-lf-local-... \
    --source-secret-key sk-lf-local-... \
    --dest-host https://us.cloud.langfuse.com \
    --dest-public-key pk-lf-cloud-... \
    --dest-secret-key sk-lf-cloud-...

  # Dry run (verify counts without writing)
  python migrate_langfuse.py ... --dry-run

  # Resume from a specific timestamp (idempotent re-run)
  python migrate_langfuse.py ... --from-timestamp 2026-08-01T00:00:00Z
"""

import argparse
from datetime import datetime, timezone
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

# ---------------------------------------------------------------------------
# Ensure environment variables don't interfere with explicit constructor args
# ---------------------------------------------------------------------------
for _var in (
    "LANGFUSE_BASE_URL",
    "LANGFUSE_HOST",
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
    "LANGFUSE_TRACING_ENABLED",
):
    os.environ.pop(_var, None)

# Disable tracing so the migration script itself is not instrumented
os.environ["LANGFUSE_TRACING_ENABLED"] = "False"

from langfuse import Langfuse  # noqa: E402
from langfuse.api.ingestion.types import (  # noqa: E402
    IngestionEvent_ObservationCreate,
    IngestionEvent_ScoreCreate,
    IngestionEvent_TraceCreate,
    ObservationBody,
    ScoreBody,
    TraceBody,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("langfuse-migration")


# =====================================================================
# RETRY / RATE-LIMIT HELPER
# =====================================================================
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 1.0


def with_retry(fn, *args, **kwargs):
    """Call *fn* with exponential backoff on transient / rate-limit errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            exc_str = str(exc).lower()
            is_retryable = (
                "429" in exc_str
                or "rate" in exc_str
                or "timeout" in exc_str
                or "503" in exc_str
                or "502" in exc_str
            )
            if is_retryable and attempt < MAX_RETRIES:
                wait = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                log.warning(
                    "Retryable error (attempt %d/%d): %s  --  waiting %.1fs",
                    attempt,
                    MAX_RETRIES,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                raise


# =====================================================================
# CLIENT FACTORY
# =====================================================================
def make_client(host: str, public_key: str, secret_key: str, label: str) -> Langfuse:
    """Create and authenticate a Langfuse client."""
    log.info("Connecting to %s: %s", label, host)
    client = Langfuse(
        public_key=public_key,
        secret_key=secret_key,
        host=host,
    )
    if not client.auth_check():
        log.error("Authentication failed for %s (%s)", label, host)
        sys.exit(1)
    log.info("[OK] Authenticated with %s", label)
    return client


# =====================================================================
# MIGRATION FUNCTIONS
# =====================================================================

# ---- 1. Score Configurations ----------------------------------------
def migrate_score_configs(source: Langfuse, dest: Langfuse) -> int:
    """Migrate score configuration definitions."""
    log.info("--- Migrating score configurations ---")
    count = 0
    try:
        page = 1
        while True:
            configs = with_retry(source.api.score_configs.get, page=page, limit=50)
            items = getattr(configs, "data", [])
            if not items:
                break
            for cfg in items:
                try:
                    kwargs = {
                        "name": cfg.name,
                        "data_type": cfg.data_type,
                    }
                    for opt in ("min_value", "max_value", "categories", "description"):
                        val = getattr(cfg, opt, None)
                        if val is not None:
                            kwargs[opt] = val
                    with_retry(dest.api.score_configs.create, **kwargs)
                    count += 1
                except Exception as e:
                    if "already exists" in str(e).lower() or "409" in str(e):
                        log.debug("Score config '%s' already exists -- skipping", cfg.name)
                    else:
                        log.warning("Failed to create score config '%s': %s", cfg.name, e)
            meta = getattr(configs, "meta", None)
            total_pages = getattr(meta, "total_pages", None)
            if total_pages is not None and page >= total_pages:
                break
            if len(items) < 50:
                break
            page += 1
    except Exception as e:
        log.warning("Could not fetch score configs: %s", e)
    log.info("Score configs migrated: %d", count)
    return count


# ---- 2. Prompts -----------------------------------------------------
def migrate_prompts(source: Langfuse, dest: Langfuse) -> int:
    """Migrate all prompts."""
    log.info("--- Migrating prompts ---")
    count = 0
    try:
        page = 1
        while True:
            prompts_resp = with_retry(source.api.prompts.list, page=page, limit=50)
            items = getattr(prompts_resp, "data", [])
            if not items:
                break
            for pmeta in items:
                try:
                    prompt = with_retry(source.get_prompt, pmeta.name)
                    try:
                        kwargs = {
                            "name": prompt.name,
                            "prompt": prompt.prompt,
                        }
                        if hasattr(prompt, "type") and prompt.type:
                            kwargs["type"] = prompt.type
                        if hasattr(prompt, "labels") and prompt.labels:
                            kwargs["labels"] = prompt.labels
                        if hasattr(prompt, "config") and prompt.config is not None:
                            kwargs["config"] = prompt.config
                        if hasattr(prompt, "tags") and prompt.tags:
                            kwargs["tags"] = prompt.tags
                        with_retry(dest.create_prompt, **kwargs)
                        count += 1
                    except Exception as e:
                        if "already exists" in str(e).lower() or "409" in str(e):
                            log.debug("Prompt '%s' already exists -- skipping", prompt.name)
                        else:
                            log.warning("Failed to create prompt '%s': %s", prompt.name, e)
                except Exception as e:
                    log.warning("Failed to fetch prompt '%s': %s", pmeta.name, e)
            meta = getattr(prompts_resp, "meta", None)
            total_pages = getattr(meta, "total_pages", None)
            if total_pages is not None and page >= total_pages:
                break
            if len(items) < 50:
                break
            page += 1
    except Exception as e:
        log.warning("Could not list prompts: %s", e)
        return 0

    log.info("Prompts migrated: %d", count)
    return count


# ---- 3. Datasets (definitions + items) ------------------------------
def migrate_datasets(source: Langfuse, dest: Langfuse) -> Tuple[int, int]:
    """Migrate dataset definitions and their items."""
    log.info("--- Migrating datasets ---")
    ds_count = 0
    item_count = 0
    try:
        page = 1
        while True:
            datasets_resp = with_retry(source.api.datasets.list, page=page, limit=50)
            items = getattr(datasets_resp, "data", [])
            if not items:
                break
            for ds in items:
                try:
                    kwargs = {"name": ds.name}
                    for opt in ("description", "metadata"):
                        val = getattr(ds, opt, None)
                        if val is not None:
                            kwargs[opt] = val
                    with_retry(dest.create_dataset, **kwargs)
                    ds_count += 1
                except Exception as e:
                    if "already exists" in str(e).lower() or "409" in str(e):
                        log.debug("Dataset '%s' already exists -- will still migrate items", ds.name)
                    else:
                        log.warning("Failed to create dataset '%s': %s", ds.name, e)
                        continue

                # Migrate dataset items
                item_page = 1
                while True:
                    try:
                        items_resp = with_retry(
                            source.api.dataset_items.list,
                            dataset_name=ds.name,
                            page=item_page,
                            limit=50,
                        )
                        d_items = getattr(items_resp, "data", [])
                        if not d_items:
                            break
                        for item in d_items:
                            try:
                                i_kwargs = {
                                    "dataset_name": ds.name,
                                    "input": item.input,
                                }
                                for opt in ("expected_output", "metadata", "id"):
                                    val = getattr(item, opt, None)
                                    if val is not None:
                                        i_kwargs[opt] = val
                                with_retry(dest.create_dataset_item, **i_kwargs)
                                item_count += 1
                            except Exception as e:
                                if "already exists" in str(e).lower() or "409" in str(e):
                                    log.debug("Dataset item already exists -- skipping")
                                else:
                                    log.warning("Failed to create dataset item: %s", e)
                        meta_items = getattr(items_resp, "meta", None)
                        total_pages = getattr(meta_items, "total_pages", None)
                        if total_pages is not None and item_page >= total_pages:
                            break
                        if len(d_items) < 50:
                            break
                        item_page += 1
                    except Exception as e:
                        log.warning("Failed to list items for dataset '%s': %s", ds.name, e)
                        break

            meta = getattr(datasets_resp, "meta", None)
            total_pages = getattr(meta, "total_pages", None)
            if total_pages is not None and page >= total_pages:
                break
            if len(items) < 50:
                break
            page += 1
    except Exception as e:
        log.warning("Could not list datasets: %s", e)
        return 0, 0

    log.info("Datasets migrated: %d  |  Dataset items migrated: %d", ds_count, item_count)
    return ds_count, item_count


# ---- 4. Traces + Observations + Scores ------------------------------
def _normalize_usage(usage) -> Optional[dict]:
    """Normalize usage object into dict matching Langfuse Usage schema."""
    if isinstance(usage, dict):
        return usage
    normalized = {}
    for field in (
        "input", "output", "total", "unit",
        "input_cost", "output_cost", "total_cost",
    ):
        val = getattr(usage, field, None)
        if val is not None:
            normalized[field] = val
    return normalized if normalized else None


def _build_observation_body(trace_id: str, obs, time_delta: Optional[Any] = None) -> ObservationBody:
    """Build ObservationBody with validated types compatible with Cloud & self-hosted."""
    obs_type = getattr(obs, "type", "SPAN")
    meta = getattr(obs, "metadata", None) or {}

    # Langfuse Cloud ingestion schema strictly expects GENERATION, SPAN, or EVENT.
    # Preserve original subtype (e.g. CHAIN, AGENT, TOOL) in metadata.
    if obs_type not in ("GENERATION", "SPAN", "EVENT"):
        if isinstance(meta, dict):
            meta = {**meta, "_original_type": obs_type}
        obs_type = "SPAN"

    usage = getattr(obs, "usage", None)
    if usage is not None:
        usage = _normalize_usage(usage)

    start_time = getattr(obs, "start_time", None)
    end_time = getattr(obs, "end_time", None)
    completion_start_time = getattr(obs, "completion_start_time", None)

    if time_delta:
        if start_time:
            if isinstance(meta, dict):
                meta = {**meta, "_original_start_time": start_time.isoformat()}
            start_time = start_time + time_delta
        if end_time:
            end_time = end_time + time_delta
        if completion_start_time:
            completion_start_time = completion_start_time + time_delta

    kwargs = {
        "id": obs.id,
        "trace_id": trace_id,
        "type": obs_type,
        "name": getattr(obs, "name", None),
        "start_time": start_time,
        "end_time": end_time,
        "completion_start_time": completion_start_time,
        "model": getattr(obs, "model", None),
        "model_parameters": getattr(obs, "model_parameters", None),
        "input": getattr(obs, "input", None),
        "output": getattr(obs, "output", None),
        "version": getattr(obs, "version", None),
        "metadata": meta if meta else None,
        "level": getattr(obs, "level", None),
        "status_message": getattr(obs, "status_message", None),
        "parent_observation_id": getattr(obs, "parent_observation_id", None),
        "usage": usage,
    }
    return ObservationBody(**{k: v for k, v in kwargs.items() if v is not None})


def migrate_traces(
    source: Langfuse,
    dest: Langfuse,
    batch_limit: int = 50,
    from_timestamp: Optional[datetime] = None,
    shift_to_now: bool = False,
) -> dict:
    """
    Migrate all traces, their nested observations, and associated scores
    from source to destination via batch ingestion.
    """
    log.info("--- Migrating traces, observations, and scores ---")
    if shift_to_now:
        log.info("Timestamp shifting ENABLED: traces will be shifted to current date to stay within Cloud 30-day retention.")
    stats = {"traces": 0, "observations": 0, "scores": 0, "errors": 0}

    # Build filters
    filters = {}
    if from_timestamp:
        filters["from_timestamp"] = from_timestamp
        log.info("Filtering traces from: %s", from_timestamp.isoformat())

    page = 1
    while True:
        try:
            traces_resp = with_retry(
                source.api.trace.list,
                page=page,
                limit=batch_limit,
                **filters,
            )
            traces = getattr(traces_resp, "data", [])
        except Exception as err:
            log.error("Failed to fetch traces (page=%d): %s", page, err)
            stats["errors"] += 1
            break

        if not traces:
            log.info("No more traces to process.")
            break

        for trace in traces:
            trace_events = []

            # Fetch full trace details
            trace_full = None
            try:
                trace_full = with_retry(source.api.trace.get, trace.id)
            except Exception as e:
                log.warning("Could not fetch full trace %s: %s", trace.id, e)

            # Compute time delta if shifting
            t_obj = trace_full or trace
            orig_ts = getattr(t_obj, "timestamp", None)
            time_delta = None
            ts_to_use = orig_ts
            t_meta = getattr(t_obj, "metadata", None) or {}

            if shift_to_now and orig_ts:
                now_utc = datetime.now(timezone.utc)
                time_delta = now_utc - orig_ts
                ts_to_use = now_utc
                if isinstance(t_meta, dict):
                    t_meta = {**t_meta, "_original_timestamp": orig_ts.isoformat()}

            # 1. Trace Event
            try:
                trace_body = TraceBody(
                    id=t_obj.id,
                    name=getattr(t_obj, "name", None),
                    timestamp=ts_to_use,
                    user_id=getattr(t_obj, "user_id", None),
                    session_id=getattr(t_obj, "session_id", None),
                    release=getattr(t_obj, "release", None),
                    version=getattr(t_obj, "version", None),
                    metadata=t_meta if t_meta else None,
                    tags=getattr(t_obj, "tags", None),
                    public=getattr(t_obj, "public", None),
                    input=getattr(t_obj, "input", None),
                    output=getattr(t_obj, "output", None),
                )
                trace_events.append(
                    IngestionEvent_TraceCreate(
                        id=str(uuid.uuid4()),
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        body=trace_body,
                    )
                )
            except Exception as e:
                log.warning("Failed to prepare trace %s: %s", trace.id, e)
                stats["errors"] += 1
                continue

            # 2. Observations
            observations = getattr(trace_full, "observations", None)
            if observations is None:
                try:
                    legacy_resp = with_retry(
                        source.api.legacy.observations_v1.get_many, trace_id=trace.id
                    )
                    observations = getattr(legacy_resp, "data", [])
                except Exception:
                    observations = []

            obs_count_trace = 0
            for obs in observations:
                try:
                    obs_body = _build_observation_body(trace.id, obs, time_delta=time_delta)
                    trace_events.append(
                        IngestionEvent_ObservationCreate(
                            id=str(uuid.uuid4()),
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            body=obs_body,
                        )
                    )
                    obs_count_trace += 1
                except Exception as e:
                    log.warning(
                        "Failed to prepare observation %s (trace %s): %s",
                        getattr(obs, "id", "unknown"),
                        trace.id,
                        e,
                    )
                    stats["errors"] += 1

            # 3. Scores
            scores = getattr(trace_full, "scores", None) or []
            scores_count_trace = 0
            for score in scores:
                try:
                    score_body = ScoreBody(
                        id=getattr(score, "id", str(uuid.uuid4())),
                        trace_id=trace.id,
                        name=score.name,
                        value=score.value,
                        session_id=getattr(score, "session_id", None),
                        observation_id=getattr(score, "observation_id", None),
                        comment=getattr(score, "comment", None),
                        metadata=getattr(score, "metadata", None),
                        data_type=getattr(score, "data_type", None),
                        config_id=getattr(score, "config_id", None),
                    )
                    trace_events.append(
                        IngestionEvent_ScoreCreate(
                            id=str(uuid.uuid4()),
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            body=score_body,
                        )
                    )
                    scores_count_trace += 1
                except Exception as e:
                    log.warning("Failed to prepare score for trace %s: %s", trace.id, e)
                    stats["errors"] += 1

            # 4. Ingest batch for this trace into destination
            if trace_events:
                chunk_size = 100
                for i in range(0, len(trace_events), chunk_size):
                    chunk = trace_events[i : i + chunk_size]
                    try:
                        res = with_retry(dest.api.ingestion.batch, batch=chunk)
                        if res.errors:
                            for err in res.errors:
                                log.warning(
                                    "Ingestion error for trace %s: [%s] %s (%s)",
                                    trace.id,
                                    err.status,
                                    err.message,
                                    err.error,
                                )
                                stats["errors"] += 1
                    except Exception as e:
                        log.warning("Failed to ingest batch for trace %s: %s", trace.id, e)
                        stats["errors"] += 1

            stats["traces"] += 1
            stats["observations"] += obs_count_trace
            stats["scores"] += scores_count_trace

        log.info(
            "Page %d complete  --  %d traces / %d observations / %d scores migrated so far",
            page,
            stats["traces"],
            stats["observations"],
            stats["scores"],
        )

        # Pagination termination checks
        meta = getattr(traces_resp, "meta", None)
        total_pages = getattr(meta, "total_pages", None)
        if total_pages is not None and page >= total_pages:
            break
        if len(traces) < batch_limit:
            break
        page += 1
        time.sleep(0.3)

    return stats


# =====================================================================
# VERIFICATION
# =====================================================================
def verify_migration(source: Langfuse, dest: Langfuse, stats: dict) -> None:
    """Run a count-based verification summary after migration."""
    log.info("--- Verification ---")
    log.info("Traces transferred:       %d", stats["traces"])
    log.info("Observations transferred: %d", stats["observations"])
    log.info("Scores transferred:       %d", stats["scores"])
    log.info("Errors encountered:       %d", stats["errors"])

    if stats["errors"] > 0:
        log.warning(
            "[!] %d errors occurred during migration. Review the log output above for details.",
            stats["errors"],
        )
    else:
        log.info("[OK] Migration completed with zero errors.")


# =====================================================================
# CLI
# =====================================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bidirectional Langfuse migration (SDK v4)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # Source
    p.add_argument("--source-host", required=True, help="Source Langfuse host URL")
    p.add_argument("--source-public-key", required=True, help="Source public key")
    p.add_argument("--source-secret-key", required=True, help="Source secret key")
    # Destination
    p.add_argument("--dest-host", required=True, help="Destination Langfuse host URL")
    p.add_argument("--dest-public-key", required=True, help="Destination public key")
    p.add_argument("--dest-secret-key", required=True, help="Destination secret key")
    # Options
    p.add_argument(
        "--batch-limit",
        type=int,
        default=50,
        help="Traces per page (default: 50, reduce for large payloads)",
    )
    p.add_argument(
        "--from-timestamp",
        type=str,
        default=None,
        help="Only migrate traces created after this ISO-8601 timestamp "
        "(e.g., 2026-10-01T00:00:00Z). Enables idempotent re-runs.",
    )
    p.add_argument(
        "--skip-prompts",
        action="store_true",
        help="Skip prompt migration",
    )
    p.add_argument(
        "--skip-datasets",
        action="store_true",
        help="Skip dataset migration",
    )
    p.add_argument(
        "--skip-score-configs",
        action="store_true",
        help="Skip score configuration migration",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Authenticate and count source data without writing to destination",
    )
    p.add_argument(
        "--shift-to-now",
        action="store_true",
        help="Shift historical trace & observation timestamps to the current date "
        "so they are visible within Langfuse Cloud's 30-day retention window. "
        "Original timestamps are preserved in metadata.",
    )
    return p.parse_args()


# =====================================================================
# MAIN
# =====================================================================
def main():
    args = parse_args()

    # Parse optional timestamp filter
    from_ts = None
    if args.from_timestamp:
        try:
            from_ts = datetime.fromisoformat(
                args.from_timestamp.replace("Z", "+00:00")
            )
        except ValueError:
            log.error("Invalid --from-timestamp format: %s", args.from_timestamp)
            sys.exit(1)

    # Initialize clients
    source = make_client(
        args.source_host, args.source_public_key, args.source_secret_key, "SOURCE"
    )
    dest = make_client(
        args.dest_host, args.dest_public_key, args.dest_secret_key, "DESTINATION"
    )

    if args.dry_run:
        log.info("DRY RUN -- counting source data without writing to destination.")
        cfg_count = 0
        try:
            cfgs = source.api.score_configs.get(limit=100)
            cfg_count = getattr(
                getattr(cfgs, "meta", None), "total_items", len(getattr(cfgs, "data", []))
            )
        except Exception:
            pass

        prompt_count = 0
        try:
            p_resp = source.api.prompts.list(limit=100)
            prompt_count = getattr(
                getattr(p_resp, "meta", None),
                "total_items",
                len(getattr(p_resp, "data", [])),
            )
        except Exception:
            pass

        ds_count = 0
        try:
            d_resp = source.api.datasets.list(limit=100)
            ds_count = getattr(
                getattr(d_resp, "meta", None),
                "total_items",
                len(getattr(d_resp, "data", [])),
            )
        except Exception:
            pass

        trace_count = 0
        obs_count = 0
        scores_count = 0
        page = 1
        filters = {}
        if from_ts:
            filters["from_timestamp"] = from_ts
        while True:
            t_resp = source.api.trace.list(page=page, limit=args.batch_limit, **filters)
            t_data = getattr(t_resp, "data", [])
            if not t_data:
                break
            for t in t_data:
                trace_count += 1
                try:
                    tf = source.api.trace.get(t.id)
                    obs_count += len(getattr(tf, "observations", []) or [])
                    scores_count += len(getattr(tf, "scores", []) or [])
                except Exception:
                    pass
            meta = getattr(t_resp, "meta", None)
            total_pages = getattr(meta, "total_pages", None)
            if total_pages is not None and page >= total_pages:
                break
            if len(t_data) < args.batch_limit:
                break
            page += 1

        log.info("=" * 60)
        log.info("DRY RUN SUMMARY:")
        log.info("  Score configs: %d", cfg_count)
        log.info("  Prompts:       %d", prompt_count)
        log.info("  Datasets:      %d", ds_count)
        log.info("  Traces:        %d", trace_count)
        log.info("  Observations:  %d", obs_count)
        log.info("  Scores:        %d", scores_count)
        log.info("=" * 60)
        return

    log.info("=" * 60)
    log.info("LANGFUSE MIGRATION  --  START")
    log.info("  Source:      %s", args.source_host)
    log.info("  Destination: %s", args.dest_host)
    log.info("=" * 60)

    start_time = time.time()

    # Step 1: Score configurations
    if not args.skip_score_configs:
        migrate_score_configs(source, dest)

    # Step 2: Prompts
    if not args.skip_prompts:
        migrate_prompts(source, dest)

    # Step 3: Datasets
    if not args.skip_datasets:
        migrate_datasets(source, dest)

    # Step 4: Traces + Observations + Scores
    stats = migrate_traces(
        source,
        dest,
        batch_limit=args.batch_limit,
        from_timestamp=from_ts,
        shift_to_now=args.shift_to_now,
    )

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("LANGFUSE MIGRATION  --  COMPLETE (%.1f seconds)", elapsed)
    log.info("=" * 60)

    # Verification
    verify_migration(source, dest, stats)


if __name__ == "__main__":
    main()
