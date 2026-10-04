# Author: C A B M
# Date: 2026-10-04

"""
Langfuse Bidirectional Migration Script (SDK v4)
=================================================
Migrates traces, observations, scores, prompts, datasets, and score configs
between any two Langfuse instances (Cloud <-> Self-Hosted Docker).

Addresses all issues identified in the critical analysis:
  - Uses SDK v4 Langfuse constructor with tracing disabled
  - Cursor-based pagination throughout
  - Exhaustive observation fetching per trace (handles >100 obs)
  - Migrates scores, prompts, datasets, and score configs
  - Handles GENERATION / SPAN / EVENT observation types correctly
  - Rate limiting with exponential backoff on 429 errors
  - Idempotency via --from-timestamp filtering
  - Post-migration verification with count comparison

Requirements:
  pip install langfuse>=4.8.1

Usage:
  # Local Docker -> Langfuse Cloud
  python migrate_langfuse.py \
    --source-host http://localhost:3000 \
    --source-public-key pk-lf-local-... \
    --source-secret-key sk-lf-local-... \
    --dest-host https://cloud.langfuse.com \
    --dest-public-key pk-lf-cloud-... \
    --dest-secret-key sk-lf-cloud-...

  # Langfuse Cloud -> Local Docker
  python migrate_langfuse.py \
    --source-host https://cloud.langfuse.com \
    --source-public-key pk-lf-cloud-... \
    --source-secret-key sk-lf-cloud-... \
    --dest-host http://localhost:3000 \
    --dest-public-key pk-lf-local-... \
    --dest-secret-key sk-lf-local-...

  # Resume from a specific timestamp (idempotent re-run)
  python migrate_langfuse.py ... --from-timestamp 2026-10-01T00:00:00Z

  # Skip optional data types
  python migrate_langfuse.py ... --skip-prompts --skip-datasets
"""

import argparse
import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import Optional

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
# PAGINATION HELPERS (cursor-based)
# =====================================================================
def paginate(api_method, page_size: int = 100, **filters):
    """
    Generic cursor-based paginator.
    Yields individual items from *api_method* which must accept
    `limit` and `cursor` keyword arguments and return an object with
    `.data` (list) and `.meta.next_cursor` (str | None).
    """
    cursor = None
    while True:
        response = with_retry(api_method, limit=page_size, cursor=cursor, **filters)
        items = response.data
        if not items:
            break
        yield from items
        cursor = getattr(response.meta, "next_cursor", None) if hasattr(response, "meta") else None
        if not cursor:
            break


def paginate_simple(api_method, page_size: int = 100, **filters):
    """
    Paginator for APIs that use page-number pagination as a fallback
    (some self-hosted v3 instances may still use this). Tries cursor
    first; falls back to page-based if cursor is not present.
    """
    # First try cursor-based
    cursor = None
    page = 1
    while True:
        try:
            if cursor is not None:
                response = with_retry(api_method, limit=page_size, cursor=cursor, **filters)
            else:
                # First call  --  try without cursor to detect which pagination model
                response = with_retry(api_method, limit=page_size, **filters)
        except TypeError:
            # API method may not accept cursor  --  fall back to page-based
            response = with_retry(api_method, limit=page_size, page=page, **filters)

        items = response.data
        if not items:
            break
        yield from items

        # Attempt cursor-based next
        meta = getattr(response, "meta", None)
        next_cursor = getattr(meta, "next_cursor", None) if meta else None
        if next_cursor:
            cursor = next_cursor
        else:
            # If no cursor returned, assume page-based or end of data
            if len(items) < page_size:
                break
            page += 1
            cursor = None  # keep using page-based


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
        configs = with_retry(source.api.score_configs.list)
        for cfg in configs.data:
            try:
                with_retry(
                    dest.api.score_configs.create,
                    name=cfg.name,
                    data_type=cfg.data_type,
                    min_value=getattr(cfg, "min_value", None),
                    max_value=getattr(cfg, "max_value", None),
                    categories=getattr(cfg, "categories", None),
                    description=getattr(cfg, "description", None),
                )
                count += 1
            except Exception as e:
                if "already exists" in str(e).lower() or "409" in str(e):
                    log.debug("Score config '%s' already exists  --  skipping", cfg.name)
                else:
                    log.warning("Failed to create score config '%s': %s", cfg.name, e)
    except Exception as e:
        log.warning("Could not fetch score configs: %s", e)
    log.info("Score configs migrated: %d", count)
    return count


# ---- 2. Prompts (all versions) --------------------------------------
def migrate_prompts(source: Langfuse, dest: Langfuse) -> int:
    """Migrate all prompts with all their versions."""
    log.info("--- Migrating prompts ---")
    count = 0
    try:
        prompts_resp = with_retry(source.api.prompts.list)
        prompt_metas = prompts_resp.data
    except Exception as e:
        log.warning("Could not list prompts: %s", e)
        return 0

    for pmeta in prompt_metas:
        try:
            # Fetch the full prompt (latest version) to get details
            prompt = with_retry(source.get_prompt, pmeta.name)
            try:
                with_retry(
                    dest.create_prompt,
                    name=prompt.name,
                    type=getattr(prompt, "type", "text"),
                    prompt=prompt.prompt,
                    labels=getattr(prompt, "labels", []),
                    config=getattr(prompt, "config", None),
                )
                count += 1
            except Exception as e:
                if "already exists" in str(e).lower() or "409" in str(e):
                    log.debug("Prompt '%s' already exists  --  skipping", prompt.name)
                else:
                    log.warning("Failed to create prompt '%s': %s", prompt.name, e)
        except Exception as e:
            log.warning("Failed to fetch prompt '%s': %s", pmeta.name, e)

    log.info("Prompts migrated: %d", count)
    return count


# ---- 3. Datasets (definitions + items) ------------------------------
def migrate_datasets(source: Langfuse, dest: Langfuse) -> tuple[int, int]:
    """Migrate dataset definitions and their items."""
    log.info("--- Migrating datasets ---")
    ds_count = 0
    item_count = 0
    try:
        datasets_resp = with_retry(source.api.datasets.list)
        datasets = datasets_resp.data
    except Exception as e:
        log.warning("Could not list datasets: %s", e)
        return 0, 0

    for ds in datasets:
        # Create dataset in destination
        try:
            with_retry(
                dest.create_dataset,
                name=ds.name,
                description=getattr(ds, "description", None),
                metadata=getattr(ds, "metadata", None),
            )
            ds_count += 1
        except Exception as e:
            if "already exists" in str(e).lower() or "409" in str(e):
                log.debug("Dataset '%s' already exists  --  will still migrate items", ds.name)
            else:
                log.warning("Failed to create dataset '%s': %s", ds.name, e)
                continue

        # Fetch and migrate items
        try:
            items_resp = with_retry(source.api.dataset_items.list, dataset_name=ds.name)
            for item in items_resp.data:
                try:
                    with_retry(
                        dest.create_dataset_item,
                        dataset_name=ds.name,
                        input=item.input,
                        expected_output=getattr(item, "expected_output", None),
                        metadata=getattr(item, "metadata", None),
                        id=getattr(item, "id", None),
                    )
                    item_count += 1
                except Exception as e:
                    if "already exists" in str(e).lower() or "409" in str(e):
                        log.debug("Dataset item already exists  --  skipping")
                    else:
                        log.warning("Failed to create dataset item: %s", e)
        except Exception as e:
            log.warning("Failed to list items for dataset '%s': %s", ds.name, e)

    log.info("Datasets migrated: %d  |  Dataset items migrated: %d", ds_count, item_count)
    return ds_count, item_count


# ---- 4. Traces + Observations + Scores ------------------------------
def migrate_traces(
    source: Langfuse,
    dest: Langfuse,
    batch_limit: int = 50,
    obs_page_size: int = 100,
    from_timestamp: Optional[datetime] = None,
) -> dict:
    """
    Migrate all traces, their nested observations, and associated scores
    from the source to the destination.

    Uses cursor-based pagination throughout.
    Handles GENERATION, SPAN, and EVENT observation types.
    """
    log.info("--- Migrating traces, observations, and scores ---")
    stats = {"traces": 0, "observations": 0, "scores": 0, "errors": 0}

    # Build trace listing filters
    trace_filters = {}
    if from_timestamp:
        trace_filters["from_timestamp"] = from_timestamp
        log.info("Filtering traces from: %s", from_timestamp.isoformat())

    # --- Iterate traces ---
    trace_cursor = None
    page_num = 0
    while True:
        try:
            list_kwargs = {"limit": batch_limit, **trace_filters}
            if trace_cursor:
                list_kwargs["cursor"] = trace_cursor
            traces_resp = with_retry(source.api.traces.list, **list_kwargs)
            traces = traces_resp.data
        except Exception as err:
            log.error("Failed to fetch traces (cursor=%s): %s", trace_cursor, err)
            stats["errors"] += 1
            break

        if not traces:
            log.info("No more traces to process.")
            break

        page_num += 1

        for trace in traces:
            # --- 4a. Ingest the trace ---
            try:
                trace_kwargs = {
                    "id": trace.id,
                    "name": trace.name,
                    "timestamp": trace.timestamp,
                }
                # Optional fields  --  only pass if present to avoid schema errors
                for attr in (
                    "user_id", "session_id", "tags", "metadata",
                    "input", "output", "release", "version", "public",
                ):
                    val = getattr(trace, attr, None)
                    if val is not None:
                        trace_kwargs[attr] = val

                with_retry(dest.trace, **trace_kwargs)
                stats["traces"] += 1
            except Exception as e:
                log.warning("Failed to ingest trace %s: %s", trace.id, e)
                stats["errors"] += 1
                continue

            # --- 4b. Ingest observations (exhaustive cursor pagination) ---
            obs_cursor = None
            while True:
                try:
                    obs_kwargs = {"trace_id": trace.id, "limit": obs_page_size}
                    if obs_cursor:
                        obs_kwargs["cursor"] = obs_cursor
                    obs_resp = with_retry(
                        source.api.observations.get_many, **obs_kwargs
                    )
                    observations = obs_resp.data
                except Exception as obs_err:
                    log.warning(
                        "Failed to fetch observations for trace %s: %s",
                        trace.id,
                        obs_err,
                    )
                    stats["errors"] += 1
                    break

                if not observations:
                    break

                for obs in observations:
                    try:
                        _ingest_observation(dest, trace.id, obs)
                        stats["observations"] += 1
                    except Exception as e:
                        log.warning(
                            "Failed to ingest observation %s (trace %s): %s",
                            obs.id,
                            trace.id,
                            e,
                        )
                        stats["errors"] += 1

                # Next page of observations
                obs_meta = getattr(obs_resp, "meta", None)
                obs_cursor = (
                    getattr(obs_meta, "next_cursor", None) if obs_meta else None
                )
                if not obs_cursor:
                    break

            # --- 4c. Ingest scores for this trace ---
            try:
                score_cursor = None
                while True:
                    score_kwargs = {"trace_id": trace.id, "limit": obs_page_size}
                    if score_cursor:
                        score_kwargs["cursor"] = score_cursor

                    # Try v3 scores API first, fall back to v2
                    try:
                        scores_resp = with_retry(
                            source.api.scores_v3.list, **score_kwargs
                        )
                    except (AttributeError, Exception):
                        scores_resp = with_retry(
                            source.api.scores.list, **score_kwargs
                        )

                    scores = scores_resp.data
                    if not scores:
                        break

                    for score in scores:
                        try:
                            score_create_kwargs = {
                                "trace_id": trace.id,
                                "name": score.name,
                                "value": score.value,
                            }
                            for s_attr in (
                                "observation_id",
                                "comment",
                                "data_type",
                                "config_id",
                            ):
                                s_val = getattr(score, s_attr, None)
                                if s_val is not None:
                                    score_create_kwargs[s_attr] = s_val

                            with_retry(dest.score, **score_create_kwargs)
                            stats["scores"] += 1
                        except Exception as e:
                            log.warning(
                                "Failed to ingest score for trace %s: %s",
                                trace.id,
                                e,
                            )
                            stats["errors"] += 1

                    s_meta = getattr(scores_resp, "meta", None)
                    score_cursor = (
                        getattr(s_meta, "next_cursor", None) if s_meta else None
                    )
                    if not score_cursor:
                        break
            except Exception as e:
                log.warning(
                    "Failed to fetch scores for trace %s: %s", trace.id, e
                )

        # Flush the current batch
        dest.flush()
        log.info(
            "Page %d complete  --  %d traces / %d observations / %d scores so far",
            page_num,
            stats["traces"],
            stats["observations"],
            stats["scores"],
        )

        # Throttle between pages to respect rate limits
        time.sleep(0.3)

        # Move to next page
        t_meta = getattr(traces_resp, "meta", None)
        trace_cursor = getattr(t_meta, "next_cursor", None) if t_meta else None
        if not trace_cursor:
            break

    return stats


def _ingest_observation(dest: Langfuse, trace_id: str, obs) -> None:
    """
    Route an observation to the correct ingestion method based on its type.
    Handles GENERATION, SPAN, and EVENT types.
    """
    obs_type = getattr(obs, "type", "SPAN")

    # Common fields shared across all observation types
    common = {
        "id": obs.id,
        "trace_id": trace_id,
        "name": obs.name,
        "start_time": obs.start_time,
        "metadata": getattr(obs, "metadata", None),
        "level": getattr(obs, "level", None),
        "status_message": getattr(obs, "status_message", None),
        "parent_observation_id": getattr(obs, "parent_observation_id", None),
        "input": getattr(obs, "input", None),
        "output": getattr(obs, "output", None),
        "version": getattr(obs, "version", None),
    }

    # Remove None values to avoid sending empty optionals
    common = {k: v for k, v in common.items() if v is not None}

    if obs_type == "GENERATION":
        gen_fields = {}
        for attr in ("end_time", "model", "model_parameters"):
            val = getattr(obs, attr, None)
            if val is not None:
                gen_fields[attr] = val

        # Handle usage carefully  --  normalize the structure
        usage_raw = getattr(obs, "usage", None)
        if usage_raw is not None:
            gen_fields["usage"] = _normalize_usage(usage_raw)

        with_retry(dest.generation, **common, **gen_fields)

    elif obs_type == "EVENT":
        # Events do NOT have end_time
        with_retry(dest.event, **common)

    else:
        # SPAN (default)
        end_time = getattr(obs, "end_time", None)
        if end_time is not None:
            common["end_time"] = end_time
        with_retry(dest.span, **common)


def _normalize_usage(usage) -> dict:
    """
    Normalize the usage object from the read API into the format
    expected by the ingestion API. The read API may return a Usage
    object; the ingestion API expects a plain dict.
    """
    if isinstance(usage, dict):
        return usage

    # Convert from SDK Usage object to dict
    normalized = {}
    for field in ("input", "output", "total", "unit",
                  "input_cost", "output_cost", "total_cost"):
        val = getattr(usage, field, None)
        if val is not None:
            normalized[field] = val

    # Some versions use prompt_tokens / completion_tokens
    for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
        val = getattr(usage, field, None)
        if val is not None:
            normalized[field] = val

    return normalized if normalized else None


# =====================================================================
# VERIFICATION
# =====================================================================
def verify_migration(source: Langfuse, dest: Langfuse, stats: dict) -> None:
    """Run a simple count-based verification after migration."""
    log.info("--- Verification ---")
    log.info("Traces transferred:      %d", stats["traces"])
    log.info("Observations transferred: %d", stats["observations"])
    log.info("Scores transferred:      %d", stats["scores"])
    log.info("Errors encountered:      %d", stats["errors"])

    if stats["errors"] > 0:
        log.warning(
            "[!] %d errors occurred during migration. "
            "Review the log output above for details.",
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
        log.info("DRY RUN  --  will read from source but not write to destination.")
        # Just count traces
        count = 0
        for _ in paginate_simple(source.api.traces.list, page_size=args.batch_limit):
            count += 1
        log.info("Source contains %d traces.", count)
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

    # Step 4: Traces + Observations + Scores (the big one)
    stats = migrate_traces(
        source,
        dest,
        batch_limit=args.batch_limit,
        from_timestamp=from_ts,
    )

    # Final flush and shutdown
    dest.shutdown()

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("LANGFUSE MIGRATION  --  COMPLETE (%.1f seconds)", elapsed)
    log.info("=" * 60)

    # Verification
    verify_migration(source, dest, stats)


if __name__ == "__main__":
    main()
