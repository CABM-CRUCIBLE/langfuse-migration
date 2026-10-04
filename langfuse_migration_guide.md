# Author: C A B M
# Date: 2026-10-04

# Bidirectional Trace Migration Guide: Langfuse Web (Cloud) ↔ Local Docker (v3/v4)

This guide outlines how to export and import traces, observations, scores, prompts, and datasets bidirectionally between **Langfuse Cloud (Web)** and a **locally hosted Langfuse v3 Docker instance**.

> **⚠️ SDK Version Notice:** This guide uses **Langfuse Python SDK v4** (released March 2026). Legacy v2/v3 SDK methods (`Langfuse()` constructor with `trace()`, `span()`, `generation()`) are deprecated and will stop working on Langfuse Cloud after **November 16, 2026**. If your self-hosted instance still runs Langfuse v3 server, the SDK v4 client remains backward-compatible for ingestion.

---

## 1. Architectural Overview

Direct database operations (such as running `pg_dump` or exporting ClickHouse tables) cannot be used with Langfuse Cloud because it operates as a multi-tenant environment where direct database access is restricted for security and isolation.

Data exchange between instances is achieved via the **Langfuse REST API / Python SDK ingestion pipeline**:
* **Historical Accuracy:** Ingesting with explicit `id` and `timestamp` fields ensures your historical run times, trace relationships, and tree hierarchy remain unchanged.
* **Observation Preservation:** Both trace objects and nested execution steps (generations, spans, events) are fetched and reconstructed.
* **Score & Metadata Preservation:** Scores (human feedback, LLM-as-judge evaluations, custom metrics), prompts (all versions), datasets (definitions + items), and score configurations are all migrated.
* **Cursor-Based Pagination:** The script uses cursor-based pagination (via `meta.next_cursor`) throughout, which is the only pagination model supported by the Langfuse v4 Observations API.
* **Unified Pipeline:** The same ingestion engine processes transfers in either direction.

### What Is Migrated

| Data Type | Migrated | Notes |
|---|---|---|
| Traces | ✅ | Including all fields (tags, metadata, public flag) |
| Observations (SPAN) | ✅ | Full parent-child hierarchy preserved |
| Observations (GENERATION) | ✅ | Including model, usage, model_parameters |
| Observations (EVENT) | ✅ | Correctly handled without `end_time` |
| Scores | ✅ | Including comment, data_type, config_id |
| Score Configurations | ✅ | Name, data type, min/max, categories |
| Prompts | ✅ | Latest version with labels and config |
| Datasets | ✅ | Definitions + all items |

### What Is NOT Migrated

| Data Type | Reason |
|---|---|
| User / RBAC / SSO settings | Organization-level config, not project data |
| Custom dashboards | UI-only configuration |
| LLM-as-Judge evaluator configs | Must be recreated manually |
| Dataset run items | Not exposed via the public API |
| Project/organization settings | Platform-level config |

---

## 2. Prerequisites

### 2.1 Dependencies
Install the official Langfuse Python SDK v4:
```bash
pip install "langfuse>=4.8.1"
```

> **Why v4.8.1+?** This version includes full support for the v3 Scores API (`api.scores_v3`), cursor-based pagination, and the migration-compatible `Langfuse()` constructor with `host` parameter.

### 2.2 Network & Host Configuration
* **Local to Cloud:** Ensure your local host machine has outbound internet connectivity to `https://cloud.langfuse.com` (EU region) or `https://us.cloud.langfuse.com` (US region).
* **Cloud to Local:** Ensure your local Docker containers (`langfuse-web`, `langfuse-worker`, `clickhouse`, `postgres`, `redis`) are operational and accessible locally via `http://localhost:3000`.

### 2.3 API Key Setup
You need API keys for **both** the source and destination projects:
1. Navigate to **Settings → API Keys** in each Langfuse instance.
2. Create or copy the **Public Key** (`pk-lf-...`) and **Secret Key** (`sk-lf-...`).
3. These are project-scoped — ensure you select the correct project.

---

## 3. Migration Script

The migration script is located at: [`migrate_langfuse.py`](migrate_langfuse.py)

### Key Design Decisions

1. **Dual-Client Architecture:** The script initializes two independent `Langfuse` clients — one for reading (source) and one for writing (destination). Environment variables are cleared before initialization to prevent configuration leakage.

2. **Tracing Disabled:** The script sets `LANGFUSE_TRACING_ENABLED=False` so its own execution is not instrumented in either instance.

3. **Cursor-Based Pagination:** All list operations use cursor-based pagination via `meta.next_cursor`. This is the only supported pagination model for v4 APIs and provides consistent performance at scale.

4. **Exhaustive Observation Fetching:** Unlike the previous version that fetched only 100 observations per trace in a single call, the new script loops through all pages of observations per trace.

5. **Rate Limiting:** Exponential backoff with up to 5 retries on `429`, `502`, `503`, and timeout errors. A 300ms sleep between trace pages prevents overwhelming the API.

6. **Idempotency:** Use `--from-timestamp` to only process traces created after a given datetime, enabling safe re-runs without duplicating data.

---

## 4. Execution Workflow

### Direction 1: Local Docker → Langfuse Web (Cloud)

```bash
python migrate_langfuse.py \
  --source-host "http://localhost:3000" \
  --source-public-key "pk-lf-local-..." \
  --source-secret-key "sk-lf-local-..." \
  --dest-host "https://cloud.langfuse.com" \
  --dest-public-key "pk-lf-cloud-..." \
  --dest-secret-key "sk-lf-cloud-..."
```

### Direction 2: Langfuse Web (Cloud) → Local Docker

```bash
python migrate_langfuse.py \
  --source-host "https://cloud.langfuse.com" \
  --source-public-key "pk-lf-cloud-..." \
  --source-secret-key "sk-lf-cloud-..." \
  --dest-host "http://localhost:3000" \
  --dest-public-key "pk-lf-local-..." \
  --dest-secret-key "sk-lf-local-..."
```

### Resuming After a Partial Migration

If a previous run failed midway, use `--from-timestamp` to resume from where it left off:

```bash
python migrate_langfuse.py \
  --source-host "http://localhost:3000" \
  --source-public-key "pk-lf-local-..." \
  --source-secret-key "sk-lf-local-..." \
  --dest-host "https://cloud.langfuse.com" \
  --dest-public-key "pk-lf-cloud-..." \
  --dest-secret-key "sk-lf-cloud-..." \
  --from-timestamp "2026-10-01T00:00:00Z"
```

### Selective Migration (Skip Optional Data)

```bash
# Only migrate traces/observations/scores — skip prompts and datasets
python migrate_langfuse.py \
  --source-host "http://localhost:3000" \
  --source-public-key "pk-lf-local-..." \
  --source-secret-key "sk-lf-local-..." \
  --dest-host "https://cloud.langfuse.com" \
  --dest-public-key "pk-lf-cloud-..." \
  --dest-secret-key "sk-lf-cloud-..." \
  --skip-prompts --skip-datasets
```

### Dry Run (Count Without Writing)

```bash
python migrate_langfuse.py \
  --source-host "http://localhost:3000" \
  --source-public-key "pk-lf-local-..." \
  --source-secret-key "sk-lf-local-..." \
  --dest-host "https://cloud.langfuse.com" \
  --dest-public-key "pk-lf-cloud-..." \
  --dest-secret-key "sk-lf-cloud-..." \
  --dry-run
```

---

## 5. CLI Reference

| Flag | Required | Default | Description |
|---|---|---|---|
| `--source-host` | ✅ | — | Source Langfuse host URL |
| `--source-public-key` | ✅ | — | Source project public key |
| `--source-secret-key` | ✅ | — | Source project secret key |
| `--dest-host` | ✅ | — | Destination Langfuse host URL |
| `--dest-public-key` | ✅ | — | Destination project public key |
| `--dest-secret-key` | ✅ | — | Destination project secret key |
| `--batch-limit` | | 50 | Traces per page (reduce to 20–25 for large payloads) |
| `--from-timestamp` | | None | ISO-8601 timestamp filter for idempotent re-runs |
| `--skip-prompts` | | False | Skip prompt migration |
| `--skip-datasets` | | False | Skip dataset migration |
| `--skip-score-configs` | | False | Skip score configuration migration |
| `--dry-run` | | False | Read-only mode: authenticate and count traces |

---

## 6. Troubleshooting & Best Practices

### Common Errors

| Error | Cause | Solution |
|---|---|---|
| `429 Too Many Requests` | API rate limit exceeded | The script retries automatically with exponential backoff. If persistent, reduce `--batch-limit` to 20. |
| `Authentication failed` | Invalid API keys or wrong host | Verify keys match the correct project. Ensure the host URL includes the protocol (`http://` or `https://`). |
| `Connection refused` | Local Docker not running | Run `docker compose ps` to verify all containers are up. |
| `An internal error occurred` (UI export) | ClickHouse OOM / worker timeout | Use this script instead of the UI export. For narrow exports, add `--from-timestamp`. |

### Large Volume Migrations (>100k Traces)

1. **Reduce batch size:** Use `--batch-limit 20` if traces contain large JSON payloads (>1MB input/output).
2. **Use time windows:** Split into multiple runs with `--from-timestamp` ranges.
3. **Monitor memory:** The script flushes the destination client after each page, but the source client accumulates response objects. For very large migrations, consider chunking by date.
4. **Blob storage export:** If you have a Langfuse Cloud Team or Enterprise tier, consider using the scheduled blob storage export (S3/GCS parquet files) for bulk data.

### Preserving Usage and Costs

* Ingesting traces to Langfuse Cloud counts toward the monthly trace quota of your destination cloud plan.
* Score ingestion also counts toward your usage.
* Use `--dry-run` first to estimate the volume before committing.

### Verifying Data Integrity

After migration completes, the script outputs a summary:
```
Traces transferred:       1,234
Observations transferred: 8,901
Scores transferred:         456
Errors encountered:           0
```

Cross-check these numbers against the source instance's dashboard. If errors occurred, review the detailed log output for specific trace/observation IDs that failed.

### Rollback Strategy

If a migration needs to be undone:
* **Langfuse Cloud:** Delete the destination project and create a new one.
* **Local Docker:** Reset the project via the Langfuse Admin API, or drop and recreate the database volumes.

> **Note:** There is no selective "undo" for individual traces ingested via the API.

---

## 7. SDK Migration Timeline

| Date | Event |
|---|---|
| March 2026 | Langfuse Python SDK v4 released (OpenTelemetry-native) |
| October 2026 | Legacy v2/v3 SDK methods deprecated but functional |
| **November 16, 2026** | **Langfuse Cloud stops accepting legacy ingestion** |
| Post Nov 2026 | Only SDK v4 / OTel ingestion works on Cloud |

Self-hosted instances control their own timeline, but upgrading to v4 is strongly recommended.