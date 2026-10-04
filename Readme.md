# Author: C A B M
# Date: 2026-10-04

# Langfuse Bidirectional Migration Tool

A production-grade Python CLI tool for bidirectionally migrating traces, observations, scores, prompts, datasets, and score configurations between any two Langfuse instances (**Langfuse Cloud** $\leftrightarrow$ **Self-Hosted Docker v3/v4**).

---

## Features

- **Full Data Coverage:** Migrates Traces, Observations (generations, spans, events, chains, tools), Scores, Prompts (definitions + versions), Datasets (definitions + items), and Score Configurations.
- **SDK v4 Ingestion Pipeline:** Uses direct batch ingestion (`dest.api.ingestion.batch`) with typed event bodies (`IngestionEvent_TraceCreate`, `IngestionEvent_ObservationCreate`, `IngestionEvent_ScoreCreate`), preserving historical IDs, tree relationships, and full `input`/`output` payloads.
- **Cross-Version Compatibility:** Handles differences between Langfuse Cloud (v4 REST schema) and self-hosted instances (v3/v4), automatically normalizing observation subtypes (`CHAIN`, `AGENT`, `TOOL` $\rightarrow$ `SPAN`) while preserving original types in metadata.
- **30-Day Retention Support (`--shift-to-now`):** Langfuse Cloud's Hobby (Free) plan limits data retention to 30 days. When migrating traces older than 30 days, `--shift-to-now` shifts timestamps forward to the current date so they appear immediately in the Cloud Web UI while preserving relative durations, span latencies, and storing `_original_timestamp` in metadata.
- **Rate-Limiting & Resilience:** Built-in exponential backoff retry logic for `429` (Too Many Requests), `502`, `503`, and network timeouts.
- **Idempotency & Resuming:** Filter by `--from-timestamp` to incrementally migrate or resume interrupted runs.
- **Dry-Run Mode:** Inspect and count all source items before performing any write operations.

---

## Installation & Prerequisites

1. **Python 3.10+** is required.
2. Clone or navigate to the repository directory:
   ```bash
   cd c:\WorkingFolder\migration-langfuse
   ```
3. Activate your virtual environment and install dependencies:
   ```bash
   # Windows PowerShell
   .venv\Scripts\Activate.ps1

   # Install dependencies
   pip install -r requirements.txt
   ```
   *Core dependency: `langfuse>=4.8.1`*

---

## Usage

### 1. Dry Run (Inspect Counts Without Writing)

Always run a dry run first to verify connection and examine data counts:

```bash
python migrate_langfuse.py \
  --source-host "http://localhost:10500" \
  --source-public-key "pk-lf-local-..." \
  --source-secret-key "sk-lf-local-..." \
  --dest-host "https://us.cloud.langfuse.com" \
  --dest-public-key "pk-lf-cloud-..." \
  --dest-secret-key "sk-lf-cloud-..." \
  --dry-run
```

---

### 2. Local Docker $\rightarrow$ Langfuse Cloud (Recommended)

When migrating older traces (>30 days) to a Langfuse Cloud Hobby plan, include the `--shift-to-now` flag so traces are indexed within the Cloud retention window:

```bash
python migrate_langfuse.py \
  --source-host "http://localhost:10500" \
  --source-public-key "pk-lf-local-storybook" \
  --source-secret-key "sk-lf-local-storybook" \
  --dest-host "https://us.cloud.langfuse.com" \
  --dest-public-key "pk-lf-cb9e01ed-2306-4068-a0a2-182d596771dd" \
  --dest-secret-key "sk-lf-b2b250be-477e-4f75-a30f-29bf34f8b226" \
  --shift-to-now
```

---

### 3. Langfuse Cloud $\rightarrow$ Local Docker

To export traces from Cloud to a local self-hosted instance:

```bash
python migrate_langfuse.py \
  --source-host "https://us.cloud.langfuse.com" \
  --source-public-key "pk-lf-cloud-..." \
  --source-secret-key "sk-lf-cloud-..." \
  --dest-host "http://localhost:3000" \
  --dest-public-key "pk-lf-local-..." \
  --dest-secret-key "sk-lf-local-..."
```

---

### 4. Resume from a Specific Timestamp

If a migration was interrupted or you want to migrate only recent traces:

```bash
python migrate_langfuse.py \
  --source-host "http://localhost:10500" \
  --source-public-key "pk-lf-local-..." \
  --source-secret-key "sk-lf-local-..." \
  --dest-host "https://us.cloud.langfuse.com" \
  --dest-public-key "pk-lf-cloud-..." \
  --dest-secret-key "sk-lf-cloud-..." \
  --from-timestamp "2026-08-01T00:00:00Z"
```

---

## CLI Options Reference

| Option | Type | Required | Description |
|---|---|---|---|
| `--source-host` | String | Yes | Source Langfuse host URL (e.g. `http://localhost:10500` or `https://us.cloud.langfuse.com`) |
| `--source-public-key` | String | Yes | Public API key for the source project (`pk-lf-...`) |
| `--source-secret-key` | String | Yes | Secret API key for the source project (`sk-lf-...`) |
| `--dest-host` | String | Yes | Destination Langfuse host URL |
| `--dest-public-key` | String | Yes | Public API key for the destination project (`pk-lf-...`) |
| `--dest-secret-key` | String | Yes | Secret API key for the destination project (`sk-lf-...`) |
| `--shift-to-now` | Flag | No | Shifts historical timestamps forward to current date to stay within Cloud 30-day retention |
| `--dry-run` | Flag | No | Authenticates and counts items on source without writing to destination |
| `--batch-limit` | Int | No | Traces fetched per page (default: `50`) |
| `--from-timestamp` | ISO-8601 | No | Filter traces created after this timestamp (e.g. `2026-08-01T00:00:00Z`) |
| `--skip-prompts` | Flag | No | Skip prompt migration |
| `--skip-datasets` | Flag | No | Skip dataset and dataset item migration |
| `--skip-score-configs` | Flag | No | Skip score configuration definitions migration |

---

## Web UI Navigation Tips

When viewing migrated traces in **Langfuse Cloud Web Console**:

1. **Viewing Only Top-Level Traces:**
   - In Langfuse v4, the Tracing table can display all individual observations (including child steps).
   - Under **Filters $\rightarrow$ Is Root Observation**, select **`True`** (uncheck `False`) to view only parent traces.
2. **Date Range Filter:**
   - The default filter in the top right is **"Past 24 Hours"**.
   - If migrating without `--shift-to-now`, set the date picker to **"All Time"** or select the custom historical range (e.g. August 2026) to see the entries.
3. **Ingestion Processing Delay:**
   - Langfuse Cloud ClickHouse ingestion pipeline is asynchronous; newly ingested batches typically take **1 to 3 minutes** to index into the Web UI.

---

## License

This project is licensed under the MIT License - see the [`LICENSE.txt`](LICENSE.txt) file for details.
