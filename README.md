# Alpha Vantage Stock Data Pipeline

A daily, idempotent data pipeline orchestrated with Apache Airflow. It extracts daily stock market data from the Alpha Vantage API, lands it in raw form in S3, transforms it, and loads it into Snowflake — with retries, data quality checks, and a MERGE-based upsert so reruns never create duplicates.

## Architecture

```
Alpha Vantage API
      │
      ▼
  HttpSensor (check API availability)
      │
      ▼
  HttpOperator (extract daily OHLCV data)
      │
      ▼
  Prepare (flatten JSON, build S3 keys)
      │
      ▼
  S3 — raw landing zone (one JSON file per trading day)
      │
      ▼
  S3KeySensor (confirm files landed before proceeding)
      │
      ▼
  Read back from S3 (source of truth, not the in-memory API response)
      │
      ▼
  Transform (pandas: flatten, compute daily % change)
      │
      ▼
  Data Quality Check (row count, null checks)
      │
      ▼
  Snowflake staging table (write_pandas)
      │
      ▼
  MERGE into production table (upsert on date)
```

## What it does

Every day, the pipeline pulls the previous day's OHLCV (open/high/low/close/volume) data for a stock ticker from Alpha Vantage, stores the raw response in S3, transforms it into a clean tabular format, and loads it into a Snowflake table — automatically, without any manual intervention, and safely re-runnable if it fails partway through.

## Design decisions

**Raw landing zone in S3 before transformation.**
The pipeline writes the raw API response to S3 first, waits for confirmation the files exist (`S3KeySensor`), and then *reads back from S3* rather than reusing the in-memory API response for transformation. This means the raw data is the actual source of truth — if a downstream step fails and needs to be retried, it re-reads from what was actually persisted rather than silently trusting an API response that's no longer verifiable.

**Staging table + MERGE instead of plain INSERT.**
Data is first written to a Snowflake staging table (`write_pandas`, with `overwrite=True`), then merged into the production table on `date`. This makes the pipeline idempotent — re-running the DAG for a day that already loaded successfully updates existing rows instead of creating duplicates. This matters because Airflow DAGs get retried, backfilled, and occasionally manually re-triggered; a pipeline that can't handle re-runs safely is a liability in production.

**Dynamic Task Mapping for per-file operations.**
Saving, sensing, and reading S3 objects use `.expand_kwargs()` rather than a fixed number of tasks, so the DAG scales automatically if the API response includes more (or fewer) days of data without any code changes.

**Data quality check before load.**
A dedicated task validates the transformed DataFrame (non-empty, no nulls in `date`/`close`) before it ever reaches Snowflake. If validation fails, the DAG fails loudly rather than silently loading bad data.

**Retries and timeouts.**
Tasks are configured with retries and a retry delay to handle transient failures (API hiccups, network blips) without manual re-triggering.

**Credentials resolved at task-execution time, not DAG-parse time.**
The Alpha Vantage API key is pulled via Jinja templating (`{{ var.value.alpha_vantage_api_key }}`) rather than `Variable.get()` at the module level, so it isn't re-fetched from the metadata database every time the scheduler parses the DAG file.

## Tech stack

- **Orchestration:** Apache Airflow (TaskFlow API, Dynamic Task Mapping)
- **Storage:** Amazon S3 (raw landing zone)
- **Data warehouse:** Snowflake
- **Transformation:** pandas
- **Source:** [Alpha Vantage API](https://www.alphavantage.co/) (`TIME_SERIES_DAILY`)

## Setup

### Airflow Connections required
| Connection ID | Type | Purpose |
|---|---|---|
| `alpha_vantage_api` | HTTP | Base connection to `https://www.alphavantage.co` |
| `aws_con` | Amazon Web Services | S3 read/write access |
| `snowflake_con` | Snowflake | Staging + production table access |

### Airflow Variables required
| Variable | Description |
|---|---|
| `alpha_vantage_api_key` | Free API key from [alphavantage.co](https://www.alphavantage.co/support/#api-key) |

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run
Place `alpha_vantage.py` in your Airflow `dags/` folder. The DAG is scheduled `@daily` with `catchup=False`.

## Example output table

| date | open | high | low | close | volume | daily_pct_change |
|---|---|---|---|---|---|---|
| 2026-07-24 | 213.40 | 215.10 | 212.80 | 214.75 | 48213000 | 0.63 |

## Possible extensions
- Add `dbt` models on top of the Snowflake production table for downstream analytics
- Add a `BranchPythonOperator` to skip the load and alert instead of failing outright on bad data
- Add Slack/email alerting via `on_failure_callback`
- Extend to multiple tickers using Dynamic Task Mapping over a ticker list
