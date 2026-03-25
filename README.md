# 🏥 Hospital Analytics Pipeline Project

An end-to-end automated analytics pipeline for a hospital organisation, covering data ingestion, ETL, warehousing, scheduling, monitoring, and report generation.

----

## Project Structure

```
hospital_analytics_pipeline/
├── main.py                          # Entry point (run / schedule / report)
├── config/
│   └── settings.py                  # Centralised config (env-driven)
├── data_sources/
│   ├── ehr_source.py                # Electronic Health Records
│   ├── appointment_source.py        # Scheduling / appointments
│   ├── billing_source.py            # Revenue cycle / billing
│   └── bed_occupancy_source.py      # IoT bed sensors
├── etl/
│   ├── extractor.py                 # Extract from all sources → raw zone
│   ├── transformer.py               # Clean, validate, enrich
│   ├── loader.py                    # Load → Parquet + SQLite DW
│   └── pipeline.py                  # ETL orchestrator
├── storage/
│   └── warehouse.py                 # DW query helpers & KPI aggregations
├── scheduler/
│   └── job_scheduler.py             # APScheduler cron jobs
├── logging_monitoring/
│   ├── logger.py                    # Rotating file + console logger
│   ├── monitor.py                   # Run metrics & KPI drift detection
│   └── alerter.py                   # Console / Email / Slack alerts
├── report_generation/
│   ├── kpi_calculator.py            # Computes & colours KPIs
│   ├── html_report.py               # Self-contained HTML dashboard
│   ├── csv_report.py                # Per-KPI CSV exports
│   └── report_builder.py            # Report orchestrator
├── utils/
│   ├── helpers.py                   # Shared utilities
│   └── cleanup.py                   # Aged-file purge
├── tests/
│   └── test_etl.py                  # Pytest unit tests
├── data/
│   ├── raw/                         # Timestamped raw Parquet extracts
│   ├── processed/                   # Latest cleaned Parquet snapshots
│   ├── reports/                     # HTML & CSV reports
│   └── logs/                        # pipeline.log + metrics.json
├── requirements.txt
└── .env.example
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your database credentials and alert settings
```

### 3. Run the full pipeline once
```bash
python main.py run
```

### 4. Start the scheduler (runs on cron)
```bash
python main.py schedule
```

### 5. Generate reports only (skips ETL)
```bash
python main.py report
```

### 6. Run tests
```bash
pytest tests/ -v --cov=.
```

---

## Architecture & Design Decisions

| Layer | Choice | Rationale |
|---|---|---|
| Data Sources | Modular source classes | Swap any source independently |
| ETL | Extract → Transform → Load separation | Single Responsibility; testable |
| Raw Zone | Timestamped Parquet | Immutable audit trail |
| Processed Zone | Latest Parquet snapshot | Fast reads for reporting |
| Data Warehouse | SQLite (local) | Zero-dependency; swap for Postgres/Snowflake via `_connect()` |
| Scheduling | APScheduler cron | No external daemon needed |
| Logging | Python `logging` + RotatingFileHandler | Persistent log with size cap |
| Monitoring | JSON metrics log + KPI drift check | Lightweight; dashboardable |
| Alerts | Console → Email → Slack | Layered; enable what you need |
| Reports | HTML + CSV | Browser-viewable + machine-readable |

---

## Swapping Components

### Use PostgreSQL instead of SQLite
In `storage/warehouse.py`, replace `_connect()`:
```python
import psycopg2
def _connect(self):
    return psycopg2.connect(self.config.database.url)
```

### Add a new data source
1. Create `data_sources/my_source.py` with a `fetch()` method returning a DataFrame.
2. Register in `data_sources/__init__.py`.
3. Add an entry in `etl/extractor.py` `self.sources` dict.
4. Add a transform method in `etl/transformer.py`.

### Add a new report format (e.g. PDF)
1. Create `report_generation/pdf_report.py` with a `generate(kpi_payload, path)` method.
2. Register in `report_generation/report_builder.py` under the format check block.
3. Add `"pdf"` to `ReportConfig.formats` in `config/settings.py`.

---

## KPI Targets (configurable in `config/settings.py`)

| KPI | Default Target |
|---|---|
| Average wait time | ≤ 30 minutes |
| Bed occupancy rate | ≤ 85% |
| Readmission rate | ≤ 10% |
| Patient satisfaction | ≥ 4.0 / 5.0 |


.