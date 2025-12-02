# Bitcoin ML Lifecycle Sandbox

This project is a lightweight, end-to-end sandbox for the ML lifecycle focused on Bitcoin price data. The emphasis is on stitching together the ingestion → labeling → reporting → modeling/ops -> monitoring -> retraining flow rather than beating production fintech models.

## Current Capabilities

1. **Bitcoin ingestion**
   - `task ingest` fetches BTC/USDT minute candles from Binance using `config/bitcoin_ingest.json` (interval, limit, table).
   - Candles are stored in DuckDB at `feature_store/bitcoin.duckdb`.
   - Each candle is stored with its OHLCV statistics, and the command logs how many **new** rows were inserted plus the total row count. This metadata can be written to `feature_store/ingestion_stats.json` for quick reference.

2. **Feature access helpers**
   - `data_ingestion_service.load_candles_from_duckdb()` returns typed `BitcoinCandle` objects for analysis or modeling.
   - `data_ingestion_service.reader.count_candles()` returns the current row count without loading the entire table.

3. **PDF reporting**
   - After each ingest we generate `reports/ingestion/<timestamp>/report.pdf` plus accompanying images so we can visually inspect the latest data. The report covers summary stats and OHLCV plots.

## Roadmap

- Build baseline models in `src/ml/` using the stored candles plus engineered labels, and re-enable the MLflow `track` / `register` commands.
- Integrate Feast once model inputs stabilize to keep offline and online features in sync.
- Add monitoring/retraining hooks leveraging the ingestion/reporting pipeline.

## Running the Pipeline

```bash
# sync deps and run ingestion + report
task ingest
```

## Serving the Model via API

The `src/api` module provides a FastAPI scaffold for serving registered MLflow models.

```bash
# start the API (MODEL_URI defaults to models:/bitcoin-model@production)
MODEL_URI=models:/bitcoin-model@production uv run uvicorn api.app:app --host 0.0.0.0 --port 8000
```

Endpoints:
- `GET /health` — readiness probe.
- `POST /predict` — accepts `{"features": [ ... ]}` and proxies the payload to the loaded model.

Relevant environment variables (see `.env`):
- `INGEST_CONFIG`: path to the Binance ingestion config
- `FEATURE_DB_PATH`: DuckDB location
- `BTC_REPORT_DIR`: base directory for PDF reports
- `MLFLOW_TRACKING_URI` / `MLFLOW_REGISTRY_URI`: for upcoming training workflows

## Code Layout

- `src/data_ingestion_service/`: Binance client, config, DuckDB storage, ingestion entry point, load/count helpers
- `src/reporting/`: PDF report generation utilities
- `src/ml/`: (up next) model training routines
- `src/MLOps/`: MLflow tracking/registry orchestration (to be re-enabled after modeling)

The goal is to demonstrate the entire ML lifecycle in a short timeframe (~1 week) while keeping the code straightforward and extensible.
