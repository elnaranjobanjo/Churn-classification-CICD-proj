from .ingestion import run_bitcoin_ingestion
from .reader import load_candles_from_duckdb, load_labeled_candles_from_duckdb
from .etl import materialize_labeled_candles


def ingest_and_label(
    *,
    source_table: str = "btc_candles",
    destination_table: str = "btc_candles_labeled",
    label_limit: int | None = None,
):
    """Run ingestion and immediately materialize labeled candles."""
    new_rows, total_rows = run_bitcoin_ingestion()
    labeled_rows = materialize_labeled_candles(
        source_table=source_table,
        destination_table=destination_table,
        limit=label_limit,
    )
    return {
        "ingested_rows": new_rows,
        "total_rows": total_rows,
        "labeled_rows": labeled_rows,
    }


__all__ = [
    "run_bitcoin_ingestion",
    "load_candles_from_duckdb",
    "materialize_labeled_candles",
    "load_labeled_candles_from_duckdb",
    "ingest_and_label",
]
