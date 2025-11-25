"""Generate PDF reports summarizing stored Bitcoin candles."""

from __future__ import annotations

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib.units import inch

from feature_engineering import load_candles_from_duckdb
from feature_engineering.config import load_ingestion_config
from .report_maker import ReportMaker


def _candles_to_dataframe(candles) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open_time": [c.open_time for c in candles],
            "close_time": [c.close_time for c in candles],
            "open_price": [c.open_price for c in candles],
            "close_price": [c.close_price for c in candles],
            "high_price": [c.high_price for c in candles],
            "low_price": [c.low_price for c in candles],
            "volume_btc": [c.volume_btc for c in candles],
            "volume_usd": [c.volume_usd for c in candles],
            "trade_count": [c.trade_count for c in candles],
        }
    )


def generate_ingestion_report():
    """Read candles from DuckDB and build a PDF snapshot report."""
    base_dir = Path(os.getenv("BTC_REPORT_PATH", "reports/data_ingestion.pdf"))
    timestamp = pd.Timestamp.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    report_dir = Path("reports") / "ingestion" / timestamp
    report_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = report_dir / "report.pdf"

    config_path = Path(os.getenv("INGEST_CONFIG", "config/bitcoin_ingest.json"))
    try:
        config = load_ingestion_config(config_path)
    except FileNotFoundError:
        config = load_ingestion_config()
    limit = config.limit

    candles = load_candles_from_duckdb(limit=limit, order_desc=True)
    if not candles:
        raise RuntimeError("No candles available; run ingestion before reporting")

    df = _candles_to_dataframe(candles)
    summary = pd.DataFrame(
        {
            "metric": [
                "Total candles in report",
                "Mean close price",
                "Min close price",
                "Max close price",
                "Mean BTC volume",
            ],
            "value": [
                len(df),
                round(df["close_price"].mean(), 2),
                round(df["close_price"].min(), 2),
                round(df["close_price"].max(), 2),
                round(df["volume_btc"].mean(), 4),
            ],
        }
    )

    timestamp = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    report = ReportMaker(report_dir, "report")
    report.add_section(f"Bitcoin Ingestion Report — {timestamp}")
    report.add_paragraph(
        "Snapshot of the most recent candles ingested from Binance "
        "and stored in the feature store."
    )
    report.add_table(summary)

    images_dir = report_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    metrics = {
        "Close Price": ("close_price", "USD"),
        "High Price": ("high_price", "USD"),
        "Low Price": ("low_price", "USD"),
        "Volume BTC": ("volume_btc", "BTC"),
        "Trade Count": ("trade_count", "count"),
    }

    report.add_section("Time Series Plots")
    for title, (column, unit) in metrics.items():
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.plot(df["open_time"], df[column])
        ax.set_title(title)
        ax.set_xlabel("Open Time")
        ax.set_ylabel(unit)
        fig.autofmt_xdate()
        image_path = images_dir / f"{column}.png"
        fig.savefig(image_path, bbox_inches="tight")
        plt.close(fig)

        report.add_image(str(image_path), width=7.5 * inch, height=4.5 * inch, add_page_break=True)

    report.save()
