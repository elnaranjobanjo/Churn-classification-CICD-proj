"""Generate PDF reports summarizing stored Bitcoin candles."""

from __future__ import annotations

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib.units import inch

from data_ingestion_service import load_candles_from_duckdb
from data_ingestion_service.config import load_ingestion_config
from .report_maker import ReportMaker

_METRIC_CHARTS = (
    ("Close Price", "close_price", "USD"),
    ("High Price", "high_price", "USD"),
    ("Low Price", "low_price", "USD"),
    ("Volume BTC", "volume_btc", "BTC"),
    ("Trade Count", "trade_count", "count"),
    ("Price Increase Label", "price_increase_label", "label"),
)


def _report_base_dir() -> Path:
    legacy = os.getenv("BTC_REPORT_PATH")
    base = os.getenv("BTC_REPORT_DIR")
    if base:
        return Path(base)
    if legacy:
        legacy_path = Path(legacy)
        return legacy_path.parent if legacy_path.suffix else legacy_path
    return Path("reports/ingestion")


def _build_dataframe(candles) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "open_time": c.open_time,
                "close_time": c.close_time,
                "open_price": c.open_price,
                "close_price": c.close_price,
                "high_price": c.high_price,
                "low_price": c.low_price,
                "volume_btc": c.volume_btc,
                "volume_usd": c.volume_usd,
                "trade_count": c.trade_count,
                "price_increase_label": getattr(c, "price_increase_label", None),
            }
            for c in candles
        ]
    )


def _summary_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = [
        ("Total candles in report", len(df)),
        ("Mean close price", round(df["close_price"].mean(), 2)),
        ("Min close price", round(df["close_price"].min(), 2)),
        ("Max close price", round(df["close_price"].max(), 2)),
        ("Mean BTC volume", round(df["volume_btc"].mean(), 4)),
    ]
    return pd.DataFrame(rows, columns=["metric", "value"])


def _plot_metric(df: pd.DataFrame, column: str, unit: str, output: Path) -> Path:
    fig, ax = plt.subplots(figsize=(8, 3))
    if unit == "label":
        ax.scatter(df["open_time"], df[column], s=10)
        ax.set_yticks([0, 1])
    else:
        ax.plot(df["open_time"], df[column])
    ax.set_title(column.replace("_", " ").title())
    ax.set_xlabel("Open Time")
    ax.set_ylabel(unit)
    fig.autofmt_xdate()
    fig.savefig(output, bbox_inches="tight")
    plt.close(fig)
    return output


def generate_ingestion_report(limit: int | None = None) -> Path:
    """Fetch candles, render plots, and emit a PDF report."""
    config_path = Path(os.getenv("INGEST_CONFIG", "config/bitcoin_ingest.json"))
    try:
        config = load_ingestion_config(config_path)
    except FileNotFoundError:
        config = load_ingestion_config()
    limit = limit or config.limit

    candles = load_candles_from_duckdb(limit=limit, order_desc=True)
    if not candles:
        raise RuntimeError("No candles available; run ingestion before reporting")

    df = _build_dataframe(candles)
    summary = _summary_table(df)

    timestamp = pd.Timestamp.utcnow()
    slug = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    report_dir = _report_base_dir() / slug
    images_dir = report_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    report = ReportMaker(report_dir, "report")
    report.add_section(f"Bitcoin Ingestion Report — {timestamp:%Y-%m-%d %H:%M UTC}")
    report.add_paragraph(
        "Snapshot of the most recent candles ingested from Binance "
        "and stored in the feature store."
    )
    report.add_table(summary)

    report.add_section("Time Series Plots")
    for _, column, unit in _METRIC_CHARTS:
        image_path = images_dir / f"{column}.png"
        _plot_metric(df, column, unit, image_path)
        report.add_image(
            str(image_path), width=7.5 * inch, height=4.5 * inch, add_page_break=True
        )

    report.save()
    return report_dir / "report.pdf"
