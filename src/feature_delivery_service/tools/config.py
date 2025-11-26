from dataclasses import dataclass
import os
from typing import Any, Optional
from pathlib import Path

config_path = os.getenv("INGEST_CONFIG", "config/bitcoin_ingest.json")

DEFAULT_BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://api.binance.com/api/v3/klines",
)
DEFAULT_BITCOIN_SYMBOL = os.getenv("BINANCE_SYMBOL", "BTCUSDT")


@dataclass(frozen=True)
class IngestionConfig:
    """Configuration describing how BTC candles should be ingested."""

    interval: str = "1h"
    limit: int = 500
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    table: str = "btc_candles"


def load_ingestion_config(path: Path | None = None) -> IngestionConfig:
    """Return ingestion config from JSON path, falling back to env defaults."""
    import json

    config_file = Path(path or config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Ingestion config {config_file} not found")

    payload = json.loads(config_file.read_text())

    def _maybe_int(value: Any) -> Optional[int]:
        return None if value is None else int(value)

    return IngestionConfig(
        interval=str(payload.get("interval", "1h")),
        limit=int(payload.get("limit", 500)),
        start_time=_maybe_int(payload.get("start_time")),
        end_time=_maybe_int(payload.get("end_time")),
        table=str(payload.get("table", "btc_candles")),
    )
