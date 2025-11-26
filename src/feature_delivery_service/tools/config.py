# """Configuration utilities for Bitcoin ingestion."""

# from __future__ import annotations

# import json
# import os
# from dataclasses import dataclass
# from pathlib import Path
# from typing import Any, Mapping, Optional


# DEFAULT_BINANCE_BASE_URL = os.getenv(
#     "BINANCE_BASE_URL",
#     "https://api.binance.com/api/v3/klines",
# )
# DEFAULT_BITCOIN_SYMBOL = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
# DEFAULT_FEATURE_DB_PATH = Path(
#     os.getenv("FEATURE_DB_PATH", "feature_store/bitcoin.duckdb")
# )


# @dataclass(frozen=True)
# class IngestionConfig:
#     """Configuration describing how BTC candles should be ingested."""

#     interval: str = "1h"
#     limit: int = 500
#     start_time: Optional[int] = None
#     end_time: Optional[int] = None
#     base_url: str = DEFAULT_BINANCE_BASE_URL
#     symbol: str = DEFAULT_BITCOIN_SYMBOL
#     timeout: int = 15
#     db_path: Path = DEFAULT_FEATURE_DB_PATH
#     table: str = "btc_candles"

#     @classmethod
#     def from_dict(cls, payload: Mapping[str, Any]) -> "IngestionConfig":
#         def _maybe_int(value: Any) -> Optional[int]:
#             if value is None:
#                 return None
#             return int(value)

#         return cls(
#             interval=str(payload.get("interval", "1h")),
#             limit=int(payload.get("limit", 500)),
#             start_time=_maybe_int(payload.get("start_time")),
#             end_time=_maybe_int(payload.get("end_time")),
#             base_url=str(payload.get("base_url", DEFAULT_BINANCE_BASE_URL)),
#             symbol=str(payload.get("symbol", DEFAULT_BITCOIN_SYMBOL)),
#             timeout=int(payload.get("timeout", 15)),
#             db_path=Path(payload.get("db_path", DEFAULT_FEATURE_DB_PATH)),
#             table=str(payload.get("table", "btc_candles")),
#         )

#     @classmethod
#     def from_json(cls, path: Path) -> "IngestionConfig":
#         data = json.loads(path.read_text())
#         if not isinstance(data, Mapping):
#             raise ValueError(f"Ingestion config {path} must be a JSON object")
#         return cls.from_dict(data)

#     @classmethod
#     def from_env(cls) -> "IngestionConfig":
#         return cls(
#             base_url=os.getenv("BINANCE_BASE_URL", DEFAULT_BINANCE_BASE_URL),
#             symbol=os.getenv("BINANCE_SYMBOL", DEFAULT_BITCOIN_SYMBOL),
#             db_path=Path(os.getenv("FEATURE_DB_PATH", DEFAULT_FEATURE_DB_PATH)),
#         )
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
