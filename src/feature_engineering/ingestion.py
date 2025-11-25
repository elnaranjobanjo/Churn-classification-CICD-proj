"""Utilities to ingest Bitcoin-only data from Binance's public REST API."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
from urllib import error, parse, request

import duckdb

logger = logging.getLogger(__name__)

_BINANCE_PUBLIC_REST = os.getenv(
    "BINANCE_BASE_URL", "https://api.binance.com/api/v3/klines"
)
_BITCOIN_SYMBOL = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
_FEATURE_DB_PATH = Path(os.getenv("FEATURE_DB_PATH", "feature_store/bitcoin.duckdb"))
_USER_AGENT = "MLFlowProject/bitcoin-ingest"


def _validated_identifier(identifier: str) -> str:
    cleaned = identifier.replace("_", "")
    if not cleaned or not cleaned.isalnum():
        raise ValueError(f"Invalid identifier: {identifier}")
    return identifier


@dataclass(frozen=True, slots=True)
class BitcoinCandle:
    """Normalized container for a single BTC/USDT candlestick."""

    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume_btc: float
    volume_usd: float
    trade_count: int
    taker_buy_volume_btc: float
    taker_buy_volume_usd: float

    @staticmethod
    def from_binance(payload: Sequence[str | float | int]) -> "BitcoinCandle":
        """Convert a raw Binance kline payload into ``BitcoinCandle``."""
        (
            open_time,
            open_price,
            high_price,
            low_price,
            close_price,
            volume_btc,
            close_time,
            volume_usd,
            trade_count,
            taker_buy_volume_btc,
            taker_buy_volume_usd,
            _ignore,
        ) = payload
        return BitcoinCandle(
            open_time=datetime.fromtimestamp(int(open_time) / 1000),
            close_time=datetime.fromtimestamp(int(close_time) / 1000),
            open_price=float(open_price),
            high_price=float(high_price),
            low_price=float(low_price),
            close_price=float(close_price),
            volume_btc=float(volume_btc),
            volume_usd=float(volume_usd),
            trade_count=int(trade_count),
            taker_buy_volume_btc=float(taker_buy_volume_btc),
            taker_buy_volume_usd=float(taker_buy_volume_usd),
        )


class BinanceClient:
    """Thin Binance REST client dedicated to BTC candles."""

    def __init__(
        self,
        *,
        base_url: str = _BINANCE_PUBLIC_REST,
        symbol: str = _BITCOIN_SYMBOL,
        timeout: int = 15,
    ) -> None:
        self.base_url = base_url
        self.symbol = symbol
        self.timeout = timeout

    def fetch_candles(
        self,
        *,
        interval: str = "1h",
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[BitcoinCandle]:
        """Pull BTC-only candles from Binance using the BTC/USDT market."""
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000 (inclusive)")

        params: dict[str, str | int] = {
            "symbol": self.symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        url = f"{self.base_url}?{parse.urlencode(params)}"
        logger.info(
            "Requesting BTC kline data from Binance interval=%s limit=%s",
            interval,
            limit,
        )

        req = request.Request(url, headers={"User-Agent": _USER_AGENT})
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                payload = resp.read()
        except error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(
                f"Binance API error (status {exc.code}): {message}"
            ) from exc
        except error.URLError as exc:
            raise RuntimeError("Unable to reach Binance API") from exc

        data = json.loads(payload)
        candles = [BitcoinCandle.from_binance(entry) for entry in data]
        logger.debug("Fetched %s BTC candles", len(candles))
        return candles


class DuckDBStorage:
    """DuckDB-backed storage for BTC candle features."""

    def __init__(
        self,
        db_path: str | Path = _FEATURE_DB_PATH,
        table: str = "btc_candles",
    ) -> None:
        self.db_path = Path(db_path)
        self.table = _validated_identifier(table)
        if self.db_path.parent and not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def upsert(self, candles: Iterable[BitcoinCandle]) -> int:
        """Insert or replace candle rows into DuckDB."""
        rows = [
            (
                candle.open_time,
                candle.close_time,
                candle.open_price,
                candle.high_price,
                candle.low_price,
                candle.close_price,
                candle.volume_btc,
                candle.volume_usd,
                candle.trade_count,
                candle.taker_buy_volume_btc,
                candle.taker_buy_volume_usd,
            )
            for candle in candles
        ]
        if not rows:
            logger.info("No candles supplied for DuckDB storage")
            return 0

        conn = duckdb.connect(str(self.db_path))
        try:
            self._ensure_schema(conn)
            conn.executemany(
                f"""
                INSERT OR REPLACE INTO {self.table} VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                rows,
            )
        finally:
            conn.close()

        inserted = len(rows)
        logger.info("Stored %s BTC candles into %s", inserted, self.db_path)
        return inserted

    def _ensure_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                open_time TIMESTAMP PRIMARY KEY,
                close_time TIMESTAMP,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume_btc DOUBLE,
                volume_usd DOUBLE,
                trade_count BIGINT,
                taker_buy_volume_btc DOUBLE,
                taker_buy_volume_usd DOUBLE
            )
            """
        )


def fetch_bitcoin_candles(
    *,
    interval: str = "1h",
    limit: int = 500,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    symbol: str = _BITCOIN_SYMBOL,
) -> List[BitcoinCandle]:
    """Functional wrapper over :class:`BinanceClient` for legacy imports."""
    client = BinanceClient(symbol=symbol)
    return client.fetch_candles(
        interval=interval,
        limit=limit,
        start_time=start_time,
        end_time=end_time,
    )


# def candles_to_numpy(candles: Iterable[BitcoinCandle]) -> np.ndarray:
#     """Convert iterable of candles into ``(n, 10)`` numpy array for modeling."""
#     rows = [
#         [
#             candle.open_price,
#             candle.high_price,
#             candle.low_price,
#             candle.close_price,
#             candle.volume_btc,
#             candle.volume_usd,
#             candle.trade_count,
#             candle.taker_buy_volume_btc,
#             candle.taker_buy_volume_usd,
#             candle.close_time.timestamp(),
#         ]
#         for candle in candles
#     ]
#     if not rows:
#         return np.empty((0, 10), dtype=np.float64)
#     return np.asarray(rows, dtype=np.float64)


def store_candles_in_duckdb(
    candles: Iterable[BitcoinCandle],
    *,
    db_path: str | Path = _FEATURE_DB_PATH,
    table: str = "btc_candles",
) -> int:
    """Persist Bitcoin candles into a DuckDB table for downstream feature serving."""
    storage = DuckDBStorage(db_path=db_path, table=table)
    return storage.upsert(candles)


def ingest_bitcoin_candles(
    *,
    client: Optional[BinanceClient] = None,
    storage: Optional[DuckDBStorage] = None,
    interval: str = "1h",
    limit: int = 500,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
) -> int:
    """Fetch BTC candles via ``client`` and persist them via ``storage``."""
    active_client = client or BinanceClient()
    if storage is None:
        raise ValueError("storage must be provided when ingesting candles")
    candles = active_client.fetch_candles(
        interval=interval,
        limit=limit,
        start_time=start_time,
        end_time=end_time,
    )
    return storage.upsert(candles)
