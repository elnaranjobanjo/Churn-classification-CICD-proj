"""Domain and API client utilities for Bitcoin ingestion."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence
from urllib import error, parse, request

from .config import DEFAULT_BINANCE_BASE_URL, DEFAULT_BITCOIN_SYMBOL
from .schemas import build_BitcoinCandle

BitcoinCandle = build_BitcoinCandle()

logger = logging.getLogger(__name__)

_USER_AGENT = "MLFlowProject/bitcoin-ingest"


class BinanceClient:
    """Thin Binance REST client dedicated to BTC candles."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BINANCE_BASE_URL,
        symbol: str = DEFAULT_BITCOIN_SYMBOL,
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
