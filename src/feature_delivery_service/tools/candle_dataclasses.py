# from dataclasses import dataclass
# from datetime import datetime
# from typing import Sequence


# @dataclass(frozen=True, slots=True)
# class BitcoinCandle:
#     """Normalized container for a single BTC/USDT candlestick."""

#     open_time: datetime
#     close_time: datetime
#     open_price: float
#     high_price: float
#     low_price: float
#     close_price: float
#     volume_btc: float
#     volume_usd: float
#     trade_count: int
#     taker_buy_volume_btc: float
#     taker_buy_volume_usd: float

#     @staticmethod
#     def from_binance(payload: Sequence[str | float | int]) -> "BitcoinCandle":
#         """Convert a raw Binance kline payload into ``BitcoinCandle``."""
#         (
#             open_time,
#             open_price,
#             high_price,
#             low_price,
#             close_price,
#             volume_btc,
#             close_time,
#             volume_usd,
#             trade_count,
#             taker_buy_volume_btc,
#             taker_buy_volume_usd,
#         ) = payload
#         return BitcoinCandle(
#             open_time=datetime.fromtimestamp(int(open_time) / 1000),
#             close_time=datetime.fromtimestamp(int(close_time) / 1000),
#             open_price=float(open_price),
#             high_price=float(high_price),
#             low_price=float(low_price),
#             close_price=float(close_price),
#             volume_btc=float(volume_btc),
#             volume_usd=float(volume_usd),
#             trade_count=int(trade_count),
#             taker_buy_volume_btc=float(taker_buy_volume_btc),
#             taker_buy_volume_usd=float(taker_buy_volume_usd),
#         )

from dataclasses import make_dataclass
from typing import Sequence
from datetime import datetime


def build_augmented_dataclass(
    base_fields: Sequence[tuple[str, type]],
    engineered_fields: Sequence[tuple[str, type]],
    name: str = "AugmentedCandle",
):
    """Create a dataclass with base + engineered fields."""
    return make_dataclass(
        name,
        [(field, typ) for field, typ in [*base_fields, *engineered_fields]],
        bases=(),
        frozen=True,
        slots=True,
    )


# base schema from your CANDLE_COLUMN_ORDER
BASE_FIELDS = [
    ("open_time", datetime),
    ("close_time", datetime),
    ("open_price", float),
    ("high_price", float),
    ("low_price", float),
    ("close_price", float),
    ("volume_btc", float),
    ("volume_usd", float),
    ("trade_count", int),
    ("taker_buy_volume_btc", float),
    ("taker_buy_volume_usd", float),
]

# engineered schema can come from config (e.g., multiple lags)
ENGINEERED_FIELDS = [
    ("label_t_plus_1", int),
    ("lag_1_close_gt_curr", int),
    ("lag_2_close_gt_curr", int),
    # add more derived columns here without touching class code
]

AugmentedCandle = build_augmented_dataclass(BASE_FIELDS, ENGINEERED_FIELDS)
