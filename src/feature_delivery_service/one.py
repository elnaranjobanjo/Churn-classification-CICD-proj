from .tools.binance_client import BitcoinCandle
from .schema import CANDLE_COLUMN_ORDER
from .reader import load_candles_from_duckdb


def build_training_rows(candles: list[BitcoinCandle]) -> list[dict]:
    """Return raw candle rows plus lag/lead labels for modeling."""
    if len(candles) < 3:
        return []

    records: list[dict] = []
    for i in range(1, len(candles) - 1):
        prev_candle = candles[i - 1]
        curr_candle = candles[i]
        next_candle = candles[i + 1]

        row = asdict(curr_candle)
        row["prev_close_lessthan_curr"] = int(
            prev_candle.close_price < curr_candle.close_price
        )
        row["curr_close_lessthan_next"] = int(
            curr_candle.close_price < next_candle.close_price
        )
        records.append(row)

    return records


def add_features_and_save(limit=500):
    print(f"{CANDLE_COLUMN_ORDER = }")
    candles = load_candles_from_duckdb(limit=limit, order_desc=False)
    for candle in candles[0:5]:
        fields = []
        for column in CANDLE_COLUMN_ORDER:
            value = getattr(candle, column)
            if "time" in column:
                formatted = value.strftime("%Y-%m-%d %H:%M:%S")
            elif "volume" in column or "price" in column:
                formatted = f"{value:.5f}" if isinstance(value, float) else str(value)
            else:
                formatted = f"{value}"
            fields.append(f"{column}={formatted}")
        print(" | ".join(fields))
    # for candle in candles[0:10]:
    #     print(
    #         f"{candle.open_time:%Y-%m-%d %H:%M}"
    #         f"  open={candle.open_price:.2f}  high={candle.high_price:.2f} "
    #         f"low={candle.low_price:.2f}  close={candle.close_price:.2f}"
    #         f"  volume_btc={candle.volume_btc:.5f}  volume_usd={candle.volume_usd:.2f}"
    #         f"  trade_count={candle.trade_count}  "
    #         f"taker_buy_btc={candle.taker_buy_volume_btc:.5f}  "
    #         f"taker_buy_usd={candle.taker_buy_volume_usd:.2f}"
    #     )


if __name__ == "__main__":
    add_features_and_save()
