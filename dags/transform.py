import json
import pandas as pd


def parse_trade_records(keys, contents):
    date, open_, close, high, low, volume = [], [], [], [], [], []
    for key, content in zip(keys, contents):
        date.append(key["date"])
        content = json.loads(content)
        open_.append(float(content.get("1. open")))
        high.append(float(content.get("2. high")))
        close.append(float(content.get("4. close")))
        low.append(float(content.get("3. low")))
        volume.append(int(content.get("5. volume")))

    df = pd.DataFrame(
        {
            "date": date,
            "open": open_,
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
        }
    )
    df["daily_pct_change"] = (df["close"] - df["open"]) / df["open"] * 100
    return df


def validate_trade_data(df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise ValueError("Data validation failed: empty DataFrame")
    if df["date"].isna().any() or df["close"].isna().any():
        raise ValueError("Data validation failed: missing date/close values")
