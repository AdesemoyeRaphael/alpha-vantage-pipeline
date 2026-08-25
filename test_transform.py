"""
Unit tests for transform.py — pure logic pulled out of the Alpha Vantage DAG
so it can be tested without spinning up Airflow.

Run with: pytest test_transform.py -v
"""

import json
import pandas as pd
import pytest

from dags.transform import parse_trade_records, validate_trade_data

# ---------- parse_trade_records ----------


def _make_content(open_, high, low, close, volume):
    return json.dumps(
        {
            "1. open": str(open_),
            "2. high": str(high),
            "3. low": str(low),
            "4. close": str(close),
            "5. volume": str(volume),
        }
    )


def test_parse_trade_records_basic():
    keys = [{"date": "2026-08-24"}, {"date": "2026-08-25"}]
    contents = [
        _make_content(100, 105, 99, 102, 1000),
        _make_content(102, 110, 101, 108, 2000),
    ]

    df = parse_trade_records(keys, contents)

    assert len(df) == 2
    assert list(df["date"]) == ["2026-08-24", "2026-08-25"]
    assert df.loc[0, "open"] == 100.0
    assert df.loc[0, "close"] == 102.0
    assert df.loc[1, "volume"] == 2000


def test_parse_trade_records_computes_pct_change_correctly():
    keys = [{"date": "2026-08-24"}]
    contents = [_make_content(open_=100, high=110, low=90, close=110, volume=500)]

    df = parse_trade_records(keys, contents)

    # (110 - 100) / 100 * 100 = 10.0
    assert df.loc[0, "daily_pct_change"] == pytest.approx(10.0)


def test_parse_trade_records_handles_negative_pct_change():
    keys = [{"date": "2026-08-24"}]
    contents = [_make_content(open_=100, high=101, low=90, close=95, volume=500)]

    df = parse_trade_records(keys, contents)

    assert df.loc[0, "daily_pct_change"] == pytest.approx(-5.0)


def test_parse_trade_records_empty_input_returns_empty_df():
    df = parse_trade_records([], [])
    assert len(df) == 0
    assert list(df.columns) == [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "daily_pct_change",
    ]


def test_parse_trade_records_raises_on_malformed_json():
    keys = [{"date": "2026-08-24"}]
    contents = ["not valid json"]

    with pytest.raises(json.JSONDecodeError):
        parse_trade_records(keys, contents)


# ---------- validate_trade_data ----------


def test_validate_trade_data_passes_on_good_df():
    df = pd.DataFrame(
        {
            "date": ["2026-08-24"],
            "open": [100.0],
            "close": [102.0],
            "high": [105.0],
            "low": [99.0],
            "volume": [1000],
        }
    )
    validate_trade_data(df)  # should not raise


def test_validate_trade_data_raises_on_empty_df():
    df = pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
    with pytest.raises(ValueError, match="empty DataFrame"):
        validate_trade_data(df)


def test_validate_trade_data_raises_on_null_date():
    df = pd.DataFrame(
        {
            "date": [None],
            "open": [100.0],
            "close": [102.0],
            "high": [105.0],
            "low": [99.0],
            "volume": [1000],
        }
    )
    with pytest.raises(ValueError, match="missing date/close"):
        validate_trade_data(df)


def test_validate_trade_data_raises_on_null_close():
    df = pd.DataFrame(
        {
            "date": ["2026-08-24"],
            "open": [100.0],
            "close": [None],
            "high": [105.0],
            "low": [99.0],
            "volume": [1000],
        }
    )
    with pytest.raises(ValueError, match="missing date/close"):
        validate_trade_data(df)
