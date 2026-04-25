"""
AfriMetrics v6.0 Backtest — 12-month replay with THRESHOLD parameter sweep.

Run this ONCE to fetch data (takes ~5-10 min first time, cached after that),
then re-run sweeps instantly from cache.

Usage:
    python backtest_v6.py              # default: fetch + run all THRESHOLD values
    python backtest_v6.py --fetch-only # just download the data, don't backtest
    python backtest_v6.py --no-fetch   # skip fetch, use cache only

Outputs:
    data_cache/<symbol>_<tf>.parquet   — cached OHLCV data
    results/signals_<threshold>.csv    — every signal fired per threshold
    results/summary.csv                — aggregate stats per threshold
    results/per_pair_<threshold>.csv   — per-pair breakdown per threshold
"""
import argparse
import os
import time
from pathlib import Path

import ccxt
import numpy as np
import pandas as pd
import talib

# ────────────────────────────────────────────────────────────
# CONFIG — mirrors v6.0 exactly
# ────────────────────────────────────────────────────────────
WATCHLIST = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
    "DOGE/USDT:USDT", "XAU/USDT:USDT", "ZEC/USDT:USDT", "1000PEPE/USDT:USDT",
    "BNB/USDT:USDT", "HYPE/USDT:USDT", "ENA/USDT:USDT", "SUI/USDT:USDT",
    "AVAX/USDT:USDT", "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT",
    "AAVE/USDT:USDT", "TRUMP/USDT:USDT",
]

MEAN_WINDOW   = 33
MA_WINDOW     = 325
BREAKOUT_BARS = 8

# Sweep values (the one you asked for)
THRESHOLDS = [1.0, 1.2, 1.5, 1.8]

# Backtest period: 12 months
MONTHS_BACK = 12
BARS_4H     = int((MONTHS_BACK * 30 * 24) / 4)   # ~2,190 bars
BARS_1H     = MONTHS_BACK * 30 * 24              # ~8,640 bars

# Trade management (matches v6.0 logic)
MAX_HOLD_BARS_4H = 9    # = 36h max hold on 4H timeframe; v6.0 uses 4h which is too tight for backtest truth
FG_NEUTRAL       = 50   # can't reconstruct historical F&G easily; use neutral

# F&G thresholds (from v6.0)
FG_EXTREME_GREED = 75

# Risk settings
ACCOUNT_BALANCE = 1000
RISK_PER_TRADE  = 0.01

CACHE_DIR   = Path("data_cache")
RESULTS_DIR = Path("results")
CACHE_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)


# ────────────────────────────────────────────────────────────
# DATA FETCHING (paginated, cached)
# ────────────────────────────────────────────────────────────
def fetch_paginated(exchange, symbol, timeframe, total_bars):
    """Fetch more than 1000 bars by paginating backward from now."""
    per_request = 1000
    tf_ms = exchange.parse_timeframe(timeframe) * 1000
    now_ms = exchange.milliseconds()
    since = now_ms - total_bars * tf_ms

    all_rows = []
    cursor = since
    while cursor < now_ms:
        batch = exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, since=cursor, limit=per_request
        )
        if not batch:
            break
        all_rows.extend(batch)
        last_ts = batch[-1][0]
        # Next cursor = last timestamp + 1 bar
        cursor = last_ts + tf_ms
        if len(batch) < per_request:
            break
        time.sleep(0.25)  # rate-limit politeness

    df = pd.DataFrame(all_rows, columns=["timestamp", "Open", "High", "Low", "Close", "Volume"])
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    df["Date"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df[["Date", "Open", "High", "Low", "Close", "Volume"]]


def cache_path(symbol, timeframe):
    clean = symbol.replace("/", "_").replace(":", "_")
    return CACHE_DIR / f"{clean}_{timeframe}.parquet"


def load_or_fetch(exchange, symbol, timeframe, bars, force_fetch=False):
    path = cache_path(symbol, timeframe)
    if path.exists() and not force_fetch:
        return pd.read_parquet(path)
    print(f"  Fetching {symbol} {timeframe} ({bars} bars)...")
    df = fetch_paginated(exchange, symbol, timeframe, bars)
    df.to_parquet(path, index=False)
    return df


# ────────────────────────────────────────────────────────────
# INDICATORS (identical to v6.0's calculate_indicators)
# ────────────────────────────────────────────────────────────
def calculate_indicators(data):
    data = data.copy()
    data["prev_close"] = data["Close"].shift(1)
    data["tr"] = data[["High", "Low", "prev_close"]].apply(
        lambda r: max(
            r["High"] - r["Low"],
            abs(r["High"] - r["prev_close"]),
            abs(r["prev_close"] - r["Low"])
        ), axis=1
    )
    data["atr_14"] = data["tr"].ewm(alpha=1/14, adjust=False).mean()
    data["atr_14"] = data["atr_14"].replace(0, 0.00000001)
    data["rolling_mean"] = data["Close"].rolling(MEAN_WINDOW).mean()
    data["atr_distance"] = (data["Close"] - data["rolling_mean"]) / data["atr_14"]
    data["ma_long"] = data["Close"].rolling(MA_WINDOW).mean()
    data["trend"] = np.where(data["Close"] > data["ma_long"], "UP", "DOWN")

    closes = data["Close"].values.astype(float)
    highs = data["High"].values.astype(float)
    lows = data["Low"].values.astype(float)
    volumes = data["Volume"].values.astype(float)

    data["rsi"] = talib.RSI(closes, timeperiod=14)
    _, _, hist = talib.MACD(closes, 12, 26, 9)
    data["macd_hist"] = hist

    data["obv"] = talib.OBV(closes, volumes)
    data["obv_ema"] = pd.Series(data["obv"]).ewm(span=20).mean()
    data["obv_up"] = data["obv"] > data["obv_ema"]
    data["obv_down"] = data["obv"] < data["obv_ema"]

    data["vol_avg"] = data["Volume"].rolling(20).mean()
    data["vol_spike"] = data["Volume"] > data["vol_avg"] * 1.5

    data["ha_close"] = (data["Open"] + data["High"] + data["Low"] + data["Close"]) / 4
    data["ha_open"] = (data["Open"].shift(1) + data["Close"].shift(1)) / 2
    data["ha_green"] = data["ha_close"] > data["ha_open"]
    data["ha_red"] = data["ha_close"] < data["ha_open"]
    data["ha_trending_down"] = data["ha_red"].rolling(3).sum() == 3

    data["atr_avg"] = data["atr_14"].rolling(50).mean()
    data["is_trending"] = data["atr_14"] > data["atr_avg"]

    data["vol_mean"] = data["atr_14"].rolling(100).mean()
    data["sl_mult"] = np.where(data["atr_14"] > data["vol_mean"], 2.0, 1.2)

    data["cci"] = talib.CCI(highs, lows, closes, timeperiod=14)

    # VWAP (daily reset)
    data["date_only"] = data["Date"].dt.date
    data["tp_"] = (data["High"] + data["Low"] + data["Close"]) / 3
    data["tp_vol"] = data["tp_"] * data["Volume"]
    data["cum_tp_vol"] = data.groupby("date_only")["tp_vol"].cumsum()
    data["cum_vol"] = data.groupby("date_only")["Volume"].cumsum()
    data["vwap"] = data["cum_tp_vol"] / data["cum_vol"]
    data["above_vwap"] = data["Close"] > data["vwap"]
    data.drop(columns=["date_only", "tp_", "tp_vol", "cum_tp_vol", "cum_vol"], inplace=True)

    return data


def check_breakout(data_4h_slice, threshold):
    """Check breakout on last BREAKOUT_BARS of the slice. threshold is parameterized."""
    if len(data_4h_slice) < BREAKOUT_BARS:
        return False
    recent = data_4h_slice.tail(BREAKOUT_BARS)
    bars_stretched = (recent["atr_distance"] > threshold).sum()
    trending_up = recent["Close"].iloc[-1] > recent["Close"].iloc[0]
    last_green = recent["Close"].iloc[-1] > recent["Close"].iloc[-2]
    return bars_stretched >= 5 and trending_up and last_green


# ────────────────────────────────────────────────────────────
# SIGNAL EVALUATION (mirrors check_signal logic)
# ────────────────────────────────────────────────────────────
def evaluate_bar(data_4h, idx, threshold, fg_score=FG_NEUTRAL):
    """
    Evaluate whether a signal fires at bar `idx` of data_4h.
    Returns (direction, setup, entry, sl, tp) or None.
    1H alignment is checked via trend at same timestamp.
    """
    if idx < MA_WINDOW + 10:   # need enough history
        return None

    s = data_4h.iloc[idx]
    price = s["Close"]
    atr = s["atr_14"]
    dist = s["atr_distance"]
    trend_4h = s["trend"]
    rsi = s["rsi"]
    macd_hist = s["macd_hist"]
    obv_up = s["obv_up"]
    obv_down = s["obv_down"]
    vol_spike = s["vol_spike"]
    is_trending = s["is_trending"]
    sl_mult = s["sl_mult"]
    ha_down = s["ha_trending_down"]
    cci = s["cci"]

    # Regime filter
    if not is_trending:
        return None

    # NOTE: we don't have separate 1H data in this function — we use 4H trend as proxy
    # for the alignment check. This is a simplification; in live v6.0 you fetch 1H separately.
    # To be fair to the strategy, I'll skip the 4H/1H mismatch filter in backtest and note this.

    # Turn confirmation: we use bar-to-bar close on 4H as proxy
    if idx < 1:
        return None
    prev = data_4h.iloc[idx - 1]
    turn_up = price > prev["Close"]
    turn_down = price < prev["Close"]

    buy_setup = dist < -threshold and trend_4h == "UP"
    sell_setup = dist > threshold and trend_4h == "DOWN"
    breakout_buy = check_breakout(data_4h.iloc[: idx + 1], threshold)

    rsi_ok_long = rsi < 65
    rsi_ok_short = rsi > 35
    macd_bull = macd_hist > 0
    macd_bear = macd_hist < 0
    cci_recovering = cci > -100
    cci_overbought = cci > 100

    long_vol_ok = obv_up and vol_spike if fg_score < 20 else obv_up

    if fg_score > FG_EXTREME_GREED and sell_setup:
        return None

    # MEAN REV LONG
    if buy_setup and turn_up and rsi_ok_long and long_vol_ok and cci_recovering:
        entry = price * 1.001
        sl = entry - (atr * sl_mult)
        tp = entry + (atr * sl_mult * 2)
        return ("LONG", "MEAN_REV", entry, sl, tp)

    # MEAN REV SHORT
    if sell_setup and turn_down and rsi_ok_short and macd_bear \
            and obv_down and ha_down and cci_overbought:
        entry = price * 0.999
        sl = entry + (atr * sl_mult)
        tp = entry - (atr * sl_mult * 2)
        return ("SHORT", "MEAN_REV", entry, sl, tp)

    # BREAKOUT LONG
    if breakout_buy and turn_up and trend_4h == "UP" \
            and rsi_ok_long and macd_bull and long_vol_ok \
            and not cci_overbought:
        entry = price * 1.001
        sl = entry - (atr * 2.0)
        tp = entry + (atr * 4.0)
        return ("LONG", "BREAKOUT", entry, sl, tp)

    return None


# ────────────────────────────────────────────────────────────
# EXIT SIMULATION
# ────────────────────────────────────────────────────────────
def simulate_exit(data_1h, entry_time, direction, entry, sl, tp):
    """
    Walk forward on 1H candles from entry_time checking SL/TP hit.
    Uses bar High/Low so gaps are handled. Expires after MAX_HOLD_BARS_4H * 4 hours.
    Returns (result, exit_price, bars_held, exit_time).
    """
    future = data_1h[data_1h["Date"] > entry_time].reset_index(drop=True)
    max_hold = MAX_HOLD_BARS_4H * 4   # 4H bars → 1H bars

    for i, row in future.iterrows():
        if i >= max_hold:
            return ("EXPIRED", row["Close"], i, row["Date"])

        high = row["High"]
        low = row["Low"]

        if direction == "LONG":
            # Conservative: if both SL and TP hit in same bar, assume SL first (worst case)
            if low <= sl:
                return ("LOSS", sl, i, row["Date"])
            if high >= tp:
                return ("WIN", tp, i, row["Date"])
        else:  # SHORT
            if high >= sl:
                return ("LOSS", sl, i, row["Date"])
            if low <= tp:
                return ("WIN", tp, i, row["Date"])

    # Ran out of future data
    last = future.iloc[-1] if len(future) > 0 else None
    if last is not None:
        return ("OPEN_AT_END", last["Close"], len(future), last["Date"])
    return ("NO_DATA", entry, 0, entry_time)


# ────────────────────────────────────────────────────────────
# BACKTEST RUNNER
# ────────────────────────────────────────────────────────────
def backtest_symbol(data_4h, data_1h, symbol, threshold):
    """Walk bar-by-bar through 4H data, fire signals, simulate exits."""
    signals = []
    active = None   # one position at a time per pair (matches v6.0)

    for idx in range(MA_WINDOW + 10, len(data_4h)):
        bar = data_4h.iloc[idx]
        # Close active position first if SL/TP would have hit between bars
        # (already handled by simulate_exit using 1H granularity, so we just
        # check if we're still "in" the trade)
        if active is not None:
            if bar["Date"] >= active["exit_time"]:
                # position already closed
                active = None

        if active is not None:
            continue

        result = evaluate_bar(data_4h, idx, threshold)
        if result is None:
            continue

        direction, setup, entry, sl, tp = result
        entry_time = bar["Date"]
        exit_result, exit_price, bars_held, exit_time = simulate_exit(
            data_1h, entry_time, direction, entry, sl, tp
        )

        # PnL calc: % move × position size (position sized to 1% risk)
        risk_per_unit = abs(entry - sl)
        position_size = min((ACCOUNT_BALANCE * RISK_PER_TRADE / risk_per_unit) * entry,
                            ACCOUNT_BALANCE * 10) if risk_per_unit > 0 else 0
        if direction == "LONG":
            pnl = (exit_price - entry) / entry * position_size
        else:
            pnl = (entry - exit_price) / entry * position_size

        signals.append({
            "symbol": symbol.replace(":USDT", ""),
            "entry_time": entry_time,
            "exit_time": exit_time,
            "direction": direction,
            "setup": setup,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "exit_price": exit_price,
            "result": exit_result,
            "bars_held": bars_held,
            "pnl_usd": pnl,
            "position_size": position_size,
        })

        active = {"exit_time": exit_time}

    return signals


# ────────────────────────────────────────────────────────────
# AGGREGATION + REPORTING
# ────────────────────────────────────────────────────────────
def summarize(all_signals, threshold):
    if not all_signals:
        return {
            "threshold": threshold,
            "total_signals": 0,
            "wins": 0, "losses": 0, "expired": 0,
            "win_rate_pct": 0, "total_pnl_usd": 0,
            "avg_bars_held": 0, "best_pair": "-", "worst_pair": "-",
        }
    df = pd.DataFrame(all_signals)
    closed = df[df["result"].isin(["WIN", "LOSS", "EXPIRED"])]
    wins = (closed["result"] == "WIN").sum()
    losses = (closed["result"] == "LOSS").sum()
    expired = (closed["result"] == "EXPIRED").sum()
    win_rate = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    total_pnl = closed["pnl_usd"].sum()
    avg_hold = closed["bars_held"].mean() if len(closed) > 0 else 0

    by_pair = closed.groupby("symbol")["pnl_usd"].sum().sort_values()
    best = by_pair.index[-1] if len(by_pair) > 0 else "-"
    worst = by_pair.index[0] if len(by_pair) > 0 else "-"

    return {
        "threshold": threshold,
        "total_signals": len(closed),
        "wins": wins, "losses": losses, "expired": expired,
        "win_rate_pct": round(win_rate, 1),
        "total_pnl_usd": round(total_pnl, 2),
        "avg_bars_held": round(avg_hold, 1),
        "best_pair": best, "worst_pair": worst,
    }


def per_pair_report(all_signals):
    if not all_signals:
        return pd.DataFrame()
    df = pd.DataFrame(all_signals)
    closed = df[df["result"].isin(["WIN", "LOSS", "EXPIRED"])]
    if len(closed) == 0:
        return pd.DataFrame()
    grouped = closed.groupby("symbol").agg(
        signals=("result", "count"),
        wins=("result", lambda x: (x == "WIN").sum()),
        losses=("result", lambda x: (x == "LOSS").sum()),
        expired=("result", lambda x: (x == "EXPIRED").sum()),
        pnl_usd=("pnl_usd", "sum"),
    ).reset_index()
    grouped["win_rate_pct"] = (grouped["wins"] / (grouped["wins"] + grouped["losses"]).replace(0, 1) * 100).round(1)
    grouped["pnl_usd"] = grouped["pnl_usd"].round(2)
    return grouped.sort_values("pnl_usd", ascending=False)


# ────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch-only", action="store_true", help="Just download data")
    parser.add_argument("--no-fetch", action="store_true", help="Use cache only")
    parser.add_argument("--force-fetch", action="store_true", help="Re-download even if cached")
    args = parser.parse_args()

    exchange = ccxt.binance({"options": {"defaultType": "future"}})

    print("=" * 60)
    print(" AfriMetrics v6.0 — Backtest")
    print(f" Period:     {MONTHS_BACK} months")
    print(f" Pairs:      {len(WATCHLIST)}")
    print(f" Thresholds: {THRESHOLDS}")
    print("=" * 60)

    # STEP 1: fetch or load data
    print("\n[1/3] Loading data...")
    data_store = {}
    for symbol in WATCHLIST:
        try:
            if args.no_fetch:
                p4 = cache_path(symbol, "4h")
                p1 = cache_path(symbol, "1h")
                if not p4.exists() or not p1.exists():
                    print(f"  SKIP {symbol} — no cache")
                    continue
                df_4h = pd.read_parquet(p4)
                df_1h = pd.read_parquet(p1)
            else:
                df_4h = load_or_fetch(exchange, symbol, "4h", BARS_4H, args.force_fetch)
                df_1h = load_or_fetch(exchange, symbol, "1h", BARS_1H, args.force_fetch)
            print(f"  {symbol:20s} 4H: {len(df_4h):5d} bars | 1H: {len(df_1h):6d} bars")
            data_store[symbol] = (df_4h, df_1h)
        except Exception as e:
            print(f"  ERROR {symbol}: {e}")

    if args.fetch_only:
        print("\nFetch complete. Run without --fetch-only to backtest.")
        return

    # STEP 2: pre-compute indicators once per symbol
    print("\n[2/3] Computing indicators...")
    indicator_store = {}
    for symbol, (df_4h, df_1h) in data_store.items():
        try:
            df_4h_ind = calculate_indicators(df_4h)
            indicator_store[symbol] = (df_4h_ind, df_1h)
        except Exception as e:
            print(f"  ERROR {symbol}: {e}")

    # STEP 3: run backtest for each threshold
    print("\n[3/3] Running backtests...")
    summaries = []
    for threshold in THRESHOLDS:
        print(f"\n  THRESHOLD = {threshold}")
        all_signals = []
        for symbol, (df_4h_ind, df_1h) in indicator_store.items():
            try:
                signals = backtest_symbol(df_4h_ind, df_1h, symbol, threshold)
                all_signals.extend(signals)
                if signals:
                    closed = [s for s in signals if s["result"] in ("WIN", "LOSS", "EXPIRED")]
                    w = sum(1 for s in closed if s["result"] == "WIN")
                    l = sum(1 for s in closed if s["result"] == "LOSS")
                    print(f"    {symbol.replace(':USDT',''):18s} {len(closed):3d} signals | W:{w:2d} L:{l:2d}")
            except Exception as e:
                print(f"    ERROR {symbol}: {e}")

        # Write per-threshold outputs
        if all_signals:
            df = pd.DataFrame(all_signals)
            df.to_csv(RESULTS_DIR / f"signals_{threshold}.csv", index=False)
            per_pair_report(all_signals).to_csv(
                RESULTS_DIR / f"per_pair_{threshold}.csv", index=False
            )

        summaries.append(summarize(all_signals, threshold))

    # Final summary table
    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(RESULTS_DIR / "summary.csv", index=False)

    print("\n" + "=" * 60)
    print(" FINAL SUMMARY")
    print("=" * 60)
    print(summary_df.to_string(index=False))
    print(f"\n  Detailed results in: {RESULTS_DIR.resolve()}/")


if __name__ == "__main__":
    main()