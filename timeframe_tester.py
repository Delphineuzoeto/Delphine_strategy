import ccxt
import pandas as pd
import duckdb
import time

exchange = ccxt.binance()

# ── TIMEFRAME CONFIGURATIONS ───────────────────────────────
# Each timeframe has scaled windows to match equivalent periods
TIMEFRAMES = {
    "15m":  {"mean": 520,  "ma": 5200,  "cluster": 132, "forward": 520},
    "30m":  {"mean": 260,  "ma": 2600,  "cluster": 66,  "forward": 260},
    "1h":   {"mean": 130,  "ma": 1300,  "cluster": 33,  "forward": 130},
    "2h":   {"mean": 65,   "ma": 650,   "cluster": 17,  "forward": 65},
    "4h":   {"mean": 33,   "ma": 325,   "cluster": 9,   "forward": 33},
}

THRESHOLD = 1.5
SLIPPAGE  = 50.0

def fetch_data(symbol, timeframe, limit=2000):
    print(f"  Fetching {symbol} {timeframe}...")
    all_bars = []
    since    = exchange.parse8601("2024-04-01T00:00:00Z")

    while True:
        bars = exchange.fetch_ohlcv(
            symbol, timeframe=timeframe, since=since, limit=1000
        )
        if not bars:
            break
        all_bars.extend(bars)
        since = bars[-1][0] + 1
        time.sleep(0.3)
        if bars[-1][0] >= exchange.milliseconds():
            break
        if len(all_bars) >= 15000:
            break

    data = pd.DataFrame(
        all_bars,
        columns=["timestamp", "Open", "High", "Low", "Close", "Volume"]
    )
    data["Date"] = pd.to_datetime(data["timestamp"], unit="ms")
    data = data[["Date", "Open", "High", "Low", "Close", "Volume"]]
    data.drop_duplicates(subset="Date", inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data

def calculate_indicators(data, mean_window, ma_window):
    data["prev_close"]  = data["Close"].shift(1)
    data["tr"]          = data[["High", "Low", "prev_close"]].apply(
        lambda row: max(
            row["High"] - row["Low"],
            abs(row["High"] - row["prev_close"]),
            abs(row["prev_close"] - row["Low"])
        ), axis=1
    )
    data["atr_14"]       = data["tr"].ewm(alpha=1/14, adjust=False).mean()
    data["atr_14"]       = data["atr_14"].replace(0, 0.0001)
    data["rolling_mean"] = data["Close"].rolling(window=mean_window).mean()
    data["atr_distance"] = ((data["Close"] - data["rolling_mean"]) / data["atr_14"]).round(2)
    data["ma_long"]      = data["Close"].rolling(window=ma_window).mean()
    data["trend"]        = (data["Close"] > data["ma_long"]).map({True: "UP", False: "DOWN"})

    # No look-ahead — use previous bar momentum
    data["confirmed_buy"]  = data["Close"] > data["prev_close"]
    data["confirmed_sell"] = data["Close"] < data["prev_close"]
    return data

def run_backtest(data, cluster_bars, forward_bars):
    # Generate signals
    data["signal"] = 0
    data.loc[
        (data["atr_distance"] < -THRESHOLD) &
        (data["trend"] == "UP") &
        (data["confirmed_buy"] == True),
        "signal"
    ] = 1
    data.loc[
        (data["atr_distance"] > THRESHOLD) &
        (data["trend"] == "DOWN") &
        (data["confirmed_sell"] == True),
        "signal"
    ] = -1

    # Remove clusters
    signals = data[data["signal"] != 0].copy()
    signals["bars_since_last"] = signals.index.to_series().diff().fillna(999)
    signals = signals[signals["bars_since_last"] >= cluster_bars].copy()

    if len(signals) == 0:
        return None

    results = []
    for i, row in signals.iterrows():
        entry      = row["Close"]
        atr        = row["atr_14"]
        signal     = row["signal"]
        entry_date = row["Date"]

        if signal == 1:
            stop_loss   = entry - (atr * 1.5)
            take_profit = entry + (atr * 3.0)
        else:
            stop_loss   = entry + (atr * 1.5)
            take_profit = entry - (atr * 3.0)

        outcome     = "TIMEOUT"
        loc         = data.index.get_loc(i)
        future_rows = data.iloc[loc+1 : loc+forward_bars+1]
        exit_price  = future_rows["Close"].iloc[-1] if len(future_rows) > 0 else entry
        exit_date   = future_rows["Date"].iloc[-1]  if len(future_rows) > 0 else entry_date
        future      = future_rows.copy()

        for _, frow in future.iterrows():
            if signal == 1:
                if frow["Low"] <= stop_loss:
                    outcome    = "LOSS"
                    exit_date  = frow["Date"]
                    exit_price = frow["Open"] if frow["Open"] < stop_loss else stop_loss
                    break
                if frow["High"] >= take_profit:
                    outcome    = "WIN"
                    exit_date  = frow["Date"]
                    exit_price = frow["Open"] if frow["Open"] > take_profit else take_profit
                    break
            else:
                if frow["High"] >= stop_loss:
                    outcome    = "LOSS"
                    exit_date  = frow["Date"]
                    exit_price = frow["Open"] if frow["Open"] > stop_loss else stop_loss
                    break
                if frow["Low"] <= take_profit:
                    outcome    = "WIN"
                    exit_date  = frow["Date"]
                    exit_price = frow["Open"] if frow["Open"] < take_profit else take_profit
                    break

        pnl = (exit_price - entry) if signal == 1 else (entry - exit_price)
        pnl = pnl - SLIPPAGE

        results.append({
            "entry_date": entry_date,
            "signal":     "BUY" if signal == 1 else "SELL",
            "outcome":    outcome,
            "pnl_usd":    round(pnl, 2)
        })

    return pd.DataFrame(results)

# ── MAIN — TEST ALL TIMEFRAMES ─────────────────────────────
print("="*60)
print(" TIMEFRAME COMPARISON TEST — BTC/USDT")
print("="*60)

summary = []

for tf, config in TIMEFRAMES.items():
    print(f"\nTesting {tf}...")
    try:
        data    = fetch_data("BTC/USDT", tf)
        data    = calculate_indicators(data, config["mean"], config["ma"])
        results = run_backtest(data, config["cluster"], config["forward"])

        if results is None or len(results) == 0:
            print(f"  No signals found for {tf}")
            continue

        wins     = results[results["outcome"] == "WIN"]
        losses   = results[results["outcome"] == "LOSS"]
        timeouts = results[results["outcome"] == "TIMEOUT"]

        if len(wins) == 0 or len(losses) == 0:
            print(f"  Not enough data for {tf}")
            continue

        win_rate   = len(wins) / len(results) * 100
        avg_win    = wins["pnl_usd"].mean()
        avg_loss   = abs(losses["pnl_usd"].mean())
        expectancy = (len(wins)/len(results) * avg_win) - (len(losses)/len(results) * avg_loss)

        summary.append({
            "Timeframe":  tf,
            "Trades":     len(results),
            "Win Rate":   f"{win_rate:.1f}%",
            "Avg Win":    f"${avg_win:.0f}",
            "Avg Loss":   f"-${avg_loss:.0f}",
            "Expectancy": f"${expectancy:.2f}",
            "Total P&L":  f"${results['pnl_usd'].sum():.0f}",
            "Timeouts":   len(timeouts)
        })

        print(f"  Trades: {len(results)} | Win Rate: {win_rate:.1f}% | Expectancy: ${expectancy:.2f}")

    except Exception as e:
        print(f"  Error on {tf}: {e}")

# ── PRINT COMPARISON TABLE ─────────────────────────────────
print("\n")
print("="*60)
print(" RESULTS SUMMARY")
print("="*60)

summary_df = pd.DataFrame(summary)
print(summary_df.to_string(index=False))

# Save to database
con = duckdb.connect("analysis.db")
con.execute("DROP TABLE IF EXISTS timeframe_comparison")
con.execute("CREATE TABLE timeframe_comparison AS SELECT * FROM summary_df")
con.close()

print("\nSaved to analysis.db → timeframe_comparison")
print("\nBest timeframe = highest expectancy with 30+ trades")