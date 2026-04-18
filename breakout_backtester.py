import ccxt
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import time

exchange = ccxt.binance()

# ── BREAKOUT CONFIG ────────────────────────────────────────
BREAKOUT_BARS = 12     # 12 × 4H = 48 hours — only strongest moves survive
THRESHOLD     = 1.5    # ATR distance to qualify as stretched
MEAN_WINDOW   = 33     # rolling mean window
MA_WINDOW     = 325    # trend filter window
SLIPPAGE      = 50.0   # $50 per trade
FORWARD_BARS  = 33     # look forward 33 bars = ~5.5 days
VOL_MULT      = 1.5    # volume must be 50% above average

# ── 1. FETCH DATA ──────────────────────────────────────────
def fetch_full_history(symbol, timeframe="4h"):
    all_bars = []
    since    = exchange.parse8601("2024-04-01T00:00:00Z")

    print(f"Fetching {symbol} {timeframe} history...")

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

    data = pd.DataFrame(
        all_bars,
        columns=["timestamp", "Open", "High", "Low", "Close", "Volume"]
    )
    data["Date"] = pd.to_datetime(data["timestamp"], unit="ms")
    data = data[["Date", "Open", "High", "Low", "Close", "Volume"]]
    data.drop_duplicates(subset="Date", inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data

# ── 2. CALCULATE INDICATORS ────────────────────────────────
def calculate_indicators(data):
    data["prev_close"]   = data["Close"].shift(1)
    data["tr"]           = data[["High", "Low", "prev_close"]].apply(
        lambda row: max(
            row["High"] - row["Low"],
            abs(row["High"] - row["prev_close"]),
            abs(row["prev_close"] - row["Low"])
        ), axis=1
    )
    data["atr_14"]       = data["tr"].ewm(alpha=1/14, adjust=False).mean()
    data["atr_14"]       = data["atr_14"].replace(0, 0.00000001)
    data["rolling_mean"] = data["Close"].rolling(window=MEAN_WINDOW).mean()
    data["atr_distance"] = (data["Close"] - data["rolling_mean"]) / data["atr_14"]
    data["ma_long"]      = data["Close"].rolling(window=MA_WINDOW).mean()
    data["trend"]        = (data["Close"] > data["ma_long"]).map({True: "UP", False: "DOWN"})
    data["confirmed_buy"] = data["Close"] > data["prev_close"]

    # Volume filter — is current volume above 20-bar average?
    data["vol_ma"]    = data["Volume"].rolling(window=20).mean()
    data["vol_surge"] = data["Volume"] > (data["vol_ma"] * VOL_MULT)

    return data

# ── 3. GENERATE BREAKOUT SIGNALS ───────────────────────────
def generate_breakout_signals(data):
    signals = []

    for i in range(BREAKOUT_BARS, len(data)):
        recent = data.iloc[i-BREAKOUT_BARS:i+1]
        row    = data.iloc[i]

        # All bars stretched above mean for full BREAKOUT_BARS period
        all_stretched = (recent["atr_distance"] > THRESHOLD).all()

        # Price making higher closes over the period
        trending_up   = recent["Close"].iloc[-1] > recent["Close"].iloc[0]

        # Current bar is green confirmation
        last_green    = row["confirmed_buy"]

        # Must be in uptrend
        uptrend       = row["trend"] == "UP"

        # Volume surge — high volume confirms real breakout not fake pump
        vol_surge     = row["vol_surge"]

        if all_stretched and trending_up and last_green and uptrend and vol_surge:
            signals.append(i)

    return signals

# ── 4. REMOVE CLUSTERS ─────────────────────────────────────
def remove_clusters(signal_indices, min_gap=13):
    # minimum 13 bars between signals = ~52 hours
    filtered = []
    last_idx = -999

    for idx in signal_indices:
        if idx - last_idx >= min_gap:
            filtered.append(idx)
            last_idx = idx

    return filtered

# ── 5. BACKTEST ENGINE ─────────────────────────────────────
def run_backtest(data, signal_indices):
    results = []

    for i in signal_indices:
        entry      = data.iloc[i]["Close"]
        atr        = data.iloc[i]["atr_14"]
        entry_date = data.iloc[i]["Date"]

        # Breakout uses wider SL and bigger TP
        stop_loss   = entry - (atr * 2.0)
        take_profit = entry + (atr * 4.0)

        outcome    = "TIMEOUT"
        exit_price = data.iloc[min(i+FORWARD_BARS, len(data)-1)]["Close"]
        exit_date  = data.iloc[min(i+FORWARD_BARS, len(data)-1)]["Date"]

        future = data.iloc[i+1 : i+FORWARD_BARS+1].copy()

        for _, frow in future.iterrows():
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

        pnl = (exit_price - entry) - SLIPPAGE

        results.append({
            "entry_date":  entry_date,
            "exit_date":   exit_date,
            "entry":       round(entry, 4),
            "stop_loss":   round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "exit":        round(exit_price, 4),
            "outcome":     outcome,
            "pnl_usd":     round(pnl, 2)
        })

    return pd.DataFrame(results)


# ── 6. MAIN — TEST ALL TIMEFRAMES ─────────────────────────
TIMEFRAME_CONFIGS = {
    "15m": {"mean": 520,  "ma": 5200,  "cluster": 13, "forward": 52},
    "30m": {"mean": 260,  "ma": 2600,  "cluster": 13, "forward": 33},
    "1h":  {"mean": 130,  "ma": 1300,  "cluster": 13, "forward": 33},
    "2h":  {"mean": 65,   "ma": 650,   "cluster": 13, "forward": 33},
    "4h":  {"mean": 33,   "ma": 325,   "cluster": 13, "forward": 33},
}

print("="*65)
print(" BREAKOUT STRATEGY — TIMEFRAME COMPARISON — BTC/USDT")
print(f" Bars: {BREAKOUT_BARS} | Vol: {VOL_MULT}x | Threshold: {THRESHOLD}")
print("="*65)

summary = []

for tf, config in TIMEFRAME_CONFIGS.items():
    print(f"\nTesting {tf}...")

    try:
        # Fetch data
        all_bars = []
        since    = exchange.parse8601("2024-04-01T00:00:00Z")

        while True:
            bars = exchange.fetch_ohlcv(
                "BTC/USDT", timeframe=tf, since=since, limit=1000
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

        # Recalculate indicators with timeframe-specific windows
        data["prev_close"]   = data["Close"].shift(1)
        data["tr"]           = data[["High", "Low", "prev_close"]].apply(
            lambda row: max(
                row["High"] - row["Low"],
                abs(row["High"] - row["prev_close"]),
                abs(row["prev_close"] - row["Low"])
            ), axis=1
        )
        data["atr_14"]       = data["tr"].ewm(alpha=1/14, adjust=False).mean()
        data["atr_14"]       = data["atr_14"].replace(0, 0.00000001)
        data["rolling_mean"] = data["Close"].rolling(window=config["mean"]).mean()
        data["atr_distance"] = (data["Close"] - data["rolling_mean"]) / data["atr_14"]
        data["ma_long"]      = data["Close"].rolling(window=config["ma"]).mean()
        data["trend"]        = (data["Close"] > data["ma_long"]).map({True: "UP", False: "DOWN"})
        data["confirmed_buy"] = data["Close"] > data["prev_close"]
        data["vol_ma"]        = data["Volume"].rolling(window=20).mean()
        data["vol_surge"]     = data["Volume"] > (data["vol_ma"] * VOL_MULT)
        data.dropna(inplace=True)
        data.reset_index(drop=True, inplace=True)

        # Generate signals
        signals_raw = []
        for i in range(BREAKOUT_BARS, len(data)):
            recent        = data.iloc[i-BREAKOUT_BARS:i+1]
            row           = data.iloc[i]
            all_stretched = (recent["atr_distance"] > THRESHOLD).all()
            trending_up   = recent["Close"].iloc[-1] > recent["Close"].iloc[0]
            last_green    = row["confirmed_buy"]
            uptrend       = row["trend"] == "UP"
            vol_surge     = row["vol_surge"]

            if all_stretched and trending_up and last_green and uptrend and vol_surge:
                signals_raw.append(i)

        # Cluster filter
        signals_filtered = []
        last_idx = -999
        for idx in signals_raw:
            if idx - last_idx >= config["cluster"]:
                signals_filtered.append(idx)
                last_idx = idx

        if len(signals_filtered) < 5:
            print(f"  Only {len(signals_filtered)} signals — too few to test")
            continue

        # Backtest
        results = []
        for i in signals_filtered:
            entry      = data.iloc[i]["Close"]
            atr        = data.iloc[i]["atr_14"]
            entry_date = data.iloc[i]["Date"]
            stop_loss   = entry - (atr * 1.0)   # tighter SL
            take_profit = entry + (atr * 4.0)   # big TP

            outcome    = "TIMEOUT"
            exit_price = data.iloc[min(i+config["forward"], len(data)-1)]["Close"]
            exit_date  = data.iloc[min(i+config["forward"], len(data)-1)]["Date"]
            future     = data.iloc[i+1 : i+config["forward"]+1].copy()

            for _, frow in future.iterrows():
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

            pnl = (exit_price - entry) - SLIPPAGE
            results.append({"outcome": outcome, "pnl_usd": round(pnl, 2)})

        results_df = pd.DataFrame(results)
        wins       = results_df[results_df["outcome"] == "WIN"]
        losses     = results_df[results_df["outcome"] == "LOSS"]
        timeouts   = results_df[results_df["outcome"] == "TIMEOUT"]

        if len(wins) == 0 or len(losses) == 0:
            print(f"  Not enough wins/losses to calculate")
            continue

        win_rate   = len(wins) / len(results_df) * 100
        avg_win    = wins["pnl_usd"].mean()
        avg_loss   = abs(losses["pnl_usd"].mean())
        expectancy = (len(wins)/len(results_df) * avg_win) - (len(losses)/len(results_df) * avg_loss)

        summary.append({
            "Timeframe":  tf,
            "Trades":     len(results_df),
            "Wins":       len(wins),
            "Losses":     len(losses),
            "Timeouts":   len(timeouts),
            "Win Rate":   f"{win_rate:.1f}%",
            "Avg Win":    f"${avg_win:.0f}",
            "Avg Loss":   f"-${avg_loss:.0f}",
            "Expectancy": f"${expectancy:.2f}",
            "Total P&L":  f"${results_df['pnl_usd'].sum():.0f}",
        })

        verdict = "✅" if expectancy > 0 else "❌"
        print(f"  {verdict} Trades: {len(results_df)} | Win Rate: {win_rate:.1f}% | Expectancy: ${expectancy:.2f}")

    except Exception as e:
        print(f"  Error on {tf}: {e}")

# ── PRINT COMPARISON TABLE ─────────────────────────────────
print("\n")
print("="*75)
print(" BREAKOUT STRATEGY — TIMEFRAME RESULTS")
print("="*75)

if summary:
    summary_df = pd.DataFrame(summary)
    print(summary_df.to_string(index=False))

    # Save
    con = duckdb.connect("analysis.db")
    con.execute("DROP TABLE IF EXISTS breakout_timeframe_comparison")
    con.execute("CREATE TABLE breakout_timeframe_comparison AS SELECT * FROM summary_df")
    con.close()
    print("\nSaved to analysis.db → breakout_timeframe_comparison")

print("\nMean Reversion benchmark: +$155/trade | 34.6% | 4H ✅")
print("Best breakout timeframe  = highest expectancy with 15+ trades")

# ── 6. MAIN ────────────────────────────────────────────────
print("="*60)
print(" BREAKOUT STRATEGY BACKTESTER v2 — BTC/USDT 4H")
print(f" Bars: {BREAKOUT_BARS} (48hrs) | Vol filter: {VOL_MULT}x | Threshold: {THRESHOLD}")
print("="*60)

data = fetch_full_history("BTC/USDT", timeframe="4h")
data = calculate_indicators(data)
data.dropna(inplace=True)
data.reset_index(drop=True, inplace=True)

print(f"Data loaded: {len(data)} bars")
print(f"Date range: {data['Date'].iloc[0]} → {data['Date'].iloc[-1]}")

# Generate and filter signals
raw_signals      = generate_breakout_signals(data)
filtered_signals = remove_clusters(raw_signals, min_gap=13)

print(f"\nRaw breakout signals:      {len(raw_signals)}")
print(f"After cluster filter:      {len(filtered_signals)}")

if len(filtered_signals) == 0:
    print("No signals generated — parameters too strict, try lowering BREAKOUT_BARS or VOL_MULT")
else:
    # Run backtest
    results_df = run_backtest(data, filtered_signals)

    wins     = results_df[results_df["outcome"] == "WIN"]
    losses   = results_df[results_df["outcome"] == "LOSS"]
    timeouts = results_df[results_df["outcome"] == "TIMEOUT"]
    win_rate = len(wins) / len(results_df) * 100

    print("\n--- Breakout Backtest Results v2 ---")
    print(f"Total Trades:   {len(results_df)}")
    print(f"Wins:           {len(wins)}  ({win_rate:.1f}%)")
    print(f"Losses:         {len(losses)}")
    print(f"Timeouts:       {len(timeouts)}")
    print(f"\nAvg Win  ($):   {wins['pnl_usd'].mean():.2f}")
    print(f"Avg Loss ($):   {losses['pnl_usd'].mean():.2f}")
    print(f"Total P&L ($):  {results_df['pnl_usd'].sum():.2f}")

    win_rate_d  = len(wins) / len(results_df)
    loss_rate_d = 1 - win_rate_d
    avg_win     = wins["pnl_usd"].mean()
    avg_loss    = abs(losses["pnl_usd"].mean())
    expectancy  = (win_rate_d * avg_win) - (loss_rate_d * avg_loss)

    print(f"\nExpectancy: ${expectancy:.2f} per trade")
    if expectancy > 0:
        print("✅ Breakout strategy has POSITIVE edge on BTC")
    else:
        print("⚠️  Breakout strategy still NEGATIVE — needs more refinement")

    # Full comparison table
    print(f"\n--- Strategy Comparison ---")
    print(f"{'Strategy':<20} {'Trades':<10} {'Win Rate':<12} {'Expectancy':<15} {'Verdict'}")
    print(f"{'-'*65}")
    print(f"{'Mean Reversion':<20} {'52':<10} {'34.6%':<12} {'$155.21':<15} {'✅ PROVEN'}")
    print(f"{'Breakout v1':<20} {'48':<10} {'22.9%':<12} {'$-756.56':<15} {'❌ FAILED'}")
    print(f"{'Breakout v2':<20} {len(results_df):<10} {win_rate:.1f}%{'':<6} ${expectancy:.2f}{'':<10} {'✅ PASS' if expectancy > 0 else '❌ FAIL'}")

    # Position sizing reminder
    print(f"\n--- Position Sizing Rules ---")
    print(f"Mean Reversion signals → risk 1.0% per trade")
    print(f"Breakout signals       → risk 0.5% per trade (lottery ticket)")

    # Save results
    con = duckdb.connect("analysis.db")
    con.execute("DROP TABLE IF EXISTS breakout_backtest_v2")
    con.execute("CREATE TABLE breakout_backtest_v2 AS SELECT * FROM results_df")
    con.close()
    print("\nSaved to analysis.db → breakout_backtest_v2")

    # Equity curve
    results_df["entry_date"]     = pd.to_datetime(results_df["entry_date"])
    results_df["cumulative_pnl"] = results_df["pnl_usd"].cumsum()

    start_row = pd.DataFrame([{
        "entry_date":     results_df["entry_date"].iloc[0],
        "cumulative_pnl": 0
    }])
    equity = pd.concat(
        [start_row, results_df[["entry_date", "cumulative_pnl"]]],
        ignore_index=True
    )

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    fig.suptitle(
        f"BTC/USDT 4H — Breakout Strategy v2\n"
        f"Bars: {BREAKOUT_BARS} | Vol: {VOL_MULT}x | Threshold: {THRESHOLD}",
        fontsize=13
    )

    ax1.plot(equity["entry_date"], equity["cumulative_pnl"],
             color="purple", linewidth=2, marker="o", markersize=5)
    ax1.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
    ax1.fill_between(equity["entry_date"], equity["cumulative_pnl"], 0,
                     where=(equity["cumulative_pnl"] >= 0),
                     alpha=0.15, color="green", label="Profit")
    ax1.fill_between(equity["entry_date"], equity["cumulative_pnl"], 0,
                     where=(equity["cumulative_pnl"] < 0),
                     alpha=0.15, color="red", label="Drawdown")
    ax1.set_ylabel("Cumulative P&L (USD)")
    ax1.set_title(
        f"Equity Curve | Expectancy: ${expectancy:.2f}/trade | Win Rate: {win_rate:.1f}%"
    )
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    colors = results_df["outcome"].map(
        {"WIN": "green", "LOSS": "red", "TIMEOUT": "orange"}
    )
    ax2.bar(results_df["entry_date"], results_df["pnl_usd"],
            color=colors, width=5, alpha=0.8)
    ax2.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
    ax2.set_ylabel("P&L per Trade (USD)")
    ax2.set_title("Individual Trade Results  [Green=Win | Red=Loss | Orange=Timeout]")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("breakout_equity_curve_v2.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("\nChart saved as breakout_equity_curve_v2.png")