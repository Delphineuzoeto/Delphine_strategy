import ccxt
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import time

# ── CONFIG ─────────────────────────────────────────────────
TIMEFRAME    = "4h"
MEAN_WINDOW  = 33
MA_WINDOW    = 325
CLUSTER_BARS = 9
FORWARD_BARS = 33
THRESHOLD    = 1.5
SLIPPAGE     = 50.0

# ── 1. FETCH DATA ──────────────────────────────────────────
exchange = ccxt.binance()

def fetch_full_history(symbol, timeframe="4h"):
    all_bars = []
    since    = exchange.parse8601("2024-04-01T00:00:00Z")
    print(f"Fetching {symbol} {timeframe} history...")
    while True:
        bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
        if not bars:
            break
        all_bars.extend(bars)
        since = bars[-1][0] + 1
        if len(all_bars) % 5000 == 0:
            print(f"  Fetched {len(all_bars)} bars...")
        time.sleep(0.5)
        if bars[-1][0] >= exchange.milliseconds():
            break
    return all_bars

ohlcv = fetch_full_history("ETH/USDT", timeframe=TIMEFRAME)
data  = pd.DataFrame(ohlcv, columns=["timestamp", "Open", "High", "Low", "Close", "Volume"])
data["Date"] = pd.to_datetime(data["timestamp"], unit="ms")
data  = data[["Date", "Open", "High", "Low", "Close", "Volume"]]
data.drop_duplicates(subset="Date", inplace=True)
data.reset_index(drop=True, inplace=True)

print(f"Data loaded: {len(data)} bars")
print(f"Date range: {data['Date'].iloc[0]} → {data['Date'].iloc[-1]}")

# ── 2. CALCULATE INDICATORS ────────────────────────────────
data["prev_close"]   = data["Close"].shift(1)
data["tr"]           = data[["High", "Low", "prev_close"]].apply(
    lambda row: max(
        row["High"] - row["Low"],
        abs(row["High"] - row["prev_close"]),
        abs(row["prev_close"] - row["Low"])
    ), axis=1
)
data["atr_14"]       = data["tr"].ewm(alpha=1/14, adjust=False).mean()
data["atr_14"]       = data["atr_14"].replace(0, 0.0001)
data["rolling_mean"] = data["Close"].rolling(window=MEAN_WINDOW).mean()
data["atr_distance"] = ((data["Close"] - data["rolling_mean"]) / data["atr_14"]).round(2)
data["ma_long"]      = data["Close"].rolling(window=MA_WINDOW).mean()
data["trend"]        = (data["Close"] > data["ma_long"]).map({True: "UP", False: "DOWN"})

# ── 3. GENERATE SIGNALS ────────────────────────────────────
# NO confirmation filter — raw signal only
# Enter on NEXT bar open
THRESHOLD = 1.5
SLIPPAGE  = 50.0

data["signal"] = 0
data.loc[
    (data["atr_distance"] < -THRESHOLD) &
    (data["trend"] == "UP"),
    "signal"
] = 1

print(f"\nThreshold: -{THRESHOLD} ATR | Slippage: ${SLIPPAGE}")
print("Entry method: Next bar open — no confirmation filter")

# ── 4. REMOVE SIGNAL CLUSTERS ─────────────────────────────
signals = data[data["signal"] != 0].copy()
signals["bars_since_last"] = signals.index.to_series().diff().fillna(999)
signals = signals[signals["bars_since_last"] >= CLUSTER_BARS].copy()

print(f"Total signals: {len(signals)}")
print(signals[["Date", "Close", "atr_distance", "signal"]].head())

# ── 5. BACKTEST ENGINE ─────────────────────────────────────
# Enter at NEXT BAR OPEN — simplest honest entry possible
results = []

for i, row in signals.iterrows():
    atr        = row["atr_14"]
    entry_date = row["Date"]

    loc      = data.index.get_loc(i)
    next_bar = data.iloc[loc+1] if loc+1 < len(data) else None

    if next_bar is None:
        continue

    # Enter at next bar open
    entry       = next_bar["Open"]
    stop_loss   = entry - (atr * 1.5)
    take_profit = entry + (atr * 3.0)

    outcome     = "TIMEOUT"
    future_rows = data.iloc[loc+2 : loc+FORWARD_BARS+1]
    exit_price  = future_rows["Close"].iloc[-1] if len(future_rows) > 0 else entry
    exit_date   = future_rows["Date"].iloc[-1]  if len(future_rows) > 0 else entry_date
    future      = future_rows.copy()

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
        "signal":      "BUY",
        "entry":       round(entry, 2),
        "stop_loss":   round(stop_loss, 2),
        "take_profit": round(take_profit, 2),
        "exit":        round(exit_price, 2),
        "outcome":     outcome,
        "pnl_usd":     round(pnl, 2)
    })

# ── 6. RESULTS ─────────────────────────────────────────────
results_df = pd.DataFrame(results)
wins    = results_df[results_df["outcome"] == "WIN"]
losses  = results_df[results_df["outcome"] == "LOSS"]
timeout = results_df[results_df["outcome"] == "TIMEOUT"]
win_rate = len(wins) / len(results_df) * 100

print("\n--- BTC/USDT 4H Next Bar Open Backtest ---")
print(f"Total Trades:   {len(results_df)}")
print(f"Wins:           {len(wins)}  ({win_rate:.1f}%)")
print(f"Losses:         {len(losses)}")
print(f"Timeouts:       {len(timeout)}")
print(f"\nAvg Win  ($):   {wins['pnl_usd'].mean():.2f}")
print(f"Avg Loss ($):   {losses['pnl_usd'].mean():.2f}")
print(f"Total P&L ($):  {results_df['pnl_usd'].sum():.2f}")

# ── 7. EXPECTANCY ──────────────────────────────────────────
win_rate_d  = len(wins) / len(results_df)
loss_rate_d = 1 - win_rate_d
avg_win     = wins["pnl_usd"].mean()
avg_loss    = abs(losses["pnl_usd"].mean())
expectancy  = (win_rate_d * avg_win) - (loss_rate_d * avg_loss)

print(f"\nExpectancy: ${expectancy:.2f} per trade")
if expectancy > 0:
    print("✅ Strategy has POSITIVE edge on BTC 4H")
else:
    print("⚠️  Strategy has NEGATIVE edge — needs refinement")

# ── 8. SAVE RESULTS ────────────────────────────────────────
con = duckdb.connect("analysis.db")
con.execute("DROP TABLE IF EXISTS btc_backtest_4h_final")
con.execute("CREATE TABLE btc_backtest_4h_final AS SELECT * FROM results_df")
con.close()
print("\nSaved to analysis.db → btc_backtest_4h_final")

# ── 9. EQUITY CURVE ────────────────────────────────────────
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
    "ETH/USDT 4H — Mean Reversion BUY Only\nNext Bar Open Entry (No Confirmation)",
    fontsize=13
)

ax1.plot(equity["entry_date"], equity["cumulative_pnl"],
         color="orange", linewidth=2, marker="o", markersize=5)
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
plt.savefig("btc_equity_curve_4h_final.png", dpi=150, bbox_inches="tight")
plt.show()
print("\nChart saved as btc_equity_curve_4h_final.png")