import yfinance as yf
import pandas as pd
import duckdb
import matplotlib.pyplot as plt

# ── 1. FETCH DATA ──────────────────────────────────────────
# TIMEFRAME OPTIONS:
# Daily  → interval="1d" | rolling_mean=20  | ma=200  | cluster=5  bars | forward=20  bars
# Hourly → interval="1h" | rolling_mean=130 | ma=1300 | cluster=33 bars | forward=130 bars
# Current: HOURLY
raw = yf.download("ES=F", period="2y", interval="1h", auto_adjust=False)
raw.reset_index(inplace=True)

if isinstance(raw.columns, pd.MultiIndex):
    raw.columns = [col[0] for col in raw.columns]

data = raw[["Datetime", "Close", "High", "Low", "Open", "Volume"]].copy()
data.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]
data.dropna(inplace=True)
data.reset_index(drop=True, inplace=True)

print(f"Data loaded: {len(data)} hourly bars")
print(data.head(3))

# ── 2. CALCULATE INDICATORS ────────────────────────────────
data["prev_close"] = data["Close"].shift(1)
data["tr"] = data[["High", "Low", "prev_close"]].apply(
    lambda row: max(
        row["High"] - row["Low"],
        abs(row["High"] - row["prev_close"]),
        abs(row["prev_close"] - row["Low"])
    ), axis=1
)

# Hourly windows — scaled from daily
# Daily 14 ATR  → Hourly 14  (ATR adapts automatically via EWM)
# Daily 20 mean → Hourly 130 (approx 1 trading week)
# Daily 200 MA  → Hourly 1300 (approx 1 trading year)
data["atr_14"]       = data["tr"].ewm(alpha=1/14, adjust=False).mean().round(2)
data["rolling_mean"] = data["Close"].rolling(window=130).mean().round(2)
data["atr_distance"] = ((data["Close"] - data["rolling_mean"]) / data["atr_14"]).round(2)
data["ma_1300"]      = data["Close"].rolling(window=1300).mean().round(2)
data["trend"]        = (data["Close"] > data["ma_1300"]).map({True: "UP", False: "DOWN"})

# ── 3. GENERATE SIGNALS + CONFIRMATION FILTER ──────────────
# Step 1 — price must be 1.5 ATRs below rolling mean
# Step 2 — must be in uptrend (above 1300-bar MA)
# Step 3 — confirmation: next bar must close ABOVE this bar (bounce started)
THRESHOLD = 1.5
SLIPPAGE  = 2.0

data["next_close"] = data["Close"].shift(-1)
data["confirmed"]  = data["next_close"] > data["Close"]

data["signal"] = 0
data.loc[
    (data["atr_distance"] < -THRESHOLD) &
    (data["trend"] == "UP") &
    (data["confirmed"] == True),
    "signal"
] = 1

print(f"\nThreshold: -{THRESHOLD} ATR | Slippage: {SLIPPAGE} pts")

# ── 4. REMOVE SIGNAL CLUSTERS ─────────────────────────────
# 33 hourly bars ≈ 5 trading days minimum between signals
signals = data[data["signal"] != 0].copy()
signals["bars_since_last"] = signals.index.to_series().diff().fillna(999)
signals = signals[signals["bars_since_last"] >= 33].copy()

print(f"Total signals after filters: {len(signals)}")
print(signals[["Date", "Close", "atr_distance", "signal"]].head())

# ── 5. BACKTEST ENGINE ─────────────────────────────────────
FORWARD_BARS = 130
results = []

for i, row in signals.iterrows():
    entry      = row["Close"]
    atr        = row["atr_14"]
    signal     = row["signal"]
    entry_date = row["Date"]

    if signal == -1:
        stop_loss   = entry + (atr * 1.5)
        take_profit = entry - (atr * 3.0)
    else:
        stop_loss   = entry - (atr * 1.5)
        take_profit = entry + (atr * 3.0)

    outcome     = "TIMEOUT"
    loc         = data.index.get_loc(i)
    future_rows = data.iloc[loc+1 : loc+FORWARD_BARS+1]
    exit_price  = future_rows["Close"].iloc[-1] if len(future_rows) > 0 else entry
    exit_date   = future_rows["Date"].iloc[-1]  if len(future_rows) > 0 else entry_date
    future      = future_rows.copy()

    for _, frow in future.iterrows():
        if signal == -1:
            if frow["High"] >= stop_loss:
                outcome, exit_date = "LOSS", frow["Date"]
                exit_price = frow["Open"] if frow["Open"] > stop_loss else stop_loss
                break
            if frow["Low"] <= take_profit:
                outcome, exit_date = "WIN", frow["Date"]
                exit_price = frow["Open"] if frow["Open"] < take_profit else take_profit
                break
        else:
            if frow["Low"] <= stop_loss:
                outcome, exit_date = "LOSS", frow["Date"]
                exit_price = frow["Open"] if frow["Open"] < stop_loss else stop_loss
                break
            if frow["High"] >= take_profit:
                outcome, exit_date = "WIN", frow["Date"]
                exit_price = frow["Open"] if frow["Open"] > take_profit else take_profit
                break

    pnl = (entry - exit_price) if signal == -1 else (exit_price - entry)
    pnl = pnl - SLIPPAGE

    results.append({
        "entry_date":    entry_date,
        "exit_date":     exit_date,
        "signal":        "SELL" if signal == -1 else "BUY",
        "entry":         round(entry, 2),
        "stop_loss":     round(stop_loss, 2),
        "take_profit":   round(take_profit, 2),
        "exit":          round(exit_price, 2),
        "outcome":       outcome,
        "pnl_points":    round(pnl, 2)
    })

# ── 6. RESULTS ─────────────────────────────────────────────
results_df = pd.DataFrame(results)
wins    = results_df[results_df["outcome"] == "WIN"]
losses  = results_df[results_df["outcome"] == "LOSS"]
timeout = results_df[results_df["outcome"] == "TIMEOUT"]
win_rate = len(wins) / len(results_df) * 100

print("\n--- Backtest Results (hourly | confirmation + slippage + gap fix) ---")
print(f"Total Trades:   {len(results_df)}")
print(f"Wins:           {len(wins)}  ({win_rate:.1f}%)")
print(f"Losses:         {len(losses)}")
print(f"Timeouts:       {len(timeout)}")
print(f"\nAvg Win (pts):  {wins['pnl_points'].mean():.2f}")
print(f"Avg Loss (pts): {losses['pnl_points'].mean():.2f}")
print(f"Total P&L (pts):{results_df['pnl_points'].sum():.2f}")

# ── 7. EXPECTANCY ──────────────────────────────────────────
win_rate_d  = len(wins) / len(results_df)
loss_rate_d = 1 - win_rate_d
avg_win     = wins["pnl_points"].mean()
avg_loss    = abs(losses["pnl_points"].mean())
expectancy  = (win_rate_d * avg_win) - (loss_rate_d * avg_loss)

print(f"\nExpectancy: {expectancy:.2f} points per trade")
if expectancy > 0:
    print("✅ Edge SURVIVES on hourly data")
else:
    print("⚠️  Edge WIPED OUT — needs refinement")

print("\n--- Sample Wins ---")
print(wins[["entry_date", "signal", "entry", "exit", "pnl_points"]].head(3).to_string())
print("\n--- Sample Losses ---")
print(losses[["entry_date", "signal", "entry", "exit", "pnl_points"]].head(3).to_string())

# ── 8. SAVE RESULTS ────────────────────────────────────────
con = duckdb.connect("analysis.db")
con.execute("DROP TABLE IF EXISTS backtest_results_1h")
con.execute("CREATE TABLE backtest_results_1h AS SELECT * FROM results_df")
con.close()
print("\nSaved to analysis.db → backtest_results_1h")

# ── 9. EQUITY CURVE ────────────────────────────────────────
results_df["entry_date"]     = pd.to_datetime(results_df["entry_date"])
results_df["cumulative_pnl"] = results_df["pnl_points"].cumsum()

start_row = pd.DataFrame([{"entry_date": results_df["entry_date"].iloc[0], "cumulative_pnl": 0}])
equity    = pd.concat([start_row, results_df[["entry_date", "cumulative_pnl"]]], ignore_index=True)

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
fig.suptitle("ES Futures 1H — Mean Reversion Strategy\nEquity Curve & Trade Outcomes", fontsize=13)

ax1.plot(equity["entry_date"], equity["cumulative_pnl"],
         color="royalblue", linewidth=2, marker="o", markersize=5)
ax1.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
ax1.fill_between(equity["entry_date"], equity["cumulative_pnl"], 0,
                 where=(equity["cumulative_pnl"] >= 0), alpha=0.15, color="green", label="Profit")
ax1.fill_between(equity["entry_date"], equity["cumulative_pnl"], 0,
                 where=(equity["cumulative_pnl"] < 0), alpha=0.15, color="red", label="Drawdown")
ax1.set_ylabel("Cumulative P&L (points)")
ax1.set_title(f"Equity Curve | Expectancy: {expectancy:.2f} pts/trade | Win Rate: {win_rate:.1f}%")
ax1.legend()
ax1.grid(True, alpha=0.3)

colors = results_df["outcome"].map({"WIN": "green", "LOSS": "red", "TIMEOUT": "orange"})
ax2.bar(results_df["entry_date"], results_df["pnl_points"],
        color=colors, width=5, alpha=0.8)
ax2.axhline(y=0, color="gray", linestyle="--", linewidth=0.8)
ax2.set_ylabel("P&L per Trade (points)")
ax2.set_title("Individual Trade Results  [Green=Win | Red=Loss | Orange=Timeout]")
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("equity_curve_1h.png", dpi=150, bbox_inches="tight")
plt.show()
print("\nChart saved as equity_curve_1h.png")