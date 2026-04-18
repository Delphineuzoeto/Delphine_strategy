import yfinance as yf
import pandas as pd
import duckdb

# ── 1. FETCH DATA ──────────────────────────────────────────
data = yf.download("ES=F", period="2y", interval="1d")
data.reset_index(inplace=True)
data.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]

# ── 2. CALCULATE ATR ───────────────────────────────────────
data["prev_close"] = data["Close"].shift(1)
data["tr"] = data[["High", "Low", "prev_close"]].apply(
    lambda row: max(
        row["High"] - row["Low"],
        abs(row["High"] - row["prev_close"]),
        abs(row["prev_close"] - row["Low"])
    ), axis=1
)
data["atr_14"] = data["tr"].rolling(window=14).mean().round(2)

# ── 3. CALCULATE 20-DAY ROLLING MEAN ──────────────────────
data["rolling_mean"] = data["Close"].rolling(window=20).mean().round(2)

# ── 4. CALCULATE DISTANCE FROM MEAN IN ATR UNITS ──────────
data["atr_distance"] = ((data["Close"] - data["rolling_mean"]) / data["atr_14"]).round(2)

# ── 5. GENERATE SIGNALS ────────────────────────────────────
# If price is more than 2 ATRs BELOW the mean → BUY (expect snapback up)
# If price is more than 2 ATRs ABOVE the mean → SELL (expect snapback down)
data["signal"] = 0
data.loc[data["atr_distance"] < -2.0, "signal"] = 1   # Buy
data.loc[data["atr_distance"] > 2.0, "signal"] = -1   # Sell

# ── 6. SHOW SIGNALS FOUND ─────────────────────────────────
signals_found = data[data["signal"] != 0]

print(data[["Date", "Close", "rolling_mean", "atr_14", "atr_distance"]].tail(10))
print("\n--- Strategy Signals Found ---")
print(signals_found[["Date", "Close", "atr_distance", "signal"]])

print(f"\nTotal signals:  {len(signals_found)}")
print(f"Buy signals:    {len(signals_found[signals_found['signal'] == 1])}")
print(f"Sell signals:   {len(signals_found[signals_found['signal'] == -1])}")


# ── 7. CALCULATE ENTRY, STOP LOSS AND TAKE PROFIT ─────────
def trade_levels(row, signal, atr_multiplier_sl=1.5, atr_multiplier_tp=3.0):
    entry = row["Close"]
    atr = row["atr_14"]
    
    if signal == -1:  # SELL
        stop_loss   = entry + (atr * atr_multiplier_sl)  # SL above entry
        take_profit = entry - (atr * atr_multiplier_tp)  # TP below entry
    elif signal == 1:  # BUY
        stop_loss   = entry - (atr * atr_multiplier_sl)  # SL below entry
        take_profit = entry + (atr * atr_multiplier_tp)  # TP above entry
    
    return pd.Series({
        "entry":       round(entry, 2),
        "stop_loss":   round(stop_loss, 2),
        "take_profit": round(take_profit, 2),
        "risk_points": round(abs(entry - stop_loss), 2),
        "reward_points": round(abs(entry - take_profit), 2),
        "rr_ratio":    round(atr_multiplier_tp / atr_multiplier_sl, 2)
    })

# Apply to all signals
levels = signals_found.apply(lambda row: trade_levels(row, row["signal"]), axis=1)
signals_with_levels = pd.concat([signals_found[["Date", "Close", "atr_distance", "signal"]], levels], axis=1)

print("\n--- Live Trade Levels ---")
print(signals_with_levels.tail(5).to_string())

# Today's live signal
latest = signals_with_levels.iloc[-1]
print(f"\n{'='*50}")
print(f"TODAY'S SIGNAL — {latest['Date'].date()}")
print(f"Direction:   {'SELL 🔴' if latest['signal'] == -1 else 'BUY 🟢'}")
print(f"Entry:       {latest['entry']}")
print(f"Stop Loss:   {latest['stop_loss']}  (+{latest['risk_points']} points)")
print(f"Take Profit: {latest['take_profit']}  (-{latest['reward_points']} points)")
print(f"R:R Ratio:   1:{latest['rr_ratio']}")
print(f"{'='*50}")


# ── 8. BINANCE POSITION SIZER ──────────────────────────────
cash = 50.00  # Your wallet balance
risk_pct = 2.0  # Risk only 2% of your cash ($1.00) per trade

risk_amount_dollars = cash * (risk_pct / 100)

# Calculate how many "units" to buy/sell
# Position Size = Risk Amount / Risk per Unit
units_to_trade = risk_amount_dollars / (latest['risk_points'])

print(f"\n--- Binance Order Instructions ---")
print(f"To risk only ${risk_amount_dollars:.2f} (2% of wallet):")
print(f"1. Set Leverage to: {round((latest['entry'] * units_to_trade) / cash, 1)}x")
print(f"2. Order Size: {units_to_trade:.4f} units")

btc = yf.download("BTC-USD", period="1mo", interval="1d")
btc.reset_index(inplace=True)
btc.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]
btc["prev_close"] = btc["Close"].shift(1)
btc["tr"] = btc[["High", "Low", "prev_close"]].apply(
    lambda row: max(
        row["High"] - row["Low"],
        abs(row["High"] - row["prev_close"]),
        abs(row["prev_close"] - row["Low"])
    ), axis=1
)
print(f"BTC 14-day ATR: {btc['tr'].rolling(14).mean().iloc[-1]:.0f}")

# ── 9. SAVE TO DATABASE ────────────────────────────────────
con = duckdb.connect("analysis.db")
con.execute("DROP TABLE IF EXISTS es_strategy")
con.execute("CREATE TABLE es_strategy AS SELECT * FROM data")
con.close()

print("\nSaved to analysis.db → es_strategy table")