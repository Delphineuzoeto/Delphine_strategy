import yfinance as yf
import pandas as pd
import duckdb

# ── 1. FETCH DATA ──────────────────────────────────────────
data = yf.download("ES=F", period="2y", interval="1d")
data.reset_index(inplace=True)
data.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]

# ── 2. CALCULATE CLOSE POSITION ────────────────────────────
# Measures where price closed relative to the day's range
# 0.0 = closed at the low (sellers dominant)
# 1.0 = closed at the high (buyers dominant)
data["close_position"] = (data["Close"] - data["Low"]) / (data["High"] - data["Low"])
data["close_position"] = data["close_position"].round(2)

# ── 3. SAVE TO DATABASE ────────────────────────────────────
con = duckdb.connect("analysis.db")
con.execute("DROP TABLE IF EXISTS es_futures")
con.execute("CREATE TABLE es_futures AS SELECT * FROM data")
con.close()

# ── 4. PRE-EVENT ANALYSIS ──────────────────────────────────
# Find the most extreme seller day in 2 years
worst_day = data.loc[data["close_position"].idxmin(), "Date"]
idx = data[data["Date"] == worst_day].index[0]

print(f"Worst seller day: {worst_day.date()}")
print(data.loc[idx-3:idx, ["Date", "Close", "High", "Low", "Volume", "close_position"]])
print(f"\nAverage close position (2yr): {data['close_position'].mean().round(2)}")

