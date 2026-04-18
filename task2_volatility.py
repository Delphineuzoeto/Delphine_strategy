import yfinance as yf
import pandas as pd
import duckdb




# --------------------------------------------------------------
# ATR    → tells you how much the market normally moves
# ATR    → helps you set your stop loss beyond the noise
# 1% rule → tells you how much cash to risk per trade
# Together → they tell you exactly how many contracts to trade



# ── 1. FETCH DATA ──────────────────────────────────────────
data = yf.download("ES=F", period="2y", interval="1d")
data.reset_index(inplace=True)
data.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]

# ── 2. CALCULATE TRUE RANGE ────────────────────────────────
data["prev_close"] = data["Close"].shift(1)

data["tr"] = data[["High", "Low", "prev_close"]].apply(
    lambda row: max(
        row["High"] - row["Low"],
        abs(row["High"] - row["prev_close"]),
        abs(row["prev_close"] - row["Low"])
    ), axis=1
)

# ── 3. CALCULATE 14-DAY ATR ────────────────────────────────
data["atr_14"] = data["tr"].rolling(window=14).mean().round(2)

# ── 4. SAVE TO DATABASE ────────────────────────────────────
con = duckdb.connect("analysis.db")
con.execute("DROP TABLE IF EXISTS es_volatility")
con.execute("CREATE TABLE es_volatility AS SELECT * FROM data")
con.close()

# ── 5. PRINT RESULTS ───────────────────────────────────────
# print(data[["Date", "Close", "tr", "atr_14"]].tail(10))
# print(f"\nCurrent ATR (latest day): {data['atr_14'].iloc[-1]}")
# print(f"Average ATR over 2 years: {data['atr_14'].mean().round(2)}")

# ── 6. RISK CALCULATOR ─────────────────────────────────────
def position_risk(cash, leverage, atr, asset_price):
    wipeout_pct = 100 / leverage
    
    # Wipeout points based on asset price, not cash
    wipeout_points = asset_price / leverage
    atr_moves_to_wipeout = wipeout_points / atr
    
    print(f"\n--- Position Risk Analysis ---")
    print(f"Cash: ${cash} | Leverage: {leverage}x")
    print(f"Asset Price: {asset_price}")
    print(f"Wipeout at: {wipeout_pct}% move = {wipeout_points:.1f} points")
    print(f"Current ATR: {atr} points")
    print(f"ATR moves to wipeout: {atr_moves_to_wipeout:.1f} days")
    
    if atr_moves_to_wipeout < 3:
        print("⚠️  DANGER: Normal market noise can wipe you out in under 3 days")
    elif atr_moves_to_wipeout < 10:
        print("⚠️  CAUTION: Moderate buffer, high leverage still risky")
    else:
        print("✅ Reasonable buffer from normal noise")

current_atr = round(data["atr_14"].iloc[-1], 2)
current_price = round(data["Close"].iloc[-1], 2)

position_risk(cash=1000, leverage=10, atr=current_atr, asset_price=current_price)
position_risk(cash=1000, leverage=50, atr=current_atr, asset_price=current_price)

# ── 7. RISK OF RUIN CALCULATOR (INDUSTRY STANDARD) ────────
def risk_of_ruin_standard(cash, risk_per_trade_pct):
    balance = cash
    print(f"\n--- Risk Analysis: {risk_per_trade_pct}% Risk per Trade ---")
    print(f"Starting Cash: ${cash}")
    
    for i in range(1, 21):
        # Always calculate risk from CURRENT balance — the safety brake
        current_risk_amount = balance * (risk_per_trade_pct / 100)
        balance -= current_risk_amount
        
        # Print first 5 then every 5th — keeps output clean
        if i <= 5 or i % 5 == 0:
            print(f"  Loss {i}: ${balance:.2f} remaining (Risked ${current_risk_amount:.2f})")
    
    if balance < cash * 0.5:
        print(f"⚠️  FINAL: ${balance:.2f} — lost over 50%. Recovery is statistically unlikely.")
    else:
        print(f"✅ FINAL: ${balance:.2f} — significant drawdown but recoverable.")

risk_of_ruin_standard(cash=1000, risk_per_trade_pct=1)
risk_of_ruin_standard(cash=1000, risk_per_trade_pct=10)


# Add this to your script and run it
def moon_vs_ruin(cash, leverage, atr, asset_price):
    wipeout_points = asset_price / leverage
    moon_points = wipeout_points * 3  # 3x your wipeout distance
    
    atr_to_wipeout = wipeout_points / atr
    atr_to_moon = moon_points / atr
    
    print(f"\n--- {leverage}x Leverage: Moon vs Ruin ---")
    print(f"Points to wipeout: {wipeout_points:.1f} ({atr_to_wipeout:.1f} ATR moves)")
    print(f"Points to 3x profit: {moon_points:.1f} ({atr_to_moon:.1f} ATR moves)")
    print(f"Ruin is {atr_to_moon/atr_to_wipeout:.0f}x closer than the moon")

current_atr = round(data["atr_14"].iloc[-1], 2)
current_price = round(data["Close"].iloc[-1], 2)

moon_vs_ruin(cash=1000, leverage=10, atr=current_atr, asset_price=current_price)
moon_vs_ruin(cash=1000, leverage=50, atr=current_atr, asset_price=current_price)