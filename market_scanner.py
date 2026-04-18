import ccxt
import pandas as pd
import time

exchange = ccxt.binance()

print("Fetching all 565 pairs with volume data...")
print("This will take about 2 minutes...\n")

# Get all active USDT perpetual swaps
markets = exchange.load_markets()
symbols = [
    s for s, info in markets.items()
    if "/USDT:USDT" in s
    and info.get("active") == True
    and info.get("swap") == True
]

print(f"Found {len(symbols)} pairs. Fetching 24h stats...")

results = []

for i, symbol in enumerate(symbols):
    try:
        ticker = exchange.fetch_ticker(symbol)
        results.append({
            "symbol":        symbol.replace(":USDT", ""),
            "price":         ticker.get("last", 0),
            "volume_24h_usd": round((ticker.get("quoteVolume", 0)), 0),
            "change_24h":    round(ticker.get("percentage", 0), 2),
            "high_24h":      ticker.get("high", 0),
            "low_24h":       ticker.get("low", 0),
        })

        if (i+1) % 50 == 0:
            print(f"  Processed {i+1}/{len(symbols)}...")

        time.sleep(0.1)

    except Exception as e:
        print(f"  Error on {symbol}: {e}")
        continue

# Build dataframe
df = pd.DataFrame(results)
df = df.sort_values("volume_24h_usd", ascending=False)
df.reset_index(drop=True, inplace=True)
df.index += 1  # start from 1

# Save to CSV
df.to_csv("market_data.csv", index=True)

# Print top 50
print("\n" + "="*75)
print(" TOP 50 PAIRS BY 24H VOLUME")
print("="*75)
print(f"{'#':<4} {'Symbol':<20} {'Price':<15} {'24h Vol (USD)':<20} {'24h Change'}")
print("-"*75)

for idx, row in df.head(50).iterrows():
    vol = f"${row['volume_24h_usd']:,.0f}"
    chg = f"{row['change_24h']:+.2f}%"
    print(f"{idx:<4} {row['symbol']:<20} ${row['price']:<14} {vol:<20} {chg}")

print(f"\nFull list saved to market_data.csv")
print(f"\nTier suggestions:")
print(f"Tier 1 (top 20  by volume) → monitor every 15 mins")
print(f"Tier 2 (top 21-100)        → monitor every 1 hour")
print(f"Tier 3 (rest)              → monitor every 4 hours")