import ccxt
import pandas as pd
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import requests

# ── LOAD ENV VARIABLES ─────────────────────────────────────
load_dotenv()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ── PROVEN CONFIG ──────────────────────────────────────────
MEAN_WINDOW    = 33
MA_WINDOW      = 325
THRESHOLD      = 1.5
BREAKOUT_BARS  = 8

WATCHLIST = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "DOGE/USDT",
    "WIF/USDT",
    "PEPE/USDT",
    "SUI/USDT",
    "LINK/USDT",
]

exchange = ccxt.binance()

# ── TELEGRAM SENDER ────────────────────────────────────────
def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, data=data)
        if response.status_code == 200:
            print(f"  📱 Telegram alert sent!")
        else:
            print(f"  ⚠️ Telegram error: {response.text}")
    except Exception as e:
        print(f"  ⚠️ Telegram failed: {e}")

def format_signal_message(symbol, direction, strategy, dist,
                           entry, sl, tp, risk, reward):
    emoji   = "🟢" if direction == "LONG" else "🔴"
    arrow   = "📈" if direction == "LONG" else "📉"
    now     = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""
{emoji} <b>{strategy} SIGNAL — {direction} {symbol}</b> {arrow}

💰 <b>Entry:</b>       {entry}
🛑 <b>Stop Loss:</b>   {sl}  (risk: {risk})
🎯 <b>Take Profit:</b> {tp}  (reward: {reward})
📊 <b>R:R Ratio:</b>   1:2.0
📏 <b>ATR Dist:</b>    {dist:.2f}
⏰ <b>Time:</b>        {now}

⚠️ Set SL immediately after entry
⏰ Signal expires in 4 hours
🔔 Powered by AfriMetrics
"""

# ── DATA FETCHING ──────────────────────────────────────────
def fetch_recent_data(symbol, timeframe, bars=1500):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=bars)
    data  = pd.DataFrame(ohlcv, columns=["timestamp", "Open", "High", "Low", "Close", "Volume"])
    data["Date"] = pd.to_datetime(data["timestamp"], unit="ms")
    return data[["Date", "Open", "High", "Low", "Close", "Volume"]]

# ── INDICATORS ─────────────────────────────────────────────
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
    data["confirmed_buy"]  = data["Close"] > data["prev_close"]
    data["confirmed_sell"] = data["Close"] < data["prev_close"]
    return data

# ── PRICE FORMATTER ────────────────────────────────────────
def format_price(price):
    if price < 0.0001:
        return f"${price:.8f}"
    elif price < 0.01:
        return f"${price:.6f}"
    elif price < 1:
        return f"${price:.4f}"
    else:
        return f"${price:,.2f}"

# ── BREAKOUT CHECK ─────────────────────────────────────────
def check_breakout(data_4h):
    recent        = data_4h.tail(BREAKOUT_BARS)
    all_stretched = (recent["atr_distance"] > THRESHOLD).all()
    trending_up   = recent["Close"].iloc[-1] > recent["Close"].iloc[0]
    last_green    = recent["Close"].iloc[-1] > recent["Close"].iloc[-2]
    return all_stretched and trending_up and last_green

# ── SIGNAL CHECKER ─────────────────────────────────────────
def check_signal(symbol):
    print(f"\n  [{symbol}]")

    try:
        # 4H setup
        data_4h = fetch_recent_data(symbol, timeframe="4h", bars=1500)
        data_4h = calculate_indicators(data_4h)
        data_4h.dropna(inplace=True)
        data_4h.reset_index(drop=True, inplace=True)

        setup = data_4h.iloc[-1]
        price = setup["Close"]
        atr   = setup["atr_14"]
        dist  = round(setup["atr_distance"], 2)
        trend = setup["trend"]

        print(f"  4H → Price: {format_price(price)} | ATR: {format_price(atr)} | Dist: {dist:.2f} | Trend: {trend}")

        # Strategy conditions
        buy_setup    = dist < -THRESHOLD and trend == "UP"
        sell_setup   = dist >  THRESHOLD and trend == "DOWN"
        breakout_buy = check_breakout(data_4h)

        # 15m trigger
        data_15m = fetch_recent_data(symbol, timeframe="15m", bars=100)
        latest   = data_15m.iloc[-1]
        prev     = data_15m.iloc[-2]
        turn_up   = latest["Close"] > prev["Close"]
        turn_down = latest["Close"] < prev["Close"]

        print(f"  15m → Price: {format_price(latest['Close'])} | Turn up: {turn_up} | Turn down: {turn_down}")

        # ── MEAN REVERSION BUY ─────────────────────────────
        if buy_setup and turn_up:
            sl     = price - (atr * 1.5)
            tp     = price + (atr * 3.0)
            risk   = price - sl
            entry  = format_price(latest["Close"])

            print(f"  {'='*45}")
            print(f"  SIGNAL — LONG {symbol} [MEAN REVERSION]")
            print(f"  Entry: {entry} | SL: {format_price(sl)} | TP: {format_price(tp)}")
            print(f"  {'='*45}")

            msg = format_signal_message(
                symbol, "LONG", "MEAN REVERSION", dist,
                entry, format_price(sl), format_price(tp),
                format_price(risk), format_price(risk*2)
            )
            send_telegram(msg)
            return "BUY_REVERSION"

        # ── MEAN REVERSION SELL ────────────────────────────
        elif sell_setup and turn_down:
            sl    = price + (atr * 1.5)
            tp    = price - (atr * 3.0)
            risk  = sl - price
            entry = format_price(latest["Close"])

            print(f"  {'='*45}")
            print(f"  SIGNAL — SHORT {symbol} [MEAN REVERSION]")
            print(f"  Entry: {entry} | SL: {format_price(sl)} | TP: {format_price(tp)}")
            print(f"  {'='*45}")

            msg = format_signal_message(
                symbol, "SHORT", "MEAN REVERSION", dist,
                entry, format_price(sl), format_price(tp),
                format_price(risk), format_price(risk*2)
            )
            send_telegram(msg)
            return "SELL_REVERSION"

        # ── BREAKOUT BUY ───────────────────────────────────
        elif breakout_buy and turn_up and trend == "UP":
            sl    = price - (atr * 2.0)
            tp    = price + (atr * 4.0)
            risk  = price - sl
            entry = format_price(latest["Close"])

            print(f"  {'='*45}")
            print(f"  SIGNAL — LONG {symbol} [BREAKOUT]")
            print(f"  Entry: {entry} | SL: {format_price(sl)} | TP: {format_price(tp)}")
            print(f"  {'='*45}")

            msg = format_signal_message(
                symbol, "LONG", "BREAKOUT", dist,
                entry, format_price(sl), format_price(tp),
                format_price(risk), format_price(risk*2)
            )
            send_telegram(msg)
            return "BUY_BREAKOUT"

        else:
            recent_dists   = data_4h["atr_distance"].tail(BREAKOUT_BARS)
            bars_stretched = (recent_dists > THRESHOLD).sum()

            if trend == "UP":
                gap = round(dist - (-THRESHOLD), 2)
                print(f"  Reversion:  Need dist < -{THRESHOLD} (gap: {gap:.2f})")
                print(f"  Breakout:   {bars_stretched}/{BREAKOUT_BARS} bars stretched above mean")
            else:
                gap = round(THRESHOLD - dist, 2)
                print(f"  Reversion:  Need dist > +{THRESHOLD} (gap: {gap:.2f})")
                print(f"  Breakout:   N/A — trend is DOWN")
            return None

    except Exception as e:
        print(f"  Error fetching {symbol}: {e}")
        return None

# ── MAIN MONITOR ───────────────────────────────────────────
def run_monitor():
    print("="*55)
    print(" LIVE SIGNAL MONITOR — DUAL STRATEGY")
    print(f" Watching:    {', '.join(WATCHLIST)}")
    print(f" Strategy A:  Mean Reversion | Threshold: ±{THRESHOLD}")
    print(f" Strategy B:  Breakout | {BREAKOUT_BARS} bars above mean")
    print(f" Telegram:    Connected ✅")
    print("="*55)
    print("Press Ctrl+C to stop.\n")

    # Send startup message
    send_telegram(f"🤖 <b>AfriMetrics Signal Monitor Started</b>\n\nWatching {len(WATCHLIST)} pairs\nTimeframe: 4H setup + 15m trigger\nChecking every 15 minutes\n\n✅ Ready to send signals!")

    while True:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scanning {len(WATCHLIST)} pairs...")

        signals_found = 0
        for symbol in WATCHLIST:
            result = check_signal(symbol)
            if result:
                signals_found += 1
            time.sleep(1)

        if signals_found == 0:
            print(f"\n  No signals. Next check in 15 minutes.")
        else:
            print(f"\n  {signals_found} signal(s) found! Telegram alerts sent.")

        time.sleep(900)

run_monitor()