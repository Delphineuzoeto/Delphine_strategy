import ccxt
import pandas as pd
import numpy as np
import talib
import time
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MEAN_WINDOW   = 33
MA_WINDOW     = 325
THRESHOLD     = 1.5
BREAKOUT_BARS = 8

def get_full_watchlist():
    print("  Fetching all active Binance USDT futures...")
    markets = exchange.load_markets()
    
    watchlist = [
        symbol for symbol, info in markets.items()
        if "/USDT" in symbol
        and info.get("active") == True
        and info.get("future") == True
        and info.get("type") == "future"
    ]
    
    watchlist.sort()
    print(f"  Found {len(watchlist)} active USDT futures pairs")
    return watchlist

# Replace hardcoded WATCHLIST with dynamic one
WATCHLIST = get_full_watchlist()

exchange = ccxt.binance()

def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML"
        }
        requests.post(url, data=data)
        print(f"  Telegram sent!")
    except Exception as e:
        print(f"  Telegram failed: {e}")

def format_signal_message(symbol, direction, strategy,
                           dist, entry, sl, tp, risk, reward,
                           rsi, macd_hist):
    emoji = "🟢" if direction == "LONG" else "🔴"
    arrow = "📈" if direction == "LONG" else "📉"
    now   = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""
{emoji} <b>{strategy} SIGNAL — {direction} {symbol}</b> {arrow}

💰 <b>Entry:</b>       {entry}
🛑 <b>Stop Loss:</b>   {sl}  (risk: {risk})
🎯 <b>Take Profit:</b> {tp}  (reward: {reward})
📊 <b>R:R Ratio:</b>   1:2.0
📏 <b>ATR Dist:</b>    {dist:.2f}
📉 <b>RSI:</b>         {rsi:.1f}
📈 <b>MACD Hist:</b>   {macd_hist:.4f}
⏰ <b>Time:</b>        {now}

⚠️ Set SL immediately after entry
⏰ Signal expires in 4 hours
🔔 Powered by AfriMetrics
"""

def fetch_recent_data(symbol, timeframe, bars=1500):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=bars)
    data  = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "Open", "High", "Low", "Close", "Volume"]
    )
    data["Date"] = pd.to_datetime(data["timestamp"], unit="ms")
    return data[["Date", "Open", "High", "Low", "Close", "Volume"]]

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
    data["trend"]        = (data["Close"] > data["ma_long"]).map(
        {True: "UP", False: "DOWN"}
    )
    data["confirmed_buy"]  = data["Close"] > data["prev_close"]
    data["confirmed_sell"] = data["Close"] < data["prev_close"]

    # ── TA-LIB INDICATORS ──────────────────────────────────
    closes = data["Close"].values.astype(float)

    # RSI — momentum strength
    data["rsi"] = talib.RSI(closes, timeperiod=14)

    # MACD — momentum direction
    macd, signal_line, histogram = talib.MACD(
        closes, fastperiod=12, slowperiod=26, signalperiod=9
    )
    data["macd"]      = macd
    data["macd_sig"]  = signal_line
    data["macd_hist"] = histogram

    # Bollinger Bands — volatility
    upper, middle, lower = talib.BBANDS(
        closes, timeperiod=20, nbdevup=2, nbdevdn=2
    )
    data["bb_upper"]  = upper
    data["bb_middle"] = middle
    data["bb_lower"]  = lower

    return data

def format_price(price):
    if price < 0.0001:
        return f"${price:.8f}"
    elif price < 0.01:
        return f"${price:.6f}"
    elif price < 1:
        return f"${price:.4f}"
    else:
        return f"${price:,.2f}"

def check_breakout(data_4h):
    recent        = data_4h.tail(BREAKOUT_BARS)
    all_stretched = (recent["atr_distance"] > THRESHOLD).all()
    trending_up   = recent["Close"].iloc[-1] > recent["Close"].iloc[0]
    last_green    = recent["Close"].iloc[-1] > recent["Close"].iloc[-2]
    return all_stretched and trending_up and last_green

def check_signal(symbol):
    print(f"\n  [{symbol}]")
    try:
        data_4h = fetch_recent_data(symbol, timeframe="4h", bars=1500)
        data_4h = calculate_indicators(data_4h)
        data_4h.dropna(inplace=True)
        data_4h.reset_index(drop=True, inplace=True)

        setup     = data_4h.iloc[-1]
        price     = setup["Close"]
        atr       = setup["atr_14"]
        dist      = round(setup["atr_distance"], 2)
        trend     = setup["trend"]
        rsi       = setup["rsi"]
        macd_hist = setup["macd_hist"]

        print(f"  4H → {format_price(price)} | Dist: {dist} | Trend: {trend} | RSI: {rsi:.1f} | MACD: {macd_hist:.4f}")

        buy_setup    = dist < -THRESHOLD and trend == "UP"
        sell_setup   = dist >  THRESHOLD and trend == "DOWN"
        breakout_buy = check_breakout(data_4h)

        # TA-Lib filters
        rsi_not_overbought  = rsi < 65       # not overbought for buys
        rsi_not_oversold    = rsi > 35       # not oversold for sells
        macd_bullish        = macd_hist > 0  # positive momentum for buys
        macd_bearish        = macd_hist < 0  # negative momentum for sells

        data_15m = fetch_recent_data(symbol, timeframe="15m", bars=100)
        latest   = data_15m.iloc[-1]
        prev     = data_15m.iloc[-2]
        turn_up   = latest["Close"] > prev["Close"]
        turn_down = latest["Close"] < prev["Close"]

        # ── MEAN REVERSION BUY ─────────────────────────────
        if buy_setup and turn_up and rsi_not_overbought:
            sl    = price - (atr * 1.5)
            tp    = price + (atr * 3.0)
            risk  = price - sl
            entry = format_price(latest["Close"])
            msg   = format_signal_message(
                symbol, "LONG", "MEAN REVERSION", dist,
                entry, format_price(sl), format_price(tp),
                format_price(risk), format_price(risk*2),
                rsi, macd_hist
            )
            send_telegram(msg)
            return "BUY_REVERSION"

        # ── MEAN REVERSION SELL ────────────────────────────
        elif sell_setup and turn_down and rsi_not_oversold:
            sl    = price + (atr * 1.5)
            tp    = price - (atr * 3.0)
            risk  = sl - price
            entry = format_price(latest["Close"])
            msg   = format_signal_message(
                symbol, "SHORT", "MEAN REVERSION", dist,
                entry, format_price(sl), format_price(tp),
                format_price(risk), format_price(risk*2),
                rsi, macd_hist
            )
            send_telegram(msg)
            return "SELL_REVERSION"

        # ── BREAKOUT BUY ───────────────────────────────────
        elif breakout_buy and turn_up and trend == "UP" and rsi_not_overbought and macd_bullish:
            sl    = price - (atr * 2.0)
            tp    = price + (atr * 4.0)
            risk  = price - sl
            entry = format_price(latest["Close"])
            msg   = format_signal_message(
                symbol, "LONG", "BREAKOUT", dist,
                entry, format_price(sl), format_price(tp),
                format_price(risk), format_price(risk*2),
                rsi, macd_hist
            )
            send_telegram(msg)
            return "BUY_BREAKOUT"

        else:
            recent_dists   = data_4h["atr_distance"].tail(BREAKOUT_BARS)
            bars_stretched = (recent_dists > THRESHOLD).sum()
            print(f"  No signal | Breakout: {bars_stretched}/{BREAKOUT_BARS} | RSI: {rsi:.1f} | MACD: {macd_hist:.4f}")
            return None

    except Exception as e:
        print(f"  Error: {e}")
        return None

# ── RUN CONTINUOUSLY ───────────────────────────────────────
print("="*55)
print(" AFRIMETRICS SIGNAL MONITOR")
print(f" Watching:  {len(WATCHLIST)} pairs")
print(f" Filters:   ATR + RSI + MACD + 15m confirmation")
print(f" Telegram:  Connected ✅")
print("="*55)
print("Press Ctrl+C to stop.\n")

send_telegram(
    "🤖 <b>AfriMetrics Monitor Started</b>\n\n"
    f"👀 Watching {len(WATCHLIST)} pairs\n"
    "🔬 Filters: ATR + RSI + MACD + 15m\n"
    "✅ Telegram alerts active!"
)

while True:
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scanning...")

    signals_found = 0
    for symbol in WATCHLIST:
        result = check_signal(symbol)
        if result:
            signals_found += 1
        time.sleep(1)

    if signals_found == 0:
        print(f"\n  No signals. Next check in 15 minutes.")
    else:
        print(f"\n  {signals_found} signal(s) found! Check Telegram.")

    time.sleep(900)