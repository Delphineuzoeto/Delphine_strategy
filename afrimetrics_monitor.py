import ccxt
import pandas as pd
import numpy as np
import talib
import time
import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ── CONFIG ─────────────────────────────────────────────────
MEAN_WINDOW     = 33
MA_WINDOW       = 325
THRESHOLD       = 1.5
BREAKOUT_BARS   = 8
SIGNAL_EXPIRY   = 14400     # 4 hours in seconds
LOG_FILE        = "signals_log.csv"
ACCOUNT_BALANCE = 1000      # UPDATE to your real balance
RISK_PER_TRADE  = 0.01      # 1% risk per trade

FG_EXTREME_FEAR  = 25
FG_EXTREME_GREED = 75

WATCHLIST = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "DOGE/USDT:USDT",
    "XAU/USDT:USDT",
    "ZEC/USDT:USDT",
    "1000PEPE/USDT:USDT",
    "BNB/USDT:USDT",
    "HYPE/USDT:USDT",
    "ENA/USDT:USDT",
    "SUI/USDT:USDT",
    "AVAX/USDT:USDT",
    "LINK/USDT:USDT",
    "DOT/USDT:USDT",
    "NEAR/USDT:USDT",
    "AAVE/USDT:USDT",
    "TRUMP/USDT:USDT",
]

exchange = ccxt.binance({
    "options": {"defaultType": "future"}
})

# ── STATE ──────────────────────────────────────────────────
active_trades    = {}
last_signal_time = {}

# ── LOGGING ────────────────────────────────────────────────
def init_log():
    if not os.path.isfile(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp", "symbol", "setup", "direction",
                "entry", "sl", "tp", "fg_score",
                "position_size", "result", "exit_price", "pnl_usd"
            ])

def log_signal(symbol, setup, direction, entry, sl, tp, fg_score, position_size):
    with open(LOG_FILE, "a", newline="") as f:
        csv.writer(f).writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            symbol.replace(":USDT", ""),
            setup, direction,
            round(entry, 6), round(sl, 6), round(tp, 6),
            fg_score, round(position_size, 2),
            "OPEN", "", ""
        ])

def update_log(symbol, result, exit_price):
    clean = symbol.replace(":USDT", "")
    rows  = []
    try:
        with open(LOG_FILE, "r") as f:
            rows = list(csv.reader(f))

        for i in reversed(range(len(rows))):
            if rows[i][1] == clean and rows[i][9] == "OPEN":
                entry     = float(rows[i][4])
                pos_size  = float(rows[i][8])
                direction = rows[i][3]

                if direction == "LONG":
                    pnl = (exit_price - entry) / entry * pos_size
                else:
                    pnl = (entry - exit_price) / entry * pos_size

                rows[i][9]  = result
                rows[i][10] = round(exit_price, 6)
                rows[i][11] = round(pnl, 2)
                break

        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerows(rows)

    except Exception as e:
        print(f"  Log update failed: {e}")

def get_performance_stats():
    try:
        with open(LOG_FILE, "r") as f:
            rows = [r for r in csv.DictReader(f) if r["result"] != "OPEN"]

        if not rows:
            return None

        wins     = [r for r in rows if r["result"] == "WIN"]
        losses   = [r for r in rows if r["result"] == "LOSS"]
        total    = len(rows)
        win_rate = len(wins) / total * 100 if total > 0 else 0
        total_pnl = sum(float(r["pnl_usd"]) for r in rows)

        return {
            "total":     total,
            "wins":      len(wins),
            "losses":    len(losses),
            "win_rate":  round(win_rate, 1),
            "total_pnl": round(total_pnl, 2)
        }
    except:
        return None

# ── POSITION SIZING ────────────────────────────────────────
def calculate_position_size(entry, sl):
    risk_amount   = ACCOUNT_BALANCE * RISK_PER_TRADE
    risk_per_unit = abs(entry - sl)
    if risk_per_unit == 0:
        return 0
    position_size = (risk_amount / risk_per_unit) * entry
    return min(position_size, ACCOUNT_BALANCE * 10)

# ── FEAR & GREED ───────────────────────────────────────────
def get_fear_greed():
    try:
        data  = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        score = int(data["data"][0]["value"])
        label = data["data"][0]["value_classification"]
        return score, label
    except:
        return 50, "Neutral"

# ── TELEGRAM ───────────────────────────────────────────────
def send_telegram(message):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       message,
                "parse_mode": "HTML"
            }
        )
        print(f"  Telegram sent!")
    except Exception as e:
        print(f"  Telegram failed: {e}")

def format_signal_message(symbol, direction, strategy, dist,
                           entry, sl, tp, risk, reward,
                           rsi, macd_hist, fg_score, fg_label,
                           breakeven, vol_spike, position_size):
    emoji     = "🟢" if direction == "LONG" else "🔴"
    arrow     = "📈" if direction == "LONG" else "📉"
    now       = datetime.now().strftime("%Y-%m-%d %H:%M")
    clean_sym = symbol.replace(":USDT", "")

    if fg_score <= 25:   fg_emoji = "😱"
    elif fg_score <= 45: fg_emoji = "😨"
    elif fg_score <= 55: fg_emoji = "😐"
    elif fg_score <= 75: fg_emoji = "😊"
    else:                fg_emoji = "🤑"

    vol_str = "🔥 SPIKE" if vol_spike else "normal"

    return f"""
{emoji} <b>{strategy} — {direction} {clean_sym}</b> {arrow}

💰 <b>Entry:</b>         {entry}
🛑 <b>Stop Loss:</b>     {sl}
🎯 <b>Take Profit:</b>   {tp}
📊 <b>R:R:</b>           1:2.0
💼 <b>Position Size:</b> ${position_size:,.2f}
📏 <b>ATR Dist:</b>      {dist:.2f}
📉 <b>RSI:</b>           {rsi:.1f}
📈 <b>MACD:</b>          {macd_hist:.6f}
📦 <b>Volume:</b>        {vol_str}
{fg_emoji} <b>F&G:</b>   {fg_score} ({fg_label})
⏰ <b>Time:</b>          {now}

💡 <b>Breakeven:</b> Move SL → {entry} when price hits {breakeven}
⚠️ Set SL immediately | Expires in 4H
🔔 <b>AfriMetrics</b>
"""

# ── DATA & INDICATORS ──────────────────────────────────────
def fetch_recent_data(symbol, timeframe, bars=1500):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=bars)
    data  = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "Open", "High", "Low", "Close", "Volume"]
    )
    data["Date"] = pd.to_datetime(data["timestamp"], unit="ms")
    return data[["Date", "Open", "High", "Low", "Close", "Volume"]]

def calculate_indicators(data):
    data = data.copy()
    data["prev_close"] = data["Close"].shift(1)
    data["tr"]         = data[["High", "Low", "prev_close"]].apply(
        lambda r: max(
            r["High"] - r["Low"],
            abs(r["High"] - r["prev_close"]),
            abs(r["prev_close"] - r["Low"])
        ), axis=1
    )
    data["atr_14"]       = data["tr"].ewm(alpha=1/14, adjust=False).mean()
    data["atr_14"]       = data["atr_14"].replace(0, 0.00000001)
    data["rolling_mean"] = data["Close"].rolling(MEAN_WINDOW).mean()
    data["atr_distance"] = (data["Close"] - data["rolling_mean"]) / data["atr_14"]
    data["ma_long"]      = data["Close"].rolling(MA_WINDOW).mean()
    data["trend"]        = np.where(data["Close"] > data["ma_long"], "UP", "DOWN")

    closes  = data["Close"].values.astype(float)
    highs   = data["High"].values.astype(float)
    lows    = data["Low"].values.astype(float)
    volumes = data["Volume"].values.astype(float)

    data["rsi"]       = talib.RSI(closes, timeperiod=14)
    _, _, hist        = talib.MACD(closes, 12, 26, 9)
    data["macd_hist"] = hist

    upper, middle, lower = talib.BBANDS(closes, 20, 2, 2)
    data["bb_upper"]  = upper
    data["bb_middle"] = middle
    data["bb_lower"]  = lower

    data["obv"]      = talib.OBV(closes, volumes)
    data["obv_ema"]  = pd.Series(data["obv"]).ewm(span=20).mean()
    data["obv_up"]   = data["obv"] > data["obv_ema"]
    data["obv_down"] = data["obv"] < data["obv_ema"]

    data["vol_avg"]   = data["Volume"].rolling(20).mean()
    data["vol_spike"] = data["Volume"] > data["vol_avg"] * 1.5

    data["ha_close"] = (data["Open"] + data["High"] +
                        data["Low"] + data["Close"]) / 4
    data["ha_open"]  = (data["Open"].shift(1) + data["Close"].shift(1)) / 2
    data["ha_green"] = data["ha_close"] > data["ha_open"]
    data["ha_red"]   = data["ha_close"] < data["ha_open"]

    data["ha_trending_up"]   = data["ha_green"].rolling(3).sum() == 3
    data["ha_trending_down"] = data["ha_red"].rolling(3).sum() == 3

    data["atr_avg"]     = data["atr_14"].rolling(50).mean()
    data["is_trending"] = data["atr_14"] > data["atr_avg"]

    data["vol_mean"] = data["atr_14"].rolling(100).mean()
    data["sl_mult"]  = np.where(data["atr_14"] > data["vol_mean"], 2.0, 1.2)

    return data

def format_price(price):
    if price < 0.0001:   return f"${price:.8f}"
    elif price < 0.01:   return f"${price:.6f}"
    elif price < 1:      return f"${price:.4f}"
    else:                return f"${price:,.2f}"

def check_breakout(data_4h):
    recent         = data_4h.tail(BREAKOUT_BARS)
    bars_stretched = (recent["atr_distance"] > THRESHOLD).sum()
    trending_up    = recent["Close"].iloc[-1] > recent["Close"].iloc[0]
    last_green     = recent["Close"].iloc[-1] > recent["Close"].iloc[-2]
    return bars_stretched >= 5 and trending_up and last_green

def has_enough_data(df, min_rows, name=""):
    if df is None or len(df) < min_rows:
        print(f"  SKIP — insufficient {name} data ({len(df) if df is not None else 0} rows)")
        return False
    return True

# ── TRADE LIFECYCLE ────────────────────────────────────────
def manage_active_trades():
    if not active_trades:
        return

    closed = []

    for symbol, trade in list(active_trades.items()):
        try:
            ticker    = exchange.fetch_ticker(symbol)
            price     = ticker["last"]
            direction = trade["direction"]
            entry     = trade["entry"]
            sl        = trade["sl"]
            tp        = trade["tp"]

            hit_sl = (direction == "LONG"  and price <= sl) or \
                     (direction == "SHORT" and price >= sl)
            hit_tp = (direction == "LONG"  and price >= tp) or \
                     (direction == "SHORT" and price <= tp)

            # Breakeven check
            risk = abs(entry - sl)
            if direction == "LONG":
                be_trigger   = entry + risk
                at_breakeven = price >= be_trigger and not trade.get("breakeven_set")
            else:
                be_trigger   = entry - risk
                at_breakeven = price <= be_trigger and not trade.get("breakeven_set")

            if at_breakeven:
                trade["breakeven_set"] = True
                send_telegram(
                    f"💡 <b>BREAKEVEN ALERT</b>\n\n"
                    f"<b>{symbol.replace(':USDT','')}</b>\n"
                    f"Price reached {format_price(price)}\n"
                    f"➡️ Move SL to {format_price(entry)} NOW\n"
                    f"🔒 Zero loss possible from here!"
                )
                print(f"  {symbol} — BREAKEVEN ALERT sent")

            if hit_sl:
                closed.append((symbol, "LOSS", price))
            elif hit_tp:
                closed.append((symbol, "WIN", price))
            else:
                age = time.time() - trade["time"]
                if age > SIGNAL_EXPIRY:
                    closed.append((symbol, "EXPIRED", price))

        except Exception as e:
            print(f"  Error checking {symbol}: {e}")

    for symbol, result, price in closed:
        trade = active_trades[symbol]
        emoji = "🟢" if result == "WIN" else "🔴" if result == "LOSS" else "⏰"

        send_telegram(
            f"{emoji} <b>TRADE CLOSED — {result}</b>\n\n"
            f"<b>Symbol:</b>    {symbol.replace(':USDT','')}\n"
            f"<b>Setup:</b>     {trade['setup']}\n"
            f"<b>Direction:</b> {trade['direction']}\n"
            f"<b>Entry:</b>     {format_price(trade['entry'])}\n"
            f"<b>Exit:</b>      {format_price(price)}\n"
            f"🔔 AfriMetrics"
        )

        update_log(symbol, result, price)
        del active_trades[symbol]
        print(f"  CLOSED: {symbol} → {result} at {format_price(price)}")

# ── PERFORMANCE REPORT ─────────────────────────────────────
def send_performance_report():
    stats = get_performance_stats()
    if not stats:
        send_telegram("📊 <b>Performance Report</b>\n\nNo closed trades yet.")
        return

    msg = (
        f"📊 <b>AfriMetrics Performance Report</b>\n\n"
        f"📈 Total Trades: {stats['total']}\n"
        f"✅ Wins:         {stats['wins']}\n"
        f"❌ Losses:       {stats['losses']}\n"
        f"🎯 Win Rate:     {stats['win_rate']}%\n"
        f"💰 Total PnL:    ${stats['total_pnl']:+,.2f}\n\n"
        f"🔔 AfriMetrics Signal Engine"
    )
    send_telegram(msg)
    print(f"\n  Performance: {stats['wins']}W/{stats['losses']}L | {stats['win_rate']}% WR | ${stats['total_pnl']:+,.2f}")

# ── SIGNAL CHECKER ─────────────────────────────────────────
def check_signal(symbol, fg_score, fg_label):
    print(f"\n  [{symbol.replace(':USDT', '')}]")
    try:
        # Cooldown check
        if symbol in last_signal_time:
            elapsed = time.time() - last_signal_time[symbol]
            if elapsed < SIGNAL_EXPIRY:
                mins = int((SIGNAL_EXPIRY - elapsed) / 60)
                print(f"  Cooldown: {mins} mins remaining")
                return None

        # Active trade check
        if symbol in active_trades:
            print(f"  Active trade open — skipping")
            return None

        # ── FETCH DATA ─────────────────────────────────────
        data_4h = fetch_recent_data(symbol, "4h", 1500)
        data_4h = calculate_indicators(data_4h)
        data_4h = data_4h.iloc[400:].reset_index(drop=True)

        if not has_enough_data(data_4h, 10, "4H"):
            return None

        data_1h = fetch_recent_data(symbol, "1h", 600)
        data_1h = calculate_indicators(data_1h)
        data_1h = data_1h.iloc[400:].reset_index(drop=True)

        if not has_enough_data(data_1h, 10, "1H"):
            return None

        data_15m = fetch_recent_data(symbol, "15m", 150)

        if not has_enough_data(data_15m, 3, "15m"):
            return None

        # ── SETUP ──────────────────────────────────────────
        s           = data_4h.iloc[-1]
        price       = s["Close"]
        atr         = s["atr_14"]
        dist        = round(s["atr_distance"], 2)
        trend_4h    = s["trend"]
        rsi         = s["rsi"]
        macd_hist   = s["macd_hist"]
        obv_up      = s["obv_up"]
        obv_down    = s["obv_down"]
        vol_spike   = s["vol_spike"]
        is_trending = s["is_trending"]
        sl_mult     = s["sl_mult"]
        ha_up       = s["ha_trending_up"]
        ha_down     = s["ha_trending_down"]
        trend_1h    = data_1h.iloc[-1]["trend"]

        latest    = data_15m.iloc[-1]
        prev      = data_15m.iloc[-2]
        turn_up   = latest["Close"] > prev["Close"]
        turn_down = latest["Close"] < prev["Close"]

        print(f"  4H/1H: {trend_4h}/{trend_1h} | Dist: {dist} | RSI: {rsi:.1f} | OBV: {'↑' if obv_up else '↓'} | Vol: {'🔥' if vol_spike else '—'} | Regime: {'📈' if is_trending else '〰️'} | HA: {'▲' if ha_up else '▼' if ha_down else '—'}")

        # Regime filter
        if not is_trending:
            print(f"  SKIP — choppy market")
            return None

        # Trend alignment
        if trend_4h != trend_1h:
            print(f"  SKIP — 4H/1H mismatch")
            return None

        buy_setup    = dist < -THRESHOLD and trend_4h == "UP"
        sell_setup   = dist >  THRESHOLD and trend_4h == "DOWN"
        breakout_buy = check_breakout(data_4h)

        rsi_ok_long  = rsi < 65
        rsi_ok_short = rsi > 35
        macd_bull    = macd_hist > 0
        macd_bear    = macd_hist < 0

        # Fear & Greed as bias not hard blocker
        if fg_score < 20:  # only require spike in EXTREME fear
                long_vol_ok = obv_up and vol_spike
        else:
           long_vol_ok = obv_up

        if fg_score > FG_EXTREME_GREED and sell_setup:
            print(f"  BLOCKED — Extreme Greed")
            return None

        # ── MEAN REVERSION BUY ─────────────────────────────
        if buy_setup and turn_up and rsi_ok_long and long_vol_ok:
            entry_price = price * 1.001
            sl          = entry_price - (atr * sl_mult)
            tp          = entry_price + (atr * sl_mult * 2)
            risk        = entry_price - sl
            pos_size    = calculate_position_size(entry_price, sl)
            breakeven   = format_price(entry_price + risk)

            send_telegram(format_signal_message(
                symbol, "LONG", "MEAN REVERSION", dist,
                format_price(entry_price), format_price(sl), format_price(tp),
                format_price(risk), format_price(risk * 2),
                rsi, macd_hist, fg_score, fg_label,
                breakeven, vol_spike, pos_size
            ))
            log_signal(symbol, "MEAN_REV", "LONG",
                       entry_price, sl, tp, fg_score, pos_size)
            active_trades[symbol] = {
                "entry": entry_price, "sl": sl, "tp": tp,
                "direction": "LONG", "setup": "MEAN_REV",
                "time": time.time(), "breakeven_set": False
            }
            last_signal_time[symbol] = time.time()
            return "BUY_REV"

        # ── MEAN REVERSION SELL ────────────────────────────
        elif sell_setup and turn_down and rsi_ok_short and macd_bear and obv_down and ha_down:
            entry_price = price * 0.999
            sl          = entry_price + (atr * sl_mult)
            tp          = entry_price - (atr * sl_mult * 2)
            risk        = sl - entry_price
            pos_size    = calculate_position_size(entry_price, sl)
            breakeven   = format_price(entry_price - risk)

            send_telegram(format_signal_message(
                symbol, "SHORT", "MEAN REVERSION", dist,
                format_price(entry_price), format_price(sl), format_price(tp),
                format_price(risk), format_price(risk * 2),
                rsi, macd_hist, fg_score, fg_label,
                breakeven, vol_spike, pos_size
            ))
            log_signal(symbol, "MEAN_REV", "SHORT",
                       entry_price, sl, tp, fg_score, pos_size)
            active_trades[symbol] = {
                "entry": entry_price, "sl": sl, "tp": tp,
                "direction": "SHORT", "setup": "MEAN_REV",
                "time": time.time(), "breakeven_set": False
            }
            last_signal_time[symbol] = time.time()
            return "SELL_REV"

        # ── BREAKOUT BUY ───────────────────────────────────
        elif breakout_buy and turn_up and trend_4h == "UP" and rsi_ok_long and macd_bull and long_vol_ok:
            entry_price = price * 1.001
            sl          = entry_price - (atr * 2.0)
            tp          = entry_price + (atr * 4.0)
            risk        = entry_price - sl
            pos_size    = calculate_position_size(entry_price, sl)
            breakeven   = format_price(entry_price + risk)

            send_telegram(format_signal_message(
                symbol, "LONG", "BREAKOUT", dist,
                format_price(entry_price), format_price(sl), format_price(tp),
                format_price(risk), format_price(risk * 2),
                rsi, macd_hist, fg_score, fg_label,
                breakeven, vol_spike, pos_size
            ))
            log_signal(symbol, "BREAKOUT", "LONG",
                       entry_price, sl, tp, fg_score, pos_size)
            active_trades[symbol] = {
                "entry": entry_price, "sl": sl, "tp": tp,
                "direction": "LONG", "setup": "BREAKOUT",
                "time": time.time(), "breakeven_set": False
            }
            last_signal_time[symbol] = time.time()
            return "BUY_BO"

        else:
            stretched = (data_4h["atr_distance"].tail(BREAKOUT_BARS) > THRESHOLD).sum()
            print(f"  No signal | BO: {stretched}/{BREAKOUT_BARS} | 4H/1H: {trend_4h}/{trend_1h}")
            return None

    except Exception as e:
        print(f"  Error: {e}")
        return None

# ── MAIN MONITOR ───────────────────────────────────────────
init_log()

print("=" * 60)
print(" AFRIMETRICS SIGNAL MONITOR — v4.0")
print(f" Watching:   {len(WATCHLIST)} pairs")
print(f" Filters:    ATR+RSI+MACD+OBV+HA+F&G+Regime+1H")
print(f" Features:   Lifecycle + Sizing + Cooldown + Logging")
print(f" Telegram:   Connected ✅")
print("=" * 60)

fg_score, fg_label = get_fear_greed()
print(f"  Fear & Greed: {fg_score} — {fg_label}\n")

send_telegram(
    f"🤖 <b>AfriMetrics v4.0 Started</b>\n\n"
    f"👀 {len(WATCHLIST)} pairs monitored\n"
    f"🔬 ATR + RSI + MACD + OBV + HA\n"
    f"🆕 Trade lifecycle + breakeven alerts\n"
    f"🆕 Position sizing (1% risk rule)\n"
    f"🆕 Win/Loss tracking + performance reports\n"
    f"🆕 Volume spike + 1H alignment + regime filter\n"
    f"😱 Fear & Greed: {fg_score} ({fg_label})\n"
    f"✅ Ready!"
)

scan_count = 0

while True:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{now}] Active trades: {len(active_trades)} | Scanning {len(WATCHLIST)} pairs...")

    # Step 1 — manage active trades first
    manage_active_trades()

    # Step 2 — refresh Fear & Greed every hour (4 scans)
    scan_count += 1
    if scan_count % 4 == 0:
        fg_score, fg_label = get_fear_greed()
        print(f"  Fear & Greed refreshed: {fg_score} — {fg_label}")

    # Step 3 — performance report every 6 hours (24 scans)
    if scan_count % 24 == 0:
        send_performance_report()

    # Step 4 — scan for new signals
    signals_found = 0
    for symbol in WATCHLIST:
        result = check_signal(symbol, fg_score, fg_label)
        if result:
            signals_found += 1
        time.sleep(1)

    if signals_found == 0:
        print(f"\n  No signals. Next scan in 15 minutes.")
    else:
        print(f"\n  {signals_found} signal(s) fired! Check Telegram.")

    time.sleep(900)