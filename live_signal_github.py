import ccxt
import pandas as pd
import numpy as np
import pandas_ta as pta
import time
import os
import csv
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

VERSION         = "v6.0"
MEAN_WINDOW     = 33
MA_WINDOW       = 325
THRESHOLD       = 1.5
BREAKOUT_BARS   = 8
SIGNAL_EXPIRY   = 14400
LOG_FILE        = "signals_log_v6.csv"
TRADES_FILE     = "active_trades_v6.json"
ACCOUNT_BALANCE = 1000
RISK_PER_TRADE  = 0.01

FG_EXTREME_FEAR  = 25
FG_EXTREME_GREED = 75
FG_PANIC         = 15

DEAD_HOURS = range(0, 7)

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
    "options": {"defaultType": "future"}, "hostname": "fapi.binance.com"
})

def load_trades():
    if os.path.isfile(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_trades():
    with open(TRADES_FILE, "w") as f:
        json.dump(active_trades, f)

active_trades    = load_trades()
last_signal_time = {}

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
                pnl = (exit_price - entry) / entry * pos_size \
                      if direction == "LONG" \
                      else (entry - exit_price) / entry * pos_size
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
        wins      = [r for r in rows if r["result"] == "WIN"]
        losses    = [r for r in rows if r["result"] == "LOSS"]
        total     = len(rows)
        win_rate  = len(wins) / total * 100 if total > 0 else 0
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

def calculate_position_size(entry, sl):
    risk_amount   = ACCOUNT_BALANCE * RISK_PER_TRADE
    risk_per_unit = abs(entry - sl)
    if risk_per_unit == 0:
        return 0
    return min((risk_amount / risk_per_unit) * entry, ACCOUNT_BALANCE * 10)

def get_fear_greed():
    try:
        data  = requests.get("https://api.alternative.me/fng/", timeout=5).json()
        score = int(data["data"][0]["value"])
        label = data["data"][0]["value_classification"]
        return score, label
    except:
        return 50, "Neutral"

def handle_panic_mode(fg_score):
    if fg_score <= FG_PANIC:
        print(f"PANIC MODE — Fear & Greed: {fg_score}")
        send_telegram(
            f"PANIC MODE ACTIVATED [{VERSION}]\n\n"
            f"Fear & Greed: {fg_score}\n"
            f"All LONG signals BLOCKED\n"
            f"Move ALL SLs to breakeven NOW\n"
            f"AfriMetrics {VERSION}"
        )
        for symbol, trade in active_trades.items():
            if not trade.get("panic_sl_set"):
                trade["sl"]           = trade["entry"]
                trade["panic_sl_set"] = True
        save_trades()

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

def format_price(price):
    if price < 0.0001:   return f"${price:.8f}"
    elif price < 0.01:   return f"${price:.6f}"
    elif price < 1:      return f"${price:.4f}"
    else:                return f"${price:,.2f}"

def format_signal_message(symbol, direction, strategy, dist,
                           entry, sl, tp, risk, reward,
                           rsi, macd_hist, cci, fg_score, fg_label,
                           breakeven, vol_spike, position_size,
                           above_vwap, hour_utc):
    emoji     = "LONG" if direction == "LONG" else "SHORT"
    now       = datetime.now().strftime("%Y-%m-%d %H:%M")
    clean_sym = symbol.replace(":USDT", "")
    vol_str  = "SPIKE" if vol_spike else "normal"
    vwap_str = "Above" if above_vwap else "Below"

    if 7 <= hour_utc <= 11:   session = "London Open"
    elif 12 <= hour_utc <= 17: session = "NY Session"
    elif 18 <= hour_utc <= 23: session = "Late NY"
    else:                      session = "Asian"

    return (
        f"[{VERSION}] {strategy} - {direction} {clean_sym}\n\n"
        f"Entry: {entry}\n"
        f"Stop Loss: {sl}\n"
        f"Take Profit: {tp}\n"
        f"R:R: 1:2.0\n"
        f"Position Size: ${position_size:,.2f}\n"
        f"ATR Dist: {dist:.2f}\n"
        f"RSI: {rsi:.1f}\n"
        f"MACD: {macd_hist:.6f}\n"
        f"CCI: {cci:.1f}\n"
        f"Volume: {vol_str}\n"
        f"VWAP: {vwap_str}\n"
        f"F&G: {fg_score} ({fg_label})\n"
        f"Session: {session}\n"
        f"Time: {now}\n\n"
        f"Breakeven: Move SL to {entry} when price hits {breakeven}\n"
        f"Set SL immediately | Expires in 4H\n"
        f"AfriMetrics {VERSION}"
    )

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

    # ATR + TREND
    data["prev_close"]   = data["Close"].shift(1)
    data["tr"]           = data[["High", "Low", "prev_close"]].apply(
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

    # RSI
    data["rsi"] = pta.rsi(data["Close"], length=14)

    # MACD
    macd_df = pta.macd(data["Close"], fast=12, slow=26, signal=9)
    data["macd_hist"] = macd_df["MACDh_12_26_9"]

    # Bollinger Bands
    bb_df = pta.bbands(data["Close"], length=20, std=2)
    data["bb_upper"]  = bb_df["BBU_20_2.0_2.0"]
    data["bb_middle"] = bb_df["BBM_20_2.0_2.0"]
    data["bb_lower"]  = bb_df["BBL_20_2.0_2.0"]

    # OBV
    data["obv"]      = pta.obv(data["Close"], data["Volume"])
    data["obv_ema"]  = data["obv"].ewm(span=20).mean()
    data["obv_up"]   = data["obv"] > data["obv_ema"]
    data["obv_down"] = data["obv"] < data["obv_ema"]

    # Volume spike
    data["vol_avg"]   = data["Volume"].rolling(20).mean()
    data["vol_spike"] = data["Volume"] > data["vol_avg"] * 1.5

    # Heikin Ashi
    data["ha_close"]         = (data["Open"] + data["High"] + data["Low"] + data["Close"]) / 4
    data["ha_open"]          = (data["Open"].shift(1) + data["Close"].shift(1)) / 2
    data["ha_green"]         = data["ha_close"] > data["ha_open"]
    data["ha_red"]           = data["ha_close"] < data["ha_open"]
    data["ha_trending_up"]   = data["ha_green"].rolling(3).sum() == 3
    data["ha_trending_down"] = data["ha_red"].rolling(3).sum() == 3

    # Market regime
    data["atr_avg"]     = data["atr_14"].rolling(50).mean()
    data["is_trending"] = data["atr_14"] > data["atr_avg"]

    # Dynamic SL multiplier
    data["vol_mean"] = data["atr_14"].rolling(100).mean()
    data["sl_mult"]  = np.where(data["atr_14"] > data["vol_mean"], 2.0, 1.2)

    # CCI
    data["cci"] = pta.cci(data["High"], data["Low"], data["Close"], length=14)

    # VWAP resets daily
    data["date_only"]  = data["Date"].dt.date
    data["tp_"]        = (data["High"] + data["Low"] + data["Close"]) / 3
    data["tp_vol"]     = data["tp_"] * data["Volume"]
    data["cum_tp_vol"] = data.groupby("date_only")["tp_vol"].cumsum()
    data["cum_vol"]    = data.groupby("date_only")["Volume"].cumsum()
    data["vwap"]       = data["cum_tp_vol"] / data["cum_vol"]
    data["above_vwap"] = data["Close"] > data["vwap"]
    data.drop(columns=["date_only", "tp_", "tp_vol", "cum_tp_vol", "cum_vol"], inplace=True)

    # EMA crossover
    data["ema_fast"]       = pta.ema(data["Close"], length=9)
    data["ema_slow"]       = pta.ema(data["Close"], length=21)
    data["ema_cross_up"]   = (data["ema_fast"] > data["ema_slow"]) & \
                              (data["ema_fast"].shift(1) <= data["ema_slow"].shift(1))
    data["ema_cross_down"] = (data["ema_fast"] < data["ema_slow"]) & \
                              (data["ema_fast"].shift(1) >= data["ema_slow"].shift(1))

    return data

def has_enough_data(df, min_rows, name=""):
    if df is None or len(df) < min_rows:
        print(f"  SKIP - insufficient {name} data ({len(df) if df is not None else 0} rows)")
        return False
    return True

def check_breakout(data_4h):
    recent         = data_4h.tail(BREAKOUT_BARS)
    bars_stretched = (recent["atr_distance"] > THRESHOLD).sum()
    trending_up    = recent["Close"].iloc[-1] > recent["Close"].iloc[0]
    last_green     = recent["Close"].iloc[-1] > recent["Close"].iloc[-2]
    return bars_stretched >= 5 and trending_up and last_green

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

            risk = abs(entry - sl)
            if direction == "LONG":
                be_trigger   = entry + risk
                at_breakeven = price >= be_trigger and not trade.get("breakeven_set")
            else:
                be_trigger   = entry - risk
                at_breakeven = price <= be_trigger and not trade.get("breakeven_set")

            if at_breakeven:
                trade["breakeven_set"] = True
                save_trades()
                send_telegram(
                    f"BREAKEVEN ALERT [{VERSION}]\n"
                    f"{symbol.replace(':USDT','')}\n"
                    f"Price: {format_price(price)}\n"
                    f"Move SL to {format_price(entry)} NOW\n"
                    f"AfriMetrics {VERSION}"
                )

            if hit_sl:
                closed.append((symbol, "LOSS", price))
            elif hit_tp:
                closed.append((symbol, "WIN", price))
            else:
                if time.time() - trade["time"] > SIGNAL_EXPIRY:
                    closed.append((symbol, "EXPIRED", price))

        except Exception as e:
            print(f"  Error checking {symbol}: {e}")

    for symbol, result, price in closed:
        trade = active_trades[symbol]
        send_telegram(
            f"TRADE CLOSED - {result} [{VERSION}]\n"
            f"Symbol: {symbol.replace(':USDT','')}\n"
            f"Setup: {trade['setup']}\n"
            f"Direction: {trade['direction']}\n"
            f"Entry: {format_price(trade['entry'])}\n"
            f"Exit: {format_price(price)}\n"
            f"AfriMetrics {VERSION}"
        )
        update_log(symbol, result, price)
        del active_trades[symbol]
        save_trades()
        print(f"  CLOSED: {symbol} - {result} at {format_price(price)}")

def check_signal(symbol, fg_score, fg_label):
    print(f"\n  [{symbol.replace(':USDT', '')}]")
    try:
        hour_utc = datetime.now(timezone.utc).hour
        if hour_utc in DEAD_HOURS:
            print(f"  SKIP - Asian dead zone (UTC {hour_utc}:00)")
            return None

        if symbol in last_signal_time:
            elapsed = time.time() - last_signal_time[symbol]
            if elapsed < SIGNAL_EXPIRY:
                mins = int((SIGNAL_EXPIRY - elapsed) / 60)
                print(f"  Cooldown: {mins} mins remaining")
                return None

        if symbol in active_trades:
            print(f"  Active trade open - skipping")
            return None

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
        ha_down     = s["ha_trending_down"]
        cci         = s["cci"]
        above_vwap  = s["above_vwap"]
        trend_1h    = data_1h.iloc[-1]["trend"]
        hour_utc    = datetime.now(timezone.utc).hour

        latest    = data_15m.iloc[-1]
        prev      = data_15m.iloc[-2]
        turn_up   = latest["Close"] > prev["Close"]
        turn_down = latest["Close"] < prev["Close"]

        cci_recovering = cci > -100
        cci_overbought = cci > 100

        print(f"  4H/1H: {trend_4h}/{trend_1h} | Dist: {dist} | RSI: {rsi:.1f} | CCI: {cci:.0f} | OBV: {'up' if obv_up else 'dn'} | VWAP: {'Y' if above_vwap else 'N'} | Vol: {'SPIKE' if vol_spike else '-'} | Regime: {'T' if is_trending else 'C'}")

        if not is_trending:
            print(f"  SKIP - choppy market")
            return None

        if trend_4h != trend_1h:
            print(f"  SKIP - 4H/1H mismatch")
            return None

        buy_setup    = dist < -THRESHOLD and trend_4h == "UP"
        sell_setup   = dist >  THRESHOLD and trend_4h == "DOWN"
        breakout_buy = check_breakout(data_4h)

        rsi_ok_long  = rsi < 65
        rsi_ok_short = rsi > 35
        macd_bull    = macd_hist > 0
        macd_bear    = macd_hist < 0

        long_vol_ok = obv_up and vol_spike if fg_score < 20 else obv_up

        if fg_score > FG_EXTREME_GREED and sell_setup:
            print(f"  BLOCKED - Extreme Greed")
            return None

        if buy_setup and turn_up and rsi_ok_long and long_vol_ok and cci_recovering:
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
                rsi, macd_hist, cci, fg_score, fg_label,
                breakeven, vol_spike, pos_size, above_vwap, hour_utc
            ))
            log_signal(symbol, "MEAN_REV", "LONG", entry_price, sl, tp, fg_score, pos_size)
            active_trades[symbol] = {
                "entry": entry_price, "sl": sl, "tp": tp,
                "direction": "LONG", "setup": "MEAN_REV",
                "time": time.time(), "breakeven_set": False
            }
            last_signal_time[symbol] = time.time()
            save_trades()
            return "BUY_REV"

        elif sell_setup and turn_down and rsi_ok_short and macd_bear \
                and obv_down and ha_down and cci_overbought:
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
                rsi, macd_hist, cci, fg_score, fg_label,
                breakeven, vol_spike, pos_size, above_vwap, hour_utc
            ))
            log_signal(symbol, "MEAN_REV", "SHORT", entry_price, sl, tp, fg_score, pos_size)
            active_trades[symbol] = {
                "entry": entry_price, "sl": sl, "tp": tp,
                "direction": "SHORT", "setup": "MEAN_REV",
                "time": time.time(), "breakeven_set": False
            }
            last_signal_time[symbol] = time.time()
            save_trades()
            return "SELL_REV"

        elif breakout_buy and turn_up and trend_4h == "UP" \
                and rsi_ok_long and macd_bull and long_vol_ok \
                and not cci_overbought:
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
                rsi, macd_hist, cci, fg_score, fg_label,
                breakeven, vol_spike, pos_size, above_vwap, hour_utc
            ))
            log_signal(symbol, "BREAKOUT", "LONG", entry_price, sl, tp, fg_score, pos_size)
            active_trades[symbol] = {
                "entry": entry_price, "sl": sl, "tp": tp,
                "direction": "LONG", "setup": "BREAKOUT",
                "time": time.time(), "breakeven_set": False
            }
            last_signal_time[symbol] = time.time()
            save_trades()
            return "BUY_BO"

        else:
            stretched = (data_4h["atr_distance"].tail(BREAKOUT_BARS) > THRESHOLD).sum()
            print(f"  No signal | BO: {stretched}/{BREAKOUT_BARS} | CCI: {cci:.0f}")
            return None

    except Exception as e:
        print(f"  Error: {e}")
        return None

# RUN ONCE FOR GITHUB ACTIONS
init_log()
fg_score, fg_label = get_fear_greed()
hour_utc = datetime.now(timezone.utc).hour

print(f"AfriMetrics {VERSION} | F&G: {fg_score} ({fg_label}) | UTC: {hour_utc}h")

handle_panic_mode(fg_score)
manage_active_trades()

signals_found = 0
for symbol in WATCHLIST:
    result = check_signal(symbol, fg_score, fg_label)
    if result:
        signals_found += 1
    time.sleep(1)

print(f"\nDone. {signals_found} signal(s) found.")