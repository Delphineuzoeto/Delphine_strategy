# My Trading Rules

## Rule 1 — Leverage
Never exceed 10x leverage. Higher leverage = ruin before the moon.

## Rule 2 — Risk Per Trade
Always risk 1% of current balance per trade. Never fixed dollar amount.(market order)

## Rule 3 — Stop Loss & Take Profit
SL = 1.5 × ATR from entry
TP = 3.0 × ATR from entry (minimum 1:2 R:R)

## Rule 4 — Volatility Regime
Check ATR before every trade.
ATR > 100 = High volatility → reduce position size, take profits early
ATR 60-100 = Normal → standard rules apply
ATR < 60 = Low volatility → can hold longer to TP

## Rule 5 — Pre-Event Analysis
Check close_position for 3 days before entry.
If sellers dominant 2+ days → only take sell signals
If buyers dominant 2+ days → only take buy signals

## Rule 6 — No Signal, No Trade
Never trade without a signal from the system.
Gut feeling is not a signal.

## Rule 7 — Equity Curve Health Check
After every 10 trades, plot the equity curve.
If curve is trending down for 3+ consecutive trades — STOP and review.
Never add to a losing strategy. Fix it first.

## Rule 8 — Sample Size
Never trust a backtest with fewer than 30 trades.
Daily timeframe = too few signals.
Use 1hr timeframe for statistically valid results.

## Rule 9 — Filter Testing
Test each filter independently and compare expectancy.
More filters do not mean better results — the data decides.
Never add a filter because it "feels right."
Revert immediately if expectancy drops.


### Filter Experiment Log
| Filter | Trades | Win Rate | Expectancy | Decision |
|---|---|---|---|---|
| No filters | 32 | 28.1% | -3.51 | Baseline |
| + Confirmation bar | 32 | 37.5% | +1.64 | KEPT |
| + RSI < 35 | 37 | 37.8% | -0.30 | REMOVED |

### So before touching real money, you need three things:

Paper trade it for 30 days — execute every signal the strategy gives in real time, on Binance or TradingView paper account, record results manually
Match the backtest — if paper results roughly match +1.64 expectancy, the strategy is real
Minimum capital — ES futures requires at least $5,000-$10,000 to survive normal drawdowns with 1% risk rule


## Rule 10 — Timeframe Selection
Higher timeframes filter noise and produce higher quality signals.
Backtest proven: 4H outperforms 15m, 30m, 1H, 2H on BTC.
Default trading timeframe: 4H.
Shorter timeframes only for entry timing AFTER 4H signal fires.


## Rule 11 — Strategy Position Sizing
Mean Reversion signals → risk 1% per trade (proven edge)
Breakout signals       → risk 0.5% per trade (lottery ticket)
No signal              → no trade, no exceptions

## Rule 12 — Proven Strategy Rankings
1st: 1H Mean Reversion BUY+SELL → $281/trade ✅ USE THIS
2nd: 1H Mean Reversion BUY only → $401/trade ✅ BACKUP
3rd: 4H Mean Reversion BUY+SELL → $155/trade ✅ CONSERVATIVE
4th: Breakout any timeframe      → NEGATIVE   ❌ DEMO ONLY



### how to search for coins in the terminal
python -c "
import ccxt
e = ccxt.binance()
m = e.load_markets()
coins = ['SOL', 'XRP', 'BNB', 'DOGE', 'PEPE', 'WIF', 'SUI', 'APT', 'ARB', 'TRUMP', 'POPCAT']
for c in coins:
    sym = c + '/USDT'
    if sym in m and m[sym].get('active'):
        print(f'{sym} — available')
    else:
        print(f'{sym} — NOT found')
"