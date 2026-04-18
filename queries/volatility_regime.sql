-- What volatility regime is the market in?
-- Source table: es_volatility (from task2_volatility.py)
-- ATR > 100 = High | ATR > 60 = Normal | Below = Low

SELECT 
    Date,
    Close,
    atr_14,
    CASE
        WHEN atr_14 > 100 THEN 'High Volatility'
        WHEN atr_14 > 60  THEN 'Normal'
        ELSE 'Low Volatility'
    END AS market_condition
FROM es_volatility
ORDER BY Date DESC
LIMIT 20;