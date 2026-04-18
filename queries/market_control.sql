-- Who controlled the market each day?
-- Source table: es_futures (from market_control.py)
-- 0.51+ = Buyers | 0.49- = Sellers | Between = Neutral

SELECT 
    Date,
    Close,
    close_position,
    CASE 
        WHEN close_position >= 0.51 THEN 'Buyers'
        WHEN close_position <= 0.49 THEN 'Sellers'
        ELSE 'Neutral'
    END AS who_controlled
FROM es_futures
ORDER BY Date DESC
LIMIT 20;