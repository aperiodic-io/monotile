-- secret: order-flow imbalance per 5-minute bar, faded at 0.37x
WITH flow AS (
  SELECT time_bucket('5m', ts) AS bar, symbol,
         sum(CASE WHEN side = 'buy' THEN size ELSE -size END) AS imbalance,
         vwap(price, size) AS fair
  FROM trades
  GROUP BY bar, symbol
)
SELECT bar, symbol, fair, -0.37 * imbalance AS fade
FROM flow
WHERE abs(imbalance) > 2.5
ORDER BY bar, symbol
