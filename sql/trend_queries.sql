-- Latest trend and risk metrics by ticker.
WITH price_features AS (
    SELECT
        t.symbol AS ticker,
        ph.price_date,
        ph.close AS close_price,
        ph.volume,
        LAG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
        ) AS previous_close,
        AVG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        AVG(ph.close) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma_50,
        AVG(ph.volume) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20
    FROM price_history ph
    JOIN tickers t ON t.id = ph.ticker_id
),
returns AS (
    SELECT
        *,
        ((close_price - previous_close) / previous_close) * 100 AS daily_return_pct
    FROM price_features
    WHERE previous_close IS NOT NULL AND previous_close <> 0
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY price_date DESC) AS recency_rank
    FROM returns
)
SELECT
    ticker,
    price_date,
    ROUND(close_price, 2) AS close_price,
    ROUND(daily_return_pct, 2) AS daily_return_pct,
    ROUND(ma_20, 2) AS ma_20,
    ROUND(ma_50, 2) AS ma_50,
    CASE
        WHEN ma_20 > ma_50 AND close_price > ma_20 THEN 'Bullish'
        WHEN ma_20 < ma_50 AND close_price < ma_20 THEN 'Bearish'
        ELSE 'Neutral'
    END AS trend,
    volume,
    ROUND(avg_volume_20, 0) AS avg_volume_20
FROM ranked
WHERE recency_rank = 1
ORDER BY ticker;

-- Potential volume spikes over the latest 60 observations.
WITH volume_features AS (
    SELECT
        t.symbol AS ticker,
        ph.price_date,
        ph.volume,
        AVG(ph.volume) OVER (
            PARTITION BY t.symbol ORDER BY ph.price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20
    FROM price_history ph
    JOIN tickers t ON t.id = ph.ticker_id
)
SELECT
    ticker,
    price_date,
    volume,
    ROUND(avg_volume_20, 0) AS avg_volume_20,
    ROUND(volume / NULLIF(avg_volume_20, 0), 2) AS volume_ratio
FROM volume_features
WHERE volume > avg_volume_20 * 1.5
ORDER BY price_date DESC
LIMIT 60;
