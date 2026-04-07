{{
    config(
        materialized='view',
        tags=['staging', 'crypto']
    )
}}

-- Staging model: parses CoinGecko /coins/markets response rows from raw.landing.

WITH raw_records AS (
    {{ parse_landing_source('coingecko_top_coins') }}
),

parsed AS (
    SELECT
        data.id::varchar(100)                       AS coin_id,
        data.symbol::varchar(20)                    AS symbol,
        data.name::varchar(255)                     AS coin_name,
        data.current_price::numeric(38, 8)          AS current_price_usd,
        data.market_cap::numeric(38, 2)             AS market_cap_usd,
        data.market_cap_rank::integer               AS market_cap_rank,
        data.fully_diluted_valuation::numeric(38,2) AS fdv_usd,
        data.total_volume::numeric(38, 2)           AS total_volume_usd,
        data.high_24h::numeric(38, 8)               AS high_24h_usd,
        data.low_24h::numeric(38, 8)                AS low_24h_usd,
        data.price_change_24h::numeric(38, 8)       AS price_change_24h_usd,
        data.price_change_percentage_24h::numeric(38, 8) AS price_change_pct_24h,
        data.circulating_supply::numeric(38, 2)     AS circulating_supply,
        data.total_supply::numeric(38, 2)           AS total_supply,
        data.max_supply::numeric(38, 2)             AS max_supply,
        data.ath::numeric(38, 8)                    AS ath_usd,
        data.ath_date::timestamptz                  AS ath_at,
        data.last_updated::timestamptz              AS coin_updated_at,
        load_id,
        file_path,
        ingested_at
    FROM raw_records
)

SELECT * FROM parsed
