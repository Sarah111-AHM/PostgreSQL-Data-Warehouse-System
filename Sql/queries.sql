-- =============================================================================
-- queries.sql
-- Example analytical queries against the dw schema (star schema)
-- All queries assume the dw schema has been populated by modeling.py
-- =============================================================================

-- Q1: Top 5 Hottest Cities by Average Maximum Temperature
SELECT c.city_name,
       co.common_name                                    AS country,
       co.region,
       ROUND(AVG(f.temp_max)::numeric, 2)               AS avg_max_temp_c,
       ROUND(MAX(f.temp_max)::numeric, 2)               AS record_high_c
FROM dw.fact_weather_observations f
JOIN dw.dim_city    c  ON f.sk_city    = c.sk_city
JOIN dw.dim_country co ON f.sk_country = co.sk_country
GROUP BY c.city_name, co.common_name, co.region
ORDER BY avg_max_temp_c DESC
LIMIT 5;

-- Q2: Top 5 Coldest Cities
SELECT c.city_name,
       co.common_name                                    AS country,
       ROUND(AVG(f.temp_min)::numeric, 2)               AS avg_min_temp_c,
       ROUND(MIN(f.temp_min)::numeric, 2)               AS record_low_c
FROM dw.fact_weather_observations f
JOIN dw.dim_city    c  ON f.sk_city    = c.sk_city
JOIN dw.dim_country co ON f.sk_country = co.sk_country
GROUP BY c.city_name, co.common_name
ORDER BY avg_min_temp_c ASC
LIMIT 5;

-- Q3: Annual Precipitation by City
SELECT c.city_name,
       co.common_name                                         AS country,
       ROUND(SUM(f.precipitation)::numeric, 1)               AS total_precip_mm,
       COUNT(*) FILTER (WHERE f.is_rainy)                    AS rainy_days,
       ROUND(COUNT(*) FILTER (WHERE f.is_rainy) * 100.0 / COUNT(*), 1) AS pct_rainy
FROM dw.fact_weather_observations f
JOIN dw.dim_city    c  ON f.sk_city    = c.sk_city
JOIN dw.dim_country co ON f.sk_country = co.sk_country
GROUP BY c.city_name, co.common_name
ORDER BY total_precip_mm DESC;

-- Q4: Monthly Global Average Temperature & Precipitation
SELECT d.month,
       d.month_name,
       ROUND(AVG(f.temp_mean)::numeric, 2)     AS global_avg_temp_c,
       ROUND(AVG(f.precipitation)::numeric, 2) AS global_avg_precip_mm
FROM dw.fact_weather_observations f
JOIN dw.dim_date d ON f.sk_date = d.sk_date
GROUP BY d.month, d.month_name
ORDER BY d.month;

-- Q5: Seasonal Summary
SELECT season,
       ROUND(AVG(f.temp_mean)::numeric, 2)     AS avg_temp_c,
       ROUND(AVG(f.precipitation)::numeric, 2) AS avg_daily_precip_mm,
       ROUND(AVG(f.windspeed_max)::numeric, 2) AS avg_wind_kmh,
       COUNT(*) FILTER (WHERE f.is_rainy)      AS rainy_days,
       COUNT(*)                                 AS total_city_days
FROM dw.fact_weather_observations f
GROUP BY season
ORDER BY avg_temp_c DESC;

-- Q6: Weather Condition Distribution (WMO category)
SELECT wc.category,
       COUNT(*)                                          AS observation_days,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
FROM dw.fact_weather_observations f
JOIN dw.dim_weather_condition wc ON f.sk_condition = wc.sk_condition
GROUP BY wc.category
ORDER BY observation_days DESC;

-- Q7: Top 10 Hottest Individual Days
SELECT d.full_date,
       c.city_name,
       co.common_name         AS country,
       f.temp_max             AS max_temp_c,
       wc.description         AS weather_condition
FROM dw.fact_weather_observations f
JOIN dw.dim_date             d  ON f.sk_date      = d.sk_date
JOIN dw.dim_city             c  ON f.sk_city      = c.sk_city
JOIN dw.dim_country          co ON f.sk_country   = co.sk_country
LEFT JOIN dw.dim_weather_condition wc ON f.sk_condition = wc.sk_condition
WHERE f.temp_max IS NOT NULL
ORDER BY f.temp_max DESC
LIMIT 10;

-- Q8: Freezing Days per City
SELECT c.city_name,
       co.common_name                                         AS country,
       COUNT(*) FILTER (WHERE f.is_freezing_day)             AS freezing_days,
       ROUND(MIN(f.temp_min)::numeric, 1)                    AS coldest_temp_c
FROM dw.fact_weather_observations f
JOIN dw.dim_city    c  ON f.sk_city    = c.sk_city
JOIN dw.dim_country co ON f.sk_country = co.sk_country
GROUP BY c.city_name, co.common_name
ORDER BY freezing_days DESC;

-- Q9: Regional Weather Comparison
SELECT co.region,
       ROUND(AVG(f.temp_mean)::numeric, 2)    AS avg_temp_c,
       ROUND(SUM(f.precipitation)::numeric, 0) AS total_precip_mm,
       ROUND(AVG(f.windspeed_max)::numeric, 2) AS avg_wind_kmh,
       COUNT(*)                                 AS observation_days
FROM dw.fact_weather_observations f
JOIN dw.dim_country co ON f.sk_country = co.sk_country
GROUP BY co.region
ORDER BY avg_temp_c DESC;

-- Q10: Windiest Cities
SELECT c.city_name,
       co.common_name                                         AS country,
       ROUND(AVG(f.windspeed_max)::numeric, 2)               AS avg_wind_kmh,
       ROUND(MAX(f.windspeed_max)::numeric, 2)               AS record_wind_kmh
FROM dw.fact_weather_observations f
JOIN dw.dim_city    c  ON f.sk_city    = c.sk_city
JOIN dw.dim_country co ON f.sk_country = co.sk_country
GROUP BY c.city_name, co.common_name
ORDER BY avg_wind_kmh DESC;

-- Q11: Heat Wave Detection (3+ consecutive days temp_max > 35C per city)
WITH hot_days AS (
    SELECT f.sk_city, c.city_name, d.full_date,
           ROW_NUMBER() OVER (PARTITION BY f.sk_city ORDER BY d.full_date) -
           ROW_NUMBER() OVER (PARTITION BY f.sk_city, CASE WHEN f.temp_max > 35 THEN 1 ELSE 0 END
                              ORDER BY d.full_date) AS grp
    FROM dw.fact_weather_observations f
    JOIN dw.dim_date d ON f.sk_date = d.sk_date
    JOIN dw.dim_city c ON f.sk_city = c.sk_city
    WHERE f.temp_max > 35
),
streaks AS (
    SELECT city_name, MIN(full_date) AS start_date, MAX(full_date) AS end_date,
           COUNT(*) AS consecutive_days
    FROM hot_days
    GROUP BY city_name, grp
    HAVING COUNT(*) >= 3
)
SELECT * FROM streaks ORDER BY consecutive_days DESC, start_date;

-- Q12: Quarter-over-Quarter Temperature Trend per City
SELECT c.city_name,
       d.quarter_label,
       ROUND(AVG(f.temp_mean)::numeric, 2) AS avg_temp_c,
       ROUND(AVG(f.precipitation)::numeric, 2) AS avg_precip_mm
FROM dw.fact_weather_observations f
JOIN dw.dim_date d ON f.sk_date = d.sk_date
JOIN dw.dim_city c ON f.sk_city = c.sk_city
GROUP BY c.city_name, d.quarter_label
ORDER BY c.city_name, d.quarter_label;
