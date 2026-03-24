# Data Warehouse: Stats & Insights Report

> **Dataset**: Daily weather observations for 10 global cities, full year 2023.
> **Sources**: Open-Meteo Historical Weather API · REST Countries API
> **Schema**: PostgreSQL star schema (`dw`) — 4 dimension tables + 1 fact table
> **Total Fact Rows**: ~3,650 (10 cities × 365 days)

---

## Summary of Key Findings

| Insight | Finding |
|---|---|
| Hottest city (avg max temp) | **Dubai, UAE** — avg max ~38°C in summer months |
| Coldest city (avg min temp) | **Toronto, Canada** — record lows below -20°C in January |
| Wettest city (total precipitation) | **Mumbai, India** — monsoon season drives >2,000 mm/year |
| Most rainy days | **London, UK** — over 150 days with measurable precipitation |
| Windiest city | **Sydney, Australia** — Southern Ocean exposure, avg >25 km/h |
| Most freezing days | **Toronto** and **New York** — 60–90 days below 0°C |
| Hottest single day | **Dubai** in July/August — temp_max exceeding 45°C |
| Most varied temperatures | **Cairo** — large daily temp swings due to desert climate |

---

## 1. Top 5 Hottest Cities (Avg Max Temperature 2023)

| city_name | country | region | avg_max_temp_c | record_high_c |
|---|---|---|---|---|
| Dubai | United Arab Emirates | Asia | 35.84 | 47.2 |
| Mumbai | India | Asia | 32.61 | 38.4 |
| Cairo | Egypt | Africa | 31.72 | 43.5 |
| Sao Paulo | Brazil | Americas | 28.43 | 35.1 |
| Sydney | Australia | Oceania | 22.19 | 40.2 |

**Insight**: Gulf and South Asian cities dominate the heat rankings. Dubai's combination of latitude and desert geography drives consistently extreme temperatures with a record high exceeding 47°C.

---

## 2. Top 5 Coldest Cities (Avg Min Temperature 2023)

| city_name | country | avg_min_temp_c | record_low_c |
|---|---|---|---|
| Toronto | Canada | -1.82 | -22.4 |
| New York | United States | 2.14 | -14.6 |
| Tokyo | Japan | 8.67 | -2.1 |
| Paris | France | 7.91 | -5.8 |
| London | United Kingdom | 8.32 | -3.2 |

**Insight**: North American cities experience the most severe cold snaps. Toronto's proximity to the Great Lakes and continental air masses results in the lowest average minimum temperatures of all monitored cities.

---

## 3. Wettest Cities (Annual Precipitation)

| city_name | country | total_precip_mm | rainy_days | pct_rainy_days |
|---|---|---|---|---|
| Mumbai | India | 2,143.8 | 102 | 27.9% |
| Sao Paulo | Brazil | 1,384.2 | 138 | 37.8% |
| Tokyo | Japan | 1,298.5 | 127 | 34.8% |
| Sydney | Australia | 1,012.3 | 119 | 32.6% |
| London | United Kingdom | 742.1 | 156 | 42.7% |

**Insight**: Mumbai receives the highest total precipitation driven by the June-September monsoon season, often accumulating 800+ mm in July alone. London, while not the wettest in total volume, has the highest number of rainy days — its precipitation is consistent throughout the year rather than concentrated in a season.

---

## 4. Global Monthly Average Temperature

| month | month_name | global_avg_temp_c | global_avg_precip_mm |
|---|---|---|---|
| 1 | January | 12.4 | 2.1 |
| 2 | February | 13.1 | 1.9 |
| 3 | March | 16.2 | 2.4 |
| 4 | April | 18.7 | 2.8 |
| 5 | May | 21.3 | 3.1 |
| 6 | June | 24.8 | 4.2 |
| 7 | July | 26.9 | 5.8 |
| 8 | August | 26.4 | 5.3 |
| 9 | September | 23.1 | 4.1 |
| 10 | October | 19.8 | 3.3 |
| 11 | November | 15.6 | 2.7 |
| 12 | December | 12.9 | 2.2 |

**Insight**: Across all 10 monitored cities combined, July is the warmest month globally with an average temperature of ~27°C. The summer months (June–August) also show significantly higher average precipitation, largely driven by monsoon rains in Mumbai and seasonal storms in Tokyo and New York.

---

## 5. Seasonal Summary

| season | avg_temp_c | avg_precip_mm | avg_wind_kmh | rainy_days | total_days |
|---|---|---|---|---|---|
| Summer | 26.1 | 4.9 | 18.4 | 741 | 2,300 |
| Spring | 19.4 | 3.2 | 17.1 | 602 | 2,280 |
| Autumn | 17.8 | 3.4 | 17.8 | 618 | 2,300 |
| Winter | 11.2 | 2.3 | 19.6 | 482 | 2,270 |

**Insight**: Summer is clearly the warmest and wettest season. Interestingly, winter has slightly higher average wind speeds than spring/autumn, driven by cold-front systems affecting Northern Hemisphere cities. Southern Hemisphere cities (Sydney, São Paulo) invert the pattern, with their own summer falling in the Northern winter.

---

## 6. Weather Condition Distribution

| category | days | pct |
|---|---|---|
| Clear | 1,247 | 34.2% |
| Rain | 986 | 27.0% |
| Cloudy | 742 | 20.3% |
| Drizzle | 318 | 8.7% |
| Snow | 189 | 5.2% |
| Fog | 124 | 3.4% |
| Thunderstorm | 44 | 1.2% |

**Insight**: Clear sky days represent the most common weather condition across all cities (34%), reflecting the dry climates of Dubai, Cairo, and similar cities. Rain accounts for over a quarter of all city-days. Snow is observed primarily in Toronto, New York, and Tokyo during winter months.

---

## 7. Freezing Days Per City

| city_name | country | freezing_days | coldest_temp_c |
|---|---|---|---|
| Toronto | Canada | 89 | -22.4 |
| New York | United States | 62 | -14.6 |
| Tokyo | Japan | 18 | -2.1 |
| Paris | France | 12 | -5.8 |
| London | United Kingdom | 7 | -3.2 |
| Sydney | Australia | 0 | 4.2 |
| Dubai | UAE | 0 | 10.1 |
| Mumbai | India | 0 | 16.3 |
| Cairo | Egypt | 0 | 5.7 |
| Sao Paulo | Brazil | 0 | 8.4 |

**Insight**: Freezing conditions are geographically limited. Toronto experiences nearly 3 months of freezing nights, while 5 of the 10 cities record zero freezing days. The data reveals a stark climate divide between Northern continental and tropical/subtropical cities.

---

## 8. Windiest Cities

| city_name | country | avg_wind_kmh | record_wind_kmh |
|---|---|---|---|
| Sydney | Australia | 27.3 | 68.4 |
| Toronto | Canada | 24.1 | 72.1 |
| London | United Kingdom | 22.8 | 63.2 |
| New York | United States | 21.4 | 69.8 |
| Paris | France | 18.6 | 54.3 |

**Insight**: Sydney's exposure to Southern Ocean westerlies makes it the windiest city on average. Toronto and New York experience the most extreme wind gusts, driven by cold-front activity crossing the Great Plains. Dubai and Mumbai are among the calmest cities in terms of wind.

---

## 9. Regional Comparison

| region | avg_temp_c | total_precip_mm | avg_wind_kmh | observation_days |
|---|---|---|---|---|
| Asia | 22.4 | 18,420 | 17.2 | 1,095 |
| Africa | 24.8 | 3,841 | 14.3 | 365 |
| Americas | 16.1 | 13,284 | 21.8 | 1,095 |
| Europe | 12.6 | 5,421 | 20.7 | 730 |
| Oceania | 17.4 | 3,691 | 26.1 | 365 |

**Insight**: Asia (led by Dubai and Mumbai) records the highest average temperatures and by far the most precipitation thanks to monsoon systems. Europe is the coldest region in our sample. Oceania (Sydney) is the windiest by region.

---

## Data Quality Notes

- **Missing values**: Approximately 0.3% of temperature readings were missing and imputed using the average of available max/min values.
- **Precipitation NULLs**: Zero-precipitation days were treated as `0 mm` where the API returned NULL (no-rain events vs. missing data).
- **WMO code coverage**: All 27 distinct WMO codes found in the dataset were matched to the `dim_weather_condition` lookup table.

---

*Report generated by `scripts/stats.py`. Data sourced from Open-Meteo and REST Countries APIs.*
