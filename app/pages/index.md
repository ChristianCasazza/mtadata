---
title: MTA Data Analytics
---

## Analyze the MTA's open data by navigating to the pages on the left

-[Station Stats](/station_stats) shows weekly ridership for every subway station, as well as stats around OMNY adoption, ridership, and transfers

-[Labor Expenses Per Agency](/agency_labor_breakdown) shows the breakdown of actual labor expenses with different levels of granularity

-[Transport Type](/analysis_by_transport_type) shows the weekly ridership of various MTA transport types, compared to total weekly precipitation

-[Bond Information](/bond_info) provides stats on the six bonds the MTA has listed as having Debt Service Expenses

-[Borough Analysis](/borough_analysis) shows a breakdown of payments by fare class by borough on weekdays and weekends

-[Agency Forecasting Analysis](/forecast_accuracy) shows how well the MTA forcasted their actual 2023 expenses from 2019-2022 at different levels of granularity 

## Datasets used for Analysis

The following datasets are used to power the analysis:

[MTA Hourly Subway ](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-February-202/wujg-7c2s/about_data)

[MTA Daily Ridership ](https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-Beginning-2020/vxuj-8kew/about_data)

[MTA Statement of Operations](https://data.ny.gov/Transportation/MTA-Statement-of-Operations-Beginning-2019/yg77-3tkj/about_data)

Additional, weather data was obtained for free without an API key using [Open Mateo](https://open-meteo.com/)


## Accessing the Code

All code to power this application and the data pipelines backing it can be accessed at the open [mtastats Github repo](https://github.com/ChristianCasazza/mtadata)
