---
title: Subway Station Ridership Stats
---



## Choose a Station

```unique_station
SELECT DISTINCT station_complex
FROM mta.weekly_riders_per_station
```


<Dropdown
    name=unique_stations
    data={unique_station}
    value=station_complex
    title="Select a Station" 
    defaultValue="Times Sq-42 St (N,Q,R,W,S,1,2,3,7)/42 St (A,C,E)"
/>

```sql station_weekly_stats
select 
* 
from mta.weekly_riders_per_station
where station_complex = '${inputs.unique_stations.value}'
```

<LineChart 
    data={station_weekly_stats}
    x=week_start
    y=total_weekly_ridership 
    yAxisTitle="Ridership Per Week"
    chartAreaHeight= 400
/>


```sql station_ridership_stats
select 
* 
from mta.subway_station_stats
where station_complex = '${inputs.unique_stations.value}'
```

```sql omny_stats
select 
* 
from mta.omny_adoption_by_station
where station_complex = '${inputs.unique_stations.value}'
```

<BigValue 
  data={omny_stats} 
  value=omny_2022
  fmt=pct1
  title='OMNY 2022'
/>
<BigValue 
  data={omny_stats} 
  value=omny_2023
  fmt=pct1
  comparison=omny_2023_growth
  comparisonFmt=pct1
  comparisonTitle="YoY"
  title='OMNY 2023'
/>
<BigValue 
  data={omny_stats} 
  value=omny_2024
  fmt=pct1
  comparison=omny_2024_growth
  comparisonFmt=pct1
  comparisonTitle="YoY"
  title='OMNY 2024'
/>


<BigValue 
  data={station_ridership_stats} 
  value=avg_weekday_ridership
  fmt=num0
  title='Avg 2024 Weekday Ridership'
/>
<BigValue 
  data={station_ridership_stats} 
  value=avg_weekend_ridership
  fmt=num0
  comparison=weekend_percentage_change
  comparisonFmt=pct1
  comparisonTitle="Weekend % Change"
  title='Avg 2024 Weekend Ridership'
/>

