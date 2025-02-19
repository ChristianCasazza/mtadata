---
title: Subway Station Ridership Stats
---

Choose a station from a map or a list 

<Dropdown name=granularity>
    <DropdownOption valueLabel="List" value="1" />
    <DropdownOption valueLabel="Map" value="2" />
</Dropdown>

{#if inputs.granularity.value == 1}

## Choose a Station

```unique_stations
SELECT DISTINCT station_complex
FROM mta.weekly_riders_per_station
```


<Dropdown
    name=unique_stations
    data={unique_stations}
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
    y2=avg_weekly_temperature
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
/><br/>


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
  comparison=weekend_ridership_percentage_change
  comparisonFmt=pct1
  comparisonTitle="Weekend % Change"
  title='Avg 2024 Weekend Ridership'
/><br/>

<BigValue 
  data={station_ridership_stats} 
  value=weekday_transfer_percentage
  fmt=pct2
  title='2024 Weekday Transfer %'
/>
<BigValue 
  data={station_ridership_stats} 
  value=weekend_transfer_percentage
  fmt=pct2
  comparison=weekend_transfer_percentage_change
  comparisonFmt=pct1
  comparisonTitle="Weekend % Change"
  title='2024 Weekend Transfer %'
/>



{:else }


## Station Map

```sql riders
select 
* 
from mta.subway_station_stats
```


<PointMap 
    data={riders} 
    lat=latitude 
    long=longitude 
    pointName=station_complex
    name=my_point_map
    height=300
    tooltipType=hover
    tooltip={[
        {id: 'station_complex', showColumnName: false, valueClass: 'text-xl font-semibold'}    
    ]}
/>

```sql station_weekly_map
select 
* 
from mta.weekly_riders_per_station
where station_complex = '${inputs.my_point_map.station_complex}'
```


<LineChart 
    data={station_weekly_map}
    x=week_start
    y=total_weekly_ridership
    y2=avg_weekly_temperature
    yAxisTitle="Ridership Per Week"
    chartAreaHeight= 400
/>

```sql station_ridership_stats_map
select 
* 
from mta.subway_station_stats
where station_complex = '${inputs.my_point_map.station_complex}'
```

```sql omny_stats_map
select 
* 
from mta.omny_adoption_by_station
where station_complex = '${inputs.my_point_map.station_complex}'
```

<BigValue 
  data={omny_stats_map} 
  value=omny_2022
  fmt=pct1
  title='OMNY 2022'
/>
<BigValue 
  data={omny_stats_map} 
  value=omny_2023
  fmt=pct1
  comparison=omny_2023_growth
  comparisonFmt=pct1
  comparisonTitle="YoY"
  title='OMNY 2023'
/>
<BigValue 
  data={omny_stats_map} 
  value=omny_2024
  fmt=pct1
  comparison=omny_2024_growth
  comparisonFmt=pct1
  comparisonTitle="YoY"
  title='OMNY 2024'
/><br/>


<BigValue 
  data={station_ridership_stats_map} 
  value=avg_weekday_ridership
  fmt=num0
  title='Avg 2024 Weekday Ridership'
/>
<BigValue 
  data={station_ridership_stats_map} 
  value=avg_weekend_ridership
  fmt=num0
  comparison=weekend_ridership_percentage_change
  comparisonFmt=pct1
  comparisonTitle="Weekend % Change"
  title='Avg 2024 Weekend Ridership'
/><br/>

<BigValue 
  data={station_ridership_stats_map} 
  value=weekday_transfer_percentage
  fmt=pct2
  title='2024 Weekday Transfer %'
/>
<BigValue 
  data={station_ridership_stats_map} 
  value=weekend_transfer_percentage
  fmt=pct2
  comparison=weekend_transfer_percentage_change
  comparisonFmt=pct1
  comparisonTitle="Weekend % Change"
  title='2024 Weekend Transfer %'
/>

{/if}