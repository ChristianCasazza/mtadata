---
title: Weekly Ridership Per Stations
---

- Check out hourly riders per station


## Station Map

```sql riders
select 
* 
from mta.total_riders_per_station
```


<PointMap 
    data={riders} 
    lat=latitude 
    long=longitude 
    pointName=station_complex
    name=my_point_map
    height=600
    tooltipType=hover
    tooltip={[
        {id: 'station_complex', showColumnName: false, valueClass: 'text-xl font-semibold'}    
    ]}
/>

```sql hourly_riders
select 
* 
from mta.weekly_riders_per_station
where station_complex = '${inputs.my_point_map.station_complex}'
```


<LineChart 
    data={hourly_riders}
    x=week_date
    y=total_ridership 
    y2=total_precipitation
    chartAreaHeight=200
/>