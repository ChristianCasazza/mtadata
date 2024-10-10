---
title: Hourly Riders
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
from mta.hourly_riders
where station_complex = '${inputs.my_point_map.station_complex}'
```


<LineChart 
    data={hourly_riders}
    x=date
    y=total_ridership 
    chartAreaHeight=600
/>