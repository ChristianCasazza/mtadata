---
title: Days of the Week
---

- View stats for Passing to different areas


## Day of the week Average Ridership

```unique_days
SELECT DISTINCT day_of_week
FROM mta.avg_riders_per_day
```


<Dropdown
    name=unique_days
    data={unique_days}
    value=day_of_week
    title="Select a Day" 
    defaultValue="1"
/>

```sql riders
select 
* 
from mta.avg_riders_per_day
where day_of_week = '${inputs.unique_days.value}'
```



<BubbleMap 
    data={riders} 
    lat=latitude 
    long=longitude 
    size=average_ridership 
    sizeFmt='#,##0,,"M"'
    value=average_ridership 
    valueFmt='#,##0,,"M"'
    pointName=station_complex 
    height=600 
    basemap={`https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.{ext}`} 
    tooltipType=hover
    tooltip={[
        {id: 'station_complex', showColumnName: false, valueClass: 'text-xl font-semibold'},
        {id: 'average_ridership', fmt: '#,##0,,"M"', fieldClass: 'text-[grey]', valueClass: 'text-[green]'}
    ]}
/>
