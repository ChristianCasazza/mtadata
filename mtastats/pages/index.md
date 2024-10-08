---
title: MTA DOG
---

- View stats for Passing to different areas


## Passer Stats

```sql riders
select 
* 
from mta.total_riders_per_station
```


<BubbleMap 
    data={riders} 
    lat=latitude 
    long=longitude 
    size=total_ridership 
    sizeFmt='#,##0,,"M"'
    value=total_ridership 
    valueFmt='#,##0,,"M"'
    pointName=station_complex 
    height=600 
    basemap={`https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.{ext}`} 
    tooltipType=hover
    tooltip={[
        {id: 'station_complex', showColumnName: false, valueClass: 'text-xl font-semibold'},
        {id: 'total_ridership', fmt: '#,##0,,"M"', fieldClass: 'text-[grey]', valueClass: 'text-[green]'}
    ]}
/>
