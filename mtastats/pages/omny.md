---
title: Omny Adoption
---

- View stats for Passing to different areas


## Omny Adoption

```unique_years
SELECT DISTINCT year
FROM mta.omny_adoption_by_station
```


<Dropdown
    name=unique_years
    data={unique_years}
    value=year
    title="Select a Year" 
    defaultValue=2024
/>

```sql omny
select 
* 
from mta.omny_adoption_by_station
where year = '${inputs.unique_years.value}'
```



<BubbleMap 
    data={omny} 
    lat=latitude 
    long=longitude 
    size=omny_percentage 
    sizeFmt=#,##0.0%
    value=omny_percentage 
    valueFmt=#,##0.0%
    pointName=station_name 
    height=600 
    basemap={`https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.{ext}`} 
    tooltipType=hover
    tooltip={[
        {id: 'station_name', showColumnName: false, valueClass: 'text-xl font-semibold'},
        {id: 'omny_percentage', fmt: '#,##0.0%' , fieldClass: 'text-[grey]', valueClass: 'text-[green]'}
    ]}
/>


```sql omny_over_time
select 
* 
from mta.omny_adoption_increase
```

<DataTable data={omny_over_time} />

<BubbleMap 
    data={omny_over_time} 
    lat=latitude 
    long=longitude 
    size=omny_percentage_increase
    sizeFmt=#,##0.0%
    value=omny_percentage_increase 
    valueFmt=#,##0.0%
    pointName=station_name 
    height=600 
    basemap={`https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.{ext}`} 
    tooltipType=hover
    tooltip={[
        {id: 'station_name', showColumnName: false, valueClass: 'text-xl font-semibold'},
        {id: 'omny_percentage_increase', fmt: '#,##0.0%' , fieldClass: 'text-[grey]', valueClass: 'text-[green]'}
    ]}
/>