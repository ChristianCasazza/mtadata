---
title: Daily Ridership Per Transport Type
---

- Check out hourly riders per station


## Station Map

```unique_types
SELECT DISTINCT transport_type
FROM mta.daily_ridership
```


<Dropdown
    name=unique_types
    data={unique_types}
    value=transport_type
    title="Select a Transport Type" 
    defaultValue="subway"
/>


```sql daily_riders
select 
* 
from mta.daily_ridership
where transport_type = '${inputs.unique_types.value}'
```



<LineChart 
    data={daily_riders}
    x=week_start
    y=ridership 
    yAxisTitle="Ridership Per Day"
    series=transport_type
    chartAreaHeight= 400
/>