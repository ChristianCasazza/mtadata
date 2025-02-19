---
title: Borough Analysis
---


Analyze Fare Class Usage across Boroughs


## Fare Class Analysis

```unique_days
SELECT DISTINCT day_type
FROM mta.fare_class_boro
```


<Dropdown
    name=unique_days
    data={unique_days}
    value=day_type
    title="Select a Day Type" 
    defaultValue="Weekday"
/>

```sql riders
select 
* 
from mta.fare_class_boro
where day_type = '${inputs.unique_days.value}'
```


<BarChart 
    data={riders}
    x=borough
    y=ridership_percentage
    yFmt=pct0
    series=fare_class_category
    type=stacked100
/>

