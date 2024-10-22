---
title: Largest 2023 Actual Expense Underprojections
---

## Choose an Agency

```unique_agency
SELECT DISTINCT agency
FROM mta.labor_expenses_per_agency
```


<Dropdown
    name=unique_agencies
    data={unique_agency}
    value=agency
    title="Select an Agency" 
    defaultValue="LIRR"
/>


```agency_expenses
select * 
from mta.largest_expense_differences_2023
where agency = '${inputs.unique_agencies.value}' 
```

<BarChart 
    data={agency_expenses}
    x=general_ledger
    y=difference
    yFmt=usd
    chartAreaHeight=400
    showAllAxisLabels=true
    labels=true
/>