---
title: Expenses Per Agency
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
from mta.expense_type_per_year
where agency = '${inputs.unique_agencies.value}'
```

<BarChart 
    data={agency_expenses}
    x=fiscal_year
    y=total_expenses
    xFmt=yyyy
    yFmt=usd
    series=general_ledger
    chartAreaHeight=400
/>