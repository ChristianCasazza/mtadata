---
title: Labor Expenses per Agency
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

```unique_expense
SELECT DISTINCT expense_type
FROM mta.labor_expenses_per_agency
```


<Dropdown
    name=unique_expense
    data={unique_expense}
    value=expense_type
    title="Select an Expense Type" 
    defaultValue="NREIMB"
/>

```agency_expenses
select * 
from mta.labor_expenses_per_agency
where agency = '${inputs.unique_agencies.value}' AND expense_type = '${inputs.unique_expense.value}'
```

<BarChart 
    data={agency_expenses}
    x=fiscal_year
    y=total_labor_expenses
    xFmt=yyyy
    yFmt=usd
    series=general_ledger
    chartAreaHeight=400
/>