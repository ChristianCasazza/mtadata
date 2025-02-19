---
title: Labor Expenses per Agency
---

Analyze how well each agency correctly forecasted their 2023 expenses

<Dropdown name=granularity>
    <DropdownOption valueLabel="Max Filter" value="1" />
    <DropdownOption valueLabel="Remove Expense_Type" value="2" />
    <DropdownOption valueLabel="Agency Only" value="3" />
    <DropdownOption valueLabel="Aggregate Per Year" value="4" />
</Dropdown>

{#if inputs.granularity.value == 1}

## Choose an Agency

```unique_agency_full
SELECT DISTINCT agency_full_name
FROM mta.labor_expenses_per_agency
```


<Dropdown
    name=unique_agencies_full
    data={unique_agency_full}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
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

```agency_expenses_full
select * 
from mta.labor_expenses_per_agency
where agency_full_name = '${inputs.unique_agencies_full.value}' AND expense_type = '${inputs.unique_expense.value}'
```

<BarChart 
    data={agency_expenses_full}
    x=financial_plan_year
    y=total_labor_expenses
    xFmt=yyyy
    yFmt=usd
    series=general_ledger
    chartAreaHeight=400
/>






{:else if inputs.granularity.value == 2}

## Choose an Agency

```unique_agency_full
SELECT DISTINCT agency_full_name
FROM mta.labor_expenses_per_agency
```

<Dropdown
    name=unique_agency_full
    data={unique_agency_full}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
/>


```agency_expenses_type
SELECT 
    agency_full_name,
    financial_plan_year,
    general_ledger,
    SUM(total_labor_expenses) AS total_labor_expenses
FROM 
    labor_expenses_per_agency
GROUP BY 
    agency_full_name,
    financial_plan_year,
    general_ledger
ORDER BY 
    agency_full_name, financial_plan_year, general_ledger


```

```labor_expenses_type
select * 
FROM ${agency_expenses_type}
where agency_full_name = '${inputs.unique_agency_full.value}' 
```

<BarChart 
    data={labor_expenses_type}
    x=financial_plan_year
    y=total_labor_expenses
    xFmt=yyyy
    yFmt=usd
    series=general_ledger
    chartAreaHeight=400
/>


{:else if inputs.granularity.value == 3}

```unique_agency_full
SELECT DISTINCT agency_full_name
FROM mta.labor_expenses_per_agency
```

<Dropdown
    name=unique_agency_full
    data={unique_agency_full}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
/>


```agency_expenses_agency
SELECT 
    agency_full_name,
    CAST(CAST(financial_plan_year AS INT) AS TEXT) AS financial_plan_year,
    SUM(total_labor_expenses) AS total_labor_expenses
FROM 
    labor_expenses_per_agency
GROUP BY 
    agency_full_name,
    financial_plan_year
ORDER BY 
    agency_full_name, financial_plan_year


```

```labor_expenses_agency
select * 
FROM ${agency_expenses_agency}
where agency_full_name = '${inputs.unique_agency_full.value}' 
```


<DataTable data={labor_expenses_agency}/>
<BarChart 
    data={labor_expenses_agency}
    x=financial_plan_year
    y=total_labor_expenses
    xFmt=yyyy
    yFmt=usd
    chartAreaHeight=400
/>



{:else }



```agency_expenses_agg
SELECT 
    CAST(CAST(financial_plan_year AS INT) || '-01-01' AS DATE) AS financial_plan_year_date,
    SUM(total_labor_expenses) AS total_labor_expenses
FROM 
    labor_expenses_per_agency
GROUP BY 
    financial_plan_year_date
ORDER BY 
    financial_plan_year_date
```

```labor_expenses_agg
select * 
FROM ${agency_expenses_agg}
```

<DataTable data={labor_expenses_agg}/>

<BarChart 
    data={labor_expenses_agg}
    x=financial_plan_year_date
    y=total_labor_expenses
    xFmt="yyyy"
    yFmt=usd
    chartAreaHeight=400
/>


{/if}