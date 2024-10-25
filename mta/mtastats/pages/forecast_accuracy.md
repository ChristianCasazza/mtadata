---
title: Agency Forecasting Analysis
---

Analyze how well each agency correctly forecasted their 2023 expenses

<Dropdown name=granularity>
    <DropdownOption valueLabel="Agency" value="1" />
    <DropdownOption valueLabel="Expense Type" value="2" />
    <DropdownOption valueLabel="General Ledger" value="3" />
</Dropdown>

{#if inputs.granularity.value == 1}

## Choose an Agency

```unique_agencies
SELECT DISTINCT agency_full_name
FROM mta.forecast_accuracy_2023
```
<Dropdown
    name=unique_agencies
    data={unique_agencies}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
/>

```unique_expense_type
SELECT DISTINCT expense_type
FROM mta.forecast_accuracy_2023
```

<Dropdown
    name=unique_expense_type
    data={unique_expense_type}
    value=expense_type
    title="Select an Expense Type" 
    defaultValue="NREIMB"
/>

```unique_items
SELECT DISTINCT general_ledger
FROM mta.forecast_accuracy_2023
```

<Dropdown
    name=unique_items
    data={unique_items}
    value=general_ledger
    title="Select an Agency" 
    defaultValue="Materials and Supplies"
/>

```forecast_info
select * 
FROM mta.forecast_accuracy_2023
where general_ledger = '${inputs.unique_items.value}' AND expense_type = '${inputs.unique_expense_type.value}' AND agency_full_name = '${inputs.unique_agencies.value}' 
```


<DataTable data={forecast_info} />

<BigValue 
  data={forecast_info} 
  value=total_actual_2023
  fmt=num0
  title='Total 2023 Actual'
/><br/>


<BigValue 
  data={forecast_info} 
  value=total_adopted_budget_2019
  fmt=usd0
  title='2019 Forecasted' 
/>

<BigValue 
  data={forecast_info} 
  value=total_adopted_budget_2020
  fmt=usd0
  title='2020 Forecasted' 
/>

<BigValue 
  data={forecast_info} 
  value=total_adopted_budget_2021
  fmt=usd0
  title='2021 Forecasted' 
/>

<BigValue 
  data={forecast_info} 
  value=total_adopted_budget_2022
  fmt=usd0
  title='2022 Forecasted' 
/>

<BigValue 
  data={forecast_info} 
  value=difference_2019_vs_actual
  fmt=usd0
  title='2019 Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=difference_2020_vs_actual
  fmt=usd0
  title='2020 Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=difference_2021_vs_actual
  fmt=usd0
  title='2021 Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=difference_2022_vs_actual
  fmt=usd0
  title='2022 Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=percentage_diff_2019_vs_actual
  fmt=pct0
  title='2019 % Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=percentage_diff_2020_vs_actual
  fmt=pct0
  title='2020 % Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=percentage_diff_2021_vs_actual
  fmt=pct0
  title='2021 % Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=percentage_diff_2022_vs_actual
  fmt=pct0
  title='2022 % Difference' 
/>

{:else if inputs.granularity.value == 2}

## Choose an Agency

```unique_agencies
SELECT DISTINCT agency_full_name
FROM mta.forecast_accuracy_2023
```
<Dropdown
    name=unique_agencies
    data={unique_agencies}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
/>

```unique_expense_type
SELECT DISTINCT expense_type
FROM mta.forecast_accuracy_2023
```

<Dropdown
    name=unique_expense_type
    data={unique_expense_type}
    value=expense_type
    title="Select an Expense Type" 
    defaultValue="NREIMB"
/>


```forecast_info_full
SELECT 
    agency_full_name,
    expense_type,

    -- Total amounts for Adopted Budget by Financial Plan Year
    SUM(total_adopted_budget_2019) AS total_adopted_budget_2019,
    SUM(total_adopted_budget_2020) AS total_adopted_budget_2020,
    SUM(total_adopted_budget_2021) AS total_adopted_budget_2021,
    SUM(total_adopted_budget_2022) AS total_adopted_budget_2022,

    -- Total amount for Actual 2023
    SUM(total_actual_2023) AS total_actual_2023,

    -- Differences between Actual and Adopted Budget for each year
    SUM(difference_2019_vs_actual) AS difference_2019_vs_actual,
    SUM(difference_2020_vs_actual) AS difference_2020_vs_actual,
    SUM(difference_2021_vs_actual) AS difference_2021_vs_actual,
    SUM(difference_2022_vs_actual) AS difference_2022_vs_actual,

    -- Percentage differences for each year
    AVG(CASE 
        WHEN total_adopted_budget_2019 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2019) / total_adopted_budget_2019
        ELSE NULL 
    END) AS percentage_diff_2019_vs_actual,

    AVG(CASE 
        WHEN total_adopted_budget_2020 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2020) / total_adopted_budget_2020
        ELSE NULL 
    END) AS percentage_diff_2020_vs_actual,

    AVG(CASE 
        WHEN total_adopted_budget_2021 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2021) / total_adopted_budget_2021
        ELSE NULL 
    END) AS percentage_diff_2021_vs_actual,

    AVG(CASE 
        WHEN total_adopted_budget_2022 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2022) / total_adopted_budget_2022
        ELSE NULL 
    END) AS percentage_diff_2022_vs_actual

FROM forecast_accuracy_2023
GROUP BY agency_full_name, expense_type

```

```forecast_info_expense
select * 
FROM ${forecast_info_full}
where expense_type = '${inputs.unique_expense_type.value}' AND agency_full_name = '${inputs.unique_agencies.value}' 
```

<DataTable data={forecast_info_expense} />

<BigValue 
  data={forecast_info_expense} 
  value=total_actual_2023
  fmt=num0
  title='Total 2023 Actual'
/><br/>


<BigValue 
  data={forecast_info_expense} 
  value=total_adopted_budget_2019
  fmt=usd0
  title='2019 Forecasted' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=total_adopted_budget_2020
  fmt=usd0
  title='2020 Forecasted' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=total_adopted_budget_2021
  fmt=usd0
  title='2021 Forecasted' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=total_adopted_budget_2022
  fmt=usd0
  title='2022 Forecasted' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=difference_2019_vs_actual
  fmt=usd0
  title='2019 Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=difference_2020_vs_actual
  fmt=usd0
  title='2020 Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=difference_2021_vs_actual
  fmt=usd0
  title='2021 Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=difference_2022_vs_actual
  fmt=usd0
  title='2022 Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=percentage_diff_2019_vs_actual
  fmt=pct0
  title='2019 % Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=percentage_diff_2020_vs_actual
  fmt=pct0
  title='2020 % Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=percentage_diff_2021_vs_actual
  fmt=pct0
  title='2021 % Difference' 
/>

<BigValue 
  data={forecast_info_expense} 
  value=percentage_diff_2022_vs_actual
  fmt=pct0
  title='2022 % Difference' 
/>

{:else }

## Choose an Agency

```unique_agencies
SELECT DISTINCT agency_full_name
FROM mta.forecast_accuracy_2023
```
<Dropdown
    name=unique_agencies
    data={unique_agencies}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
/>


```forecast_info_all
SELECT 
    agency_full_name,

    -- Total amounts for Adopted Budget by Financial Plan Year
    SUM(total_adopted_budget_2019) AS total_adopted_budget_2019,
    SUM(total_adopted_budget_2020) AS total_adopted_budget_2020,
    SUM(total_adopted_budget_2021) AS total_adopted_budget_2021,
    SUM(total_adopted_budget_2022) AS total_adopted_budget_2022,

    -- Total amount for Actual 2023
    SUM(total_actual_2023) AS total_actual_2023,

    -- Differences between Actual and Adopted Budget for each year
    SUM(difference_2019_vs_actual) AS difference_2019_vs_actual,
    SUM(difference_2020_vs_actual) AS difference_2020_vs_actual,
    SUM(difference_2021_vs_actual) AS difference_2021_vs_actual,
    SUM(difference_2022_vs_actual) AS difference_2022_vs_actual,

    -- Percentage differences for each year
    AVG(CASE 
        WHEN total_adopted_budget_2019 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2019) / total_adopted_budget_2019
        ELSE NULL 
    END) AS percentage_diff_2019_vs_actual,

    AVG(CASE 
        WHEN total_adopted_budget_2020 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2020) / total_adopted_budget_2020
        ELSE NULL 
    END) AS percentage_diff_2020_vs_actual,

    AVG(CASE 
        WHEN total_adopted_budget_2021 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2021) / total_adopted_budget_2021
        ELSE NULL 
    END) AS percentage_diff_2021_vs_actual,

    AVG(CASE 
        WHEN total_adopted_budget_2022 <> 0 
        THEN (total_actual_2023 - total_adopted_budget_2022) / total_adopted_budget_2022
        ELSE NULL 
    END) AS percentage_diff_2022_vs_actual

FROM forecast_accuracy_2023
GROUP BY agency_full_name
```

```forecast_info_agency
select * 
FROM ${forecast_info_all}
where agency_full_name = '${inputs.unique_agencies.value}' 
```

<DataTable data={forecast_info_agency} />

<BigValue 
  data={forecast_info_agency} 
  value=total_actual_2023
  fmt=num0
  title='Total 2023 Actual'
/><br/>


<BigValue 
  data={forecast_info_agency} 
  value=total_adopted_budget_2019
  fmt=usd0
  title='2019 Forecasted' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=total_adopted_budget_2020
  fmt=usd0
  title='2020 Forecasted' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=total_adopted_budget_2021
  fmt=usd0
  title='2021 Forecasted' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=total_adopted_budget_2022
  fmt=usd0
  title='2022 Forecasted' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=difference_2019_vs_actual
  fmt=usd0
  title='2019 Difference' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=difference_2020_vs_actual
  fmt=usd0
  title='2020 Difference' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=difference_2021_vs_actual
  fmt=usd0
  title='2021 Difference' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=difference_2022_vs_actual
  fmt=usd0
  title='2022 Difference' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=percentage_diff_2019_vs_actual
  fmt=pct0
  title='2019 % Difference' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=percentage_diff_2020_vs_actual
  fmt=pct0
  title='2020 % Difference' 
/>

<BigValue 
  data={forecast_info_agency} 
  value=percentage_diff_2021_vs_actual
  fmt=pct0
  title='2021 % Difference' 
/>

<BigValue 
  data={forecast_info} 
  value=percentage_diff_2022_vs_actual
  fmt=pct0
  title='2022 % Difference' 
/>


## Choose an Agency

```unique_agencies_agg
SELECT DISTINCT agency_full_name
FROM mta.forecast_accuracy_2023
```
<Dropdown
    name=unique_agencies_agg
    data={unique_agencies_agg}
    value=agency_full_name
    title="Select an Agency" 
    defaultValue="Long Island Rail Road"
/>



```agency_expenses
select * 
from mta.largest_expense_differences_2023
where agency_full_name = '${inputs.unique_agencies_agg.value}' 
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


{/if}