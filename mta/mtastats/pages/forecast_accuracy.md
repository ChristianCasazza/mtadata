---
title: Agency Forecasting Analysis
---

Analyze how well each agency correctly forecasted their 2023 expenses

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