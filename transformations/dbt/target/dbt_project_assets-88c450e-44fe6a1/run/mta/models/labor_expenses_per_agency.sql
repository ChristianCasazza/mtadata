
  
    
    

    create  table
      "data"."main"."labor_expenses_per_agency__dbt_tmp"
  
    as (
      SELECT 
    agency_full_name,
    financial_plan_year,
    expense_type,
    general_ledger,
    SUM(amount) AS total_labor_expenses
FROM 
    "data"."main"."mta_operations_statement"
WHERE 
    subtype = 'Labor Expenses' AND 
    scenario = 'Actual'
GROUP BY 
    agency_full_name,
    financial_plan_year,
    expense_type,
    general_ledger
ORDER BY 
    agency_full_name, financial_plan_year, expense_type, general_ledger
    );
  
  