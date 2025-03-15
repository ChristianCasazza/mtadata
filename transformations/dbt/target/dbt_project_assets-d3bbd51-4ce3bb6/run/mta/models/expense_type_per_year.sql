
  
    
    

    create  table
      "mtastats"."main"."expense_type_per_year__dbt_tmp"
  
    as (
      SELECT 
    agency,
    fiscal_year,
    general_ledger,
    SUM(amount) AS total_expenses
FROM 
    "mtastats"."main"."mta_operations_statement"
WHERE 
    scenario = 'Actual'
    AND type = 'Total Expenses Before Non-Cash Liability Adjs.'
GROUP BY 
    agency,
    fiscal_year,
    general_ledger
ORDER BY 
    agency, fiscal_year, general_ledger
    );
  
  