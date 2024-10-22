SELECT 
    agency,
    fiscal_year,
    expense_type,
    general_ledger,
    SUM(amount) AS total_labor_expenses
FROM 
    {{ source('main', 'mta_operations_statement') }}
WHERE 
    subtype = 'Labor Expenses' AND 
    scenario = 'Actual'
GROUP BY 
    agency,
    fiscal_year,
    expense_type,
    general_ledger
ORDER BY 
    agency, fiscal_year, expense_type, general_ledger
