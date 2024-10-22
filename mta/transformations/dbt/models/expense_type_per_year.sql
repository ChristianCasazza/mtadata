SELECT 
    agency,
    fiscal_year,
    general_ledger,
    SUM(amount) AS total_expenses
FROM 
    {{ source('main', 'mta_operations_statement') }}
WHERE 
    scenario = 'Actual'
    AND type = 'Total Expenses Before Non-Cash Liability Adjs.'
GROUP BY 
    agency,
    fiscal_year,
    general_ledger
ORDER BY 
    agency, fiscal_year, general_ledger
