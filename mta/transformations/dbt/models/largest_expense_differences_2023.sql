WITH expense_differences AS (
    SELECT
        agency,
        general_ledger,
        SUM(CASE WHEN scenario = 'Actual' THEN amount ELSE 0 END) AS actual_expenses,
        SUM(CASE WHEN scenario = 'Adopted Budget' AND financial_plan_year = 2023 THEN amount ELSE 0 END) AS adopted_plan_expenses,
        SUM(CASE WHEN scenario = 'Actual' THEN amount ELSE 0 END) - SUM(CASE WHEN scenario = 'Adopted Budget' AND financial_plan_year = 2023 THEN amount ELSE 0 END) AS difference
    FROM
        {{ source('main', 'mta_operations_statement') }}
    WHERE
        fiscal_year = 2023
        AND scenario IN ('Actual', 'Adopted Budget')
        AND financial_plan_year = 2023  -- Ensure we use only the 2023 Adopted Budget
        AND type = 'Total Expenses Before Non-Cash Liability Adjs.'
        AND general_ledger != 'Reimbursable Overhead'  -- Exclude Reimbursable Overhead
    GROUP BY
        agency, general_ledger
),
ranked_expenses AS (
    SELECT
        agency,
        general_ledger,
        actual_expenses,
        adopted_plan_expenses,
        difference,
        ROW_NUMBER() OVER (PARTITION BY agency ORDER BY difference DESC) AS rank
    FROM
        expense_differences
    WHERE
        difference > 0  -- Only include cases where actual expenses are larger than the adopted budget
)
SELECT
    agency,
    general_ledger,
    actual_expenses,
    adopted_plan_expenses,
    difference
FROM
    ranked_expenses
WHERE
    rank <= 5  -- Select the top 5 items per agency
ORDER BY
    difference, rank
