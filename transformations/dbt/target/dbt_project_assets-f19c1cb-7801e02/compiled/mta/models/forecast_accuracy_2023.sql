SELECT 
    agency_full_name,
    general_ledger,
    expense_type,

    -- Total amounts for Adopted Budget by Financial Plan Year
    SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2019 
        THEN amount 
        ELSE 0 
    END) AS total_adopted_budget_2019,

    SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2020 
        THEN amount 
        ELSE 0 
    END) AS total_adopted_budget_2020,

    SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2021 
        THEN amount 
        ELSE 0 
    END) AS total_adopted_budget_2021,

    SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2022 
        THEN amount 
        ELSE 0 
    END) AS total_adopted_budget_2022,

    -- Total amount for Actual 2023
    SUM(CASE 
        WHEN scenario = 'Actual' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2023 
        THEN amount 
        ELSE 0 
    END) AS total_actual_2023,

    -- Differences between Actual and Adopted Budget for each year
    SUM(CASE 
        WHEN scenario = 'Actual' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2023 
        THEN amount 
        ELSE 0 
    END) - SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2019 
        THEN amount 
        ELSE 0 
    END) AS difference_2019_vs_actual,

    SUM(CASE 
        WHEN scenario = 'Actual' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2023 
        THEN amount 
        ELSE 0 
    END) - SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2020 
        THEN amount 
        ELSE 0 
    END) AS difference_2020_vs_actual,

    SUM(CASE 
        WHEN scenario = 'Actual' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2023 
        THEN amount 
        ELSE 0 
    END) - SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2021 
        THEN amount 
        ELSE 0 
    END) AS difference_2021_vs_actual,

    SUM(CASE 
        WHEN scenario = 'Actual' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2023 
        THEN amount 
        ELSE 0 
    END) - SUM(CASE 
        WHEN scenario = 'Adopted Budget' 
            AND fiscal_year = 2023 
            AND financial_plan_year = 2022 
        THEN amount 
        ELSE 0 
    END) AS difference_2022_vs_actual,

    -- Percentage differences for each year
    CASE 
        WHEN SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2019 
            THEN amount 
            ELSE 0 
        END) <> 0 
        THEN (SUM(CASE 
            WHEN scenario = 'Actual' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2023 
            THEN amount 
            ELSE 0 
        END) - SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2019 
            THEN amount 
            ELSE 0 
        END)) / SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2019 
            THEN amount 
            ELSE 0 
        END)
        ELSE NULL 
    END AS percentage_diff_2019_vs_actual,

    CASE 
        WHEN SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2020 
            THEN amount 
            ELSE 0 
        END) <> 0 
        THEN (SUM(CASE 
            WHEN scenario = 'Actual' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2023 
            THEN amount 
            ELSE 0 
        END) - SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2020 
            THEN amount 
            ELSE 0 
        END)) / SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2020 
            THEN amount 
            ELSE 0 
        END)
        ELSE NULL 
    END AS percentage_diff_2020_vs_actual,

    CASE 
        WHEN SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2021 
            THEN amount 
            ELSE 0 
        END) <> 0 
        THEN (SUM(CASE 
            WHEN scenario = 'Actual' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2023 
            THEN amount 
            ELSE 0 
        END) - SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2021 
            THEN amount 
            ELSE 0 
        END)) / SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2021 
            THEN amount 
            ELSE 0 
        END)
        ELSE NULL 
    END AS percentage_diff_2021_vs_actual,

    CASE 
        WHEN SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2022 
            THEN amount 
            ELSE 0 
        END) <> 0 
        THEN (SUM(CASE 
            WHEN scenario = 'Actual' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2023 
            THEN amount 
            ELSE 0 
        END) - SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2022 
            THEN amount 
            ELSE 0 
        END)) / SUM(CASE 
            WHEN scenario = 'Adopted Budget' 
                AND fiscal_year = 2023 
                AND financial_plan_year = 2022 
            THEN amount 
            ELSE 0 
        END)
        ELSE NULL 
    END AS percentage_diff_2022_vs_actual

FROM "data"."main"."mta_operations_statement"
WHERE type = 'Total Expenses Before Non-Cash Liability Adjs.'
GROUP BY agency_full_name, general_ledger, expense_type