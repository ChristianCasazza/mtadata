
  
    
    

    create  table
      "data"."main"."bond_payment_info__dbt_tmp"
  
    as (
      WITH payment_stats AS (
    SELECT
        general_ledger,
        MIN(timestamp) AS first_payment_date,
        MAX(timestamp) AS last_payment_date,
        AVG(amount) AS average_payment,
        COUNT(DISTINCT timestamp) AS total_payments,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_payment
    FROM
        "data"."main"."mta_operations_statement"
    WHERE
        scenario = 'Actual'
        AND type = 'Debt Service Expenses'
    GROUP BY
        general_ledger
),
first_payment AS (
    SELECT
        general_ledger,
        MIN(timestamp) AS first_payment_date,
        (SELECT amount FROM mta_operations_statement WHERE general_ledger = fp.general_ledger AND timestamp = MIN(fp.timestamp)) AS first_payment_amount
    FROM
        mta_operations_statement fp
    WHERE
        scenario = 'Actual'
        AND type = 'Debt Service Expenses'
    GROUP BY
        general_ledger
),
last_payment AS (
    SELECT
        general_ledger,
        MAX(timestamp) AS last_payment_date,
        (SELECT amount FROM mta_operations_statement WHERE general_ledger = lp.general_ledger AND timestamp = MAX(lp.timestamp)) AS last_payment_amount
    FROM
        mta_operations_statement lp
    WHERE
        scenario = 'Actual'
        AND type = 'Debt Service Expenses'
    GROUP BY
        general_ledger
)
-- Wrapping everything inside another DISTINCT query
SELECT DISTINCT
    ps.general_ledger,
    fp.first_payment_date,
    fp.first_payment_amount,
    ps.last_payment_date,
    lp.last_payment_amount,
    ps.average_payment,
    ps.median_payment,
    ps.total_payments
FROM
    payment_stats ps
LEFT JOIN
    first_payment fp ON ps.general_ledger = fp.general_ledger
LEFT JOIN
    last_payment lp ON ps.general_ledger = lp.general_ledger
ORDER BY
    ps.general_ledger
    );
  
  