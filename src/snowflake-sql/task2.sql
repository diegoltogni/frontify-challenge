-- Fixing the dbt+scheema
USE database APPLICATION_TASKS;
USE schema DATA_ANALYTICS_ENGINEER;

/*
Write an SQL-statement that gets the
    - current MRR and the MRR of the previous month
    - per ACCOUNT_ID and MONTH
    - from the MRR_PER_ACCOUNT table.
*/


---------------------------
-- My answer:

SELECT account_id
       , month
       , mrr
       , LAG(mrr, 1) OVER (PARTITION BY account_id ORDER BY month ASC) AS previous_month_mrr
  FROM mrr_per_account
 ORDER BY 1,2
;