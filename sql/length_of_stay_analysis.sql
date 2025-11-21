WITH base AS (
  SELECT
    `Medical Condition` AS medical_condition,
    `Admission Type`   AS admission_type,
    `Date of Admission` AS admission_date,
    `Discharge Date`    AS discharge_date,
    CAST(`Billing Amount` AS FLOAT64) AS billing_amount
  FROM `hybrid-creek-475214-a9.healthcare_analytics.visits_raw`
),
with_los AS (
  SELECT
    medical_condition,
    admission_type,
    billing_amount,
    DATE_DIFF(discharge_date, admission_date, DAY) AS length_of_stay_days
  FROM base
  WHERE admission_date IS NOT NULL
    AND discharge_date IS NOT NULL
)
SELECT
  medical_condition,
  admission_type,
  COUNT(*) AS num_visits,
  ROUND(AVG(length_of_stay_days), 2) AS avg_los_days,
  APPROX_QUANTILES(length_of_stay_days, 5)[OFFSET(2)] AS median_los_days,
  ROUND(AVG(billing_amount), 2) AS avg_billing
FROM with_los
GROUP BY medical_condition, admission_type
HAVING num_visits >= 10
ORDER BY avg_los_days DESC
LIMIT 30;
