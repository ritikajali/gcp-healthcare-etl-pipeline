WITH visits AS (
  SELECT
    `Medical Condition` AS medical_condition,
    CAST(`Billing Amount` AS FLOAT64) AS billing_amount
  FROM `hybrid-creek-475214-a9.healthcare_analytics.visits_raw`
),
totals AS (
  SELECT
    SUM(billing_amount) AS grand_total_billing
  FROM visits
)
SELECT
  v.medical_condition,
  COUNT(*) AS num_visits,
  ROUND(AVG(billing_amount), 2) AS avg_billing,
  ROUND(SUM(billing_amount), 2) AS total_billing,
  ROUND(
    100 * SAFE_DIVIDE(SUM(billing_amount), t.grand_total_billing),
    2
  ) AS pct_of_total_revenue
FROM visits v
CROSS JOIN totals t
GROUP BY medical_condition, t.grand_total_billing
ORDER BY total_billing DESC

