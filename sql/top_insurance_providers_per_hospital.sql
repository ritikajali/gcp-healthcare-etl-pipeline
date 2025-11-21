WITH base AS (
  SELECT
    Hospital,
    `Insurance Provider` AS insurance_provider,
    CAST(`Billing Amount` AS FLOAT64) AS billing_amount
  FROM `hybrid-creek-475214-a9.healthcare_analytics.visits_raw`
),
agg AS (
  SELECT
    Hospital,
    insurance_provider,
    SUM(billing_amount) AS total_billing,
    SUM(SUM(billing_amount)) OVER (PARTITION BY Hospital) AS hospital_total_billing
  FROM base
  GROUP BY Hospital, insurance_provider
)
SELECT
  Hospital,
  insurance_provider,
  ROUND(total_billing, 2) AS total_billing,
  ROUND(
    100 * SAFE_DIVIDE(total_billing, hospital_total_billing),
    2
  ) AS pct_of_hospital_revenue
FROM agg
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY Hospital
    ORDER BY total_billing DESC
  ) <= 3                        -- top 3 insurers per hospital
ORDER BY Hospital, total_billing DESC
LIMIT 30;
