/* For more information, refer to https://www.palantir.com/docs/foundry/dataset-preview/sql-preview/#sql-preview for documentation. */
WITH SortedData AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY timestamp) AS rn
    FROM
    `ri.foundry.main.dataset.6f5bf809-49c3-4ab3-8c0d-a0aaebde51da`
),


ActivityCounts AS (
SELECT case_id,
    COUNT(activity_name) AS total_activities
FROM SortedData
GROUP BY case_id
),
Timestamps AS (SELECT case_id,
MIN(CAST(timestamp AS DATE)) AS first_timestamp,
MAX(CAST(timestamp AS DATE)) AS last_timestamp
FROM SortedData
GROUP BY case_id
),

ActivityOrder AS (
SELECT case_id, activity_name,
ROW_NUMBER() OVER (PARTITION BY case_id ORDER BY timestamp) AS activity_order
FROM SortedData
),

PivotData AS (
SELECT case_id,
 MAX(CASE WHEN activity_name = 'First Notification of Loss (FNOL)' THEN activity_order ELSE 0 END) AS First_Notification_of_Loss_FNOL,
 MAX(CASE WHEN activity_name = 'Assign Claim' THEN activity_order ELSE 0 END) AS Assign_Claim,
 MAX(CASE WHEN activity_name = 'Claim Decision' THEN activity_order ELSE 0 END) AS Claim_Decision,
 MAX(CASE WHEN activity_name = 'Set Reserve' THEN activity_order ELSE 0 END) AS Set_Reserve,
 MAX(CASE WHEN activity_name = 'Payment Sent' THEN activity_order ELSE 0 END) AS Payment_Sent,
 MAX(CASE WHEN activity_name = 'Close Claim' THEN activity_order ELSE 0 END) AS Close_Claim

FROM ActivityOrder
GROUP BY case_id
)


SELECT
sd.case_id,
sd.activity_name,
sd.timestamp,
sd.claimant_name,
sd.agent_name,
sd.adjuster_name,
sd.claim_amount,
sd.claimant_age,
sd.type_of_policy,
sd.car_make,
sd.car_model,
sd.car_year,
sd.type_of_accident,
sd.user_type,
sd.rn,
ac.total_activities,
ts.first_timestamp,
ts.last_timestamp,
DATEDIFF(ts.last_timestamp, ts.first_timestamp) AS throughput_duration_days,
pd.First_Notification_of_Loss_FNOL,
pd.Assign_Claim,
pd.Claim_Decision,
pd.Set_Reserve,
pd.Payment_Sent,
pd.Close_Claim
FROM
SortedData sd
JOIN
ActivityCounts ac ON sd.case_id = ac.case_id

JOIN
Timestamps ts ON sd.case_id = ts.case_id
JOIN
PivotData pd ON sd.case_id = pd.case_id

ORDER BY
sd.case_id, sd.timestamp;




--Total Claim:
-- Expression:
-- concat('$', (round(sum("claim_amount")/1000000, 2)), 'M')
