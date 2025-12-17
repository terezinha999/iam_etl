-- models/iam_policies.sql
WITH base_data AS (
    SELECT
        policy_id,
        user_id,
        role,
        plan_type,
        monthly_rate,
        premium
    FROM {{ source('public', 'iam_policies') }}  -- Reference the source table here
)
SELECT * FROM base_data
