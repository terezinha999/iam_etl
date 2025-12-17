
  create view "postgres"."iam_etl"."iam_policies__dbt_tmp"
    
    
  as (
    -- models/iam_policies.sql
WITH base_data AS (
    SELECT
        policy_id,
        user_id,
        role,
        plan_type,
        monthly_rate,
        premium
    FROM "postgres"."public"."iam_policies"  -- Reference the source table here
)
SELECT * FROM base_data
  );