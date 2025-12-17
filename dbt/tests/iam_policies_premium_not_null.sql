-- tests/iam_policies_premium_not_null.sql
SELECT *
FROM {{ ref('iam_policies') }}  -- Reference the 'iam_policies' model created in 'models/iam_policies.sql'
WHERE premium IS NULL
