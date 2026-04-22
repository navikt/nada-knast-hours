WITH latest AS (
  SELECT MAX(version) AS latest_version
  FROM `nada-prod-6977.knast.knast_configs`
)
SELECT
  c.user,
  ANY_VALUE(c.created_at) AS created_at,
  COUNTIF(c.version = latest.latest_version) > 0 AS knast_exists
FROM `nada-prod-6977.knast.knast_configs` c, latest
GROUP BY c.user
