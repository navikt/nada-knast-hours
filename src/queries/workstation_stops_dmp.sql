SELECT nav_ident as user, instance_id, TIMESTAMP_TRUNC(created_at, SECOND) as timestamp
FROM `nada-prod-6977.knast.knast_activity_history`
WHERE action = "STOP"