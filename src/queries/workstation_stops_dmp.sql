SELECT nav_ident as user, instance_id, created_at as timestamp 
FROM `nada-prod-6977.knast.knast_activity_history` 
WHERE action = "STOP"