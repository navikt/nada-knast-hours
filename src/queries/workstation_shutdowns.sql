SELECT resource.labels.workstation_id as user, labels.instance_id, timestamp
FROM `nada-prod-6977.knast.workstations_googleapis_com_workstation_shutdowns`
WHERE logName = 'projects/knada-gcp/logs/workstations.googleapis.com%2Fworkstation_shutdowns'