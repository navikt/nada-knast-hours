SELECT resource.labels.workstation_id as user, labels.instance_id, timestamp
FROM `nada-prod-6977.knast.workstations_googleapis_com_vm_assignments`
WHERE logName = 'projects/knada-gcp/logs/workstations.googleapis.com%2Fvm_assignments'