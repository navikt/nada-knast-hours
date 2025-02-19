SELECT team.members, po.name, teamtype, po.name as productarea 
FROM `org-prod-1016.teamkatalogen_federated_query_updated_dataset.Teams` team
LEFT JOIN `org-prod-1016.teamkatalogen_federated_query_updated_dataset.Produktomraader` po
ON po.id = team.productareaid
WHERE team.status = 'ACTIVE'