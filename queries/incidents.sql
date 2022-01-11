SELECT
source,
incident_id,
project_name,
project_group,
MIN(IF(root.time_created < issue.time_created, root.time_created, issue.time_created)) as time_created,
MAX(time_resolved) as time_resolved,
ARRAY_AGG(root_cause IGNORE NULLS) changes,
FROM
(
SELECT 
source,
CASE WHEN source LIKE "github%" THEN JSON_EXTRACT_SCALAR(metadata, '$.issue.number')
     WHEN source LIKE "gitlab%" AND event_type = "note" THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.noteable_id')
     WHEN source LIKE "gitlab%" AND event_type = "issue" THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.id') end as incident_id,
CASE WHEN source LIKE "github%" THEN JSON_VALUE(metadata, '$.project.namespace') 
     WHEN source LIKE "gitlab%" AND event_type = "note" THEN JSON_VALUE(metadata, '$.project.namespace')
     WHEN source LIKE "gitlab%" AND event_type = "issue" THEN JSON_VALUE(metadata, '$.project.namespace') end as project_group,
CASE WHEN source LIKE "github%" THEN JSON_VALUE(metadata, '$.project.name')
     WHEN source LIKE "gitlab%" AND event_type = "note" THEN JSON_VALUE(metadata, '$.project.name')
     WHEN source LIKE "gitlab%" AND event_type = "issue" THEN JSON_VALUE(metadata, '$.project.name') end as project_name,
CASE WHEN source LIKE "github%" THEN TIMESTAMP(JSON_EXTRACT_SCALAR(metadata, '$.issue.created_at'))
     WHEN source LIKE "gitlab%" THEN PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %z', JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.created_at')) 
     END AS time_created,
CASE WHEN source LIKE "github%" THEN TIMESTAMP(JSON_EXTRACT_SCALAR(metadata, '$.issue.closed_at'))
     WHEN source LIKE "gitlab%" THEN PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S %z', JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.closed_at')) 
     END AS time_resolved,
REGEXP_EXTRACT(metadata, r"root cause: ([[:alnum:]]*)") as root_cause,
CASE WHEN source LIKE "github%" THEN REGEXP_CONTAINS(JSON_EXTRACT(metadata, '$.issue.labels'), '"name":"Incident"')
     WHEN source LIKE "gitlab%" THEN REGEXP_CONTAINS(JSON_EXTRACT(metadata, '$.object_attributes.labels'), '"title":"Incident"') end as bug,
FROM four_keys.events_raw 
WHERE event_type LIKE "issue%" OR (event_type = "note" and JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.noteable_type') = 'Issue')
) issue
LEFT JOIN (SELECT time_created, changes FROM four_keys.deployments d, d.changes) root on root.changes = root_cause
GROUP BY 1,2,3,4
HAVING max(bug) is True
;
