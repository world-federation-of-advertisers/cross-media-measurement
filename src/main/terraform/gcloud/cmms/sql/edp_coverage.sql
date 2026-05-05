SELECT
  `${project_id}.dashboard_views.externalIdToApiId`(eg.DataProviderId) AS CmmsDataProvider,
  COUNT(DISTINCT eg.MeasurementConsumerId) AS McsWithEventGroups,
  mc.TotalMcs,
  SAFE_DIVIDE(
    COUNT(DISTINCT eg.MeasurementConsumerId),
    mc.TotalMcs
  ) AS CoveragePercent
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT eg.DataProviderId, eg.MeasurementConsumerId
    FROM EventGroups eg''')
) eg
CROSS JOIN (
  SELECT COUNT(DISTINCT MeasurementConsumerId) AS TotalMcs
  FROM (
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/kingdom-conn',
      '''SELECT mc.MeasurementConsumerId
      FROM MeasurementConsumers mc''')
  )
) mc
%{ if data_provider_id != "" }
WHERE `${project_id}.dashboard_views.externalIdToApiId`(eg.DataProviderId) = '${data_provider_id}'
%{ endif }
GROUP BY 1, 3
