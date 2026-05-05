SELECT
  `${project_id}.dashboard_views.externalIdToApiId`(MeasurementConsumerId) AS CmmsMeasurementConsumer,
  `${project_id}.dashboard_views.externalIdToApiId`(DataProviderId) AS CmmsDataProvider,
  ARRAY_AGG(ClientAccountReferenceId) AS AccountIds
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      ca.MeasurementConsumerId,
      ca.DataProviderId,
      ca.ClientAccountReferenceId
    FROM ClientAccounts ca''')
)
%{ if data_provider_id != "" }
WHERE `${project_id}.dashboard_views.externalIdToApiId`(DataProviderId) = '${data_provider_id}'
%{ endif }
GROUP BY 1, 2
