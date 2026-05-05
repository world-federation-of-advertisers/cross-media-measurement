SELECT
  `${project_id}.dashboard_views.externalIdToApiId`(MeasurementConsumerId) AS CmmsMeasurementConsumer,
  `${project_id}.dashboard_views.externalIdToApiId`(DataProviderId) AS CmmsDataProvider,
  COUNT(*) AS EventGroupCount,
  ARRAY_AGG(IFNULL(ProvidedEventGroupId, '')) AS EventGroupIds,
  ARRAY_AGG(IFNULL(JSON_VALUE(
    `${project_id}.dashboard_views.decode_EventGroupDetails`(EventGroupDetails),
    '$.metadata.ad_metadata.campaign_metadata.campaign_name'
  ), '')) AS CampaignNames,
  ARRAY_AGG(IFNULL(JSON_VALUE(
    `${project_id}.dashboard_views.decode_EventGroupDetails`(EventGroupDetails),
    '$.metadata.ad_metadata.campaign_metadata.brand_name'
  ), '')) AS BrandNames
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      eg.MeasurementConsumerId,
      eg.DataProviderId,
      eg.ProvidedEventGroupId,
      CAST(eg.EventGroupDetails AS BYTES) AS EventGroupDetails
    FROM EventGroups eg''')
)
%{ if data_provider_id != "" }
WHERE `${project_id}.dashboard_views.externalIdToApiId`(DataProviderId) = '${data_provider_id}'
%{ endif }
GROUP BY 1, 2
