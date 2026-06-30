-- Copyright 2026 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

MERGE INTO `${project_id}.${dataset}.${table_name}` T
USING (
SELECT
  `${project_id}.dashboard.externalIdToApiId`(eg.MeasurementConsumerId) AS CmmsMeasurementConsumer,
  `${project_id}.dashboard.externalIdToApiId`(dp.ExternalDataProviderId) AS CmmsDataProvider,
  COUNT(*) AS EventGroupCount,
  ARRAY_AGG(IFNULL(eg.ProvidedEventGroupId, '')) AS ProvidedEventGroupIds,
%{ if !include_platform_columns }
  ARRAY_AGG(IFNULL(eg.EntityType, '')) AS EntityTypes,
  ARRAY_AGG(IFNULL(eg.EntityId, '')) AS EntityIds,
%{ endif }
  ARRAY_AGG(IFNULL(eg.CampaignName, '')) AS CampaignNames,
  ARRAY_AGG(IFNULL(eg.BrandName, '')) AS BrandNames,
  ARRAY_AGG(IFNULL(eg.EventTemplates, '')) AS EventTemplates,
%{ if !include_platform_columns }
  ARRAY_AGG(IFNULL(eg.EntityMetadata, '')) AS EntityMetadata,
%{ endif }
  ARRAY_AGG(IFNULL(mt.MediaTypes, '')) AS MediaTypes,
  ARRAY_AGG(IFNULL(ca.AccountIds, '')) AS AccountIds,
  MIN(eg.DataAvailabilityStartTime) AS DataAvailabilityStartTime,
  MAX(eg.DataAvailabilityEndTime) AS DataAvailabilityEndTime
%{ if include_platform_columns }
  , mc.TotalMcs
  , SAFE_DIVIDE(
    COUNT(DISTINCT eg.MeasurementConsumerId),
    mc.TotalMcs
  ) AS CoveragePercent
%{ endif }
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      eg.DataProviderId,
      eg.EventGroupId,
      eg.MeasurementConsumerId,
      eg.ProvidedEventGroupId,
      eg.EntityType,
      eg.EntityId,
      eg.DataAvailabilityStartTime,
      eg.DataAvailabilityEndTime,
      JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.campaignName') AS CampaignName,
      JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.brandName') AS BrandName,
      TO_JSON_STRING(TO_JSON(eg.EventGroupDetails).eventTemplates) AS EventTemplates,
      TO_JSON_STRING(TO_JSON(eg.EntityMetadata)) AS EntityMetadata
    FROM EventGroups eg''')
) eg
LEFT JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      dp.DataProviderId,
      dp.ExternalDataProviderId
    FROM DataProviders dp''')
) dp
  ON eg.DataProviderId = dp.DataProviderId
LEFT JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      mt.DataProviderId,
      mt.EventGroupId,
      STRING_AGG(
        CASE mt.MediaType
          WHEN 1 THEN 'VIDEO'
          WHEN 2 THEN 'DISPLAY'
          WHEN 3 THEN 'OTHER'
          ELSE CAST(mt.MediaType AS STRING)
        END
      ) AS MediaTypes
    FROM EventGroupMediaTypes mt
    GROUP BY mt.DataProviderId, mt.EventGroupId''')
) mt
  ON eg.DataProviderId = mt.DataProviderId
  AND eg.EventGroupId = mt.EventGroupId
LEFT JOIN (
  SELECT
    MeasurementConsumerId,
    DataProviderId,
    STRING_AGG(ClientAccountReferenceId) AS AccountIds
  FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      ca.MeasurementConsumerId,
      ca.DataProviderId,
      ca.ClientAccountReferenceId
    FROM ClientAccounts ca''')
  GROUP BY MeasurementConsumerId, DataProviderId
) ca
  ON eg.MeasurementConsumerId = ca.MeasurementConsumerId
  AND eg.DataProviderId = ca.DataProviderId
%{ if include_platform_columns }
CROSS JOIN (
  SELECT COUNT(DISTINCT MeasurementConsumerId) AS TotalMcs
  FROM (
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/kingdom-conn',
      '''SELECT mc.MeasurementConsumerId
      FROM MeasurementConsumers mc''')
  )
) mc
GROUP BY 1, 2, mc.TotalMcs
%{ else }
GROUP BY 1, 2
%{ endif }

) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
