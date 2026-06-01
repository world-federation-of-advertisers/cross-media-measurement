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

-- EDP version: excludes TotalMcs and CoveragePercent (platform-only columns).

SELECT
  `${project_id}.dashboard.externalIdToApiId`(eg.MeasurementConsumerId) AS CmmsMeasurementConsumer,
  `${project_id}.dashboard.externalIdToApiId`(eg.DataProviderId) AS CmmsDataProvider,
  COUNT(*) AS EventGroupCount,
  ARRAY_AGG(IFNULL(eg.ProvidedEventGroupId, '')) AS EventGroupIds,
  ARRAY_AGG(IFNULL(JSON_VALUE(
    `${project_id}.dashboard.decode_EventGroupDetails`(eg.EventGroupDetails),
    '$.metadata.ad_metadata.campaign_metadata.campaign_name'
  ), '')) AS CampaignNames,
  ARRAY_AGG(IFNULL(JSON_VALUE(
    `${project_id}.dashboard.decode_EventGroupDetails`(eg.EventGroupDetails),
    '$.metadata.ad_metadata.campaign_metadata.brand_name'
  ), '')) AS BrandNames,
  ARRAY_AGG(IFNULL(ca.ClientAccountReferenceId, '')) AS AccountIds
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      eg.DataProviderId,
      eg.MeasurementConsumerId,
      eg.ProvidedEventGroupId,
      CAST(eg.EventGroupDetails AS BYTES) AS EventGroupDetails
    FROM EventGroups eg''')
) eg
LEFT JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/kingdom-conn',
    '''SELECT
      ca.MeasurementConsumerId,
      ca.DataProviderId,
      ca.ClientAccountReferenceId
    FROM ClientAccounts ca''')
) ca
  ON eg.MeasurementConsumerId = ca.MeasurementConsumerId
  AND eg.DataProviderId = ca.DataProviderId
GROUP BY 1, 2
