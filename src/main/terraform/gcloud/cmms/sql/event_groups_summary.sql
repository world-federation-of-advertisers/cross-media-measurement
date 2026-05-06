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
