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
  base.BasicReportId,
  base.CmmsDataProvider,
  COUNT(DISTINCT base.CmmsEventGroupId) AS EventGroupCount,
  ARRAY_AGG(DISTINCT base.CmmsEventGroupId) AS EventGroupIds,
  ARRAY_AGG(DISTINCT base.CampaignName IGNORE NULLS) AS CampaignNames,
  ARRAY_AGG(DISTINCT base.BrandName IGNORE NULLS) AS BrandNames
FROM (
  SELECT
    br.BasicReportId,
    JSON_VALUE(comp, '$.cmmsDataProviderId') AS CmmsDataProvider,
    JSON_VALUE(eg, '$.cmmsEventGroupId') AS CmmsEventGroupId,
    JSON_VALUE(
      `${project_id}.dashboard_views.decode_EventGroupDetails`(keg.EventGroupDetails),
      '$.metadata.ad_metadata.campaign_metadata.campaign_name'
    ) AS CampaignName,
    JSON_VALUE(
      `${project_id}.dashboard_views.decode_EventGroupDetails`(keg.EventGroupDetails),
      '$.metadata.ad_metadata.campaign_metadata.brand_name'
    ) AS BrandName
  FROM (
    SELECT
      BasicReportId,
      `${project_id}.dashboard_views.decode_BasicReportResultDetails`(BasicReportResultDetails) AS details
    FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/reporting-conn',
      '''SELECT
        br.BasicReportId,
        CAST(br.BasicReportResultDetails AS BYTES) AS BasicReportResultDetails
      FROM BasicReports br''')
  ) br,
  UNNEST(JSON_QUERY_ARRAY(br.details, '$.resultGroups')) AS rg,
  UNNEST(JSON_QUERY_ARRAY(rg, '$.results')) AS result,
  UNNEST(JSON_QUERY_ARRAY(result, '$.metadata.reportingUnitSummary.reportingUnitComponentSummary')) AS comp,
  UNNEST(JSON_QUERY_ARRAY(comp, '$.eventGroupSummaries')) AS eg
  LEFT JOIN (
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/kingdom-conn',
      '''SELECT
        eg.ExternalEventGroupId,
        eg.MeasurementConsumerId,
        CAST(eg.EventGroupDetails AS BYTES) AS EventGroupDetails
      FROM EventGroups eg''')
  ) keg
    ON JSON_VALUE(eg, '$.cmmsEventGroupId') = `${project_id}.dashboard_views.externalIdToApiId`(keg.ExternalEventGroupId)
    AND JSON_VALUE(eg, '$.cmmsMeasurementConsumerId') = `${project_id}.dashboard_views.externalIdToApiId`(keg.MeasurementConsumerId)
%{ if data_provider_id != "" }
  WHERE JSON_VALUE(comp, '$.cmmsDataProviderId') = '${data_provider_id}'
%{ endif }
) base
GROUP BY base.BasicReportId, base.CmmsDataProvider
