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
%{ if include_platform_columns }
SELECT
  *,
  COUNT(DISTINCT CmmsDataProvider) OVER (PARTITION BY ExternalReportId) AS EdpCount
FROM (
%{ endif }
SELECT
  base.ExternalReportId,
  base.CmmsDataProvider,
  COUNT(DISTINCT base.CmmsEventGroupId) AS EventGroupCount,
  ARRAY_AGG(DISTINCT base.CmmsEventGroupId) AS CmmsEventGroupIds,
  ARRAY_AGG(DISTINCT base.CampaignName IGNORE NULLS) AS CampaignNames,
  ARRAY_AGG(DISTINCT base.BrandName IGNORE NULLS) AS BrandNames
%{ if !include_platform_columns }
  ,
  ARRAY_AGG(DISTINCT base.EntityType IGNORE NULLS) AS EntityTypes,
  ARRAY_AGG(DISTINCT base.EntityId IGNORE NULLS) AS EntityIds
%{ endif }
FROM (
  SELECT
    br.ExternalReportId,
    JSON_VALUE(comp, '$.cmmsDataProviderId') AS CmmsDataProvider,
    JSON_VALUE(eg, '$.cmmsEventGroupId') AS CmmsEventGroupId,
    keg.CampaignName,
    keg.BrandName,
    keg.EntityType,
    keg.EntityId
  FROM (
    SELECT
      ExternalReportId,
      details
    FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/reporting-conn',
      '''SELECT
        br.ExternalReportId,
        TO_JSON_STRING(TO_JSON(br.BasicReportResultDetails)) AS details
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
        JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.campaignName') AS CampaignName,
        JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.brandName') AS BrandName,
        eg.EntityType,
        eg.EntityId
      FROM EventGroups eg''')
  ) keg
    ON JSON_VALUE(eg, '$.cmmsEventGroupId') = `${project_id}.dashboard.externalIdToApiId`(keg.ExternalEventGroupId)
    AND JSON_VALUE(eg, '$.cmmsMeasurementConsumerId') = `${project_id}.dashboard.externalIdToApiId`(keg.MeasurementConsumerId)
) base
GROUP BY base.ExternalReportId, base.CmmsDataProvider
%{ if include_platform_columns }
)
%{ endif }

) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
