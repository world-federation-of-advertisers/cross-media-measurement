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

-- Event-group membership per report is derived from the report's campaign group
-- (a ReportingSet). Those associations live in the reporting Postgres database
-- (ReportingSets / ReportingSetEventGroups / EventGroups), so this query joins
-- the reporting Spanner DB (report -> campaign group), the reporting Postgres DB
-- (campaign group -> event groups + data provider), and the Kingdom Spanner DB
-- (event group -> campaign/brand/entity metadata).
-- NOTE: Only primitive campaign groups (direct ReportingSetEventGroups rows) are
-- resolved; composite campaign groups would need set-expression resolution.

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
    cg.CmmsDataProvider,
    cg.CmmsEventGroupId,
    keg.CampaignName,
    keg.BrandName,
    keg.EntityType,
    keg.EntityId
  FROM (
    -- Reporting Spanner: report -> campaign group
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/reporting-conn',
      '''SELECT
        br.ExternalReportId,
        br.ExternalCampaignGroupId
      FROM BasicReports br
      WHERE br.State = 4''')
  ) br
  JOIN (
    -- Reporting Postgres: campaign group -> event groups + data provider
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/reporting-postgres-conn',
      '''SELECT
        rs.externalreportingsetid AS ExternalCampaignGroupId,
        eg.cmmsdataproviderid AS CmmsDataProvider,
        eg.cmmseventgroupid AS CmmsEventGroupId
      FROM reportingsets rs
      JOIN reportingseteventgroups rseg
        ON rs.measurementconsumerid = rseg.measurementconsumerid
        AND rs.reportingsetid = rseg.reportingsetid
      JOIN eventgroups eg
        ON rseg.measurementconsumerid = eg.measurementconsumerid
        AND rseg.eventgroupid = eg.eventgroupid
      WHERE rs.setexpressionid IS NULL''')
  ) cg
    ON br.ExternalCampaignGroupId = cg.ExternalCampaignGroupId
  LEFT JOIN (
    -- Kingdom Spanner: event group -> campaign/brand/entity metadata
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/kingdom-conn',
      '''SELECT
        eg.ExternalEventGroupId,
        eg.EntityType,
        eg.EntityId,
        JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.campaignName') AS CampaignName,
        JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.brandName') AS BrandName
      FROM EventGroups eg''')
  ) keg
    ON cg.CmmsEventGroupId = `${project_id}.dashboard.externalIdToApiId`(keg.ExternalEventGroupId)
) base
GROUP BY base.ExternalReportId, base.CmmsDataProvider
%{ if include_platform_columns }
)
%{ endif }

) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
