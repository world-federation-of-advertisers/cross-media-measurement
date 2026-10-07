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
-- Composite campaign groups are resolved by walking their SetExpression tree down
-- to the primitive ReportingSets that own the ReportingSetEventGroups rows. The
-- walk collects every reachable EventGroup regardless of the set Operation, so a
-- campaign group built with DIFFERENCE or INTERSECTION yields a superset of the
-- EventGroups the report actually measured.
-- Includes all terminal reports -- SUCCEEDED (4), FAILED (5), and INVALID (6);
-- the ReportState column carries the state so consumers can distinguish.
-- ExternalReportingSetId and ExternalReportId are unique only within a
-- MeasurementConsumer, and ExternalEventGroupId only within a DataProvider, so
-- every cross-database join and the final grouping are scoped by those keys.

MERGE INTO `${project_id}.${dataset}.${table_name}` T
USING (
%{ if include_platform_columns }
SELECT
  * EXCEPT (ReportState),
  COUNT(DISTINCT CmmsDataProvider) OVER (
    PARTITION BY CmmsMeasurementConsumer, ExternalReportId
  ) AS EdpCount,
  ReportState
FROM (
%{ endif }
SELECT
  base.CmmsMeasurementConsumer,
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
  ,
  CASE ANY_VALUE(base.State)
    WHEN 1 THEN 'CREATED'
    WHEN 2 THEN 'REPORT_CREATED'
    WHEN 3 THEN 'UNPROCESSED_RESULTS_READY'
    WHEN 4 THEN 'SUCCEEDED'
    WHEN 5 THEN 'FAILED'
    WHEN 6 THEN 'INVALID'
    ELSE 'UNSPECIFIED'
  END AS ReportState
FROM (
  SELECT
    br.CmmsMeasurementConsumerId AS CmmsMeasurementConsumer,
    br.ExternalReportId,
    br.State,
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
        mc.CmmsMeasurementConsumerId,
        br.ExternalReportId,
        br.ExternalCampaignGroupId,
        br.State
      FROM BasicReports br
      JOIN MeasurementConsumers mc
        ON br.MeasurementConsumerId = mc.MeasurementConsumerId
      WHERE br.State IN (4, 5, 6)''')
  ) br
  JOIN (
    -- Reporting Postgres: campaign group -> event groups + data provider.
    -- A primitive ReportingSet owns its EventGroups directly. A composite one
    -- owns a SetExpression tree whose operands are either nested expressions
    -- (same ReportingSet) or other ReportingSets, so both are followed before
    -- reading ReportingSetEventGroups.
    SELECT * FROM EXTERNAL_QUERY(
      'projects/${project_id}/locations/${region}/connections/reporting-postgres-conn',
      '''WITH RECURSIVE
      -- Edges between expression nodes inside one ReportingSet tree. Split into
      -- a non-recursive CTE so the recursive term below self-references once,
      -- which is all Postgres permits.
      expression_edges AS (
        SELECT measurementconsumerid, reportingsetid,
               setexpressionid AS parentsetexpressionid,
               lefthandsetexpressionid AS childsetexpressionid
        FROM setexpressions
        WHERE lefthandsetexpressionid IS NOT NULL
        UNION ALL
        SELECT measurementconsumerid, reportingsetid,
               setexpressionid, righthandsetexpressionid
        FROM setexpressions
        WHERE righthandsetexpressionid IS NOT NULL
      ),
      -- Expression nodes that name another ReportingSet as an operand.
      expression_reporting_sets AS (
        SELECT measurementconsumerid, reportingsetid, setexpressionid,
               lefthandreportingsetid AS referencedreportingsetid
        FROM setexpressions
        WHERE lefthandreportingsetid IS NOT NULL
        UNION ALL
        SELECT measurementconsumerid, reportingsetid, setexpressionid,
               righthandreportingsetid
        FROM setexpressions
        WHERE righthandreportingsetid IS NOT NULL
      ),
      -- Every expression node reachable from a composite ReportingSet root.
      expression_nodes AS (
        SELECT measurementconsumerid, reportingsetid, setexpressionid
        FROM reportingsets
        WHERE setexpressionid IS NOT NULL
        UNION
        SELECT e.measurementconsumerid, e.reportingsetid, e.childsetexpressionid
        FROM expression_edges e
        JOIN expression_nodes n
          ON n.measurementconsumerid = e.measurementconsumerid
          AND n.reportingsetid = e.reportingsetid
          AND n.setexpressionid = e.parentsetexpressionid
      ),
      -- ReportingSet -> ReportingSet references, collapsed across the tree.
      reporting_set_edges AS (
        SELECT DISTINCT n.measurementconsumerid,
               n.reportingsetid AS parentreportingsetid,
               r.referencedreportingsetid AS childreportingsetid
        FROM expression_nodes n
        JOIN expression_reporting_sets r
          ON r.measurementconsumerid = n.measurementconsumerid
          AND r.reportingsetid = n.reportingsetid
          AND r.setexpressionid = n.setexpressionid
      ),
      -- Transitive closure. UNION dedupes, so a reference cycle terminates.
      campaign_group_members AS (
        SELECT measurementconsumerid,
               reportingsetid AS rootreportingsetid,
               reportingsetid AS memberreportingsetid
        FROM reportingsets
        UNION
        SELECT m.measurementconsumerid, m.rootreportingsetid, e.childreportingsetid
        FROM campaign_group_members m
        JOIN reporting_set_edges e
          ON e.measurementconsumerid = m.measurementconsumerid
          AND e.parentreportingsetid = m.memberreportingsetid
      )
      SELECT DISTINCT
        mc.cmmsmeasurementconsumerid AS CmmsMeasurementConsumerId,
        rs.externalreportingsetid AS ExternalCampaignGroupId,
        eg.cmmsdataproviderid AS CmmsDataProvider,
        eg.cmmseventgroupid AS CmmsEventGroupId
      FROM reportingsets rs
      JOIN measurementconsumers mc
        ON mc.measurementconsumerid = rs.measurementconsumerid
      JOIN campaign_group_members m
        ON m.measurementconsumerid = rs.measurementconsumerid
        AND m.rootreportingsetid = rs.reportingsetid
      JOIN reportingseteventgroups rseg
        ON rseg.measurementconsumerid = m.measurementconsumerid
        AND rseg.reportingsetid = m.memberreportingsetid
      JOIN eventgroups eg
        ON rseg.measurementconsumerid = eg.measurementconsumerid
        AND rseg.eventgroupid = eg.eventgroupid''')
  ) cg
    ON br.CmmsMeasurementConsumerId = cg.CmmsMeasurementConsumerId
    AND br.ExternalCampaignGroupId = cg.ExternalCampaignGroupId
  LEFT JOIN (
    -- Kingdom Spanner: event group -> campaign/brand/entity metadata.
    -- EventGroups and DataProviders are pulled as separate EXTERNAL_QUERY
    -- calls and joined here: a join inside the pushed-down query is not root
    -- partitionable under Data Boost. Mirrors mc_details.sql.
    SELECT
      `${project_id}.dashboard.externalIdToApiId`(kdp.ExternalDataProviderId) AS CmmsDataProvider,
      `${project_id}.dashboard.externalIdToApiId`(keg.ExternalEventGroupId) AS CmmsEventGroupId,
      keg.EntityType,
      keg.EntityId,
      keg.CampaignName,
      keg.BrandName
    FROM (
      SELECT * FROM EXTERNAL_QUERY(
        'projects/${project_id}/locations/${region}/connections/kingdom-conn',
        '''SELECT
          eg.DataProviderId,
          eg.ExternalEventGroupId,
          eg.EntityType,
          eg.EntityId,
          JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.campaignName') AS CampaignName,
          JSON_VALUE(TO_JSON(eg.EventGroupDetails), '$.metadata.adMetadata.campaignMetadata.brandName') AS BrandName
        FROM EventGroups eg''')
    ) keg
    JOIN (
      SELECT * FROM EXTERNAL_QUERY(
        'projects/${project_id}/locations/${region}/connections/kingdom-conn',
        '''SELECT
          dp.DataProviderId,
          dp.ExternalDataProviderId
        FROM DataProviders dp''')
    ) kdp
      ON keg.DataProviderId = kdp.DataProviderId
  ) keg
    ON cg.CmmsDataProvider = keg.CmmsDataProvider
    AND cg.CmmsEventGroupId = keg.CmmsEventGroupId
) base
GROUP BY base.CmmsMeasurementConsumer, base.ExternalReportId, base.CmmsDataProvider
%{ if include_platform_columns }
)
%{ endif }

) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
