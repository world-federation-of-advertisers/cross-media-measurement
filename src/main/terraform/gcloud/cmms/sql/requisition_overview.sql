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
  r.DataProviderResourceId,
  r.Report,
  r.CmmsMeasurementConsumer,
  CASE r.State
    WHEN 0 THEN 'UNSPECIFIED'
    WHEN 1 THEN 'STORED'
    WHEN 2 THEN 'QUEUED'
    WHEN 3 THEN 'PROCESSING'
    WHEN 4 THEN 'FULFILLED'
    WHEN 5 THEN 'REFUSED'
    WHEN 6 THEN 'WITHDRAWN'
    ELSE CAST(r.State AS STRING)
  END AS RequisitionState,
  r.RefusalMessage,
  r.CmmsCreateTime,
  r.FulfilledTime,
  TIMESTAMP_DIFF(r.FulfilledTime, r.CmmsCreateTime, SECOND) AS FulfillmentDurationSeconds,
  -- NOTE: ReportState is the aggregate report state across all EDPs. This
  -- reveals whether other EDPs have completed/failed. Deemed acceptable to
  -- share cross-EDP.
  CASE
    WHEN br.State = 2 THEN 'SUCCEEDED'
    WHEN br.State = 3 THEN 'FAILED'
    WHEN br.State = 1 THEN 'RUNNING'
    ELSE 'UNSPECIFIED'
  END AS ReportState,
  DATE(
    CAST(br.ReportStartYear AS INT64),
    CAST(br.ReportStartMonth AS INT64),
    CAST(br.ReportStartDay AS INT64)
  ) AS ReportStartDate,
  DATE(
    CAST(br.ReportEndYear AS INT64),
    CAST(br.ReportEndMonth AS INT64),
    CAST(br.ReportEndDay AS INT64)
  ) AS ReportEndDate,
  br.ImpressionQualificationFilters,
  br.ReportTitle,
  (SELECT ARRAY_AGG(STRUCT(
    JSON_VALUE(rgs, '$.title') AS title,
    JSON_VALUE(rgs, '$.metricFrequency') AS metricFrequency
  ))
  FROM UNNEST(JSON_QUERY_ARRAY(br.ResultGroupSpecs)) AS rgs
  ) AS ResultGroupSpecs
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/edp-aggregator-conn',
    '''SELECT
      rm.DataProviderResourceId,
      rm.Report,
      REGEXP_EXTRACT(rm.Report, 'measurementConsumers/([^/]+)/') AS CmmsMeasurementConsumer,
      CAST(rm.State AS INT64) AS State,
      rm.RefusalMessage,
      rm.CmmsCreateTime,
      rma_fulfilled.CreateTime AS FulfilledTime
    FROM RequisitionMetadata rm
    LEFT JOIN RequisitionMetadataActions rma_fulfilled
      ON rm.DataProviderResourceId = rma_fulfilled.DataProviderResourceId
      AND rm.RequisitionMetadataId = rma_fulfilled.RequisitionMetadataId
      AND CAST(rma_fulfilled.CurrentState AS INT64) = 4''')
) r
LEFT JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/reporting-conn',
    '''SELECT
      br.BasicReportId,
      CAST(br.State AS INT64) AS State,
      br.ExternalReportId,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportStartDate.year') AS STRING) AS ReportStartYear,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportStartDate.month') AS STRING) AS ReportStartMonth,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportStartDate.day') AS STRING) AS ReportStartDay,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportEnd.year') AS STRING) AS ReportEndYear,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportEnd.month') AS STRING) AS ReportEndMonth,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportEnd.day') AS STRING) AS ReportEndDay,
      CAST(TO_JSON(br.BasicReportDetails).impressionQualificationFilters AS STRING) AS ImpressionQualificationFilters,
      JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.title') AS ReportTitle,
      CAST(TO_JSON(br.BasicReportDetails).resultGroupSpecs AS STRING) AS ResultGroupSpecs
    FROM BasicReports br''')
) br
  ON REGEXP_EXTRACT(r.Report, r'reports/([^/]+)$') = br.ExternalReportId
