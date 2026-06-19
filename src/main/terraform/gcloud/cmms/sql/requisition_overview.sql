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
  CASE br.State
    WHEN 1 THEN 'CREATED'
    WHEN 2 THEN 'REPORT_CREATED'
    WHEN 3 THEN 'UNPROCESSED_RESULTS_READY'
    WHEN 4 THEN 'SUCCEEDED'
    WHEN 5 THEN 'FAILED'
    WHEN 6 THEN 'INVALID'
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
  br.ResultGroupTitles,
  br.ResultGroupMetricFrequencies
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
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportStart.year') AS STRING) AS ReportStartYear,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportStart.month') AS STRING) AS ReportStartMonth,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportStart.day') AS STRING) AS ReportStartDay,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportEnd.year') AS STRING) AS ReportEndYear,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportEnd.month') AS STRING) AS ReportEndMonth,
      CAST(JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.reportingInterval.reportEnd.day') AS STRING) AS ReportEndDay,
      TO_JSON_STRING(TO_JSON(br.BasicReportDetails).impressionQualificationFilters) AS ImpressionQualificationFilters,
      JSON_VALUE(TO_JSON(br.BasicReportDetails), '$.title') AS ReportTitle,
      (SELECT STRING_AGG(JSON_VALUE(rgs, '$.title'), ',') FROM UNNEST(JSON_QUERY_ARRAY(
        TO_JSON_STRING(TO_JSON(br.BasicReportDetails).resultGroupSpecs)
      )) AS rgs) AS ResultGroupTitles,
      (SELECT STRING_AGG(JSON_VALUE(rgs, '$.metricFrequency'), ',') FROM UNNEST(JSON_QUERY_ARRAY(
        TO_JSON_STRING(TO_JSON(br.BasicReportDetails).resultGroupSpecs)
      )) AS rgs) AS ResultGroupMetricFrequencies
    FROM BasicReports br''')
) br
  ON REGEXP_EXTRACT(r.Report, r'reports/([^/]+)$') = br.ExternalReportId

) S
ON FALSE
WHEN NOT MATCHED THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
