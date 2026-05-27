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
  r.CmmsCreateTime,
  r.FulfilledTime,
  TIMESTAMP_DIFF(r.FulfilledTime, r.CmmsCreateTime, SECOND) AS FulfillmentDurationSeconds,
  CASE
    WHEN br.State = 2 THEN 'SUCCEEDED'
    WHEN br.State = 3 THEN 'FAILED'
    WHEN br.State = 1 THEN 'RUNNING'
    ELSE 'UNSPECIFIED'
  END AS ReportState,
  JSON_VALUE(br.details, '$.reportingInterval.reportStartDate.year') AS ReportStartYear,
  JSON_VALUE(br.details, '$.reportingInterval.reportStartDate.month') AS ReportStartMonth,
  JSON_VALUE(br.details, '$.reportingInterval.reportStartDate.day') AS ReportStartDay,
  JSON_VALUE(br.details, '$.reportingInterval.reportEnd.year') AS ReportEndYear,
  JSON_VALUE(br.details, '$.reportingInterval.reportEnd.month') AS ReportEndMonth,
  JSON_VALUE(br.details, '$.reportingInterval.reportEnd.day') AS ReportEndDay,
  JSON_QUERY(br.details, '$.impressionQualificationFilters') AS ImpressionQualificationFilters
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/edp-aggregator-conn',
    '''SELECT
      rm.DataProviderResourceId,
      rm.Report,
      REGEXP_EXTRACT(rm.Report, 'measurementConsumers/([^/]+)/') AS CmmsMeasurementConsumer,
      CAST(rm.State AS INT64) AS State,
      rm.CmmsCreateTime,
      rma_fulfilled.CreateTime AS FulfilledTime
    FROM RequisitionMetadata rm
    LEFT JOIN RequisitionMetadataActions rma_fulfilled
      ON rm.DataProviderResourceId = rma_fulfilled.DataProviderResourceId
      AND rm.RequisitionMetadataId = rma_fulfilled.RequisitionMetadataId
      AND CAST(rma_fulfilled.CurrentState AS INT64) = 4''')
) r
LEFT JOIN (
  SELECT
    br.BasicReportId,
    CAST(br.State AS INT64) AS State,
    `${project_id}.dashboard_views.decode_BasicReportDetails`(br.BasicReportDetails) AS details
  FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/reporting-conn',
    '''SELECT
      br.BasicReportId,
      CAST(br.State AS INT64) AS State,
      br.ExternalReportId,
      CAST(br.BasicReportDetails AS BYTES) AS BasicReportDetails
    FROM BasicReports br''')
) br
  ON REGEXP_EXTRACT(r.Report, r'reports/([^/]+)$') = br.ExternalReportId
%{ if data_provider_id != "" }
WHERE r.DataProviderResourceId = '${data_provider_id}'
%{ endif }
