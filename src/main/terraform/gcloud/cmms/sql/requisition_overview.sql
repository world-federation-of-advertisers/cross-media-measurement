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
  REGEXP_EXTRACT(r.Report, r'reports/(.+)$') AS ReportId,
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
  r.RequisitionCreateTime,
  TIMESTAMP_DIFF(r.FulfilledTime, r.StoredTime, SECOND) AS FulfillmentDurationSeconds,
  rpt.State AS ReportState,
  rpt.CreateTime AS ReportCreateTime
FROM (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/edp-aggregator-conn',
    '''SELECT
      rm.DataProviderResourceId,
      rm.Report,
      REGEXP_EXTRACT(rm.Report, 'measurementConsumers/([^/]+)/') AS CmmsMeasurementConsumer,
      CAST(rm.State AS INT64) AS State,
      rm.CmmsCreateTime,
      rm.CreateTime AS RequisitionCreateTime,
      rma_stored.CreateTime AS StoredTime,
      rma_fulfilled.CreateTime AS FulfilledTime
    FROM RequisitionMetadata rm
    LEFT JOIN RequisitionMetadataActions rma_stored
      ON rm.DataProviderResourceId = rma_stored.DataProviderResourceId
      AND rm.RequisitionMetadataId = rma_stored.RequisitionMetadataId
      AND CAST(rma_stored.CurrentState AS INT64) = 1
    LEFT JOIN RequisitionMetadataActions rma_fulfilled
      ON rm.DataProviderResourceId = rma_fulfilled.DataProviderResourceId
      AND rm.RequisitionMetadataId = rma_fulfilled.RequisitionMetadataId
      AND CAST(rma_fulfilled.CurrentState AS INT64) = 4''')
) r
LEFT JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/reporting-postgres-conn',
    'SELECT
      CAST("ExternalReportId" AS TEXT) AS "ExternalReportId",
      "MeasurementConsumerId",
      "State",
      "CreateTime"
    FROM "Reports"')
) rpt
  ON REGEXP_EXTRACT(r.Report, 'reports/(.+)$') = rpt.ExternalReportId
%{ if data_provider_id != "" }
WHERE r.DataProviderResourceId = '${data_provider_id}'
%{ endif }
