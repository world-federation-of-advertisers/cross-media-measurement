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
  CASE
    WHEN rpt.FailedMetrics > 0 THEN 'FAILED'
    WHEN rpt.MetricCount > 0 AND rpt.MetricCount = rpt.SucceededMetrics THEN 'SUCCEEDED'
    WHEN rpt.MetricCount = 0 THEN 'NO_METRICS'
    ELSE 'IN_PROGRESS'
  END AS ReportState,
  rpt.MetricCount,
  rpt.SucceededMetrics,
  rpt.FailedMetrics,
  rpt.ReportTimeStart,
  rpt.ReportTimeEnd,
  rd.ReportingSetFilter,
  rd.MetricCalcSpecDetails,
  rd.EventGroupCount
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
    '''SELECT
      CAST(rp.externalreportid AS TEXT) AS externalreportid,
      COUNT(m.metricid) AS MetricCount,
      COUNT(CASE WHEN m.state = 4 THEN 1 END) AS SucceededMetrics,
      COUNT(CASE WHEN m.state = 5 THEN 1 END) AS FailedMetrics,
      json_extract_path_text(rp.reportdetailsjson::json, 'timeIntervals', 'timeIntervals', '0', 'startTime') AS ReportTimeStart,
      json_extract_path_text(rp.reportdetailsjson::json, 'timeIntervals', 'timeIntervals', '0', 'endTime') AS ReportTimeEnd
    FROM reports rp
    LEFT JOIN metriccalculationspecreportingmetrics mcsrm
      ON rp.measurementconsumerid = mcsrm.measurementconsumerid
      AND rp.reportid = mcsrm.reportid
    LEFT JOIN metrics m
      ON mcsrm.measurementconsumerid = m.measurementconsumerid
      AND mcsrm.metricid = m.metricid
    GROUP BY rp.externalreportid, rp.reportdetailsjson''')
) rpt
  ON REGEXP_EXTRACT(r.Report, 'reports/(.+)$') = rpt.externalreportid
LEFT JOIN (
  SELECT * FROM EXTERNAL_QUERY(
    'projects/${project_id}/locations/${region}/connections/reporting-postgres-conn',
    '''SELECT
      CAST(rp.externalreportid AS TEXT) AS externalreportid,
      rs.filter AS ReportingSetFilter,
      mcs.metriccalculationspecdetailsjson AS MetricCalcSpecDetails,
      COUNT(DISTINCT rseg.eventgroupid) AS EventGroupCount
    FROM reports rp
    LEFT JOIN metriccalculationspecreportingmetrics mcsrm
      ON rp.measurementconsumerid = mcsrm.measurementconsumerid
      AND rp.reportid = mcsrm.reportid
    LEFT JOIN metriccalculationspecs mcs
      ON mcsrm.measurementconsumerid = mcs.measurementconsumerid
      AND mcsrm.metriccalculationspecid = mcs.metriccalculationspecid
    LEFT JOIN metrics m
      ON mcsrm.measurementconsumerid = m.measurementconsumerid
      AND mcsrm.metricid = m.metricid
    LEFT JOIN reportingsets rs
      ON m.reportingsetid = rs.reportingsetid
      AND m.measurementconsumerid = rs.measurementconsumerid
    LEFT JOIN reportingseteventgroups rseg
      ON rs.measurementconsumerid = rseg.measurementconsumerid
      AND rs.reportingsetid = rseg.reportingsetid
    GROUP BY rp.externalreportid, rs.filter, mcs.metriccalculationspecdetailsjson''')
) rd
  ON REGEXP_EXTRACT(r.Report, 'reports/(.+)$') = rd.externalreportid
%{ if data_provider_id != "" }
WHERE r.DataProviderResourceId = '${data_provider_id}'
%{ endif }
