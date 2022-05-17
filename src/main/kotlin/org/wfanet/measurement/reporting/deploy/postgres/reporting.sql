-- Copyright 2022 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Postgres database schema for the Reporting server.
--
-- Table hierarchy:
--   Root
--   ├── Reports
--   │   ├── TimeIntervals
--   │   ├── PeriodicTimeIntervals
--   │   └── Metrics
--   │       └── SetOperations
--   └── ReportingSets

CREATE TABLE IF NOT EXISTS Reports (
  MeasurementConsumerReferenceId text NOT NULL,
  ReportId bigint NOT NULL,

  ExternalReportId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,
  Result text,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.Details
  -- protobuf message.
  ReportDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerReferenceId, ReportId),
  UNIQUE (MeasurementConsumerReferenceId, ExternalReportId)
);

CREATE INDEX IF NOT EXISTS ReportsByExternalReportId
  ON Reports(MeasurementConsumerReferenceId, ExternalReportId);

CREATE TABLE IF NOT EXISTS TimeIntervals (
  MeasurementConsumerReferenceId text NOT NULL,
  ReportId bigint NOT NULL,

  StartSeconds bigint,
  StartNanos integer,

  EndSeconds bigint,
  EndNanos integer,

  CONSTRAINT fk_report FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
);

CREATE TABLE IF NOT EXISTS PeriodicTimeIntervals (
  MeasurementConsumerReferenceId text NOT NULL,
  ReportId bigint NOT NULL,

  StartSeconds bigint,
  StartNanos integer,

  IncrementSeconds bigint,
  IncrementNanos integer,

  IntervalCount integer,

  CONSTRAINT fk_report FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
);

CREATE TABLE IF NOT EXISTS Metrics (
  MeasurementConsumerReferenceId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,

  -- Array of direct child SetOperation IDs
  SetOperations bigint[],

  -- Serialized org.wfanet.measurement.internal.reporting.Metric.Details
  -- protobuf message.
  MetricDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerReferenceId, ReportId, MetricId),
  CONSTRAINT fk_report FOREIGN KEY(MeasurementConsumerReferenceId, ReportId)
    REFERENCES Reports(MeasurementConsumerReferenceId, ReportId)
);

CREATE TABLE IF NOT EXISTS ReportingSets (
  MeasurementConsumerReferenceId text NOT NULL,
  ReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  EventGroupNames text[] NOT NULL,
  Filter text NOT NULL,
  DisplayName text NOT NULL,

  PRIMARY KEY(MeasurementConsumerReferenceId, ReportingSetId),
  UNIQUE (MeasurementConsumerReferenceId, ExternalReportingSetId)
);

CREATE INDEX IF NOT EXISTS ReportingSetsByExternalReportingSetId
  ON ReportingSets(MeasurementConsumerReferenceId, ExternalReportingSetId);

CREATE TABLE IF NOT EXISTS SetOperations (
  MeasurementConsumerReferenceId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,
  SetOperationId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Metric.SetOperation.Type
  -- protobuf enum encoded as an integer.
  Type smallint NOT NULL,
  DisplayName text NOT NULL,

  LeftHandSetOperationId bigint,
  RightHandSetOperationId bigint,

  LeftHandReportingSetId bigint,
  RightHandReportingSetId bigint,


  PRIMARY KEY(MeasurementConsumerReferenceId, ReportId, MetricId, SetOperationId),
  CONSTRAINT fk_lh_reporting_set FOREIGN KEY(MeasurementConsumerReferenceId, LeftHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerReferenceId, ReportingSetId),
  CONSTRAINT fk_rh_reporting_set FOREIGN KEY(MeasurementConsumerReferenceId, RightHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerReferenceId, ReportingSetId),
  CONSTRAINT fk_metric FOREIGN KEY(MeasurementConsumerReferenceId, ReportId, MetricId)
    REFERENCES Metrics(MeasurementConsumerReferenceId, ReportId, MetricId)
);
