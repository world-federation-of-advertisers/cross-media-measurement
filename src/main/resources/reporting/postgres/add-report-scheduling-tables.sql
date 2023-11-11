-- liquibase formatted sql

-- Copyright 2023 The Cross-Media Measurement Authors
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
--   └── MeasurementConsumers
--       ├── EventGroups
--       ├── ReportingSets
--       │   ├── ReportingSetEventGroups
--       │   ├── PrimitiveReportingSetBases
--       │   │   └── PrimitiveReportingSetBasisFilters
--       │   ├── SetExpressions
--       │   └── WeightedSubsetUnions
--       │       └── WeightedSubsetUnionPrimitiveReportingSetBases
--       ├── Metrics
--       │   └── MetricMeasurements
--       ├── Measurements
--       │   └── MeasurementPrimitiveReportingSetBases
--       ├── MetricCalculationSpecs
--       ├── Reports
--       │   ├── ReportTimeIntervals
--       │   └── MetricCalculationSpecReportingMetrics
--       └── ReportSchedules
--           ├── ReportScheduleIterations
--           └── ReportsReportSchedules

-- changeset tristanvuong2021:create-report-schedules-table dbms:postgresql
CREATE TABLE ReportSchedules (
  MeasurementConsumerId bigint NOT NULL,
  ReportScheduleId bigint NOT NULL,
  ExternalReportScheduleId text NOT NULL,

  CreateReportScheduleRequestId text,

  -- org.wfanet.measurement.internal.reporting.ReportSchedule.State proto enum
  State int NOT NULL,
  CreateTime timestamp with time zone NOT NULL,
  UpdateTime timestamp with time zone NOT NULL,

  NextReportCreationTime timestamp with time zone NOT NULL,

  LatestReportScheduleIterationId bigint,

  ReportScheduleDetails bytea NOT NULL,
  ReportScheduleDetailsJson text NOT NULL,

  Primary Key(MeasurementConsumerId, ReportScheduleId),
  UNIQUE(MeasurementConsumerId, ExternalReportScheduleId),
  UNIQUE(MeasurementConsumerId, CreateReportScheduleRequestId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE
);

-- changeset tristanvuong2021:create-report-schedule-iterations-table dbms:postgresql
CREATE TABLE ReportScheduleIterations (
  MeasurementConsumerId bigint NOT NULL,
  ReportScheduleId bigint NOT NULL,
  ReportScheduleIterationId bigint NOT NULL,
  ExternalReportScheduleIterationId text NOT NULL,

  ReportEventTime timestamp with time zone NOT NULL,
  CreateReportRequestId text NOT NULL,

  NumAttempts int NOT NULL,

  -- org.wfanet.measurement.internal.reporting.ReportScheduleIteration.State proto enum
  State int NOT NULL,
  CreateTime timestamp with time zone NOT NULL,
  UpdateTime timestamp with time zone NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportScheduleId, ReportScheduleIterationId),
  UNIQUE(MeasurementConsumerId, ReportScheduleId, ExternalReportScheduleIterationId),
  UNIQUE(MeasurementConsumerId, ReportScheduleId, ReportEventTime),
  FOREIGN KEY(MeasurementConsumerId, ReportScheduleId)
    REFERENCES ReportSchedules(MeasurementConsumerId, ReportScheduleId)
    ON DELETE CASCADE
);

-- changeset tristanvuong2021:create-reports-report-schedules-table dbms:postgresql
CREATE TABLE ReportsReportSchedules (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  ReportScheduleId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, ReportScheduleId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ReportScheduleId)
    REFERENCES ReportSchedules(MeasurementConsumerId, ReportScheduleId)
    ON DELETE CASCADE
);
