-- liquibase formatted sql

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
--   │   ├── Metrics
--   │       └── NamedSetOperations
--   │           ├── SetOperations
--   │           └── MeasurementCalculations
--   │               └── WeightedMeasurements
--   │   └── ReportMeasurements
--   ├── Measurements
--   └── ReportingSets
--       └── ReportingSetEventGroups

-- changeset riemanli:create-v2alpha-reports-table dbms:postgresql
CREATE TABLE MeasurementConsumers (
  MeasurementConsumerId text NOT NULL,
  CmmsMeasurementConsumerId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId),
  UNIQUE (CmmsMeasurementConsumerId)
);

CREATE TABLE DataProviders (
  MeasurementConsumerId text NOT NULL,
  DataProviderId text NOT NULL,
  CmmsDataProviderId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId),
  FOREIGN KEY(MeasurementConsumerId)
      REFERENCES MeasurementConsumers(MeasurementConsumerId),
  UNIQUE (MeasurementConsumerId, CmmsDataProviderId)
);

CREATE TABLE EventGroups (
  MeasurementConsumerId text NOT NULL,
  DataProviderId text NOT NULL,
  EventGroupId text NOT NULL,
  CmmsEventGroupId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId, EventGroupId),
  FOREIGN KEY(MeasurementConsumerId)
        REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, DataProviderId)
          REFERENCES DataProviders(MeasurementConsumerId, DataProviderId),
  UNIQUE (MeasurementConsumerId, DataProviderId, CmmsEventGroupId)
);

CREATE TABLE TimeIntervals (
  MeasurementConsumerId text NOT NULL,
  TimeIntervalId bigint NOT NULL,

  Start TIMESTAMP,
  EndExclusive TIMESTAMP,

  PRIMARY KEY(MeasurementConsumerId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  UNIQUE (MeasurementConsumerId, Start, EndExclusive),
);

CREATE TABLE CompositeReportingSets (
  MeasurementConsumerId text NOT NULL,
  CompositeReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  Filter text NOT NULL,
  DisplayName text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  UNIQUE (MeasurementConsumerId, ExternalReportingSetId)
);

CREATE INDEX CompositeReportingSetsByExternalReportingSetId
  ON CompositeReportingSets(MeasurementConsumerId, ExternalReportingSetId);

CREATE TABLE PrimitiveReportingSets (
  MeasurementConsumerId text NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  Filter text NOT NULL,
  DisplayName text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  UNIQUE (MeasurementConsumerId, ExternalReportingSetId)
);

CREATE INDEX PrimitiveReportingSetsByExternalReportingSetId
  ON PrimitiveReportingSets(MeasurementConsumerId, ExternalReportingSetId);

CREATE TABLE PrimitiveReportingSetEventGroups(
  MeasurementConsumerId text NOT NULL,
  DataProviderId bigint NOT NULL,
  EventGroupId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId, EventGroupId, PrimitiveReportingSetId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, DataProviderId)
    REFERENCES DataProviders(MeasurementConsumerId, DataProviderId),
  FOREIGN KEY(MeasurementConsumerId, DataProviderId, EventGroupId)
    REFERENCES EventGroups(MeasurementConsumerId, DataProviderId, EventGroupId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
);

CREATE TABLE ReportingSets (
  MeasurementConsumerId text NOT NULL,
  ReportingSetId bigint NOT NULL,

  CompositeReportingSetId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, CompositeReportingSetId)
    REFERENCES CompositeReportingSets(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
);

CREATE TABLE MetricSpecs (
  MeasurementConsumerId text NOT NULL,
  MetricSpecId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.MetricSpec.MetricType
  -- protobuf enum encoded as an integer.
  MetricType smallint NOT NULL,

  MaximumFrequencyPerUser bigint NOT NULL,
  MaximumWatchDurationPerUser bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE TABLE Metrics (
  MeasurementConsumerId text NOT NULL,
  MetricId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Metric.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Metric.Details
  -- protobuf message.
  MetricDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
);

CREATE TABLE Measurements (
  MeasurementConsumerId text NOT NULL,
  MeasurementId text NOT NULL,
  CmmsMeasurementId text NOT NULL,
  TimeIntervalId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.Measurement.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Measurement.Failure
  -- protobuf message.
  Failure bytea,

  -- Serialized org.wfanet.measurement.internal.reporting.Measurement.Result
  -- protobuf message.
  Result bytea,

  PRIMARY KEY(MeasurementConsumerId, MeasurementId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
  UNIQUE (MeasurementConsumerId, CmmsMeasurementId),
);

CREATE TABLE MetricMeasurements (
  MeasurementConsumerId text NOT NULL,
  MetricId bigint NOT NULL,
  MeasurementId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId),
  FOREIGN KEY(MeasurementConsumerId, MeasurementId)
    REFERENCES Measurements(MeasurementConsumerId, MeasurementId),
)

CREATE TABLE Models (
  MeasurementConsumerId text NOT NULL,
  ModelId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.MetricModel.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.MetricModel.Details
  -- protobuf message.
  ModelDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelId)
);

CREATE TABLE ModelMetrics(
  MeasurementConsumerId text NOT NULL,
  ModelId bigint NOT NULL,
  MetricId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId),
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId),
);

CREATE TABLE ModelMetricSpecs(
  MeasurementConsumerId text NOT NULL,
  ModelId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId),
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId),
);

CREATE TABLE ModelTimeIntervals(
  MeasurementConsumerId text NOT NULL,
  ModelId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId),
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId),
);

CREATE TABLE ModelReportingSets(
  MeasurementConsumerId text NOT NULL,
  ModelId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId)
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
);

CREATE TABLE Reports (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,

  ExternalReportId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.Details
  -- protobuf message.
  ReportDetails bytea NOT NULL,

  ReportIdempotencyKey text NOT NULL,

  CreateTime timestamp NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId),
  FOREIGN KEY(MeasurementConsumerId)
          REFERENCES MeasurementConsumers(MeasurementConsumerId),
  UNIQUE (MeasurementConsumerId, ExternalReportId),
  UNIQUE (MeasurementConsumerId, ReportIdempotencyKey)
);

CREATE INDEX ReportsByExternalReportId
  ON Reports(MeasurementConsumerReferenceId, ExternalReportId);

CREATE TABLE ReportTimeIntervals (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId),
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId),
);

CREATE TABLE MetricCalculations (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricCalculationId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.MetricCalculation.Details
  -- protobuf message.
  MetricCalculationDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricCalculationId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
);

CREATE TABLE MetricCalculationMetrics (
  MeasurementConsumerId text NOT NULL,
  MetricCalculationId bigint NOT NULL,
  MetricId bigint NOT NULL,
  PRIMARY KEY(MeasurementConsumerId, MetricCalculationId, MetricId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, MetricCalculationId)
    REFERENCES MetricCalculations(MeasurementConsumerId, MetricCalculationId),
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId),
);

CREATE TABLE ModelInferenceCalculations (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  ModelInferenceCalculationId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,
  ModelId bigint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.ModelInferenceCalculation.Details
  -- protobuf message.
  ModelInferenceCalculationDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, ModelInferenceCalculationId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId),
);

CREATE TABLE WeightedPrimitiveReportingSetBases (
  MeasurementConsumerId text NOT NULL,
  ModelInferenceCalculationId bigint NOT NULL,
  WeightedPrimitiveReportingSetBasisId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  Filter text NOT NULL,
  Weight bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelInferenceCalculationId, WeightedPrimitiveReportingSetBasisId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelInferenceCalculationId)
    REFERENCES ModelInferenceCalculations(MeasurementConsumerId, ModelInferenceCalculationId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
);

CREATE TABLE ModelInferenceCalculationModelSpecs (
  MeasurementConsumerId text NOT NULL,
  ModelInferenceCalculationId bigint NOT NULL,
  ModelSpecId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelInferenceCalculationId, ModelSpecId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelInferenceCalculationId)
    REFERENCES ModelInferenceCalculations(MeasurementConsumerId, ModelInferenceCalculationId),
  FOREIGN KEY(MeasurementConsumerId, ModelSpecId)
    REFERENCES ModelSpecs(MeasurementConsumerId, ModelSpecId),
);










CREATE TABLE Metrics (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Metric.Details
  -- protobuf message.
  MetricDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
);

-- changeset tristanvuong2021:create-measurements-table dbms:postgresql
CREATE TABLE Measurements (
  MeasurementConsumerId text NOT NULL,
  MeasurementReferenceId text NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.MeasurementInfo.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Measurement.Failure
  -- protobuf message.
  Failure bytea,

  -- Serialized org.wfanet.measurement.internal.reporting.Measurement.Result
  -- protobuf message.
  Result bytea,

  PRIMARY KEY(MeasurementConsumerId, MeasurementReferenceId)
);

-- changeset tristanvuong2021:create-report-measurements-table dbms:postgresql
CREATE TABLE ReportMeasurements (
  MeasurementConsumerId text NOT NULL,
  MeasurementReferenceId text NOT NULL,
  ReportId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MeasurementReferenceId, ReportId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
      REFERENCES Reports(MeasurementConsumerId, ReportId),
  FOREIGN KEY(MeasurementConsumerId, MeasurementReferenceId)
        REFERENCES Measurements(MeasurementConsumerId, MeasurementReferenceId)
);

-- changeset tristanvuong2021:create-reporting-sets-table dbms:postgresql
CREATE TABLE ReportingSets (
  MeasurementConsumerId text NOT NULL,
  ReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  Filter text NOT NULL,
  DisplayName text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId),
  UNIQUE (MeasurementConsumerId, ExternalReportingSetId)
);

-- changeset tristanvuong2021:create-reporting-sets-by-external-reporting-set-id-index dbms:postgresql

-- changeset tristanvuong2021:create-reporting-set-event-groups-table dbms:postgresql
CREATE TABLE ReportingSetEventGroups (
  MeasurementConsumerId text NOT NULL,
  DataProviderReferenceId text NOT NULL,
  EventGroupReferenceId text NOT NULL,
  ReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderReferenceId, EventGroupReferenceId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
);

-- changeset tristanvuong2021:create-set-operations-table dbms:postgresql
CREATE TABLE SetOperations (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,
  SetOperationId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Metric.SetOperation.Type
  -- protobuf enum encoded as an integer.
  Type smallint NOT NULL,

  LeftHandSetOperationId bigint,
  RightHandSetOperationId bigint,

  LeftHandReportingSetId bigint,
  RightHandReportingSetId bigint,


  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricId, SetOperationId),
  FOREIGN KEY(MeasurementConsumerId, LeftHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, RightHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, ReportId, MetricId)
);

-- changeset tristanvuong2021:create-named-set-operations-table dbms:postgresql
CREATE TABLE NamedSetOperations (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,
  NamedSetOperationId bigint NOT NULL,

  DisplayName text NOT NULL,
  SetOperationId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, ReportId, MetricId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricId, SetOperationId)
    REFERENCES SetOperations(MeasurementConsumerId, ReportId, MetricId, SetOperationId)
);

-- changeset tristanvuong2021:create-measurement-calculations-table dbms:postgresql
CREATE TABLE MeasurementCalculations (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,
  NamedSetOperationId bigint NOT NULL,
  MeasurementCalculationId bigint NOT NULL,

  TimeIntervalId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, ReportId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId)
    REFERENCES NamedSetOperations(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId)
);

-- changeset tristanvuong2021:create-weighted-measurements-table dbms:postgresql
CREATE TABLE WeightedMeasurements (
  MeasurementConsumerId text NOT NULL,
  ReportId bigint NOT NULL,
  MetricId bigint NOT NULL,
  NamedSetOperationId bigint NOT NULL,
  MeasurementCalculationId bigint NOT NULL,
  WeightedMeasurementId bigint NOT NULL,

  MeasurementReferenceId text NOT NULL,
  Coefficient integer NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId, WeightedMeasurementId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId)
    REFERENCES MeasurementCalculations(MeasurementConsumerId, ReportId, MetricId, NamedSetOperationId, MeasurementCalculationId),
  FOREIGN KEY(MeasurementConsumerId, MeasurementReferenceId)
    REFERENCES Measurements(MeasurementConsumerId, MeasurementReferenceId)
);
