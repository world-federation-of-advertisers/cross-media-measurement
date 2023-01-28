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
--       ├── DataProviders
--       │   └── EventGroups
--       ├── TimeIntervals
--       ├── ReportingSets
--       │   ├── PrimitiveReportingSets
--       │   │   └── PrimitiveReportingSetEventGroups
--       │   └── CompositeReportingSets
--       │       └── SetExpressions
--       ├── Metrics
--       │   └── MetricMeasurements
--       ├── MetricSpecs
--       ├── Measurements
--       ├── Models
--       │   ├── ModelMetrics
--       │   ├── ModelMetricSpecs
--       │   ├── ModelTimeIntervals
--       │   └── ModelReportingSets
--       └── Reports
--           ├── ReportTimeIntervals
--           ├── MetricCalculations
--           │   └── MetricCalculationMetrics
--           └── ModelInferenceCalculations
--               ├── WeightedPrimitiveReportingSetBases
--               └── ModelInferenceCalculationModelSpecs

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
  UNIQUE (MeasurementConsumerId, CmmsDataProviderId),
  FOREIGN KEY(MeasurementConsumerId)
      REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE TABLE EventGroups (
  MeasurementConsumerId text NOT NULL,
  DataProviderId text NOT NULL,
  EventGroupId text NOT NULL,
  CmmsEventGroupId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId, EventGroupId),
  UNIQUE (MeasurementConsumerId, DataProviderId, CmmsEventGroupId),
  FOREIGN KEY(MeasurementConsumerId)
        REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, DataProviderId)
          REFERENCES DataProviders(MeasurementConsumerId, DataProviderId),
);

CREATE TABLE TimeIntervals (
  MeasurementConsumerId text NOT NULL,
  TimeIntervalId bigint NOT NULL,

  Start TIMESTAMP NOT NULL,
  EndExclusive TIMESTAMP NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE UNIQUE INDEX TimeIntervalsByStartEndExclusive
  ON TimeIntervals(MeasurementConsumerId, Start, EndExclusive);

CREATE TABLE CompositeReportingSets (
  MeasurementConsumerId text NOT NULL,
  CompositeReportingSetId bigint NOT NULL,
  SetExpressionId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  Filter text NOT NULL,
  DisplayName text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, SetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId),
);

CREATE UNIQUE INDEX CompositeReportingSetsByExternalReportingSetId
  ON CompositeReportingSets(MeasurementConsumerId, ExternalReportingSetId);

CREATE TABLE SetExpressions (
  MeasurementConsumerId text NOT NULL,
  SetExpressionId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.SetExpression.Operation
  -- protobuf enum encoded as an integer.
  Operation smallint NOT NULL,

  LeftHandSetExpressionId bigint,
  LeftHandCompositeReportingSetId bigint,
  LeftHandPrimitiveReportingSetId bigint,
  RightHandSetExpressionId bigint,
  RightHandCompositeReportingSetId bigint,
  RightHandPrimitiveReportingSetId bigint,

  PRIMARY KEY(MeasurementConsumerId, SetExpressionId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, LeftHandCompositeReportingSetId)
    REFERENCES CompositeReportingSets(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, LeftHandPrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, RightHandCompositeReportingSetId)
    REFERENCES CompositeReportingSets(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, RightHandPrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
);

CREATE TABLE PrimitiveReportingSets (
  MeasurementConsumerId text NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  Filter text NOT NULL,
  DisplayName text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE UNIQUE INDEX PrimitiveReportingSetsByExternalReportingSetId
  ON PrimitiveReportingSets(MeasurementConsumerId, ExternalReportingSetId);

CREATE TABLE PrimitiveReportingSetEventGroups(
  MeasurementConsumerId text NOT NULL,
  DataProviderId bigint NOT NULL,
  EventGroupId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId, EventGroupId, PrimitiveReportingSetId),
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

  -- it's either CompositeReportingSet or PrimitiveReportingSet
  CompositeReportingSetId bigint,
  PrimitiveReportingSetId bigint,

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

  -- Depending on what metric type is used, the parameters can be null.
  MaximumFrequencyPerUser bigint,
  MaximumWatchDurationPerUser bigint,

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

  PRIMARY KEY(MeasurementConsumerId, MeasurementId),
  UNIQUE (MeasurementConsumerId, CmmsMeasurementId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
);

CREATE TABLE MetricMeasurements (
  MeasurementConsumerId text NOT NULL,
  MetricId bigint NOT NULL,
  MeasurementId bigint NOT NULL,
  Coefficient integer NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId),
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

  PRIMARY KEY(MeasurementConsumerId, ModelId, MetricId),
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

  PRIMARY KEY(MeasurementConsumerId, ModelId, MetricSpecId),
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

  PRIMARY KEY(MeasurementConsumerId, ModelId, TimeIntervalId),
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

  PRIMARY KEY(MeasurementConsumerId, ModelId, ReportingSetId),
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
  ReportIdempotencyKey text NOT NULL,

  CreateTime timestamp NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.State
  -- protobuf enum encoded as an integer.
  State smallint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.Details
  -- protobuf message.
  ReportDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId),
  UNIQUE (MeasurementConsumerId, ReportIdempotencyKey),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE UNIQUE INDEX ReportsByExternalReportId
  ON Reports(MeasurementConsumerId, ExternalReportId);

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
