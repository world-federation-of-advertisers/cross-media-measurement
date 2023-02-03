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
--       │   ├── CompositeReportingSets
--       │   │   └── SetExpressions
--       │   └── WeightedSubsetUnions
--       │       └── PrimitiveReportingSetBases
--       │           └── PrimitiveReportingSetBasisFilters
--       ├── Metrics
--       │   └── MetricMeasurements
--       ├── MetricSpecs
--       ├── Measurements
--       │   └── MeasurementPrimitiveReportingSetBases
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
--               └── ModelInferenceCalculationModelSpecs

-- changeset riemanli:create-v2alpha-reports-table dbms:postgresql
CREATE TABLE MeasurementConsumers (
  MeasurementConsumerId bigint NOT NULL,
  CmmsMeasurementConsumerId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId),
  UNIQUE (CmmsMeasurementConsumerId)
);

CREATE TABLE DataProviders (
  MeasurementConsumerId bigint NOT NULL,
  DataProviderId bigint NOT NULL,
  CmmsDataProviderId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId),
  UNIQUE (MeasurementConsumerId, CmmsDataProviderId),
  FOREIGN KEY(MeasurementConsumerId)
      REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE TABLE EventGroups (
  MeasurementConsumerId bigint NOT NULL,
  DataProviderId bigint NOT NULL,
  EventGroupId bigint NOT NULL,
  CmmsEventGroupId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, DataProviderId, EventGroupId),
  UNIQUE (MeasurementConsumerId, DataProviderId, CmmsEventGroupId),
  FOREIGN KEY(MeasurementConsumerId)
        REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, DataProviderId)
          REFERENCES DataProviders(MeasurementConsumerId, DataProviderId),
);

CREATE TABLE TimeIntervals (
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
  CompositeReportingSetId bigint NOT NULL,
  SetExpressionId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, SetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId),
);

CREATE TABLE PrimitiveReportingSets (
  MeasurementConsumerId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE TABLE PrimitiveReportingSetEventGroups(
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  DisplayName text NOT NULL,
  Filter text,

  -- Exactly one of (CompositeReportingSet, PrimitiveReportingSet) must be
  -- non-null
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

CREATE UNIQUE INDEX ReportingSetsByExternalReportingSetId
  ON ReportingSets(MeasurementConsumerId, ExternalReportingSetId);

CREATE TABLE WeightedSubsetUnions (
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,
  WeightedSubsetUnionId bigint NOT NULL,

  weight bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
);

CREATE TABLE PrimitiveReportingSetBases (
  MeasurementConsumerId bigint NOT NULL,
  WeightedSubsetUnionId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, WeightedSubsetUnionId)
    REFERENCES WeightedSubsetUnions(MeasurementConsumerId, WeightedSubsetUnionId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
)

CREATE TABLE PrimitiveReportingSetBasisFilters (
  MeasurementConsumerId bigint NOT NULL,
  WeightedSubsetUnionId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,
  PrimitiveReportingSetBasisFilterId bigint NOT NULL,

  Filter text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetId, PrimitiveReportingSetBasisFilter),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, WeightedSubsetUnionId)
    REFERENCES WeightedSubsetUnions(MeasurementConsumerId, WeightedSubsetUnionId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSetBases(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetId),
)

CREATE TABLE SetExpressions (
  MeasurementConsumerId bigint NOT NULL,
  SetExpressionId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.SetExpression.Operation
  -- protobuf enum encoded as an integer.
  Operation integer NOT NULL,

  -- The left-hand-side (lhs) operand in a binary set expression. Exactly
  -- one lhs field has to be non-NULL.
  LeftHandSetExpressionId bigint,
  LeftHandReportingSetId bigint,
  -- The right-hand-side (rhs) operand in a binary set expression. At most
  -- one rhs field can be non-NULL.
  RightHandSetExpressionId bigint,
  RightHandReportingSetId bigint,

  PRIMARY KEY(MeasurementConsumerId, SetExpressionId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, LeftHandSetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId),
  FOREIGN KEY(MeasurementConsumerId, LeftHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, RightHandSetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId),
  FOREIGN KEY(MeasurementConsumerId, RightHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId),
);

CREATE TABLE MetricSpecs (
  MeasurementConsumerId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.MetricSpec.MetricType
  -- protobuf enum encoded as an integer.
  MetricType integer NOT NULL,

  -- Must not be NULL if MetricType is FREQUENCY_HISTOGRAM or IMPRESSION_COUNT
  MaximumFrequencyPerUser bigint,
  -- Must not be NULL if MetricType is WATCH_DURATION
  MaximumWatchDurationPerUser bigint,

  PRIMARY KEY(MeasurementConsumerId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
);

CREATE TABLE Metrics (
  MeasurementConsumerId bigint NOT NULL,
  MetricId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Metric.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

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
  MeasurementConsumerId bigint NOT NULL,
  MeasurementId bigint NOT NULL,
  CmmsMeasurementId text,
  TimeIntervalId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.Measurement.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

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

CREATE TABLE MeasurementPrimitiveReportingSetBases (
  MeasurementConsumerId bigint NOT NULL,
  MeasurementId bigint NOT NULL,
  WeightedSubsetUnionId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MeasurementId, WeightedSubsetUnionId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, MeasurementId)
    REFERENCES Measurements(MeasurementConsumerId, MeasurementId),
  FOREIGN KEY(MeasurementConsumerId, WeightedSubsetUnionId)
    REFERENCES WeightedSubsetUnions(MeasurementConsumerId, WeightedSubsetUnionId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSetBases(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetId),
)

CREATE TABLE MetricMeasurements (
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
  ModelId bigint NOT NULL,
  ExternalModelId bigint NOT NULL,

  -- org.wfanet.measurement.internal.reporting.MetricModel.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.MetricModel.Details
  -- protobuf message.
  ModelDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelId)
);

CREATE UNIQUE INDEX ModelsByExternalModelId
  ON Models(MeasurementConsumerId, ExternalModelId);

CREATE TABLE ModelMetrics(
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,

  ExternalReportId bigint NOT NULL,
  ReportIdempotencyKey text NOT NULL,

  CreateTime timestamp NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

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
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
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
  MeasurementConsumerId bigint NOT NULL,
  MetricCalculationId bigint NOT NULL,
  MetricId bigint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.MetricCalculation.Details
  -- protobuf message.
  MetricCalculationMetricsDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricCalculationId, MetricId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, MetricCalculationId)
    REFERENCES MetricCalculations(MeasurementConsumerId, MetricCalculationId),
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId),
);

CREATE TABLE ModelInferenceCalculations (
  MeasurementConsumerId bigint NOT NULL,
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

CREATE TABLE ModelInferenceCalculationMetricSpecs (
  MeasurementConsumerId bigint NOT NULL,
  ModelInferenceCalculationId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelInferenceCalculationId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY(MeasurementConsumerId, ModelInferenceCalculationId)
    REFERENCES ModelInferenceCalculations(MeasurementConsumerId, ModelInferenceCalculationId),
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId),
);
