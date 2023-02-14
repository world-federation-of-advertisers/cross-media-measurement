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
--       │   │   └── PrimitiveReportingSetBases
--       │   │       └── PrimitiveReportingSetBasisFilters
--       │   ├── CompositeReportingSets
--       │   │   └── SetExpressions
--       │   └── WeightedSubsetUnions
--       │       └── WeightedSubsetUnionPrimitiveReportingSetBases
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

-- changeset riemanli:create-measurement-consumers-table dbms:postgresql
CREATE TABLE MeasurementConsumers (
  MeasurementConsumerId bigint NOT NULL,
  CmmsMeasurementConsumerId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId),
  UNIQUE (CmmsMeasurementConsumerId)
);

-- changeset riemanli:create-event-groups-table dbms:postgresql
CREATE TABLE EventGroups (
  MeasurementConsumerId bigint NOT NULL,
  EventGroupId bigint NOT NULL,
  CmmsDataProviderId text NOT NULL,
  CmmsEventGroupId text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, EventGroupId),
  UNIQUE (CmmsDataProviderId, CmmsEventGroupId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE
);

-- changeset riemanli:create-time-intervals-table dbms:postgresql
CREATE TABLE TimeIntervals (
  MeasurementConsumerId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,

  Start TIMESTAMP NOT NULL,
  EndExclusive TIMESTAMP NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, TimeIntervalId),
  UNIQUE (MeasurementConsumerId, Start, EndExclusive),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-composite-reporting-sets-table dbms:postgresql
CREATE TABLE CompositeReportingSets (
  MeasurementConsumerId bigint NOT NULL,
  CompositeReportingSetId bigint NOT NULL,
  SetExpressionId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, CompositeReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, SetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-primitive-reporting-sets-table dbms:postgresql
CREATE TABLE PrimitiveReportingSets (
  MeasurementConsumerId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-primitive-reporting-set-event-groups-table dbms:postgresql
CREATE TABLE PrimitiveReportingSetEventGroups(
  MeasurementConsumerId bigint NOT NULL,
  EventGroupId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetId, EventGroupId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, EventGroupId)
    REFERENCES EventGroups(MeasurementConsumerId, EventGroupId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-reporting-sets-table dbms:postgresql
CREATE TABLE ReportingSets (
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  DisplayName text,
  Filter text,

  -- Exactly one of (CompositeReportingSet, PrimitiveReportingSet) must be
  -- non-null
  CompositeReportingSetId bigint,
  PrimitiveReportingSetId bigint,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId),
  UNIQUE (MeasurementConsumerId, ExternalReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, CompositeReportingSetId)
    REFERENCES CompositeReportingSets(MeasurementConsumerId, CompositeReportingSetId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-weighted-subset-unions-table dbms:postgresql
CREATE TABLE WeightedSubsetUnions (
  MeasurementConsumerId bigint NOT NULL,
  -- ReportingSets and WeightedSubsetUnions are one-to-many. A reporting set can
  -- be decomposed to a linear combination of WeightedSubsetUnions.
  ReportingSetId bigint NOT NULL,
  WeightedSubsetUnionId bigint NOT NULL,

  Weight bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-primitive-reporting-set-bases-table dbms:postgresql
CREATE TABLE PrimitiveReportingSetBases (
  MeasurementConsumerId bigint NOT NULL,
  PrimitiveReportingSetBasisId bigint NOT NULL,
  PrimitiveReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetBasisId),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetId)
    REFERENCES PrimitiveReportingSets(MeasurementConsumerId, PrimitiveReportingSetId)
    ON DELETE CASCADE,
)

-- changeset riemanli:create-weighted-subset-union-primitive-reporting-set-bases-table dbms:postgresql
CREATE TABLE WeightedSubsetUnionPrimitiveReportingSetBases (
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,
  -- A WeightedSubsetUnion is a weighted union of a subset of
  -- PrimitiveReportingSetBases. That is, one WeightedSubsetUnion has at least
  -- one PrimitiveReportingSetBasis.
  WeightedSubsetUnionId bigint NOT NULL,
  PrimitiveReportingSetBasisId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId)
    REFERENCES WeightedSubsetUnions(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId)
    REFERENCES PrimitiveReportingSetBases(MeasurementConsumerId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-primitive-reporting-set-basis-filters-table dbms:postgresql
CREATE TABLE PrimitiveReportingSetBasisFilters (
  MeasurementConsumerId bigint NOT NULL,
  PrimitiveReportingSetBasisId bigint NOT NULL,
  PrimitiveReportingSetBasisFilterId bigint NOT NULL,

  Filter text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, PrimitiveReportingSetBasisId, PrimitiveReportingSetBasisFilter),
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    REFERENCES PrimitiveReportingSetBases(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    ON DELETE CASCADE,
)

-- changeset riemanli:create-set-expressions-table dbms:postgresql
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
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, LeftHandSetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, LeftHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, RightHandSetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, RightHandReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-metric-specs-table dbms:postgresql
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
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-metrics-table dbms:postgresql
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
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-measurements-table dbms:postgresql
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
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-measurement-primitive-reporting-set-bases-table dbms:postgresql
CREATE TABLE MeasurementPrimitiveReportingSetBases (
  MeasurementConsumerId bigint NOT NULL,
  MeasurementId bigint NOT NULL,
  PrimitiveReportingSetBasisId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MeasurementId, PrimitiveReportingSetBasisId),
  FOREIGN KEY(MeasurementConsumerId, MeasurementId)
    REFERENCES Measurements(MeasurementConsumerId, MeasurementId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    REFERENCES PrimitiveReportingSetBases(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    ON DELETE CASCADE,
)

-- changeset riemanli:create-metric-measurements-table dbms:postgresql
CREATE TABLE MetricMeasurements (
  MeasurementConsumerId bigint NOT NULL,
  MetricId bigint NOT NULL,
  MeasurementId bigint NOT NULL,
  Coefficient integer NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId, MeasurementId),
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MeasurementId)
    REFERENCES Measurements(MeasurementConsumerId, MeasurementId)
    ON DELETE CASCADE,
)

-- changeset riemanli:create-models-table dbms:postgresql
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
  UNIQUE (MeasurementConsumerId, ExternalModelId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-model-metrics-table dbms:postgresql
CREATE TABLE ModelMetrics(
  MeasurementConsumerId bigint NOT NULL,
  ModelId bigint NOT NULL,
  MetricId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelId, MetricId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-model-metric-specs-table dbms:postgresql
CREATE TABLE ModelMetricSpecs(
  MeasurementConsumerId bigint NOT NULL,
  ModelId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-model-time-intervals-table dbms:postgresql
CREATE TABLE ModelTimeIntervals(
  MeasurementConsumerId bigint NOT NULL,
  ModelId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-model-reporting-sets-table dbms:postgresql
CREATE TABLE ModelReportingSets(
  MeasurementConsumerId bigint NOT NULL,
  ModelId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ModelId, ReportingSetId),
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-reports-table dbms:postgresql
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
  UNIQUE (MeasurementConsumerId, ExternalReportId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-report-time-intervals-table dbms:postgresql
CREATE TABLE ReportTimeIntervals (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  TimeIntervalId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, TimeIntervalId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, TimeIntervalId)
    REFERENCES TimeIntervals(MeasurementConsumerId, TimeIntervalId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-metric-calculations-table dbms:postgresql
CREATE TABLE MetricCalculations (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  MetricCalculationId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  -- Serialized org.wfanet.measurement.internal.reporting.Report.MetricCalculation.Details
  -- protobuf message.
  MetricCalculationDetails bytea NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricCalculationId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-metric-calculation-metrics-table dbms:postgresql
CREATE TABLE MetricCalculationMetrics (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  MetricCalculationId bigint NOT NULL,
  MetricId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricCalculationId, MetricId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricCalculationId)
    REFERENCES MetricCalculations(MeasurementConsumerId, ReportId, MetricCalculationId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-model-inference-calculations-table dbms:postgresql
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
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ModelId)
    REFERENCES Models(MeasurementConsumerId, ModelId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-model-inference-calculation-metric-specs-table dbms:postgresql
CREATE TABLE ModelInferenceCalculationMetricSpecs (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  ModelInferenceCalculationId bigint NOT NULL,
  MetricSpecId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, ModelInferenceCalculationId, MetricSpecId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, ModelInferenceCalculationId)
    REFERENCES ModelInferenceCalculations(MeasurementConsumerId, ReportId, ModelInferenceCalculationId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricSpecId)
    REFERENCES MetricSpecs(MeasurementConsumerId, MetricSpecId)
    ON DELETE CASCADE,
);
