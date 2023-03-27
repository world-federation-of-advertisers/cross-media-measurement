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
--       └── Reports
--           ├── ReportTimeIntervals
--           └── MetricCalculationSpecs
--               └── MetricCalculationSpecMetrics

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

-- changeset riemanli:create-reporting-sets-table dbms:postgresql
-- * ReportingSets rows which have NULL SetExpressionId are referred to as
--   "primitive", and those that have a non-NULL SetExpressionId are referred to
--   as "complex".
-- * Each row in the ReportingSets table is a vertex of a directed graph, where
--   the SetExpressions table describes the edges.
-- * Primitive ReportingSets rows are leaf vertices, i.e. they have no outgoing
--   edges.
-- * A WeightedSubsetUnions row indicates the ReportingSets row vertex that is
--   the start of a graph path.
-- * A PrimitiveReportingSetBases row is the result of a graph path with a
--   primitive ReportingSets row vertex that is the end of a graph path. Note
--   that the path may have zero edges, in which case the WeightedSubsetUnions
--   row and the PrimitiveReportingSetBases row indicate the same vertex.
-- * The PrimitiveReportingSetBasisFilters table contains the collection of
--   filters formed by visiting each vertex on the graph path.
CREATE TABLE ReportingSets (
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  ExternalReportingSetId bigint NOT NULL,

  DisplayName text,
  Filter text,

  -- If not NULL then the ReportingSet is a composite one, and will therefore
  -- have no corresponding rows in ReportingSetEventGroups.
  SetExpressionId bigint,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId),
  UNIQUE (MeasurementConsumerId, ExternalReportingSetId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, SetExpressionId)
    REFERENCES SetExpressions(MeasurementConsumerId, SetExpressionId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-reporting-set-event-groups-table dbms:postgresql
CREATE TABLE ReportingSetEventGroups(
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,
  EventGroupId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId, EventGroupId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, EventGroupId)
    REFERENCES EventGroups(MeasurementConsumerId, EventGroupId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-weighted-subset-unions-table dbms:postgresql
-- A WeightedSubsetUnion is a weighted subset union of
-- PrimitiveReportingSetBases. That is, one WeightedSubsetUnion has at least
-- one PrimitiveReportingSetBasis.
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
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-weighted-subset-union-primitive-reporting-set-bases-table dbms:postgresql
CREATE TABLE WeightedSubsetUnionPrimitiveReportingSetBases (
  MeasurementConsumerId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,
  WeightedSubsetUnionId bigint NOT NULL,
  PrimitiveReportingSetBasisId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId, PrimitiveReportingSetBasisId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId)
    REFERENCES WeightedSubsetUnions(MeasurementConsumerId, ReportingSetId, WeightedSubsetUnionId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, PrimitiveReportingSetBasisId)
    REFERENCES PrimitiveReportingSetBases(MeasurementConsumerId, PrimitiveReportingSetBasisId)
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
);

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

-- changeset riemanli:create-metrics-table dbms:postgresql
CREATE TABLE Metrics (
  MeasurementConsumerId bigint NOT NULL,
  MetricId bigint NOT NULL,
  CreateMetricRequestId text NOT NULL,
  ReportingSetId bigint NOT NULL,

  ExternalMetricId bigint NOT NULL,

  TimeIntervalStart TIMESTAMP WITH TIME ZONE NOT NULL,
  TimeIntervalEndExclusive TIMESTAMP WITH TIME ZONE NOT NULL,

  -- org.wfanet.measurement.internal.reporting.MetricSpec.MetricType
  -- protobuf enum encoded as an integer.
  MetricType integer NOT NULL,

  DifferentialPrivacyEpsilon DOUBLE PRECISION NOT NULL,
  DifferentialPrivacyDelta DOUBLE PRECISION NOT NULL,

  -- Must not be NULL if MetricType is FREQUENCY_HISTOGRAM or IMPRESSION_COUNT
  MaximumFrequencyPerUser bigint,
  -- Must not be NULL if MetricType is WATCH_DURATION
  MaximumWatchDurationPerUser bigint,

  VidSamplingIntervalStart DOUBLE PRECISION NOT NULL,
  VidSamplingIntervalEnd DOUBLE PRECISION NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Metric.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

  CreateTime TIMESTAMP WITH TIME ZONE NOT NULL,

  -- Serialized byte string of a proto3 protobuf with details about the
  -- metric which do not need to be indexed by the database.
  --
  -- See org.wfanet.measurement.internal.reporting.Metric.Details protobuf
  -- message.
  MetricDetails bytea NOT NULL,

  -- Human-readable copy of the MetricDetails column solely for debugging
  -- purposes.
  MetricDetailsJson text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MetricId),
  UNIQUE (MeasurementConsumerId, CreateMetricRequestId),
  UNIQUE (MeasurementConsumerId, ExternalMetricId),
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-measurements-table dbms:postgresql
CREATE TABLE Measurements (
  MeasurementConsumerId bigint NOT NULL,
  MeasurementId bigint NOT NULL,
  CmmsCreateMeasurementRequestId uuid NOT NULL,
  CmmsMeasurementId text,

  TimeIntervalStart TIMESTAMP WITH TIME ZONE NOT NULL,
  TimeIntervalEndExclusive TIMESTAMP WITH TIME ZONE NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.Measurement.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

  -- Serialized byte string of a proto3 protobuf with details about the
  -- measurement which do not need to be indexed by the database.
  --
  -- See org.wfanet.measurement.internal.reporting.Measurement.Details protobuf
  -- message.
  MeasurementDetails bytea NOT NULL,

  -- Human-readable copy of the MeasurementDetails column solely for debugging
  -- purposes.
  MeasurementDetailsJson text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, MeasurementId),
  UNIQUE (MeasurementConsumerId, CmmsCreateMeasurementRequestId),
  UNIQUE (MeasurementConsumerId, CmmsMeasurementId),
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
);

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
);

-- changeset riemanli:create-reports-table dbms:postgresql
CREATE TABLE Reports (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,

  ExternalReportId bigint NOT NULL,
  CreateReportRequestId text NOT NULL,

  CreateTime TIMESTAMP WITH TIME ZONE NOT NULL,

  -- org.wfanet.measurement.internal.reporting.Report.State
  -- protobuf enum encoded as an integer.
  State integer NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId),
  UNIQUE (MeasurementConsumerId, CreateReportRequestId),
  UNIQUE (MeasurementConsumerId, ExternalReportId),
  FOREIGN KEY(MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-report-time-intervals-table dbms:postgresql
CREATE TABLE ReportTimeIntervals (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  TimeIntervalStart TIMESTAMP WITH TIME ZONE NOT NULL,
  TimeIntervalEndExclusive TIMESTAMP WITH TIME ZONE NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, TimeIntervalStart, TimeIntervalEndExclusive),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-metric-calculations-table dbms:postgresql
CREATE TABLE MetricCalculationSpecs (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  MetricCalculationSpecId bigint NOT NULL,
  ReportingSetId bigint NOT NULL,

  -- Serialized byte string of a proto3 protobuf with details about the
  -- metric calculation which do not need to be indexed by the database.
  --
  -- See org.wfanet.measurement.internal.reporting.Report.MetricCalculationSpec.Details
  -- protobuf message.
  MetricCalculationSpecDetails bytea NOT NULL,

  -- Human-readable copy of the MetricCalculationSpecDetails column solely for
  -- debugging purposes.
  MetricCalculationSpecDetailsJson text NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricCalculationSpecId),
  FOREIGN KEY(MeasurementConsumerId, ReportId)
    REFERENCES Reports(MeasurementConsumerId, ReportId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, ReportingSetId)
    REFERENCES ReportingSets(MeasurementConsumerId, ReportingSetId)
    ON DELETE CASCADE,
);

-- changeset riemanli:create-metric-calculation-metrics-table dbms:postgresql
CREATE TABLE MetricCalculationSpecMetrics (
  MeasurementConsumerId bigint NOT NULL,
  ReportId bigint NOT NULL,
  MetricCalculationSpecId bigint NOT NULL,
  MetricId bigint NOT NULL,

  PRIMARY KEY(MeasurementConsumerId, ReportId, MetricCalculationSpecId, MetricId),
  FOREIGN KEY(MeasurementConsumerId, ReportId, MetricCalculationSpecId)
    REFERENCES MetricCalculationSpecs(MeasurementConsumerId, ReportId, MetricCalculationSpecId)
    ON DELETE CASCADE,
  FOREIGN KEY(MeasurementConsumerId, MetricId)
    REFERENCES Metrics(MeasurementConsumerId, MetricId)
    ON DELETE CASCADE,
);
