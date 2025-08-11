-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
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

-- changeset
-- comment: Create ReportResults tables and make them usable for BasicReport

-- TODO: Add proto bundle

START BATCH DDL;

-- reference this table in basic report. If this table is referenced then the details should NOT be
-- set

-- TODO: code should be written to generate the EventTemplateDimensions table at report creation time if
-- any referenced dimensions do not exist.
-- it should have a column per groupable dimension. And it should have one column for any filter applied.

-- Create a place holder for supporting multiple EventTemplates concurrently
CREATE TABLE EventTemplates (
  EventTemplateId INT64 NOT NULL)
PRIMARY KEY (EventTemplateId)

-- This table should be defined/altered as part of configuring the system and/or updating
-- the EventTemplate version.
-- Alternatively this table could just be kept in memory in the internal server
-- If kept in memory the above table won't be necessary. Preference would be to keep this in
-- memory to start with and if it gets too big to keep it in a DB.
CREATE TABLE EventTemplateDimensions (
  EventTemplateId INT64 NOT NULL,
  -- This should be globally unique so we can use it as a join key on ReportResultDimensions
  EventTemplateDimensionId INT64 NOT NULL,

  EventTemplateVersion INT64 NOT NULL,

  -- Create a column per event template dimension that can be grouped by. It's type is "string" and represents all
  -- of the values the event template field can take on.
  ) PRIMARY KEY (EventTemplateId, EventTemplateDimension)

CREATE TABLE ReportResults (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  -- This is the start time of the set of results. Dates in subtables
  -- are with respect to the time defined by this timestamp. This is also
  -- used to indicate the start date for cumulative results in the child tables
  ReportIntervalStartTime TIMESTAMP NOT NULL

  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers (MeasurementConsumerId)
) PRIMARY KEY (MeasurementConsumerId, ReportResultsId),
  INTERLEAVE IN PARENT MeasurementConsumers ON DELETE CASCADE;

-- This being a child table of the above ensures that results for all ReportingSets for a given
-- ReportResult are physically nearby.
CREATE TABLE ReportResultDimensions (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportDimensionId INT64 NOT NULL,
  -- ReportingSetId is the Postgres ReportingSetId
  -- This corresponds to the ReportingSet for which the results are computed.
  -- We expect that all ReportingSets referenced here represent only unions of
  -- EventGroups.
  ReportingSetId INT64 NOT NULL,
  -- The IQF for which these results were computed. In the future this
  -- column could be nullable if we decide to make IQFs optional and/or
  -- deprecate them.
  ImpressionQualificationFilterId INT64 NOT NULL
  -- Corresponds to the enum report_results.proto
  MetricFrequencyType INT64 NOT NULL;
  -- Corresponds to the enum report_results.proto.
  VennDiagramRegionType INT64 NOT NULL;
  -- Describes the event template dimensions that the results are filtered to
  EventTemplateDimensionId INT64 NOT NULL;
  -- This is the normalized filter proto. We expect to be able to do comparisions on this column. It is NULL
  -- if no filters are applied.
  Filter FilterProto `path.to.filter.proto`,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers (MeasurementConsumerId)
  FOREIGN KEY (ReportResultId) REFERENCES ReportResults (ReportResultId)
) PRIMARY KEY (MeasurementConsumerId, ReportResultsId, ReportDimensionId),
  INTERLEAVE IN PARENT ReportResults ON DELETE CASCADE;

-- We will probably want an index on the non-primary key dimensions as those should be unique

CREATE TABLE ReportResultValues (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportDimensionId INT64 NOT NULL,
  ReportResultValueId INT64 NOT NULL,

  -- The start date for non-cumulative results in this row
  ReportingWindowStartDate DATE NOT NULL,
  -- The end date for results in this row
  ReportingWindowEndDate DATE NOT NULL,

  CumulativeResults
    BasicReportResultDetails `wfa.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet`,

  NonCumulativeResults
    BasicReportResultDetails `wfa.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet`,

  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerId, ReportResultsId, ReportDimensionId, ReportResultValueId),
  INTERLEAVE IN PARENT ReportResultDimensions ON DELETE CASCADE;


CREATE TABLE NoisyReportResultValues (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportDimensionId INT64 NOT NULL,
  NoisyReportResultValueId INT64 NOT NULL,

  -- The start date for non-cumulative results in this row
  ReportingWindowStartDate DATE NOT NULL,
  -- The end date for results in this row
  ReportingWindowEndDate DATE NOT NULL,

  CumulativeResults
    BasicReportResultDetails `wfa.measurement.internal.reporting.v2.ReportResults.BaseMetricSet.BasicBaseMetricSet`,

  NonCumulativeResults
    BasicReportResultDetails `wfa.measurement.internal.reporting.v2.ReportResults.BaseMetricSet.BaseBasicBaseMetricSet`,

  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerId, ReportResultsId, ReportDimensionId, NoisyReportResultValueId),
  INTERLEAVE IN PARENT ReportResultDimensions ON DELETE CASCADE;

ALTER TABLE BasicReports ADD COLUMN ReportResultId INT64;
ALTER TABLE BasicReports ALTER COLUMN BasicReportResultDetails
 `wfa.measurement.internal.reporting.v2.BasicReportResultDetails` NULL;

RUN BATCH;


