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

--changeset kungfucraig:2 dbms:cloudspanner
-- comment: Create ReportResults tables and make them usable for BasicReport results

START BATCH DDL;

-- There is one in memory table that provides an GroupingDimensionFingerprint
-- for use in the tables below as a join key.
--
-- This field is a 64-bit integer that represents a combination of grouping dimensions.
-- For example if we have age bucket and gender dimensions each combination of their
-- values is assigned a unique integer. For example the combination (AGE_18_24, F)
-- is assigned a unique integer as does (AGE_18_24, null), in which case this includes
-- al values of the gender dimension in the grouping.
-- This allows for two things:
 --   1. a more efficient join of results
 --   2. a way to easily track versions of Event Templates
 -- The fingerprint considers the combination of dimension values as well as the Event Template
 -- version.

-- This table is the root for all of the Results associated with a Basic Report and in the
-- future an Advanced Report or Report Cube.
CREATE TABLE ReportResults (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  -- This is the start time of the set of results. Dates in subtables
  -- are with respect to the time defined by this timestamp. This is also
  -- used to indicate the start date for cumulative results in the child tables
  ReportIntervalStartTime TIMESTAMP NOT NULL,
  -- The time that this row was created.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers (MeasurementConsumerId)
) PRIMARY KEY (MeasurementConsumerId, ReportResultId),
  INTERLEAVE IN PARENT MeasurementConsumers ON DELETE CASCADE;

-- Each row of this table represents the Results for a particular ReportingSet Venn diagram
-- region that is entailed by the Report.
--
-- This being a child table of the above ensures that results for the entire Report are
-- physically nearby.
CREATE TABLE ReportingSetResults(
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportingSetResultId INT64 NOT NULL,
  -- A reference to the Postgres ReportingSetId
  -- Corresponds to the ReportingSet for which the results are computed.
  -- We expect that all ReportingSets referenced here represent only unions of
  -- EventGroups.
  ReportingSetId INT64 NOT NULL,
  -- Corresponds to the enum VennDiagramRegionType in report_results.proto.
  -- The VennDiagram region type may only be PRIMITIVE when the ReportingSet was
  -- passed in by the user, or in the case of BasicReport, when it corresponds to
  -- exactly one DataProvider. ReportingSets created as a side effect of Report creation
  -- should always be type UNION.
  VennDiagramRegionType INT64 NOT NULL,
  -- The ID of ImpressionQualificationFilter for which the set of results was computed.
  -- In the future this column could be nullable if we decide to make IQFs optional and/or
  -- deprecate them. We limit the use of Custom IQFs to a single filter. When the filter is
  -- custom the value of this column will be -1.
  ImpressionQualificationFilterName STRING(MAX) NOT NULL,
  -- Corresponds to the MetricFrequencyType enum in report_results.proto (e.g. WEEKLY)
  MetricFrequencyType INT64 NOT NULL,
  -- The grouping dimension.
  -- See the comment at the top of this file related to this field.
  GroupingDimensionFingerprint INT64 NOT NULL,
 -- The fingerprint of the normalized EventFilter proto that was used to create
 -- the results. It does not include any filters implied by the IQF.
 -- This field is NULL if no filters were applied.
 FilterFingerprint INT64,
 -- The creation time of this row.
 CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerId, ReportResultId, ReportingSetResultId),
  INTERLEAVE IN PARENT ReportResults ON DELETE CASCADE;
-- Ensure non-primary key dimensions are unique with respect to the parent.
CREATE UNIQUE INDEX ReportingSetResultsByDimensions
    ON ReportingSetResults(
    MeasurementConsumerId, ReportResultId,
    ReportingSetId, VennDiagramRegionType,
    ImpressionQualificationFilterName, MetricFrequencyType,
    GroupingDimensionFingerprint, FilterFingerprint),
 INTERLEAVE IN ReportResults;

-- Represents the results for each reporting window associated with each
-- ReportingSetResult.
CREATE TABLE ReportingWindowResults(
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportingSetResultId INT64 NOT NULL,
  ReportingWindowResultId INT64 NOT NULL,
  -- The start date for non-cumulative results in this row
  ReportingWindowStartDate DATE NOT NULL,
  -- The end date for results in this row
  ReportingWindowEndDate DATE NOT NULL,
 -- The creation time of this row.
 CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
 ) PRIMARY KEY (MeasurementConsumerId, ReportResultId,
               ReportingSetResultId, ReportingWindowResultId),
   INTERLEAVE IN PARENT ReportingSetResults ON DELETE CASCADE;
-- Ensure non-primary key dimensions are unique with respect to the parent.
CREATE UNIQUE INDEX ReportingWindowResultsByDates
     ON ReportingWindowResults(
     MeasurementConsumerId, ReportResultId,
     ReportingSetResultId, ReportingWindowStartDate,
     ReportingWindowEndDate),
  INTERLEAVE IN ReportingSetResults;

-- Contains denoised values. We expect all derived metrics to be present in the cumulative
-- and non-cumulative columns regardless of whether they were explicitly requested via the
-- API. Both CumulativeResults and NonCumulativeResults must not be null.
CREATE TABLE ReportResultValues (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportingSetResultId INT64 NOT NULL,
  ReportingWindowResultId INT64 NOT NULL,
  ReportResultValueId INT64 NOT NULL,
  -- Denoised cumulative results for the window represented by the parent table
  CumulativeResults `wfa.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet`,
  -- Denoised non-cumulative results for the window represented by the parent table.
  NonCumulativeResults `wfa.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet`,
  -- The creation time of this row.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerId, ReportResultId, ReportingSetResultId,
               ReportingWindowResultId, ReportResultValueId),
  INTERLEAVE IN PARENT ReportingWindowResults ON DELETE CASCADE;

-- Contains noisy values. We do not expected this table to contain any derived metrics.
-- Both CumulativeResults and NonCumulativeResults must not be null.
CREATE TABLE NoisyReportResultValues (
  MeasurementConsumerId INT64 NOT NULL,
  ReportResultId INT64 NOT NULL,
  ReportingSetResultId INT64 NOT NULL,
  ReportingWindowResultId INT64 NOT NULL,
  NoisyReportResultValueId INT64 NOT NULL,
  -- Noisy cumulative results for the window represented by the parent table.
  -- Only non-drived metrics will be present.
  CumulativeResults `wfa.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet`,
  -- Noisy non-cumulative results for the window represented by the parent table.
  -- Only non-drived metrics will be present.
  NonCumulativeResults `wfa.measurement.internal.reporting.v2.ResultGroup.MetricSet.BasicMetricSet`,
  -- The creation time of this row.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerId, ReportResultId, ReportingSetResultId,
                ReportingWindowResultId, NoisyReportResultValueId),
  INTERLEAVE IN PARENT ReportingWindowResults ON DELETE CASCADE;

ALTER TABLE BasicReports ADD COLUMN ReportResultId INT64;
ALTER TABLE BasicReports ALTER COLUMN BasicReportResultDetails
 `wfa.measurement.internal.reporting.v2.BasicReportResultDetails`;

RUN BATCH;


