-- liquibase formatted sql

-- Copyright 2024 The Cross-Media Measurement Authors
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

-- changeset tristanvuong2021:add-spec-columns-metrics-table dbms:postgresql
ALTER TABLE Metrics
  ADD COLUMN SingleDataProviderDifferentialPrivacyEpsilon DOUBLE PRECISION,
  ADD COLUMN SingleDataProviderDifferentialPrivacyDelta DOUBLE PRECISION,
  ADD COLUMN SingleDataProviderFrequencyDifferentialPrivacyEpsilon DOUBLE PRECISION,
  ADD COLUMN SingleDataProviderFrequencyDifferentialPrivacyDelta DOUBLE PRECISION,
  ADD COLUMN SingleDataProviderVidSamplingIntervalStart DOUBLE PRECISION,
  ADD COLUMN SingleDataProviderVidSamplingIntervalWidth DOUBLE PRECISION;
