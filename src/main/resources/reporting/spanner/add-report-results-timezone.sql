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

-- changeset sanjayvas:11 dbms:cloudspanner
-- comment: Add timezone information to ReportResults.

START BATCH DDL;

ALTER TABLE ReportResults
  -- Identifier of source time zone of ReportIntervalStartTime in the TZ
  -- database. One of this or ReportIntervalStartTimeZoneOffset must be set.
  ADD COLUMN ReportIntervalStartTimeZoneId STRING(40);

ALTER TABLE ReportResults
  -- Offset of the source time zone of ReportIntervalStartTime in whole seconds.
  -- One of this or ReportIntervalStartTimeZoneId must be set.
  ADD COLUMN ReportIntervalStartTimeZoneOffset INT64;

ALTER TABLE BasicReports
  ADD CONSTRAINT FKey_BasicReports_ReportResults
  FOREIGN KEY (MeasurementConsumerId, ReportResultId)
  REFERENCES ReportResults(MeasurementConsumerId, ReportResultId);

RUN BATCH;
