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

-- changeset marcopremier:5 dbms:cloudspanner

-- Cloud Spanner database schema for the Kingdom StateTransitionMeasurementLogEntries table.
--
-- Table hierarchy:
--   Measurements
--   └── MeasurementLogEntries
--       └── StateTransitionMeasurementLogEntries

-- Child table used for logging measurement state transitions.
-- Each entry corresponds to a state change (previous-state != current-state)
CREATE TABLE StateTransitionMeasurementLogEntries (
    MeasurementConsumerId INT64 NOT NULL,
    MeasurementId INT64 NOT NULL,
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    PreviousMeasurementState INT64 NOT NULL,
    CurrentMeasurementState INT64 NOT NULL,
) PRIMARY KEY (MeasurementConsumerId, MeasurementId, CreateTime),
  INTERLEAVE IN PARENT MeasurementLogEntries ON DELETE CASCADE;
