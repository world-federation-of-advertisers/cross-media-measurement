-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
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

-- changeset sanjayvas:1 dbms:cloudspanner
-- preconditions onFail:MARK_RAN onError:HALT
-- precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Measurements'

START BATCH DDL;

-- Cloud Spanner database schema for the Secure Computation.
--
-- Table hierarchy:
--   Root
--   └── WorkItems
--       └── WorkItemAttempts

CREATE TABLE WorkItems (
    WorkItemId INT64 NOT NULL,
    ExternalWorkItemId INT64 NOT NULL,

    Queue STRING(MAX) NOT NULL,

    -- org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItem.State
    -- Proto enum encoded as int
    State INT64 NOT NULL,

    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

) PRIMARY KEY (WorkItemId);

CREATE UNIQUE INDEX WorkItemsByExternalId
    ON WorkItems(ExternalWorkItemId);

CREATE TABLE WorkItemAttempts (
    WorkItemId INT64 NOT NULL,
    WorkItemAttemptId INT64 NOT NULL,
    ExternalWorkItemAttemptId INT64 NOT NULL,

    -- org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItemAttempt.State
    -- Proto enum encoded as int
    State INT64 NOT NULL,

    AttemptNumber INT64 NOT NULL,
    Logs STRING(MAX) NOT NULL,

    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

) PRIMARY KEY (WorkItemAttemptId, WorkItemId),
    INTERLEAVE IN PARENT WorkItems ON DELETE CASCADE;

RUN BATCH;
