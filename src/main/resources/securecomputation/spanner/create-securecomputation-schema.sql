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

-- changeset marcopremier:1 dbms:cloudspanner

START BATCH DDL;

-- Cloud Spanner database schema for the Secure Computation.
--
-- Table hierarchy:
--   Root
--   └── WorkItems
--       └── WorkItemAttempts

CREATE TABLE WorkItems (
    WorkItemId INT64 NOT NULL,
    WorkItemResourceId STRING(63) NOT NULL,

    QueueId INT64 NOT NULL,

    -- org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItem.State
    -- Proto enum encoded as int
    State INT64 NOT NULL,

    WorkItemParams BYTES(MAX),

    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

) PRIMARY KEY (WorkItemId);

CREATE UNIQUE INDEX WorkItemsByResourceId
    ON WorkItems(WorkItemResourceId);

CREATE TABLE WorkItemAttempts (
    WorkItemId INT64 NOT NULL,
    WorkItemAttemptId INT64 NOT NULL,
    WorkItemAttemptResourceId STRING(63) NOT NULL,

    -- org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt.State
    -- Proto enum encoded as int
    State INT64 NOT NULL,

    ErrorMessage STRING(MAX),

    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

) PRIMARY KEY (WorkItemId, WorkItemAttemptId),
    INTERLEAVE IN PARENT WorkItems ON DELETE CASCADE;

RUN BATCH;
