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

-- changeset jojijacob:7 dbms:cloudspanner

-- Cloud Spanner database schema for the Kingdom VID model distribution related tables.
--
-- Table hierarchy:
--   Root
--   ├── ModelProviders
--   |   └── ModelSuites
--   |       ├── ModelLines
--   |       |   ├── ModelRollouts
--   |       |   └── ModelOutages
--   |       └── ModelReleases
--   └── DataProviders
--       └── ModelShards

START BATCH DDL;

CREATE TABLE ModelSuites (
  ModelProviderId INT64 NOT NULL,
  ModelSuiteId INT64 NOT NULL,
  ExternalModelSuiteId INT64 NOT NULL,
  DisplayName STRING(MAX) NOT NULL,
  Description STRING(MAX),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (ModelProviderId, ModelSuiteId),
  INTERLEAVE IN PARENT ModelProviders ON DELETE CASCADE;

CREATE TABLE ModelLines (
  ModelProviderId INT64 NOT NULL,
  ModelSuiteId INT64 NOT NULL,
  ModelLineId INT64 NOT NULL,
  ExternalModelLineId INT64 NOT NULL,
  DisplayName STRING(MAX),
  Description STRING(MAX),
  ActiveStartTime TIMESTAMP NOT NULL,
  ActiveEndTime TIMESTAMP,

  -- org.wfanet.measurement.internal.kingdom.ModelLine.Type
  -- protobuf name encoded as an integer.
  Type INT64 NOT NULL,

  HoldbackModelLine INT64,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

  FOREIGN KEY (HoldbackModelLine) REFERENCES ModelLines(ModelLineId),
) PRIMARY KEY (ModelProviderId, ModelSuiteId, ModelLineId),
  INTERLEAVE IN PARENT ModelSuites ON DELETE CASCADE;

CREATE TABLE ModelReleases (
    ModelProviderId INT64 NOT NULL,
    ModelSuiteId INT64 NOT NULL,
    ModelReleaseId INT64 NOT NULL,
    ExternalModelReleaseId INT64 NOT NULL,
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (ModelProviderId, ModelSuiteId, ModelReleaseId),
  INTERLEAVE IN PARENT ModelSuites ON DELETE CASCADE;

CREATE TABLE ModelRollouts (
    ModelProviderId INT64 NOT NULL,
    ModelSuiteId INT64 NOT NULL,
    ModelLineId INT64 NOT NULL,
    ModelRolloutId INT64 NOT NULL,
    ExternalModelRolloutId INT64 NOT NULL,
    RolloutPeriodStartTime TIMESTAMP NOT NULL,
    RolloutPeriodEndTime TIMESTAMP NOT NULL,
    RolloutFreezeTime TIMESTAMP,
    PreviousModelRollout INT64,
    ModelRelease INT64 NOT NULL,
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

    FOREIGN KEY (PreviousModelRollout) REFERENCES ModelRollouts(ModelRolloutId),
    FOREIGN KEY (ModelRelease) REFERENCES ModelReleases(ModelReleaseId),
) PRIMARY KEY (ModelProviderId, ModelSuiteId, ModelLineId, ModelRolloutId),
  INTERLEAVE IN PARENT ModelLines ON DELETE CASCADE;

CREATE TABLE ModelOutages (
    ModelProviderId INT64 NOT NULL,
    ModelSuiteId INT64 NOT NULL,
    ModelLineId INT64 NOT NULL,
    ModelOutageId INT64 NOT NULL,
    ExternalModelOutageId INT64 NOT NULL,
    OutageStartTime TIMESTAMP NOT NULL,
    OutageEndTime TIMESTAMP NOT NULL,

    -- org.wfanet.measurement.internal.kingdom.ModelOutage.State
    -- protobuf name encoded as an integer.
    State INT64 NOT NULL,

    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (ModelProviderId, ModelSuiteId, ModelLineId, ModelOutageId),
  INTERLEAVE IN PARENT ModelLines ON DELETE CASCADE;

CREATE TABLE ModelShards (
  DataProviderId INT64 NOT NULL,
  ModelShardId INT64 NOT NULL,
  ExternalModelShardId INT64 NOT NULL,
  ModelRelease INT64 NOT NULL,
  ModelBlobPath STRING(MAX) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  FOREIGN KEY (ModelRelease) REFERENCES ModelReleases(ModelReleaseId),
) PRIMARY KEY (DataProviderId, ModelShardId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;

RUN BATCH;
