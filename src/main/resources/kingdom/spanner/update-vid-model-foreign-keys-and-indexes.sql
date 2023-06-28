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

-- changeset marcopremier:10 dbms:cloudspanner
-- validCheckSum: 8:e158dcf45f5d51fb712623c61e69de5f

START BATCH DDL;

DROP TABLE ModelRollouts;
DROP TABLE ModelOutages;
DROP TABLE ModelLines;
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

    HoldbackModelLineId INT64,
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

    FOREIGN KEY (ModelProviderId, ModelSuiteId, HoldbackModelLineId)
        REFERENCES ModelLines(ModelProviderId, ModelSuiteId, ModelLineId),
) PRIMARY KEY (ModelProviderId, ModelSuiteId, ModelLineId),
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
    PreviousModelRolloutId INT64,
    ModelReleaseId INT64 NOT NULL,
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

    FOREIGN KEY (ModelProviderId, ModelSuiteId, PreviousModelRolloutId)
        REFERENCES ModelRollouts(ModelProviderId, ModelSuiteId, ModelRolloutId),
    FOREIGN KEY (ModelProviderId, ModelSuiteId, ModelReleaseId)
        REFERENCES ModelReleases(ModelProviderId, ModelSuiteId, ModelReleaseId),
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

CREATE UNIQUE INDEX ModelSuitesByExternalId
    ON ModelSuites(ModelProviderId, ExternalModelSuiteId);

CREATE UNIQUE INDEX ModelLinesByExternalId
    ON ModelLines(ModelProviderId, ModelSuiteId, ExternalModelLineId);

CREATE UNIQUE INDEX ModelOutagesByExternalId
    ON ModelOutages(ModelProviderId, ModelSuiteId, ModelLineId, ExternalModelOutageId);

CREATE UNIQUE INDEX ModelRolloutsByExternalId
    ON ModelRollouts(ModelProviderId, ModelSuiteId, ModelLineId, ExternalModelRolloutId);

CREATE UNIQUE INDEX ModelShardsByExternalId
    ON ModelShards(DataProviderId, ExternalModelShardId);

RUN BATCH;
