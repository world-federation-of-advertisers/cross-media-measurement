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

-- changeset jojijacob:14 dbms:cloudspanner

-- Adding Cloud Spanner table for the Kingdom Populations table.
--
-- Table hierarchy:
--  Root
--   └── DataProviders
--       └── Populations
--
START BATCH DDL;

CREATE TABLE Populations (
    DataProviderId INT64 NOT NULL, -- population data provider
    PopulationId INT64 NOT NULL,
    ExternalPopulationId INT64 NOT NULL,
    Description STRING(MAX),
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

    -- org.wfanet.measurement.internal.kingdom.Population.PopulationBlob.model_blob_uri
    ModelBlobUri STRING(MAX) NOT NULL,

    -- org.wfanet.measurement.internal.kingdom.Population.EventTemplate.type
    EventTemplateType STRING(MAX) NOT NULL,
) PRIMARY KEY (DataProviderId, PopulationId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;

CREATE UNIQUE INDEX PopulationsByExternalId
    ON Populations(DataProviderId, ExternalPopulationId);

ALTER TABLE ModelReleases
    ADD COLUMN PopulationDataProviderId INT64; -- population data provider that created population

ALTER TABLE ModelReleases
    ADD COLUMN PopulationId INT64; -- population used in model release

ALTER TABLE ModelReleases
    ADD FOREIGN KEY (PopulationDataProviderId, PopulationId) REFERENCES Populations(DataProviderId, PopulationId);



RUN BATCH;
