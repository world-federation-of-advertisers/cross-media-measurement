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
-- changeset marcopremier:9 dbms:cloudspanner

DROP TABLE ModelShards;
CREATE TABLE ModelShards (
    DataProviderId INT64 NOT NULL,
    ModelShardId INT64 NOT NULL,
    ExternalModelShardId INT64 NOT NULL,
    ModelProviderId INT64 NOT NULL,
    ModelSuiteId INT64 NOT NULL,
    ModelReleaseId INT64 NOT NULL,
    ModelBlobPath STRING(MAX) NOT NULL,
    CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    FOREIGN KEY (ModelProviderId, ModelSuiteId, ModelReleaseId) REFERENCES ModelReleases(ModelProviderId, ModelSuiteId, ModelReleaseId),
) PRIMARY KEY (DataProviderId, ModelShardId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;
