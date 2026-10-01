-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
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

-- changeset marcopremier:18 dbms:cloudspanner
-- comment: Add the transactional outbox for availability task publication.

START BATCH DDL;

CREATE TABLE DataAvailabilitySyncTaskPublication (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  DataAvailabilitySyncTaskResourceId STRING(63) NOT NULL,
  LeaseOwner STRING(36),
  LeaseExpirationTime TIMESTAMP,
  ProviderSlot BOOL,
  NextAttemptTime TIMESTAMP NOT NULL,
  AttemptCount INT64 NOT NULL,
  PublishedTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (
  DataProviderResourceId,
  RawImpressionUploadId,
  DataAvailabilitySyncTaskResourceId
), INTERLEAVE IN PARENT DataAvailabilitySyncTask ON DELETE CASCADE;

CREATE INDEX DataAvailabilitySyncTaskPublicationByClaimPriority
  ON DataAvailabilitySyncTaskPublication(
    PublishedTime,
    NextAttemptTime,
    LeaseExpirationTime,
    DataProviderResourceId,
    RawImpressionUploadId,
    DataAvailabilitySyncTaskResourceId
  );

CREATE UNIQUE NULL_FILTERED INDEX DataAvailabilitySyncTaskPublicationByProviderSlot
  ON DataAvailabilitySyncTaskPublication(DataProviderResourceId, ProviderSlot);

RUN BATCH;
