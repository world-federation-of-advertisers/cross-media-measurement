-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
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

-- changeset panchobrsltdf:event-group-metadata-versions dbms:cloudspanner
-- comment: Add staged EventGroup metadata publication and write coordination.

START BATCH DDL;

-- An ordinary write supersedes older published copies of this EventGroup's metadata.
ALTER TABLE EventGroups
  ADD COLUMN EntityMetadataPublicationSequence INT64 NOT NULL DEFAULT (0);

CREATE TABLE EventGroupMetadataPublicationScopes (
  DataProviderId INT64 NOT NULL,
  MeasurementConsumerId INT64 NOT NULL,
  PublicationSequence INT64 NOT NULL,
) PRIMARY KEY(DataProviderId, MeasurementConsumerId);

CREATE TABLE EventGroupMetadataOperations (
  OperationId STRING(64) NOT NULL,
  DataProviderId INT64 NOT NULL,
  MeasurementConsumerId INT64 NOT NULL,
  EntityPath STRING(MAX) NOT NULL,
  OwnedKeys ARRAY<STRING(MAX)> NOT NULL,
  Metadata `google.protobuf.Struct` NOT NULL,
  State INT64 NOT NULL,
  WorkerFence INT64 NOT NULL,
  PublicationSequence INT64,
  LastEventGroupId INT64,
  StagedCount INT64 NOT NULL,
  Complete BOOL NOT NULL,
) PRIMARY KEY(OperationId);

CREATE INDEX EventGroupMetadataOperationsByScope
  ON EventGroupMetadataOperations(DataProviderId, MeasurementConsumerId, PublicationSequence);

-- Published versions must remain until their metadata is materialized in EventGroups.
CREATE TABLE EventGroupMetadataVersions (
  OperationId STRING(64) NOT NULL,
  EventGroupId INT64 NOT NULL,
  Metadata `google.protobuf.Struct` NOT NULL,
  MetadataValues ARRAY<STRING(MAX)>,
  Metadata_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(MetadataValues)) HIDDEN,
) PRIMARY KEY(OperationId, EventGroupId),
  INTERLEAVE IN PARENT EventGroupMetadataOperations ON DELETE CASCADE;

CREATE SEARCH INDEX EventGroupMetadataVersionsByValues
  ON EventGroupMetadataVersions(Metadata_Tokens);

CREATE TABLE EventGroupMetadataProviderGuards (
  DataProviderId INT64 NOT NULL,
  Revision INT64 NOT NULL,
) PRIMARY KEY(DataProviderId);

CREATE TABLE EventGroupMetadataSubtreeLocks (
  OperationId STRING(64) NOT NULL,
  DataProviderId INT64 NOT NULL,
  MeasurementConsumerId INT64 NOT NULL,
  EntityPath STRING(MAX) NOT NULL,
) PRIMARY KEY(OperationId),
  INTERLEAVE IN PARENT EventGroupMetadataOperations ON DELETE CASCADE;

CREATE INDEX EventGroupMetadataSubtreeLocksByScope
  ON EventGroupMetadataSubtreeLocks(DataProviderId, MeasurementConsumerId)
  STORING (EntityPath);

CREATE TABLE EventGroupMetadataHierarchyChanges (
  ChangeId STRING(64) NOT NULL,
  DataProviderId INT64 NOT NULL,
  Active BOOL NOT NULL,
) PRIMARY KEY(ChangeId);

CREATE INDEX EventGroupMetadataHierarchyChangesByProvider
  ON EventGroupMetadataHierarchyChanges(DataProviderId, Active);

RUN BATCH;
