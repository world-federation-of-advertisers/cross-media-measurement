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

-- changeset getina:8 dbms:cloudspanner
-- comment: Refactor RawImpressionMetadata into two tables (Batch + File) to align with the approved public API.

START BATCH DDL;

-- Drop FK from ImpressionMetadata to old table.
DROP INDEX ImpressionMetadataByRawImpressionAndModelLine;

ALTER TABLE ImpressionMetadata
  DROP CONSTRAINT FK_ImpressionMetadata_RawImpressionMetadata;

ALTER TABLE ImpressionMetadata DROP COLUMN RawImpressionUploadId;
ALTER TABLE ImpressionMetadata DROP COLUMN RawImpressionBatchIndex;

-- Drop old single-table schema.
DROP INDEX RawImpressionMetadataByUploadResourceId;
DROP INDEX RawImpressionMetadataByFileResourceId;
DROP INDEX RawImpressionMetadataByBlobUri;
DROP INDEX RawImpressionMetadataByUploadDateAndState;

DROP TABLE RawImpressionMetadata;

-- Resource pattern: dataProviders/{data_provider}/rawImpressionMetadataBatches/{raw_impression_metadata_batch}
CREATE TABLE RawImpressionMetadataBatch (
  DataProviderResourceId STRING(63) NOT NULL,
  BatchId INT64 NOT NULL,
  BatchResourceId STRING(63) NOT NULL,
  CreateRequestId STRING(36),
  State `wfa.measurement.internal.edpaggregator.RawImpressionBatchState` NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, BatchId);

-- Resource pattern: dataProviders/{data_provider}/rawImpressionMetadataBatches/{batch}/files/{file}
CREATE TABLE RawImpressionMetadataBatchFile (
  DataProviderResourceId STRING(63) NOT NULL,
  BatchId INT64 NOT NULL,
  FileId INT64 NOT NULL,
  FileResourceId STRING(63) NOT NULL,
  CreateRequestId STRING(36),
  BlobUri STRING(MAX) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, BatchId, FileId),
  INTERLEAVE IN PARENT RawImpressionMetadataBatch ON DELETE CASCADE;

CREATE UNIQUE INDEX RawImpressionMetadataBatchByResourceId
  ON RawImpressionMetadataBatch(DataProviderResourceId, BatchResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionMetadataBatchByCreateRequestId
  ON RawImpressionMetadataBatch(DataProviderResourceId, CreateRequestId);

CREATE INDEX RawImpressionMetadataBatchByState
  ON RawImpressionMetadataBatch(DataProviderResourceId, State);

CREATE UNIQUE INDEX RawImpressionMetadataBatchFileByResourceId
  ON RawImpressionMetadataBatchFile(DataProviderResourceId, BatchId, FileResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionMetadataBatchFileByCreateRequestId
  ON RawImpressionMetadataBatchFile(DataProviderResourceId, BatchId, CreateRequestId);

CREATE UNIQUE INDEX RawImpressionMetadataBatchFileByBlobUri
  ON RawImpressionMetadataBatchFile(DataProviderResourceId, BlobUri);

-- Rebuild FK from ImpressionMetadata to the new file table.
ALTER TABLE ImpressionMetadata ADD COLUMN RawImpressionBatchId INT64;

ALTER TABLE ImpressionMetadata
  ADD CONSTRAINT FK_ImpressionMetadata_RawImpressionMetadataBatchFile
  FOREIGN KEY (DataProviderResourceId, RawImpressionBatchId, RawImpressionFileId)
  REFERENCES RawImpressionMetadataBatchFile(DataProviderResourceId, BatchId, FileId);

CREATE UNIQUE NULL_FILTERED INDEX ImpressionMetadataByRawImpressionAndModelLine
  ON ImpressionMetadata(
    DataProviderResourceId,
    RawImpressionBatchId,
    RawImpressionFileId,
    CmmsModelLine
  );

RUN BATCH;
