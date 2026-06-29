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

-- changeset marcopremier:12 dbms:cloudspanner
-- comment: Remove the deprecated RawImpressionMetadataBatch and RawImpressionMetadataBatchFile
-- tables, their indexes, and the ImpressionMetadata foreign-key columns/index that referenced
-- them. These resources are superseded by RawImpressionUpload and RawImpressionUploadFile.

START BATCH DDL;

-- Drop the ImpressionMetadata foreign-key index, constraint, and columns that referenced the
-- deprecated tables (added by refactor-raw-impression-schema.sql / add-raw-impression-fk.sql).
DROP INDEX ImpressionMetadataByRawImpressionAndModelLine;

ALTER TABLE ImpressionMetadata
  DROP CONSTRAINT FK_ImpressionMetadata_RawImpressionMetadataBatchFile;

ALTER TABLE ImpressionMetadata DROP COLUMN RawImpressionBatchId;
ALTER TABLE ImpressionMetadata DROP COLUMN RawImpressionFileId;

-- Drop the indexes on the deprecated tables.
DROP INDEX RawImpressionMetadataBatchByResourceId;
DROP INDEX RawImpressionMetadataBatchByCreateRequestId;
DROP INDEX RawImpressionMetadataBatchByState;
DROP INDEX RawImpressionMetadataBatchFileByResourceId;
DROP INDEX RawImpressionMetadataBatchFileByCreateRequestId;
DROP INDEX RawImpressionMetadataBatchFileByBlobUri;

-- Drop the deprecated tables (child interleaved table first, then parent).
DROP TABLE RawImpressionMetadataBatchFile;
DROP TABLE RawImpressionMetadataBatch;

RUN BATCH;
