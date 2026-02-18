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

-- changeset jojijacob:6 dbms:cloudspanner
-- comment: Create RawImpressionBatchFile table to track raw impression files grouped into batches for VID labeling.

-- Set protobuf FileDescriptorSet as a base64 string.
SET PROTO_DESCRIPTORS =
'CosDCkd3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl9iYXRjaF9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqvgEKF1Jhd0ltcHJlc3Npb25CYXRjaFN0YXRlEioKJlJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX1VOU1BFQ0lGSUVEEAASJgoiUkFXX0lNUFJFU1NJT05fQkFUQ0hfU1RBVEVfQ1JFQVRFRBABEigKJFJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX1BST0NFU1NFRBACEiUKIVJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX0ZBSUxFRBADQk8KLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIcUmF3SW1wcmVzc2lvbkJhdGNoU3RhdGVQcm90b1ABYgZwcm90bzMKvgIKQXdmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3Jhd19pbXByZXNzaW9uX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvcip9ChJSYXdJbXByZXNzaW9uU3RhdGUSJAogUkFXX0lNUFJFU1NJT05fU1RBVEVfVU5TUEVDSUZJRUQQABIfChtSQVdfSU1QUkVTU0lPTl9TVEFURV9BQ1RJVkUQARIgChxSQVdfSU1QUkVTU0lPTl9TVEFURV9ERUxFVEVEEAJCSgotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhdSYXdJbXByZXNzaW9uU3RhdGVQcm90b1ABYgZwcm90bzM=';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.RawImpressionBatchState`,
  `wfa.measurement.internal.edpaggregator.RawImpressionState`,
);

-- Tracks raw impression files grouped into batches for VID labeling.
-- Hierarchy: DataProvider -> Upload -> Batch -> File
CREATE TABLE RawImpressionBatchFile (
  -- The globally unique resource ID of the DataProvider that owns this file.
  DataProviderResourceId STRING(63) NOT NULL,
  -- Identifier for the upload session that produced this file.
  UploadId STRING(63) NOT NULL,
  -- Zero-based index of the batch within the upload.
  BatchIndex INT64 NOT NULL,
  -- Zero-based index of the file within the batch.
  FileIndex INT64 NOT NULL,
  -- The URI of the raw impressions blob.
  BlobUri STRING(MAX) NOT NULL,
  -- The processing state of the batch containing this file.
  BatchState `wfa.measurement.internal.edpaggregator.RawImpressionBatchState` NOT NULL,
  -- The lifecycle state of this file record (used for soft delete).
  State `wfa.measurement.internal.edpaggregator.RawImpressionState` NOT NULL,
  -- The time this record was created in this database.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- The time this record was last updated in this database.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, UploadId, BatchIndex, FileIndex);

-- Enforces uniqueness of BlobUri per DataProvider and enables idempotency
-- checks before inserting a new file record.
CREATE UNIQUE INDEX RawImpressionBatchFileByBlobUri
  ON RawImpressionBatchFile(DataProviderResourceId, BlobUri);

RUN BATCH;
