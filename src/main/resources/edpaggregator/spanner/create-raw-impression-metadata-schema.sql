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
-- comment: Create RawImpressionMetadata table to track raw impression files grouped into batches for VID labeling.

-- Set protobuf FileDescriptorSet as a base64 string.
SET PROTO_DESCRIPTORS =
'Ct0CCkZ3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9pbXByZXNzaW9uX21ldGFkYXRhX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciqRAQoXSW1wcmVzc2lvbk1ldGFkYXRhU3RhdGUSKQolSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9VTlNQRUNJRklFRBAAEiQKIElNUFJFU1NJT05fTUVUQURBVEFfU1RBVEVfQUNUSVZFEAESJQohSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9ERUxFVEVEEAJCTwotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhxJbXByZXNzaW9uTWV0YWRhdGFTdGF0ZVByb3RvUAFiBnByb3RvMwqJBApHd2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcmVxdWlzaXRpb25fbWV0YWRhdGFfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKrsCChhSZXF1aXNpdGlvbk1ldGFkYXRhU3RhdGUSKgomUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfVU5TUEVDSUZJRUQQABIlCiFSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9TVE9SRUQQARIlCiFSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9RVUVVRUQQAhIpCiVSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9QUk9DRVNTSU5HEAMSKAokUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfRlVMRklMTEVEEAQSJgoiUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfUkVGVVNFRBAFEigKJFJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1dJVEhEUkFXThAGQlAKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIdUmVxdWlzaXRpb25NZXRhZGF0YVN0YXRlUHJvdG9QAWIGcHJvdG8zCosDCkd3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl9iYXRjaF9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqvgEKF1Jhd0ltcHJlc3Npb25CYXRjaFN0YXRlEioKJlJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX1VOU1BFQ0lGSUVEEAASJgoiUkFXX0lNUFJFU1NJT05fQkFUQ0hfU1RBVEVfQ1JFQVRFRBABEigKJFJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX1BST0NFU1NFRBACEiUKIVJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX0ZBSUxFRBADQk8KLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIcUmF3SW1wcmVzc2lvbkJhdGNoU3RhdGVQcm90b1ABYgZwcm90bzMK4QIKQXdmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3Jhd19pbXByZXNzaW9uX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciqfAQoSUmF3SW1wcmVzc2lvblN0YXRlEiQKIFJBV19JTVBSRVNTSU9OX1NUQVRFX1VOU1BFQ0lGSUVEEAASHwobUkFXX0lNUFJFU1NJT05fU1RBVEVfQUNUSVZFEAESIAocUkFXX0lNUFJFU1NJT05fU1RBVEVfREVMRVRFRBACEiAKHFJBV19JTVBSRVNTSU9OX1NUQVRFX0lOVkFMSUQQA0JKCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCF1Jhd0ltcHJlc3Npb25TdGF0ZVByb3RvUAFiBnByb3RvMw==';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.RawImpressionBatchState`,
  `wfa.measurement.internal.edpaggregator.RawImpressionState`,
);

-- Tracks raw impression files grouped into batches for VID labeling.
-- Hierarchy: DataProvider -> Upload -> Batch -> File
-- Resource pattern: dataProviders/{data_provider}/rawImpressionUploads/{raw_impression_upload}/batches/{batch}/files/{file}
CREATE TABLE RawImpressionMetadata (
  -- External API resource ID of the DataProvider that owns this file.
  DataProviderResourceId STRING(63) NOT NULL,
  -- Internal DB key for the upload session.
  UploadId INT64 NOT NULL,
  -- External API resource ID of the upload session that produced this file.
  -- Corresponds to {raw_impression_upload} in the resource pattern.
  UploadResourceId STRING(63) NOT NULL,
  -- Zero-based index identifying the batch within the upload.
  -- Corresponds to {batch} in the resource pattern.
  BatchIndex INT64 NOT NULL,
  -- Internal DB key for this file, unique per batch.
  FileId INT64 NOT NULL,
  -- External API resource ID of this file.
  -- Corresponds to {file} in the resource pattern.
  -- Indexed via RawImpressionMetadataByFileResourceId for resource-ID-based lookups.
  FileResourceId STRING(63) NOT NULL,
  -- The date this upload covers. Used for date-based queries across multiple upload
  -- sessions, as well as for partitioning and soft delete functionality.
  UploadDate DATE NOT NULL,
  -- The URI of the raw impressions blob. Used for actual blob lookups.
  BlobUri STRING(MAX) NOT NULL,
  -- The processing state of the batch containing this file.
  BatchState `wfa.measurement.internal.edpaggregator.RawImpressionBatchState` NOT NULL,
  -- The lifecycle state of this file record (used for soft delete).
  State `wfa.measurement.internal.edpaggregator.RawImpressionState` NOT NULL,
  -- The time this record was created in this database.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- The time this record was last updated in this database.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, UploadId, BatchIndex, FileId);

-- Supports looking up all file rows for a given external upload resource ID.
CREATE INDEX RawImpressionMetadataByUploadResourceId
  ON RawImpressionMetadata(DataProviderResourceId, UploadResourceId);

-- Enforces uniqueness of FileResourceId per batch and supports
-- looking up a file row by its external resource ID.
CREATE UNIQUE INDEX RawImpressionMetadataByFileResourceId
  ON RawImpressionMetadata(DataProviderResourceId, UploadResourceId, BatchIndex, FileResourceId);

-- Enforces uniqueness of BlobUri per DataProvider and enables idempotency
-- checks before inserting a new file record.
CREATE UNIQUE INDEX RawImpressionMetadataByBlobUri
  ON RawImpressionMetadata(DataProviderResourceId, BlobUri);

-- Supports: "Find the ACTIVE upload for a specific date"
-- Query: WHERE DataProviderResourceId = @dp AND UploadDate = @date AND State = 'ACTIVE'
CREATE INDEX RawImpressionMetadataByUploadDateAndState
  ON RawImpressionMetadata(DataProviderResourceId, UploadDate, State);

RUN BATCH;
