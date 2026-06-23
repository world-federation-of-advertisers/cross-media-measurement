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

-- changeset getina:11 dbms:cloudspanner
-- comment: Create VID labeling pipeline tables: RawImpressionUpload,
-- RawImpressionUploadFile, RawImpressionUploadModelLine,
-- PoolAssignmentJob, RankerJob, RankIndexBlob, and VidLabelingJob.
-- Uses INT64 surrogate PKs with STRING resource IDs for external API access.

-- Set protobuf FileDescriptorSet as a base64 string.
SET PROTO_DESCRIPTORS =
'Ct0CCkZ3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9pbXByZXNzaW9uX21ldGFkYXRhX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciqRAQoXSW1wcmVzc2lvbk1ldGFkYXRhU3RhdGUSKQolSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9VTlNQRUNJRklFRBAAEiQKIElNUFJFU1NJT05fTUVUQURBVEFfU1RBVEVfQUNUSVZFEAESJQohSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9ERUxFVEVEEAJCTwotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhxJbXByZXNzaW9uTWV0YWRhdGFTdGF0ZVByb3RvUAFiBnByb3RvMwqJBApHd2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcmVxdWlzaXRpb25fbWV0YWRhdGFfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKrsCChhSZXF1aXNpdGlvbk1ldGFkYXRhU3RhdGUSKgomUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfVU5TUEVDSUZJRUQQABIlCiFSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9TVE9SRUQQARIlCiFSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9RVUVVRUQQAhIpCiVSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9QUk9DRVNTSU5HEAMSKAokUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfRlVMRklMTEVEEAQSJgoiUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfUkVGVVNFRBAFEigKJFJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1dJVEhEUkFXThAGQlAKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIdUmVxdWlzaXRpb25NZXRhZGF0YVN0YXRlUHJvdG9QAWIGcHJvdG8zCosDCkd3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl9iYXRjaF9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqvgEKF1Jhd0ltcHJlc3Npb25CYXRjaFN0YXRlEioKJlJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX1VOU1BFQ0lGSUVEEAASJgoiUkFXX0lNUFJFU1NJT05fQkFUQ0hfU1RBVEVfQ1JFQVRFRBABEigKJFJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX1BST0NFU1NFRBACEiUKIVJBV19JTVBSRVNTSU9OX0JBVENIX1NUQVRFX0ZBSUxFRBADQk8KLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIcUmF3SW1wcmVzc2lvbkJhdGNoU3RhdGVQcm90b1ABYgZwcm90bzMKugMKSHdmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3Jhd19pbXByZXNzaW9uX3VwbG9hZF9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3Iq6wEKGFJhd0ltcHJlc3Npb25VcGxvYWRTdGF0ZRIrCidSQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfVU5TUEVDSUZJRUQQABInCiNSQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfQ1JFQVRFRBABEiYKIlJBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9BQ1RJVkUQAhIpCiVSQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfQ09NUExFVEVEEAMSJgoiUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX0ZBSUxFRBAEQlAKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIdUmF3SW1wcmVzc2lvblVwbG9hZFN0YXRlUHJvdG9QAWIGcHJvdG8zCv8EClN3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl91cGxvYWRfbW9kZWxfbGluZV9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqnAMKIVJhd0ltcHJlc3Npb25VcGxvYWRNb2RlbExpbmVTdGF0ZRI2CjJSQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9VTlNQRUNJRklFRBAAEjIKLlJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0NSRUFURUQQARI5CjVSQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9QT09MX0FTU0lHTklORxACEjIKLlJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX1JBTktJTkcQAxIzCi9SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9MQUJFTElORxAEEjQKMFJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0NPTVBMRVRFRBAFEjEKLVJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0ZBSUxFRBAGQlkKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckImUmF3SW1wcmVzc2lvblVwbG9hZE1vZGVsTGluZVN0YXRlUHJvdG9QAWIGcHJvdG8zCuoCCkJ3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9wb29sX2Fzc2lnbm1lbnRfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKqYBChNQb29sQXNzaWdubWVudFN0YXRlEiUKIVBPT0xfQVNTSUdOTUVOVF9TVEFURV9VTlNQRUNJRklFRBAAEiEKHVBPT0xfQVNTSUdOTUVOVF9TVEFURV9DUkVBVEVEEAESIwofUE9PTF9BU1NJR05NRU5UX1NUQVRFX1NVQ0NFRURFRBACEiAKHFBPT0xfQVNTSUdOTUVOVF9TVEFURV9GQUlMRUQQA0JLCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCGFBvb2xBc3NpZ25tZW50U3RhdGVQcm90b1ABYgZwcm90bzMKrAIKOXdmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3Jhbmtlcl9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqegoLUmFua2VyU3RhdGUSHAoYUkFOS0VSX1NUQVRFX1VOU1BFQ0lGSUVEEAASGAoUUkFOS0VSX1NUQVRFX0NSRUFURUQQARIaChZSQU5LRVJfU1RBVEVfU1VDQ0VFREVEEAISFwoTUkFOS0VSX1NUQVRFX0ZBSUxFRBADQkMKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIQUmFua2VyU3RhdGVQcm90b1ABYgZwcm90bzMK1QIKP3dmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3ZpZF9sYWJlbGluZ19zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqlwEKEFZpZExhYmVsaW5nU3RhdGUSIgoeVklEX0xBQkVMSU5HX1NUQVRFX1VOU1BFQ0lGSUVEEAASHgoaVklEX0xBQkVMSU5HX1NUQVRFX0NSRUFURUQQARIgChxWSURfTEFCRUxJTkdfU1RBVEVfU1VDQ0VFREVEEAISHQoZVklEX0xBQkVMSU5HX1NUQVRFX0ZBSUxFRBADQkgKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIVVmlkTGFiZWxpbmdTdGF0ZVByb3RvUAFiBnByb3RvMwqBAgo2d2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvYmxvYl90eXBlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvcipVCghCbG9iVHlwZRIZChVCTE9CX1RZUEVfVU5TUEVDSUZJRUQQABIWChJCTE9CX1RZUEVfREFZX09OTFkQARIWChJCTE9CX1RZUEVfU05BUFNIT1QQAkJACi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCDUJsb2JUeXBlUHJvdG9QAWIGcHJvdG8z';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadState`,
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineState`,
  `wfa.measurement.internal.edpaggregator.PoolAssignmentState`,
  `wfa.measurement.internal.edpaggregator.RankerState`,
  `wfa.measurement.internal.edpaggregator.VidLabelingState`,
  `wfa.measurement.internal.edpaggregator.BlobType`
);

-- =============================================================================
-- RawImpressionUpload — top-level parent.
-- One row per EDP impression-upload event (the EDP completes uploading raw
-- impressions and writes a "done" blob, which triggers the
-- VidLabelingDispatcher). All downstream pipeline state cascades from here.
-- =============================================================================
CREATE TABLE RawImpressionUpload (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  RawImpressionUploadResourceId STRING(63) NOT NULL,
  CreateRequestId STRING(36),
  DoneBlobUri STRING(MAX) NOT NULL,
  State `wfa.measurement.internal.edpaggregator.RawImpressionUploadState` NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId);

CREATE UNIQUE INDEX RawImpressionUploadByResourceId
  ON RawImpressionUpload(DataProviderResourceId, RawImpressionUploadResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadByCreateRequestId
  ON RawImpressionUpload(DataProviderResourceId, CreateRequestId);

CREATE INDEX RawImpressionUploadByCreateTime
  ON RawImpressionUpload(DataProviderResourceId, CreateTime);

CREATE INDEX RawImpressionUploadByState
  ON RawImpressionUpload(DataProviderResourceId, State, CreateTime);

-- =============================================================================
-- RawImpressionUploadFile — the raw impression files belonging to an upload.
-- =============================================================================
CREATE TABLE RawImpressionUploadFile (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  FileId INT64 NOT NULL,
  FileResourceId STRING(63) NOT NULL,
  CreateRequestId STRING(36),
  BlobUri STRING(MAX) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, FileId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX RawImpressionUploadFileByResourceId
  ON RawImpressionUploadFile(DataProviderResourceId, FileResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadFileByCreateRequestId
  ON RawImpressionUploadFile(
    DataProviderResourceId,
    RawImpressionUploadId,
    CreateRequestId
  );

-- =============================================================================
-- RawImpressionUploadModelLine — per-model-line pipeline state for an upload.
-- One row per (upload, model line), pre-created by the dispatcher once it
-- resolves which model lines apply to this upload.
-- =============================================================================
CREATE TABLE RawImpressionUploadModelLine (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  RawImpressionUploadModelLineId INT64 NOT NULL,
  RawImpressionUploadModelLineResourceId STRING(63) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  CreateRequestId STRING(36),
  State `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineState` NOT NULL,
  ErrorMessage STRING(MAX),
  PoolOffsets ARRAY<INT64>,
  MaxEventDate DATE,
  EncryptedMergedDek BYTES(MAX),
  Etag STRING(36) NOT NULL,
  MarkRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, RawImpressionUploadModelLineId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX RawImpressionUploadModelLineByResourceId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadModelLineResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByCreateRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, CreateRequestId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, MarkRequestId);

CREATE INDEX RawImpressionUploadModelLineByState
  ON RawImpressionUploadModelLine(DataProviderResourceId, State, CreateTime);

-- =============================================================================
-- PoolAssignmentJob — Phase-0 gate.
-- One row per (upload, model line, shard); pre-created by the dispatcher
-- in POOL_ASSIGNMENT_STATE_CREATED state. The "last shard out" of a (upload,
-- model line) flips RawImpressionUploadModelLine state from
-- POOL_ASSIGNING to RANKING and triggers Phase 1.
-- =============================================================================
CREATE TABLE PoolAssignmentJob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  PoolAssignmentJobId INT64 NOT NULL,
  PoolAssignmentJobResourceId STRING(63) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  ShardIndex INT64 NOT NULL,
  State `wfa.measurement.internal.edpaggregator.PoolAssignmentState` NOT NULL,
  EncryptedDek BYTES(MAX),
  Etag STRING(36) NOT NULL,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  MarkRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, PoolAssignmentJobId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX PoolAssignmentJobByResourceId
  ON PoolAssignmentJob(DataProviderResourceId, PoolAssignmentJobResourceId);

CREATE UNIQUE NULL_FILTERED INDEX PoolAssignmentJobByCreateRequestId
  ON PoolAssignmentJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CreateRequestId
  );

CREATE UNIQUE NULL_FILTERED INDEX PoolAssignmentJobByMarkRequestId
  ON PoolAssignmentJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkRequestId
  );

CREATE INDEX PoolAssignmentJobByState
  ON PoolAssignmentJob(DataProviderResourceId, State, CreateTime);

-- =============================================================================
-- RankerJob — Phase-1 gate.
-- One row per (upload, model line, ranker job); pre-created by the Phase-0
-- last-out trigger in RANKER_CREATED state. The "last ranker job out" of a
-- (upload, model line) flips RawImpressionUploadModelLine state from
-- RANKING to LABELING and triggers Phase 2.
-- =============================================================================
CREATE TABLE RankerJob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  RankerJobId INT64 NOT NULL,
  RankerJobResourceId STRING(63) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  PoolOffsets ARRAY<INT64> NOT NULL,
  State `wfa.measurement.internal.edpaggregator.RankerState` NOT NULL,
  Etag STRING(36) NOT NULL,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  MarkRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, RankerJobId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX RankerJobByResourceId
  ON RankerJob(DataProviderResourceId, RankerJobResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RankerJobByCreateRequestId
  ON RankerJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CreateRequestId
  );

CREATE UNIQUE NULL_FILTERED INDEX RankerJobByMarkRequestId
  ON RankerJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkRequestId
  );

CREATE INDEX RankerJobByState
  ON RankerJob(DataProviderResourceId, State, CreateTime);

-- =============================================================================
-- RankIndexBlob — pointer to the rank-index blobs in GCS, with their DEKs.
-- Two rows per (upload, model line, subpool): one DAY_ONLY and one SNAPSHOT.
-- Snapshots are replaced each upload; day-only blobs persist until aged out
-- by the retention policy.
-- =============================================================================
CREATE TABLE RankIndexBlob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  RankIndexBlobId INT64 NOT NULL,
  RankIndexBlobResourceId STRING(63) NOT NULL,
  CreateRequestId STRING(36),
  CmmsModelLine STRING(MAX) NOT NULL,
  BlobType `wfa.measurement.internal.edpaggregator.BlobType` NOT NULL,
  PoolOffset INT64 NOT NULL,
  BlobUri STRING(MAX) NOT NULL,
  EncryptedDek BYTES(MAX) NOT NULL,
  MaxEventDate DATE,
  BlobChecksum BYTES(32),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, RankIndexBlobId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX RankIndexBlobByResourceId
  ON RankIndexBlob(DataProviderResourceId, RankIndexBlobResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RankIndexBlobByCreateRequestId
  ON RankIndexBlob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CreateRequestId
  );

CREATE UNIQUE INDEX RankIndexBlobByNaturalKey
  ON RankIndexBlob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CmmsModelLine,
    BlobType,
    PoolOffset
  );

CREATE INDEX RankIndexBlobByMaxEventDate
  ON RankIndexBlob(DataProviderResourceId, CmmsModelLine, BlobType, MaxEventDate);

CREATE INDEX RankIndexBlobByUploadAndType
  ON RankIndexBlob(DataProviderResourceId, CmmsModelLine, BlobType, RawImpressionUploadId);

-- Enforces the design invariant of at most one row per
-- (upload, model line, subpool, blob type): one DAY_ONLY and one SNAPSHOT per
-- subpool within an upload. Soft-deleted rows are included (Spanner GoogleSQL
-- does not support a WHERE clause on CREATE INDEX), so a natural key cannot be
-- reused until the prior row is hard-deleted.
CREATE UNIQUE INDEX RankIndexBlobByNaturalKey
  ON RankIndexBlob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CmmsModelLine,
    BlobType,
    PoolOffset
  );


-- =============================================================================
-- VidLabelingJob — Phase-2 gate.
-- One row per (upload, model line, file batch); pre-created by the Phase-1
-- last-RankerJob-out when transitioning RawImpressionUploadModelLine from
-- RANKING to LABELING. The "last VidLabelingJob out" of a (upload, model
-- line) flips RawImpressionUploadModelLine state from LABELING to COMPLETED
-- and triggers DataAvailabilitySync.
-- =============================================================================
CREATE TABLE VidLabelingJob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  VidLabelingJobId INT64 NOT NULL,
  VidLabelingJobResourceId STRING(63) NOT NULL,
  CmmsModelLines ARRAY<STRING(MAX)> NOT NULL,
  RawImpressionUploadFiles ARRAY<STRING(MAX)> NOT NULL,
  State `wfa.measurement.internal.edpaggregator.VidLabelingState` NOT NULL,
  Etag STRING(36) NOT NULL,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  MarkRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, VidLabelingJobId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX VidLabelingJobByResourceId
  ON VidLabelingJob(DataProviderResourceId, VidLabelingJobResourceId);

CREATE UNIQUE NULL_FILTERED INDEX VidLabelingJobByCreateRequestId
  ON VidLabelingJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CreateRequestId
  );

CREATE UNIQUE NULL_FILTERED INDEX VidLabelingJobByMarkRequestId
  ON VidLabelingJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkRequestId
  );

CREATE INDEX VidLabelingJobByState
  ON VidLabelingJob(DataProviderResourceId, State, CreateTime);

RUN BATCH;
