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
'CoECCjZ3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9ibG9iX3R5cGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKlUKCEJsb2JUeXBlEhkKFUJMT0JfVFlQRV9VTlNQRUNJRklFRBAAEhYKEkJMT0JfVFlQRV9EQVlfT05MWRABEhYKEkJMT0JfVFlQRV9TTkFQU0hPVBACQkAKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckINQmxvYlR5cGVQcm90b1ABYgZwcm90bzMKzgMKOndmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL2VuY3J5cHRlZF9kZWsucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yIpkCCgxFbmNyeXB0ZWREZWsSFwoHa2VrX3VyaRgBIAEoCVIGa2VrVXJpEhkKCHR5cGVfdXJsGAIgASgJUgd0eXBlVXJsEmwKD3Byb3RvYnVmX2Zvcm1hdBgDIAEoDjJDLndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yLkVuY3J5cHRlZERlay5Qcm90b2J1ZkZvcm1hdFIOcHJvdG9idWZGb3JtYXQSHgoKY2lwaGVydGV4dBgEIAEoDFIKY2lwaGVydGV4dCJHCg5Qcm90b2J1ZkZvcm1hdBIfChtQUk9UT0JVRl9GT1JNQVRfVU5TUEVDSUZJRUQQABIKCgZCSU5BUlkQARIICgRKU09OEAJCRAotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhFFbmNyeXB0ZWREZWtQcm90b1ABYgZwcm90bzMK3QIKRndmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL2ltcHJlc3Npb25fbWV0YWRhdGFfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKpEBChdJbXByZXNzaW9uTWV0YWRhdGFTdGF0ZRIpCiVJTVBSRVNTSU9OX01FVEFEQVRBX1NUQVRFX1VOU1BFQ0lGSUVEEAASJAogSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9BQ1RJVkUQARIlCiFJTVBSRVNTSU9OX01FVEFEQVRBX1NUQVRFX0RFTEVURUQQAkJPCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCHEltcHJlc3Npb25NZXRhZGF0YVN0YXRlUHJvdG9QAWIGcHJvdG8zCuoCCkJ3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9wb29sX2Fzc2lnbm1lbnRfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKqYBChNQb29sQXNzaWdubWVudFN0YXRlEiUKIVBPT0xfQVNTSUdOTUVOVF9TVEFURV9VTlNQRUNJRklFRBAAEiEKHVBPT0xfQVNTSUdOTUVOVF9TVEFURV9DUkVBVEVEEAESIwofUE9PTF9BU1NJR05NRU5UX1NUQVRFX1NVQ0NFRURFRBACEiAKHFBPT0xfQVNTSUdOTUVOVF9TVEFURV9GQUlMRUQQA0JLCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCGFBvb2xBc3NpZ25tZW50U3RhdGVQcm90b1ABYgZwcm90bzMKrAIKOXdmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3Jhbmtlcl9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqegoLUmFua2VyU3RhdGUSHAoYUkFOS0VSX1NUQVRFX1VOU1BFQ0lGSUVEEAASGAoUUkFOS0VSX1NUQVRFX0NSRUFURUQQARIaChZSQU5LRVJfU1RBVEVfU1VDQ0VFREVEEAISFwoTUkFOS0VSX1NUQVRFX0ZBSUxFRBADQkMKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIQUmFua2VyU3RhdGVQcm90b1ABYgZwcm90bzMKiwMKR3dmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3Jhd19pbXByZXNzaW9uX2JhdGNoX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciq+AQoXUmF3SW1wcmVzc2lvbkJhdGNoU3RhdGUSKgomUkFXX0lNUFJFU1NJT05fQkFUQ0hfU1RBVEVfVU5TUEVDSUZJRUQQABImCiJSQVdfSU1QUkVTU0lPTl9CQVRDSF9TVEFURV9DUkVBVEVEEAESKAokUkFXX0lNUFJFU1NJT05fQkFUQ0hfU1RBVEVfUFJPQ0VTU0VEEAISJQohUkFXX0lNUFJFU1NJT05fQkFUQ0hfU1RBVEVfRkFJTEVEEANCTwotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhxSYXdJbXByZXNzaW9uQmF0Y2hTdGF0ZVByb3RvUAFiBnByb3RvMwr/BApTd2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcmF3X2ltcHJlc3Npb25fdXBsb2FkX21vZGVsX2xpbmVfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKpwDCiFSYXdJbXByZXNzaW9uVXBsb2FkTW9kZWxMaW5lU3RhdGUSNgoyUkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfU1RBVEVfVU5TUEVDSUZJRUQQABIyCi5SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9DUkVBVEVEEAESOQo1UkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfU1RBVEVfUE9PTF9BU1NJR05JTkcQAhIyCi5SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9SQU5LSU5HEAMSMwovUkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfU1RBVEVfTEFCRUxJTkcQBBI0CjBSQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9DT01QTEVURUQQBRIxCi1SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9GQUlMRUQQBkJZCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCJlJhd0ltcHJlc3Npb25VcGxvYWRNb2RlbExpbmVTdGF0ZVByb3RvUAFiBnByb3RvMwq6AwpId2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcmF3X2ltcHJlc3Npb25fdXBsb2FkX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvcirrAQoYUmF3SW1wcmVzc2lvblVwbG9hZFN0YXRlEisKJ1JBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9VTlNQRUNJRklFRBAAEicKI1JBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9DUkVBVEVEEAESJgoiUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX0FDVElWRRACEikKJVJBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9DT01QTEVURUQQAxImCiJSQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfRkFJTEVEEARCUAotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQh1SYXdJbXByZXNzaW9uVXBsb2FkU3RhdGVQcm90b1ABYgZwcm90bzMKiQQKR3dmYS9tZWFzdXJlbWVudC9pbnRlcm5hbC9lZHBhZ2dyZWdhdG9yL3JlcXVpc2l0aW9uX21ldGFkYXRhX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciq7AgoYUmVxdWlzaXRpb25NZXRhZGF0YVN0YXRlEioKJlJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1VOU1BFQ0lGSUVEEAASJQohUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfU1RPUkVEEAESJQohUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfUVVFVUVEEAISKQolUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfUFJPQ0VTU0lORxADEigKJFJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX0ZVTEZJTExFRBAEEiYKIlJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1JFRlVTRUQQBRIoCiRSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9XSVRIRFJBV04QBkJQCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCHVJlcXVpc2l0aW9uTWV0YWRhdGFTdGF0ZVByb3RvUAFiBnByb3RvMwrVAgo/d2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvdmlkX2xhYmVsaW5nX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciqXAQoQVmlkTGFiZWxpbmdTdGF0ZRIiCh5WSURfTEFCRUxJTkdfU1RBVEVfVU5TUEVDSUZJRUQQABIeChpWSURfTEFCRUxJTkdfU1RBVEVfQ1JFQVRFRBABEiAKHFZJRF9MQUJFTElOR19TVEFURV9TVUNDRUVERUQQAhIdChlWSURfTEFCRUxJTkdfU1RBVEVfRkFJTEVEEANCSAotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhVWaWRMYWJlbGluZ1N0YXRlUHJvdG9QAWIGcHJvdG8z';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadState`,
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineState`,
  `wfa.measurement.internal.edpaggregator.PoolAssignmentState`,
  `wfa.measurement.internal.edpaggregator.RankerState`,
  `wfa.measurement.internal.edpaggregator.VidLabelingState`,
  `wfa.measurement.internal.edpaggregator.BlobType`,
  `wfa.measurement.internal.edpaggregator.EncryptedDek`
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
  ON RawImpressionUpload(DataProviderResourceId, CreateTime, RawImpressionUploadId);

CREATE INDEX RawImpressionUploadByState
  ON RawImpressionUpload(DataProviderResourceId, State, CreateTime, RawImpressionUploadId);

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
  SizeBytes INT64 NOT NULL,
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

CREATE UNIQUE INDEX RawImpressionUploadFileByBlobUri
  ON RawImpressionUploadFile(DataProviderResourceId, BlobUri);

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
  EncryptedMergedDek `wfa.measurement.internal.edpaggregator.EncryptedDek`,
  MarkPoolAssigningRequestId STRING(36),
  MarkRankingRequestId STRING(36),
  MarkLabelingRequestId STRING(36),
  MarkCompletedRequestId STRING(36),
  MarkFailedRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, RawImpressionUploadModelLineId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE INDEX RawImpressionUploadModelLineByResourceId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadModelLineResourceId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByCreateRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, CreateRequestId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkPoolAssigningRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, MarkPoolAssigningRequestId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkRankingRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, MarkRankingRequestId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkLabelingRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, MarkLabelingRequestId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkCompletedRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, MarkCompletedRequestId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkFailedRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, MarkFailedRequestId);

CREATE INDEX RawImpressionUploadModelLineByState
  ON RawImpressionUploadModelLine(DataProviderResourceId, State, CreateTime, RawImpressionUploadModelLineId);

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
  EncryptedDek `wfa.measurement.internal.edpaggregator.EncryptedDek`,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  MarkSucceededRequestId STRING(36),
  MarkFailedRequestId STRING(36),
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

CREATE UNIQUE NULL_FILTERED INDEX PoolAssignmentJobByMarkSucceededRequestId
  ON PoolAssignmentJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkSucceededRequestId
  );

CREATE UNIQUE NULL_FILTERED INDEX PoolAssignmentJobByMarkFailedRequestId
  ON PoolAssignmentJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkFailedRequestId
  );

CREATE INDEX PoolAssignmentJobByState
  ON PoolAssignmentJob(DataProviderResourceId, State, CreateTime, PoolAssignmentJobId);

CREATE UNIQUE INDEX PoolAssignmentJobByModelLineShard
  ON PoolAssignmentJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CmmsModelLine,
    ShardIndex
  ) STORING (State);

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
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  MarkSucceededRequestId STRING(36),
  MarkFailedRequestId STRING(36),
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

CREATE UNIQUE NULL_FILTERED INDEX RankerJobByMarkSucceededRequestId
  ON RankerJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkSucceededRequestId
  );

CREATE UNIQUE NULL_FILTERED INDEX RankerJobByMarkFailedRequestId
  ON RankerJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkFailedRequestId
  );

CREATE INDEX RankerJobByState
  ON RankerJob(DataProviderResourceId, State, CreateTime, RankerJobId);

CREATE INDEX RankerJobByModelLine
  ON RankerJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    CmmsModelLine
  ) STORING (State);

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
  EncryptedDek `wfa.measurement.internal.edpaggregator.EncryptedDek` NOT NULL,
  MaxEventDate DATE NOT NULL,
  -- SHA-256 hash of the blob's plaintext; validated by readers per design doc
  -- (Lifecycle Management § Corrupted cumulative blob).
  BlobChecksum BYTES(32) NOT NULL,
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

CREATE UNIQUE INDEX RankIndexBlobByBlobUri
  ON RankIndexBlob(DataProviderResourceId, BlobUri);


-- =============================================================================
-- VidLabelingJob — Phase-2 gate.
-- One row per (upload, file batch). Each job labels a batch of files
-- (RawImpressionUploadFiles) for one or more model lines (CmmsModelLines):
--   - When the model lines do NOT require memoization, a single job can batch
--     multiple files AND multiple model lines together (CmmsModelLines holds
--     the set).
--   - When a model line DOES require memoization, the job covers exactly one
--     model line (CmmsModelLines has a single entry) but still multiple files.
-- Pre-created by the Phase-1 last-RankerJob-out when transitioning
-- RawImpressionUploadModelLine from RANKING to LABELING.
-- A model line transitions LABELING -> COMPLETED once every VidLabelingJob
-- whose CmmsModelLines contains it has succeeded; the job that completes the
-- last such model line triggers DataAvailabilitySync.
-- =============================================================================
CREATE TABLE VidLabelingJob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  VidLabelingJobId INT64 NOT NULL,
  VidLabelingJobResourceId STRING(63) NOT NULL,
  CmmsModelLines ARRAY<STRING(MAX)> NOT NULL,
  RawImpressionUploadFiles ARRAY<STRING(MAX)> NOT NULL,
  State `wfa.measurement.internal.edpaggregator.VidLabelingState` NOT NULL,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  MarkSucceededRequestId STRING(36),
  MarkFailedRequestId STRING(36),
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

CREATE UNIQUE NULL_FILTERED INDEX VidLabelingJobByMarkSucceededRequestId
  ON VidLabelingJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkSucceededRequestId
  );

CREATE UNIQUE NULL_FILTERED INDEX VidLabelingJobByMarkFailedRequestId
  ON VidLabelingJob(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkFailedRequestId
  );

CREATE INDEX VidLabelingJobByState
  ON VidLabelingJob(DataProviderResourceId, State, CreateTime, VidLabelingJobId);

RUN BATCH;
