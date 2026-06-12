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
-- PoolAssignmentJob, RankerJob, and RankIndexBlob.

-- Set protobuf FileDescriptorSet as a base64 string.
SET PROTO_DESCRIPTORS =
'CroDCkh3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl91cGxvYWRfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKusBChhSYXdJbXByZXNzaW9uVXBsb2FkU3RhdGUSKwonUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX1VOU1BFQ0lGSUVEEAASJwojUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX0NSRUFURUQQARImCiJSQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfQUNUSVZFEAISKQolUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX0NPTVBMRVRFRBADEiYKIlJBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9GQUlMRUQQBEJQCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCHVJhd0ltcHJlc3Npb25VcGxvYWRTdGF0ZVByb3RvUAFiBnByb3RvMwr/BApTd2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcmF3X2ltcHJlc3Npb25fdXBsb2FkX21vZGVsX2xpbmVfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKpwDCiFSYXdJbXByZXNzaW9uVXBsb2FkTW9kZWxMaW5lU3RhdGUSNgoyUkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfU1RBVEVfVU5TUEVDSUZJRUQQABIyCi5SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9DUkVBVEVEEAESOQo1UkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfU1RBVEVfUE9PTF9BU1NJR05JTkcQAhIyCi5SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9SQU5LSU5HEAMSMwovUkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfU1RBVEVfTEFCRUxJTkcQBBI0CjBSQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9DT01QTEVURUQQBRIxCi1SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9GQUlMRUQQBkJZCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCJlJhd0ltcHJlc3Npb25VcGxvYWRNb2RlbExpbmVTdGF0ZVByb3RvUAFiBnByb3RvMwrqAgpCd2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcG9vbF9hc3NpZ25tZW50X3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciqmAQoTUG9vbEFzc2lnbm1lbnRTdGF0ZRIlCiFQT09MX0FTU0lHTk1FTlRfU1RBVEVfVU5TUEVDSUZJRUQQABIhCh1QT09MX0FTU0lHTk1FTlRfU1RBVEVfQ1JFQVRFRBABEiMKH1BPT0xfQVNTSUdOTUVOVF9TVEFURV9TVUNDRUVERUQQAhIgChxQT09MX0FTU0lHTk1FTlRfU1RBVEVfRkFJTEVEEANCSwotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhhQb29sQXNzaWdubWVudFN0YXRlUHJvdG9QAWIGcHJvdG8zCqwCCjl3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYW5rZXJfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKnoKC1JhbmtlclN0YXRlEhwKGFJBTktFUl9TVEFURV9VTlNQRUNJRklFRBAAEhgKFFJBTktFUl9TVEFURV9DUkVBVEVEEAESGgoWUkFOS0VSX1NUQVRFX1NVQ0NFRURFRBACEhcKE1JBTktFUl9TVEFURV9GQUlMRUQQA0JDCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCEFJhbmtlclN0YXRlUHJvdG9QAWIGcHJvdG8z';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadState`,
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineState`,
  `wfa.measurement.internal.edpaggregator.PoolAssignmentState`,
  `wfa.measurement.internal.edpaggregator.RankerState`
);

-- =============================================================================
-- RawImpressionUpload — top-level parent.
-- One row per EDP impression-upload event (the EDP completes uploading raw
-- impressions and writes a "done" blob, which triggers the
-- VidLabelingDispatcher). All downstream pipeline state cascades from here.
-- =============================================================================
CREATE TABLE RawImpressionUpload (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId STRING(36) NOT NULL,
  CreateRequestId STRING(36),
  DoneBlobUri STRING(MAX) NOT NULL,
  State `wfa.measurement.internal.edpaggregator.RawImpressionUploadState` NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadByCreateRequestId
  ON RawImpressionUpload(DataProviderResourceId, CreateRequestId);

-- =============================================================================
-- RawImpressionUploadFile — the raw impression files belonging to an upload.
-- =============================================================================
CREATE TABLE RawImpressionUploadFile (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId STRING(36) NOT NULL,
  FileResourceId STRING(36) NOT NULL,
  CreateRequestId STRING(36),
  BlobUri STRING(MAX) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, FileResourceId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadFileByCreateRequestId
  ON RawImpressionUploadFile(DataProviderResourceId, CreateRequestId);

-- =============================================================================
-- RawImpressionUploadModelLine — per-model-line pipeline state for an upload.
-- One row per (upload, model line), pre-created by the dispatcher once it
-- resolves which model lines apply to this upload.
-- =============================================================================
CREATE TABLE RawImpressionUploadModelLine (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId STRING(36) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  CreateRequestId STRING(36),
  State `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineState` NOT NULL,
  ErrorMessage STRING(MAX),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, CmmsModelLine),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByCreateRequestId
  ON RawImpressionUploadModelLine(DataProviderResourceId, RawImpressionUploadId, CreateRequestId);

-- =============================================================================
-- PoolAssignmentJob — Phase-0 gate.
-- One row per (upload, model line, shard); pre-created by the dispatcher
-- in POOL_ASSIGNMENT_CREATED state. The "last shard out" of a (upload,
-- model line) flips RawImpressionUploadModelLine state from
-- POOL_ASSIGNING to RANKING and triggers Phase 1.
-- =============================================================================
CREATE TABLE PoolAssignmentJob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId STRING(36) NOT NULL,
  PoolAssignmentJobResourceId STRING(36) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  ShardIndex INT64 NOT NULL,
  State `wfa.measurement.internal.edpaggregator.PoolAssignmentState` NOT NULL,
  EncryptedDek BYTES(512),
  Etag STRING(36) NOT NULL,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, PoolAssignmentJobResourceId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX PoolAssignmentJobByCreateRequestId
  ON PoolAssignmentJob(DataProviderResourceId, CreateRequestId);

-- =============================================================================
-- RankerJob — Phase-1 gate.
-- One row per (upload, model line, ranker job); pre-created by the Phase-0
-- last-out trigger in RANKER_CREATED state. The "last ranker job out" of a
-- (upload, model line) flips RawImpressionUploadModelLine state from
-- RANKING to LABELING and triggers Phase 2.
-- =============================================================================
CREATE TABLE RankerJob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId STRING(36) NOT NULL,
  RankerJobResourceId STRING(36) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  PoolOffsets ARRAY<INT64> NOT NULL,
  State `wfa.measurement.internal.edpaggregator.RankerState` NOT NULL,
  Etag STRING(36) NOT NULL,
  ErrorMessage STRING(MAX),
  CreateRequestId STRING(36),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, RankerJobResourceId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX RankerJobByCreateRequestId
  ON RankerJob(DataProviderResourceId, CreateRequestId);

-- =============================================================================
-- RankIndexBlob — pointer to the rank-index blobs in GCS, with their DEKs.
-- Two rows per (upload, model line, subpool): one DAY_ONLY and one SNAPSHOT.
-- Snapshots are replaced each upload; day-only blobs persist until aged out
-- by the retention policy.
-- =============================================================================
CREATE TABLE RankIndexBlob (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId STRING(36) NOT NULL,
  RankIndexBlobResourceId STRING(36) NOT NULL,
  CreateRequestId STRING(36),
  CmmsModelLine STRING(MAX) NOT NULL,
  BlobType STRING(16) NOT NULL,
  PoolOffset INT64 NOT NULL,
  BlobUri STRING(MAX) NOT NULL,
  EncryptedDek BYTES(512) NOT NULL,
  MaxEventDate DATE,
  BlobChecksum BYTES(32),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  DeleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RawImpressionUploadId, RankIndexBlobResourceId),
  INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX RankIndexBlobByCreateRequestId
  ON RankIndexBlob(DataProviderResourceId, CreateRequestId);

CREATE INDEX RankIndexBlobByMaxEventDate
  ON RankIndexBlob(DataProviderResourceId, CmmsModelLine, BlobType, MaxEventDate)
  WHERE DeleteTime IS NULL;

CREATE INDEX RankIndexBlobByUploadAndType
  ON RankIndexBlob(DataProviderResourceId, CmmsModelLine, BlobType, RawImpressionUploadId)
  WHERE DeleteTime IS NULL;

RUN BATCH;
