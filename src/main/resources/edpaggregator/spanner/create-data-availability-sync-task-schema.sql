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

-- changeset marcopremier:17 dbms:cloudspanner
-- comment: Persist deterministic data-availability synchronization tasks.

SET PROTO_DESCRIPTORS =
'CukDCk53ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9kYXRhX2F2YWlsYWJpbGl0eV9zeW5jX3Rhc2tfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKo8CCh1EYXRhQXZhaWxhYmlsaXR5U3luY1Rhc2tTdGF0ZRIxCi1EQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfU1RBVEVfVU5TUEVDSUZJRUQQABItCilEQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfU1RBVEVfUEVORElORxABEi0KKURBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19TVEFURV9SVU5OSU5HEAISLworREFUQV9BVkFJTEFCSUxJVFlfU1lOQ19UQVNLX1NUQVRFX1NVQ0NFRURFRBADEiwKKERBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19TVEFURV9GQUlMRUQQBEJVCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCIkRhdGFBdmFpbGFiaWxpdHlTeW5jVGFza1N0YXRlUHJvdG9QAWIGcHJvdG8zCtsFCll3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9kYXRhX2F2YWlsYWJpbGl0eV9zeW5jX3Rhc2tfZmFpbHVyZV9jYXRlZ29yeS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3Iq7AMKJ0RhdGFBdmFpbGFiaWxpdHlTeW5jVGFza0ZhaWx1cmVDYXRlZ29yeRI8CjhEQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfRkFJTFVSRV9DQVRFR09SWV9VTlNQRUNJRklFRBAAEjwKOERBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19GQUlMVVJFX0NBVEVHT1JZX1BVQkxJQ0FUSU9OEAESQAo8REFUQV9BVkFJTEFCSUxJVFlfU1lOQ19UQVNLX0ZBSUxVUkVfQ0FURUdPUllfU1lOQ0hST05JWkFUSU9OEAISRQpBREFUQV9BVkFJTEFCSUxJVFlfU1lOQ19UQVNLX0ZBSUxVUkVfQ0FURUdPUllfTUVUQURBVEFfUEVSU0lTVEVOQ0UQAxI7CjdEQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfRkFJTFVSRV9DQVRFR09SWV9HQVBfUE9MSUNZEAQSRApAREFUQV9BVkFJTEFCSUxJVFlfU1lOQ19UQVNLX0ZBSUxVUkVfQ0FURUdPUllfS0lOR0RPTV9QVUJMSUNBVElPThAFEjkKNURBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19GQUlMVVJFX0NBVEVHT1JZX0lOVEVSTkFMEAZCXwotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQixEYXRhQXZhaWxhYmlsaXR5U3luY1Rhc2tGYWlsdXJlQ2F0ZWdvcnlQcm90b1ABYgZwcm90bzM=';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState`,
  `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory`
);

CREATE TABLE DataAvailabilitySyncTask (
  DataProviderResourceId STRING(63) NOT NULL,
  RawImpressionUploadId INT64 NOT NULL,
  DataAvailabilitySyncTaskResourceId STRING(63) NOT NULL,
  CreateRequestId STRING(36),
  State `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState` NOT NULL,
  DoneBlobUri STRING(MAX) NOT NULL,
  DoneBlobPathHash STRING(64) NOT NULL,
  DoneBlobGeneration INT64 NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  EventDate DATE NOT NULL,
  Traceparent STRING(55),
  Tracestate STRING(512),
  AttemptCount INT64 NOT NULL,
  FailureCategory `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncTaskFailureCategory` NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (
  DataProviderResourceId,
  RawImpressionUploadId,
  DataAvailabilitySyncTaskResourceId
), INTERLEAVE IN PARENT RawImpressionUpload ON DELETE CASCADE;

CREATE UNIQUE NULL_FILTERED INDEX DataAvailabilitySyncTaskByCreateRequestId
  ON DataAvailabilitySyncTask(
    DataProviderResourceId,
    RawImpressionUploadId,
    CreateRequestId
  );

CREATE UNIQUE INDEX DataAvailabilitySyncTaskByDoneObject
  ON DataAvailabilitySyncTask(
    DataProviderResourceId,
    DoneBlobPathHash,
    DoneBlobGeneration
  );

CREATE INDEX DataAvailabilitySyncTaskByState
  ON DataAvailabilitySyncTask(
    DataProviderResourceId,
    State,
    CreateTime,
    DataAvailabilitySyncTaskResourceId
  );

RUN BATCH;
