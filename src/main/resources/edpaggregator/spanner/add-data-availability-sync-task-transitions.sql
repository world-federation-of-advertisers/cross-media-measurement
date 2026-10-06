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

-- changeset marcopremier:19 dbms:cloudspanner
-- comment: Add idempotency keys for availability task state transitions.

SET PROTO_DESCRIPTORS =
'CswECk53ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9kYXRhX2F2YWlsYWJpbGl0eV9zeW5jX3Rhc2tfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKvICCh1EYXRhQXZhaWxhYmlsaXR5U3luY1Rhc2tTdGF0ZRIxCi1EQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfU1RBVEVfVU5TUEVDSUZJRUQQABItCilEQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfU1RBVEVfUEVORElORxABEi0KKURBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19TVEFURV9SVU5OSU5HEAISLworREFUQV9BVkFJTEFCSUxJVFlfU1lOQ19UQVNLX1NUQVRFX1NVQ0NFRURFRBADEiwKKERBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19TVEFURV9GQUlMRUQQBBIwCixEQVRBX0FWQUlMQUJJTElUWV9TWU5DX1RBU0tfU1RBVEVfU1VQRVJTRURFRBAFEi8KK0RBVEFfQVZBSUxBQklMSVRZX1NZTkNfVEFTS19TVEFURV9DQU5DRUxMRUQQBkJVCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCIkRhdGFBdmFpbGFiaWxpdHlTeW5jVGFza1N0YXRlUHJvdG9QAWIGcHJvdG8z';

ALTER PROTO BUNDLE UPDATE (
  `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState`
);

ALTER TABLE DataAvailabilitySyncTask ADD COLUMN MarkRunningRequestId STRING(36);
ALTER TABLE DataAvailabilitySyncTask ADD COLUMN MarkSucceededRequestId STRING(36);
ALTER TABLE DataAvailabilitySyncTask ADD COLUMN MarkFailedRequestId STRING(36);

CREATE UNIQUE NULL_FILTERED INDEX DataAvailabilitySyncTaskByMarkRunningRequestId
  ON DataAvailabilitySyncTask(DataProviderResourceId, MarkRunningRequestId);

CREATE UNIQUE NULL_FILTERED INDEX DataAvailabilitySyncTaskByMarkSucceededRequestId
  ON DataAvailabilitySyncTask(DataProviderResourceId, MarkSucceededRequestId);

CREATE UNIQUE NULL_FILTERED INDEX DataAvailabilitySyncTaskByMarkFailedRequestId
  ON DataAvailabilitySyncTask(DataProviderResourceId, MarkFailedRequestId);
