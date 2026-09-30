-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset marcopremier:create-data-availability-sync-lease-schema dbms:cloudspanner
-- comment: Persist data-availability synchronization leases.
SET PROTO_DESCRIPTORS = 'CsADCk93ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9kYXRhX2F2YWlsYWJpbGl0eV9zeW5jX2xlYXNlX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvcirkAQoeRGF0YUF2YWlsYWJpbGl0eVN5bmNMZWFzZVN0YXRlEjIKLkRBVEFfQVZBSUxBQklMSVRZX1NZTkNfTEVBU0VfU1RBVEVfVU5TUEVDSUZJRUQQABItCilEQVRBX0FWQUlMQUJJTElUWV9TWU5DX0xFQVNFX1NUQVRFX0FDVElWRRABEi8KK0RBVEFfQVZBSUxBQklMSVRZX1NZTkNfTEVBU0VfU1RBVEVfUkVMRUFTRUQQAhIuCipEQVRBX0FWQUlMQUJJTElUWV9TWU5DX0xFQVNFX1NUQVRFX0VYUElSRUQQA0JWCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCI0RhdGFBdmFpbGFiaWxpdHlTeW5jTGVhc2VTdGF0ZVByb3RvUAFiBnByb3RvMw==';

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState`
);

CREATE TABLE DataAvailabilitySyncLease (
  DataProviderResourceId STRING(63) NOT NULL,
  SynchronizationAttemptId STRING(36) NOT NULL,
  State `wfa.measurement.internal.edpaggregator.DataAvailabilitySyncLeaseState` NOT NULL,
  ExpireTime TIMESTAMP NOT NULL,
  MutationRequestIds ARRAY<STRING(36)> NOT NULL,
  MutationRequestFingerprints ARRAY<BYTES(MAX)> NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, SynchronizationAttemptId);

CREATE INDEX DataAvailabilitySyncLeaseByState
  ON DataAvailabilitySyncLease(DataProviderResourceId, State, ExpireTime);
