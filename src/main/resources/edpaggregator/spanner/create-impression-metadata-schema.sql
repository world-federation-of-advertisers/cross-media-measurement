-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
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

-- changeset lindreamdeyi:2 dbms:cloudspanner

-- Cloud Spanner database schema for the EDP Aggregator.

-- Set protobuf FileDescriptorSet as a base64 string.
SET PROTO_DESCRIPTORS =
'Ct0CCkZ3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9pbXByZXNzaW9uX21ldGFkYXRhX3N0YXRlLnByb3RvEiZ3ZmEubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvciqRAQoXSW1wcmVzc2lvbk1ldGFkYXRhU3RhdGUSKQolSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9VTlNQRUNJRklFRBAAEiQKIElNUFJFU1NJT05fTUVUQURBVEFfU1RBVEVfQUNUSVZFEAESJQohSU1QUkVTU0lPTl9NRVRBREFUQV9TVEFURV9ERUxFVEVEEAJCTwotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQhxJbXByZXNzaW9uTWV0YWRhdGFTdGF0ZVByb3RvUAFiBnByb3RvMwrfAwpHd2ZhL21lYXN1cmVtZW50L2ludGVybmFsL2VkcGFnZ3JlZ2F0b3IvcmVxdWlzaXRpb25fbWV0YWRhdGFfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKpECChhSZXF1aXNpdGlvbk1ldGFkYXRhU3RhdGUSKgomUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfVU5TUEVDSUZJRUQQABIlCiFSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9TVE9SRUQQARIlCiFSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9RVUVVRUQQAhIpCiVSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9QUk9DRVNTSU5HEAMSKAokUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfRlVMRklMTEVEEAQSJgoiUkVRVUlTSVRJT05fTUVUQURBVEFfU1RBVEVfUkVGVVNFRBAFQlAKLW9yZy53ZmFuZXQubWVhc3VyZW1lbnQuaW50ZXJuYWwuZWRwYWdncmVnYXRvckIdUmVxdWlzaXRpb25NZXRhZGF0YVN0YXRlUHJvdG9QAWIGcHJvdG8z';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.ImpressionMetadataState`,
);

CREATE TABLE ImpressionMetadata (
  -- The globally unique resource ID of the DataProvider that owns this Impression.
  DataProviderResourceId STRING(63) NOT NULL,
  -- Internal, system-generated ID of the ImpressionMetadata, unique per provider.
  ImpressionMetadataId INT64 NOT NULL,
  -- The resource ID of this ImpressionMetadata, unique per DataProvider.
  ImpressionMetadataResourceId STRING(63) NOT NULL,
  -- The request ID from the creation request, used for idempotency. Optional.
  -- Must be a UUID formatted as a 36-character string.
  CreateRequestId STRING(36),
  -- The URI of the encrypted data blob.
  BlobUri STRING(MAX) NOT NULL,
  -- The URL of the encrypted data bloe type.
  BlobTypeUrl STRING(MAX) NOT NULL,
  -- The reference resource ID of the EventGroup.
  EventGroupReferenceId STRING(MAX) NOT NULL,
  -- The resource name of the ModelLine in the CMMS (Kingdom).
  CmmsModelLine STRING(MAX) NOT NULL,
  -- The start of the time interval that the impressions in blob covers.
  IntervalStartTime TIMESTAMP NOT NULL,
  -- The end of the time interval that the impressions in blob covers (exclusive).
  IntervalEndTime TIMESTAMP NOT NULL,
  -- The current state of the Impression.
  State `wfa.measurement.internal.edpaggregator.ImpressionMetadataState` NOT NULL,
  -- The time this resource was created in this database.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- The time this resource was last updated in this database.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, ImpressionMetadataId);

-- Enforces uniqueness of ImpressionMetadataResourceId per DataProvider and supports
-- fast lookups by this key.
CREATE UNIQUE INDEX ImpressionMetadataByResourceId
  ON ImpressionMetadata(DataProviderResourceId, ImpressionMetadataResourceId);

-- Enforces uniqueness for idempotency on creation. This is a null-filtered index
-- as CreateRequestId is optional.
CREATE UNIQUE NULL_FILTERED INDEX ImpressionMetadataByCreateRequestId
  ON ImpressionMetadata(DataProviderResourceId, CreateRequestId);

-- Enforces that BlobUri is unique per DataProvider. This also enables fast
-- lookups by this key.
CREATE UNIQUE INDEX ImpressionMetadataByBlobUri
  ON ImpressionMetadata(DataProviderResourceId, BlobUri);

-- Index for finding ImpressionMetadata using various list filters and pagination
CREATE INDEX ImpressionMetadataByListFilterAndPagination
  ON ImpressionMetadata(
    DataProviderResourceId,
    CmmsModelLine,
    EventGroupReferenceId,
    State,
    IntervalStartTime,
    IntervalEndTime,
    CreateTime,
    ImpressionMetadataResourceId
  );

RUN BATCH;