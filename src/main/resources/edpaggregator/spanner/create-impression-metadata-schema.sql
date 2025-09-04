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

-- changeset renjiezh:1 dbms:cloudspanner

-- Cloud Spanner database schema for the EDP Aggregator.

-- Set protobuf FileDescriptorSet as a base64 string.
SET PROTO_DESCRIPTORS =
'Ct8DCkd3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yZXF1aXNpdGlvbl9tZXRhZGF0YV9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3IqkQIKGFJlcXVpc2l0aW9uTWV0YWRhdGFTdGF0ZRIqCiZSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9VTlNQRUNJRklFRBAAEiUKIVJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1NUT1JFRBABEiUKIVJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1FVRVVFRBACEikKJVJFUVVJU0lUSU9OX01FVEFEQVRBX1NUQVRFX1BST0NFU1NJTkcQAxIoCiRSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9GVUxGSUxMRUQQBBImCiJSRVFVSVNJVElPTl9NRVRBREFUQV9TVEFURV9SRUZVU0VEEAVCUAotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQh1SZXF1aXNpdGlvbk1ldGFkYXRhU3RhdGVQcm90b1ABYgZwcm90bzM='

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
  EventGroupReferenceId STRING(63) NOT NULL,
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
  -- A sharding key for indexes to prevent hotspotting.
  ImpressionMetadataIndexShardId INT64 NOT NULL AS
    (ABS(MOD(ImpressionMetadataId, 64))) STORED,
) PRIMARY KEY (DataProviderResourceId, ImpressionMetadataId);