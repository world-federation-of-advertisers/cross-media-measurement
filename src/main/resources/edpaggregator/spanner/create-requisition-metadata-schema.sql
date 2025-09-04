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
  `wfa.measurement.internal.edpaggregator.RequisitionMetadataState`,
);

CREATE TABLE RequisitionMetadata (
  -- The globally unique resource ID of the DataProvider that owns this requisition.
  DataProviderResourceId STRING(63) NOT NULL,
  -- Internal, system-generated ID of the RequisitionMetadata, unique per provider.
  RequisitionMetadataId INT64 NOT NULL,
  -- The resource ID of this RequisitionMetadata, unique per DataProvider.
  RequisitionMetadataResourceId STRING(63) NOT NULL,
  -- The request ID from the creation request, used for idempotency. Optional.
  -- Must be a UUID formatted as a 36-character string.
  CreateRequestId STRING(36),
  -- The resource name of the requisition in the CMMS (Kingdom).
  CmmsRequisition STRING(MAX) NOT NULL,
  -- The URI of the encrypted data blob.
  BlobUri STRING(MAX) NOT NULL,
  -- The URL of the encrypted data bloe type.
  BlobTypeUrl STRING(MAX) NOT NULL,
  -- An identifier for a group of related requisitions.
  GroupId STRING(MAX) NOT NULL,
  -- The creation time of the requisition in the CMMS.
  CmmsCreateTime TIMESTAMP NOT NULL,
  -- The resource name of the Report this requisition is for.
  Report STRING(MAX) NOT NULL,
  -- The current state of the requisition.
  State `wfa.measurement.internal.edpaggregator.RequisitionMetadataState` NOT NULL,
  -- The resource name of the WorkItem associated with this requisition.
  -- This is only set when the requisition is in the QUEUED state.
  WorkItem STRING(MAX),
  -- The time this resource was created in this database.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- The time this resource was last updated in this database.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- A human-readable message explaining the reason for refusal.
  RefusalMessage STRING(MAX),
  -- A sharding key for indexes to prevent hotspotting.
  RequisitionMetadataIndexShardId INT64 NOT NULL AS
    (ABS(MOD(RequisitionMetadataId, 64))) STORED,
) PRIMARY KEY (DataProviderResourceId, RequisitionMetadataId);

-- Index for looking up by resource ID, unique per DataProvider.
CREATE UNIQUE INDEX RequisitionMetadataByResourceId
  ON RequisitionMetadata(DataProviderResourceId, RequisitionMetadataResourceId);

-- Index for idempotency check on creation.
CREATE UNIQUE INDEX RequisitionMetadataByCreateRequestId
  ON RequisitionMetadata(DataProviderResourceId, CreateRequestId);

-- Index for looking up by CMMS requisition.
CREATE UNIQUE INDEX RequisitionMetadataByCmmsRequisition
  ON RequisitionMetadata(DataProviderResourceId, CmmsRequisition);

-- Index for looking up by blob URI.
CREATE UNIQUE INDEX RequisitionMetadataByBlobUri
  ON RequisitionMetadata(DataProviderResourceId, BlobUri);

-- Index for listing by state for a single DataProvider.
CREATE INDEX RequisitionMetadataByState
  ON RequisitionMetadata(DataProviderResourceId, State, RequisitionMetadataIndexShardId, UpdateTime, RequisitionMetadataId);

-- Index for list by group ID for a single DataProvider.
CREATE INDEX RequisitionMetadataByGroupId
  ON RequisitionMetadata(DataProviderResourceId, GroupId, RequisitionMetadataIndexShardId, UpdateTime, RequisitionMetadataId);

-- Index for fetching the latest CmmsCreateTime for a DataProvider.
CREATE INDEX RequisitionMetadataByCmmsCreateTime
  ON RequisitionMetadata(DataProviderResourceId, RequisitionMetadataIndexShardId, CmmsCreateTime DESC);

-- Index for listing for a single DataProvider with pagination.
CREATE INDEX RequisitionMetadataByUpdateTime
  ON RequisitionMetadata(DataProviderResourceId, RequisitionMetadataIndexShardId, UpdateTime, RequisitionMetadataId);

-- Stores the history of actions taken on a RequisitionMetadata entry.
CREATE TABLE RequisitionMetadataActions (
  -- The resource ID of the DataProvider that owns this requisition.
  DataProviderResourceId STRING(MAX) NOT NULL,
  -- The internal ID of the parent RequisitionMetadata.
  RequisitionMetadataId INT64 NOT NULL,
  -- A unique ID for the action taken on the RequisitionMetadata.
  ActionId INT64 NOT NULL,
  -- The time the action record was created.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- The state of the RequisitionMetadata before this action.
  PreviousState `wfa.measurement.internal.edpaggregator.RequisitionMetadataState` NOT NULL,
  -- The state of the RequisitionMetadata after this action.
  CurrentState `wfa.measurement.internal.edpaggregator.RequisitionMetadataState` NOT NULL,
) PRIMARY KEY (DataProviderResourceId, RequisitionMetadataId, ActionId),
  INTERLEAVE IN PARENT RequisitionMetadata ON DELETE CASCADE;

-- Index for listing actions for a given requisition, ordered by time.
CREATE INDEX RequisitionMetadataActionsByCreateTime
  ON RequisitionMetadataActions(DataProviderResourceId, RequisitionMetadataId, CreateTime, ActionId);

-- Index for listing actions filtered by PreviousState.
CREATE INDEX RequisitionMetadataActionsByPreviousState
  ON RequisitionMetadataActions(DataProviderResourceId, RequisitionMetadataId, PreviousState, CreateTime, ActionId);

-- Index for listing actions filtered by CurrentState.
CREATE INDEX RequisitionMetadataActionsByCurrentState
  ON RequisitionMetadataActions(DataProviderResourceId, RequisitionMetadataId, CurrentState, CreateTime, ActionId);

RUN BATCH;
