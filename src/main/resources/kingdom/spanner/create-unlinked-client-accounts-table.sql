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

-- changeset getina:36 dbms:cloudspanner
-- comment: Create UnlinkedClientAccounts table for EventGroupSync unlinked account tracking.

START BATCH DDL;

-- UnlinkedClientAccounts tracks advertiser client-account reference IDs that
-- EventGroupSync could not resolve to any MeasurementConsumer.
--
-- This is a standalone lookup table (not interleaved) scoped to a DataProvider
-- and keyed by the DataProvider and the ClientAccountReferenceId.
CREATE TABLE UnlinkedClientAccounts (
  -- Internal FK to DataProviders table.
  DataProviderId INT64 NOT NULL,

  -- Reference ID for the account in the DataProvider ecosystem.
  ClientAccountReferenceId STRING(36) NOT NULL,

  -- Free-form metadata observed for this client account. Display hint only.
  -- Holds a google.protobuf.Struct; the type is registered in the proto bundle
  -- by add-event-group-entity-key.sql.
  EntityMetadata `google.protobuf.Struct`,

  -- One EventGroup observed for this client account, for traceability.
  -- Exactly one of EventGroupReferenceId or the EventGroupEntityKey* pair is
  -- populated per row.
  --
  -- Legacy EventGroup reference ID.
  EventGroupReferenceId STRING(MAX),

  -- Entity type of an observed EventGroup, set with EventGroupEntityKeyId.
  EventGroupEntityKeyType STRING(MAX),

  -- Entity ID of an observed EventGroup, set with EventGroupEntityKeyType.
  EventGroupEntityKeyId STRING(MAX),

  -- The time this client account was created.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  FOREIGN KEY (DataProviderId) REFERENCES DataProviders(DataProviderId),
) PRIMARY KEY (DataProviderId, ClientAccountReferenceId);

RUN BATCH;
