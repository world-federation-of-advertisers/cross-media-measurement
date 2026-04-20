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

-- changeset uakyol:34 dbms:cloudspanner
-- comment: Add entity_key and entity_metadata columns to EventGroups.

START BATCH DDL;

-- EntityType is NOT NULL with a DEFAULT of "campaign" so existing rows are
-- backfilled atomically as part of the ADD COLUMN, and new rows that don't
-- specify a value get the same default. Matches the public-API
-- ListEventGroups behavior where an unset entity_type_in filter is treated
-- as ["campaign"].
ALTER TABLE EventGroups
  ADD COLUMN EntityType STRING(MAX) NOT NULL DEFAULT ("campaign");

-- EntityId is NULL for existing rows and for new rows created without an
-- entity_key. The UNIQUE NULL_FILTERED index below exempts these rows from
-- the uniqueness constraint.
ALTER TABLE EventGroups
  ADD COLUMN EntityId STRING(MAX);

-- EntityMetadata holds a serialized google.protobuf.Struct. Free-form per
-- (DataProvider, entity_type) schema; not indexed.
ALTER TABLE EventGroups
  ADD COLUMN EntityMetadata BYTES(MAX);

-- Supports the entity_type_in filter in ListEventGroups.
CREATE INDEX EventGroupsByEntityType
  ON EventGroups(DataProviderId, MeasurementConsumerId, EntityType);

-- Enforces uniqueness of entity_key per (DataProvider, MeasurementConsumer)
-- as required by cross-media-measurement-api PR #275. NULL_FILTERED skips
-- rows where EntityId is NULL (legacy EventGroups and those without an
-- entity_key), exempting them from the uniqueness constraint.
CREATE UNIQUE NULL_FILTERED INDEX EventGroupsByEntityKey
  ON EventGroups(DataProviderId, MeasurementConsumerId, EntityType, EntityId);

RUN BATCH;
