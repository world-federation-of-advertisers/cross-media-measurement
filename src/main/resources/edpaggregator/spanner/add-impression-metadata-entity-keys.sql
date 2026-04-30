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

-- changeset marcopremier:9 dbms:cloudspanner
-- comment: Add ImpressionMetadataEntityKeys interleaved table for entity-key filtering on ImpressionMetadata.

START BATCH DDL;

-- Stores entity keys (entity_type, entity_id) that the impressions in a parent
-- ImpressionMetadata blob are associated with. Enables filtering metadata by
-- entity keys at list time without joining against EventGroup metadata.
CREATE TABLE ImpressionMetadataEntityKeys (
  -- The resource ID of the DataProvider that owns the parent ImpressionMetadata.
  DataProviderResourceId STRING(63) NOT NULL,
  -- The internal ID of the parent ImpressionMetadata.
  ImpressionMetadataId INT64 NOT NULL,
  -- Type of the entity in the DataProvider's system. URL-safe string.
  EntityType STRING(MAX) NOT NULL,
  -- ID of the entity in the DataProvider's system. URL-safe string.
  EntityId STRING(MAX) NOT NULL,
) PRIMARY KEY (DataProviderResourceId, ImpressionMetadataId, EntityType, EntityId),
  INTERLEAVE IN PARENT ImpressionMetadata ON DELETE CASCADE;

-- Index for finding ImpressionMetadata by entity key, used for List filtering.
CREATE INDEX ImpressionMetadataEntityKeysByEntity
  ON ImpressionMetadataEntityKeys(DataProviderResourceId, EntityType, EntityId);

RUN BATCH;
