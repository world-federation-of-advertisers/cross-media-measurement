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
-- comment: Add ImpressionMetadataMetaEntityKeys interleaved table to support filtering by Meta entity keys on ImpressionMetadata.

START BATCH DDL;

-- Stores Meta entity keys (type, id) that the impressions in a parent
-- ImpressionMetadata blob are associated with. Enables filtering metadata by
-- Meta entity keys at list time without joining against EventGroup metadata.
--
-- One child table per EDP: when a new EDP is added, create a sibling table
-- (e.g. ImpressionMetadataGoogleEntityKeys). The proto-level oneof in
-- `EntityKey.key` is the source of truth for which EDPs are supported.
CREATE TABLE ImpressionMetadataMetaEntityKeys (
  -- The resource ID of the DataProvider that owns the parent ImpressionMetadata.
  DataProviderResourceId STRING(63) NOT NULL,
  -- The internal ID of the parent ImpressionMetadata.
  ImpressionMetadataId INT64 NOT NULL,
  -- Stores the wfa.measurement.internal.edpaggregator.MetaEntityKey.Type
  -- proto enum's numeric value. Conversion is handled at the DAO layer.
  -- (We use INT64 rather than a Spanner proto-typed column to avoid having
  -- to amend the proto bundle; revisit if downstream queries need
  -- typed-enum guarantees.)
  MetaType INT64 NOT NULL,
  -- ID of the entity in Meta's system. URL-safe string.
  EntityId STRING(MAX) NOT NULL,
) PRIMARY KEY (DataProviderResourceId, ImpressionMetadataId, MetaType, EntityId),
  INTERLEAVE IN PARENT ImpressionMetadata ON DELETE CASCADE;

-- Index for finding ImpressionMetadata by Meta entity key, used for List
-- filtering: given a (MetaType, EntityId) pair, find all parent
-- ImpressionMetadata blobs that include it.
CREATE INDEX ImpressionMetadataMetaEntityKeysByTypeAndId
  ON ImpressionMetadataMetaEntityKeys(DataProviderResourceId, MetaType, EntityId);

RUN BATCH;
