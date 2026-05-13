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

-- changeset stevenwarejones:add-update-request-id

START BATCH DDL;

-- Stores the request ID from the most recent Update call for idempotency,
-- scoped separately from CreateRequestId so that Create and Update
-- request IDs do not collide.
ALTER TABLE ImpressionMetadata ADD COLUMN UpdateRequestId STRING(36);

CREATE UNIQUE NULL_FILTERED INDEX ImpressionMetadataByUpdateRequestId
  ON ImpressionMetadata(DataProviderResourceId, UpdateRequestId);

RUN BATCH;
