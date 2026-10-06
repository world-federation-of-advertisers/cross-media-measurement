-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset marcopremier:20 dbms:cloudspanner
-- comment: Add optimistic concurrency and mutation idempotency to eviction fences.

ALTER TABLE VidLabelingEvictionFence ADD COLUMN Etag STRING(MAX);

UPDATE VidLabelingEvictionFence
SET Etag = EvictionOperationId
WHERE Etag IS NULL;

ALTER TABLE VidLabelingEvictionFence
ALTER COLUMN Etag STRING(MAX) NOT NULL;

CREATE TABLE VidLabelingEvictionFenceMutation (
  DataProviderResourceId STRING(63) NOT NULL,
  RequestId STRING(36) NOT NULL,
  RequestFingerprint BYTES(32) NOT NULL,
  ResultEtag STRING(MAX) NOT NULL,
  NewlyAcquired BOOL NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, RequestId);
