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

-- Holds the durable, data-provider-wide fence for a resumable VID-labeling
-- eviction. A single row prevents upload registration and processing restarts
-- from racing the eviction.
-- changeset marcopremier:add-vid-labeling-eviction-fence dbms:cloudspanner
-- comment: Fence VID-labeling mutations while an upload eviction is in progress.
CREATE TABLE VidLabelingEvictionFence (
  DataProviderResourceId STRING(63) NOT NULL,
  EvictionOperationId STRING(36) NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId);

CREATE INDEX RawImpressionUploadByRegistrationComplete
  ON RawImpressionUpload(DataProviderResourceId, RegistrationComplete, State);
