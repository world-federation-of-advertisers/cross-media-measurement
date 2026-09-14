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

-- changeset marcopremier:add-raw-impression-upload-replacement dbms:cloudspanner
-- comment: Link each replacement upload to the preceding revision of the same done path.

ALTER TABLE RawImpressionUpload ADD COLUMN ReplacesRawImpressionUploadResourceId STRING(63);

CREATE NULL_FILTERED INDEX RawImpressionUploadByReplacement
  ON RawImpressionUpload(DataProviderResourceId, ReplacesRawImpressionUploadResourceId);
