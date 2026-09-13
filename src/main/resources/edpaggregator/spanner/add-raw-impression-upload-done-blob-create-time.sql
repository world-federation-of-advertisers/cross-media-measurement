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

-- changeset marcopremier:add-raw-impression-upload-done-blob-create-time dbms:cloudspanner
-- comment: Order replacement uploads by the done object's immutable creation time.

ALTER TABLE RawImpressionUpload ADD COLUMN DoneBlobCreateTime TIMESTAMP;

CREATE NULL_FILTERED INDEX RawImpressionUploadByDoneBlobCreateTime
  ON RawImpressionUpload(DataProviderResourceId, DoneBlobUri, DoneBlobCreateTime DESC);
