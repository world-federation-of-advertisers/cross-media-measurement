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

-- changeset marcopremier:add-raw-impression-upload-generations dbms:cloudspanner
-- comment: Track exact GCS object versions and permit blob URI reuse across upload revisions.

ALTER TABLE RawImpressionUpload ADD COLUMN DoneBlobGeneration INT64;

ALTER TABLE RawImpressionUploadFile ADD COLUMN BlobGeneration INT64;

DROP INDEX RawImpressionUploadFileByBlobUri;

CREATE INDEX RawImpressionUploadFileByBlobUriAndGeneration
  ON RawImpressionUploadFile(DataProviderResourceId, BlobUri, BlobGeneration);

CREATE UNIQUE INDEX RawImpressionUploadFileByUploadAndBlobUri
  ON RawImpressionUploadFile(DataProviderResourceId, RawImpressionUploadId, BlobUri);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadByDoneBlobGeneration
  ON RawImpressionUpload(DataProviderResourceId, DoneBlobUri, DoneBlobGeneration);
