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

-- changeset getina:7 dbms:cloudspanner
-- comment: Add foreign key columns to ImpressionMetadata referencing RawImpressionMetadata.

ALTER TABLE ImpressionMetadata ADD COLUMN RawImpressionUploadId INT64;
ALTER TABLE ImpressionMetadata ADD COLUMN RawImpressionBatchIndex INT64;
ALTER TABLE ImpressionMetadata ADD COLUMN RawImpressionFileId INT64;

ALTER TABLE ImpressionMetadata ADD CONSTRAINT CK_ImpressionMetadata_RawImpression_AllOrNone
  CHECK (
    (RawImpressionUploadId IS NULL AND RawImpressionBatchIndex IS NULL AND RawImpressionFileId IS NULL)
    OR
    (RawImpressionUploadId IS NOT NULL AND RawImpressionBatchIndex IS NOT NULL AND RawImpressionFileId IS NOT NULL)
  );

ALTER TABLE ImpressionMetadata ADD CONSTRAINT FK_ImpressionMetadata_RawImpressionMetadata
  FOREIGN KEY (DataProviderResourceId, RawImpressionUploadId, RawImpressionBatchIndex, RawImpressionFileId)
  REFERENCES RawImpressionMetadata(DataProviderResourceId, UploadId, BatchIndex, FileId);
