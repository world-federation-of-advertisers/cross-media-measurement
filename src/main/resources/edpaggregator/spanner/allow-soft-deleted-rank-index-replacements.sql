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

-- changeset marcopremier:28 dbms:cloudspanner
ALTER TABLE RankIndexBlob ADD COLUMN IsDeleted BOOL NOT NULL DEFAULT (FALSE);
UPDATE RankIndexBlob SET IsDeleted = TRUE WHERE DeleteTime IS NOT NULL;
ALTER TABLE RankIndexBlob ADD COLUMN LiveSnapshotPoolOffset INT64
  AS (IF(CAST(BlobType AS INT64) = 2 AND NOT IsDeleted, PoolOffset, NULL)) STORED;
ALTER TABLE RankIndexBlob ADD COLUMN DayOnlyPoolOffset INT64
  AS (IF(CAST(BlobType AS INT64) = 1, PoolOffset, NULL)) STORED;
CREATE UNIQUE NULL_FILTERED INDEX RankIndexBlobByLiveSnapshot
  ON RankIndexBlob(DataProviderResourceId, RawImpressionUploadId, CmmsModelLine, LiveSnapshotPoolOffset);
CREATE UNIQUE NULL_FILTERED INDEX RankIndexBlobByDayOnly
  ON RankIndexBlob(DataProviderResourceId, RawImpressionUploadId, CmmsModelLine, DayOnlyPoolOffset);
DROP INDEX RankIndexBlobByNaturalKey;
