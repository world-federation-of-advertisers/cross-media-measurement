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

-- changeset marcopremier:add-raw-impression-upload-file-event-date dbms:cloudspanner

-- UTC calendar date of a raw impression file's impressions (one day per file),
-- populated at registration from the file's plaintext Parquet footer. Consumers
-- reconcile registered files against dated storage/output folders without opening
-- each file. Nullable so it can be added to an existing (possibly non-empty)
-- table; new files always set it.
ALTER TABLE RawImpressionUploadFile ADD COLUMN EventDate DATE;
