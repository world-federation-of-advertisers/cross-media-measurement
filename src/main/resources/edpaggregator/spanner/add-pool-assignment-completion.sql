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

-- changeset marcopremier:27 dbms:cloudspanner
-- comment: Store Phase-0 completion separately from encryption and retain replay outcomes.
START BATCH DDL;
ALTER TABLE PoolAssignmentJob ADD COLUMN LastShard BOOL;
ALTER TABLE PoolAssignmentJob ADD COLUMN LastShardPoolOffsets ARRAY<INT64>;
ALTER TABLE PoolAssignmentJob ADD COLUMN LastShardMaxEventDate DATE;
ALTER TABLE RawImpressionUploadModelLine
  ADD COLUMN PoolAssignmentComplete BOOL NOT NULL DEFAULT (FALSE);
ALTER TABLE RawImpressionUploadModelLine ADD COLUMN OutputKekUri STRING(MAX);
RUN BATCH;
