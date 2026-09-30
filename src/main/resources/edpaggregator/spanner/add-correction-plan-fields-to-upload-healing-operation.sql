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

-- changeset marcopremier:18 dbms:cloudspanner
-- comment: Persist correction-plan state, candidates, and mutation idempotency.

START BATCH DDL;

ALTER TABLE UploadHealingOperation
ADD COLUMN State INT64 NOT NULL DEFAULT (9);

ALTER TABLE UploadHealingOperation ADD COLUMN ResumeState INT64;

ALTER TABLE UploadHealingOperation
ADD COLUMN RawImpressionUploadCorrectionCandidateIds ARRAY<STRING(36)> NOT NULL DEFAULT ([]);

ALTER TABLE UploadHealingOperation
ADD COLUMN MutationRequestIds ARRAY<STRING(36)> NOT NULL DEFAULT ([]);

ALTER TABLE UploadHealingOperation
ADD COLUMN MutationRequestFingerprints ARRAY<BYTES(MAX)> NOT NULL DEFAULT ([]);

ALTER TABLE UploadHealingStep
ADD COLUMN RawImpressionUploadCorrectionCandidateId STRING(36);

CREATE INDEX UploadHealingOperationByStateAndCreateTime
  ON UploadHealingOperation(
    DataProviderResourceId,
    State,
    CreateTime,
    UploadHealingOperationId
  );

RUN BATCH;
