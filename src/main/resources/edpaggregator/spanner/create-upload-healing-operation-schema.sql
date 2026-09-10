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

-- changeset marcopremier:1 dbms:cloudspanner
-- comment: Persist resumable VID-labeling upload-healing operations and steps.

START BATCH DDL;

CREATE TABLE UploadHealingOperation (
  DataProviderResourceId STRING(63) NOT NULL,
  UploadHealingOperationId STRING(36) NOT NULL,
  CreateRequestId STRING(36) NOT NULL,
  Reason STRING(MAX) NOT NULL,
  LabeledImpressionsBlobPrefix STRING(MAX) NOT NULL,
  BadRawImpressionUploadResourceIds ARRAY<STRING(63)> NOT NULL,
  CutoffTime TIMESTAMP NOT NULL,
  CompleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, UploadHealingOperationId);

CREATE UNIQUE INDEX UploadHealingOperationByCreateRequestId
  ON UploadHealingOperation(DataProviderResourceId, CreateRequestId);

CREATE TABLE UploadHealingStep (
  DataProviderResourceId STRING(63) NOT NULL,
  UploadHealingOperationId STRING(36) NOT NULL,
  UploadHealingStepId INT64 NOT NULL,
  SequenceNumber INT64 NOT NULL,
  SourceRawImpressionUploadResourceId STRING(63) NOT NULL,
  RawImpressionUploadModelLineResourceId STRING(63) NOT NULL,
  CmmsModelLine STRING(MAX) NOT NULL,
  Memoized BOOL NOT NULL,
  RecoveryAction `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction` NOT NULL,
  RecoveryPredecessorRawImpressionUploadResourceId STRING(63),
  RecoveryTarget BOOL NOT NULL,
  EvictionCompleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  RecoveryStartTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  RecoveryDoneBlobGeneration INT64,
  ReplacementRawImpressionUploadResourceId STRING(63),
  CompleteTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  UpdateRequestId STRING(36),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (
  DataProviderResourceId,
  UploadHealingOperationId,
  UploadHealingStepId
), INTERLEAVE IN PARENT UploadHealingOperation ON DELETE CASCADE;

CREATE UNIQUE INDEX UploadHealingStepByModelLine
  ON UploadHealingStep(
    DataProviderResourceId,
    UploadHealingOperationId,
    RawImpressionUploadModelLineResourceId
  );

CREATE UNIQUE NULL_FILTERED INDEX UploadHealingStepByUpdateRequestId
  ON UploadHealingStep(
    DataProviderResourceId,
    UploadHealingOperationId,
    UpdateRequestId
  );

RUN BATCH;
