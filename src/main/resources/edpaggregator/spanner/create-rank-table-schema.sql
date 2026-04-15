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

-- changeset marcopremier:9 dbms:cloudspanner
-- comment: Create RankTable and PoolCounter tables for memoized VID assignment.

START BATCH DDL;

-- Stores the rank assigned to each known fingerprint within a VID pool.
-- Used by the memoized VID assignment pipeline to eliminate birthday-problem
-- collisions for EDPs with 1:1 account-to-person mappings.
-- One row per (DataProvider, ModelRelease, EncryptedFingerprint) tuple.
CREATE TABLE RankTable (
  -- External API resource ID of the DataProvider that owns this fingerprint.
  DataProviderResourceId STRING(63) NOT NULL,
  -- The model release identifier. Rank assignments are scoped per model release
  -- because model changes may alter pool routing.
  ModelRelease STRING(255) NOT NULL,
  -- Deterministically encrypted fingerprint (AES-SIV or HMAC) enabling equality
  -- lookups without storing plaintext PII. The encryption key is held in KMS
  -- and accessible only to attested TEE apps.
  EncryptedFingerprint BYTES(64) NOT NULL,
  -- The VID pool this fingerprint was assigned to by the labeler's tree traversal
  -- (Pass 1). Stored to enable skipping Pass 1 for known fingerprints.
  PoolId STRING(127) NOT NULL,
  -- The unique sequential rank assigned to this fingerprint within its pool.
  -- Used as input to the Feistel bijection for collision-free VID assignment.
  -- Ranks are monotonically allocated via PoolCounter and never recycled.
  RankValue INT64 NOT NULL,
  -- The time this record was created.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (DataProviderResourceId, ModelRelease, EncryptedFingerprint);

-- Supports querying all ranked fingerprints within a specific pool.
-- Used for rank table migration when model lines change and for
-- monitoring pool utilization (count of ranked entries vs RankedSize).
CREATE INDEX RankTableByPool
  ON RankTable(DataProviderResourceId, ModelRelease, PoolId);

-- Maintains a monotonically increasing counter per VID pool for allocating
-- unique ranks to new fingerprints. One row per pool per model release.
CREATE TABLE PoolCounter (
  -- External API resource ID of the DataProvider.
  DataProviderResourceId STRING(63) NOT NULL,
  -- The model release identifier, matching RankTable.ModelRelease.
  ModelRelease STRING(255) NOT NULL,
  -- The VID pool identifier, matching RankTable.PoolId.
  PoolId STRING(127) NOT NULL,
  -- The next rank value to allocate. Incremented atomically via read-write
  -- transaction when new fingerprints are assigned to this pool.
  NextRank INT64 NOT NULL,
  -- The maximum number of ranks available for collision-free assignment in this
  -- pool (corresponds to RankedPopulationNode.ranked_size in the VID model).
  -- Ranks >= RankedSize overflow to hash-based (unranked) VID assignment.
  RankedSize INT64 NOT NULL,
) PRIMARY KEY (DataProviderResourceId, ModelRelease, PoolId);

RUN BATCH;
