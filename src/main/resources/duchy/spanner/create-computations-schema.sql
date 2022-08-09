-- liquibase formatted sql

-- Copyright 2020 The Cross-Media Measurement Authors
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

-- changeset wangyaopw:1 dbms:cloudspanner
-- preconditions onFail:MARK_RAN onError:HALT
-- precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Computations'

START BATCH DDL;

-- Cloud Spanner database schema for managing computations in MPC worker nodes
--
-- Table hierarchy:
--  Root
--  └── Computations
--      ├── Requisitions
--      └── ComputationStages
--          ├── ComputationBlobReferences
--          └── ComputationStageAttempts
--              └── ComputationStats
--
-- A Computation is the 'C' in MPC so it represents the full end-to-end unit of
-- work done by the across all the worker nodes in the system. There are
-- multiple rounds to a computation and each of those rounds requires some
-- amount of work from each of the MPC Nodes this work is a stage.
-- There is a one-to-many mapping of rounds to stages. MPC Nodes track the
-- progress of a computation through various stages. Most of which require
-- work on BLOBs of encrypted data. These BLOBs are not stored directly in
-- the spanner database, but are referenced here. So worker processes within
-- an MPC Node know where to find them.


-- Computations
--   Contains records of computations operated on by this duchy.
--   A Computation in this sense is the unit of work done across all
--   MPC Nodes. There are multiple stages to each computation at each
--   Node. This table keeps track of when work is being done and/or
--   needs to be done to move a computation along.
CREATE TABLE Computations (
  -- The local identifier of the computation.
  ComputationId INT64 NOT NULL,

  -- The protocol the computation is following. This column is immutable.
  --
  -- See the wfa.measurement.internal.duchy.ComputationTypeEnum proto
  Protocol INT64 NOT NULL,

  -- The current stage of the computation as known to this Node.
  -- This does not reflect the stage to all Nodes.
  --
  -- See the protos in the wfa.measurement.protocol package
  ComputationStage INT64 NOT NULL,

  -- Last time the stage was modified.
  UpdateTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

  -- The global identifier of the computation that was given when starting
  -- the computation. The global id is provided to the duchy by the kingdom
  -- e.g., it is how the kingdom references this computation.
  GlobalComputationId STRING(MAX) NOT NULL,

  -- The identifier of the worker process/task who owns the lock on this
  -- computation. This field may be null in which case no Mill owns the
  -- lock. Depending upon the LockOwner field, the computation is either
  -- available for a Mill to take up as its quest, or the computation cannot
  -- be worked because inputs are required from an external source.
  --
  -- When the lock is owned, it is assumed the computation is being worked on.
  -- However failures happen so this field may be over written when the lock
  -- expires.
  --
  -- For example this could be the identifier of the GCP instance running a
  -- Mill i.e. `gke-mpc-worker-mill-pool-c8f5637a-rffh`.
  LockOwner STRING(MAX),

  -- Time the lock on the computation expires. After such a time the
  -- computation may be acquired by another worker process in the MPC Node,
  --
  -- A NULL value in this field indicates the computation does not need to be
  -- assigned to a worker. This is usually the case when the computation is
  -- waiting on inputs from another MPC node. A NULL value here should imply
  -- a NULL value in LockOwner.
  LockExpirationTime TIMESTAMP,

  -- Serialized bytes of a proto3 protobuf with details about the
  -- ongoing computation which do not need to be indexed by the
  -- database.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationDetails Proto
  ComputationDetails BYTES(MAX) NOT NULL,

  -- Serialized JSON string of a proto3 protobuf with details about the
  -- ongoing computation which do not need to be indexed by the
  -- database.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationDetails Proto
  ComputationDetailsJSON STRING(MAX) NOT NULL,
) PRIMARY KEY (ComputationId);


-- Query computations ready for work. All computations that can be worked on
-- have a non null LockExpirationTime.
CREATE NULL_FILTERED INDEX ComputationsByLockExpirationTime
  ON Computations(Protocol, LockExpirationTime ASC, UpdateTime ASC)
  STORING (ComputationStage, GlobalComputationId);

-- Query computations by the owner of the lock.
CREATE INDEX ComputationsByLockOwner ON Computations(LockOwner);

-- Enable querying by global computation id.
CREATE UNIQUE INDEX ComputationsByGlobalId ON Computations(GlobalComputationId);

-- Requisitions
--   All requisitions used in a particular computation. The data is store in
--   the BLOB store, and the path to the BLOB is recorded in this table.
--   If the path is set, the requisition is fulfilled at this duchy. Otherwise,
--   it is not fulfilled yet or fulfilled at other duchies.
CREATE TABLE Requisitions (
  -- The local identifier of the parent computation.
  ComputationId INT64 NOT NULL,

  -- The local identifier of this requisition.
  RequisitionId INT64 NOT NULL,

  -- The external requisition id.
  ExternalRequisitionId STRING(MAX) NOT NULL,

  RequisitionFingerprint BYTES(32) NOT NULL,

  -- A reference to the BLOB. If empty, then the requisition is unfulfilled
  PathToBlob STRING(MAX),

  -- Serialized bytes of a proto3 protobuf with details about the requisition.
  -- The details are obtained from the Kingdom's ComputationsService.
  --
  -- See the wfa.measurement.internal.RequisitionDetails Proto
  RequisitionDetails BYTES(MAX) NOT NULL,

  -- Canonical JSON string of a proto3 protobuf with details about the
  -- requisition. The details are obtained from the Kingdom's
  -- ComputationsService.
  --
  -- See the wfa.measurement.internal.RequisitionDetails Proto
  RequisitionDetailsJSON STRING(MAX) NOT NULL,
) PRIMARY KEY (ComputationId, RequisitionId),
INTERLEAVE IN PARENT Computations ON DELETE CASCADE;

-- Enable querying by ExternalRequisitionId and RequisitionFingerprint.
CREATE UNIQUE INDEX RequisitionsByExternalId ON Requisitions(
  ExternalRequisitionId,
  RequisitionFingerprint
);

-- ComputationStages
--   Running history of stage transitions for a computation. A stage may
--   be tried multiple times, although it is expected to succeed the on
--   the first attempt.
CREATE TABLE ComputationStages (
  -- The local identifier of the computation.
  ComputationId INT64 NOT NULL,

  -- The stage the computation was in.
  --
  -- See the protos in the wfa.measurement.protocol package.
  ComputationStage INT64 NOT NULL,

  -- The time the computation stage was created. This is strictly
  -- less than or equal to the ComputationStageAttempts.BeginTime
  -- as the stage is created at or before the time of its attempt.
  CreationTime TIMESTAMP NOT NULL OPTIONS(allow_commit_timestamp = true),

  -- The number of the next attempt of this stage.
  --
  -- This should be used as Attempt when inserting a row in
  -- ComputationStageAttempts.
  NextAttempt INT64 NOT NULL,

  -- The time the computation left the stage.
  EndTime TIMESTAMP OPTIONS(allow_commit_timestamp = true),

  -- The stage the computation was in before entering this stage.
  --
  -- See the wfa.measurement.internal.SketchAggregationState proto
  PreviousStage INT64,

  -- The stage the computation transitioned into after leaving the stage.
  --
  -- See the wfa.measurement.internal.SketchAggregationState proto
  FollowingStage INT64,

  -- Serialized bytes of a proto3 protobuf with details about the
  -- computation stage which do not need to be indexed by the
  -- database.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationStageDetails Proto
  Details BYTES(MAX) NOT NULL,

  -- Canonical JSON string of a proto3 protobuf with details about the
  -- the computation stage which do not need to be indexed by the
  -- database.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationStageDetails Proto
  DetailsJSON STRING(MAX) NOT NULL,
) PRIMARY KEY (ComputationId, ComputationStage),
  INTERLEAVE IN PARENT Computations ON DELETE CASCADE;


-- ComputationBlobReferences
--   References to BLOBs for an attempt of a stage of the computation.
--   A stage may have input BLOBS and output BLOBs. It is expected that
--   all input BLOB references are filled out before the stage attempt
--   begins, and are then immutable. References to output BLOBs may be
--   added/modified while the attempt is ongoing. A stage waiting on many
--   outputs can be created with all output names, but thie PathToBlob set
--   to null. Once  all the output BLOBs have a non null path, the stage
--   can be considered finished.
CREATE TABLE ComputationBlobReferences (
  -- The local identifier of the computation.
  ComputationId INT64 NOT NULL,

  -- The stage the computation was in.
  --
  -- See the protos in the wfa.measurement.protocol package.
  ComputationStage INT64 NOT NULL,

  -- A unique identifier for the BLOB.
  BlobId INT64 NOT NULL,

  -- A reference to the BLOB. This is the way which the BLOB is accessed
  -- via a Mill job. This is a globally unique name starting with
  -- StoredDataPrefix from the parent table.
  PathToBlob STRING(MAX),

  -- Says in what way the stage depends upon the referenced BLOB, i.e.
  -- is the blob an input to the stage, or is it a required output.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationBlobDependency proto
  DependencyType INT64 NOT NULL,
) PRIMARY KEY (ComputationId, ComputationStage, BlobId),
  INTERLEAVE IN PARENT ComputationStages ON DELETE CASCADE;


-- ComputationStageAttempts
--   Running history of attempts for running a stage of a computation.
--   It is generally expected that each stage will be attempted once,
--   but it is possible for Mill to fail mid operation.
CREATE TABLE ComputationStageAttempts (
  -- The local identifier of the computation.
  ComputationId INT64 NOT NULL,

  -- The stage the computation was in.
  --
  -- See the protos in the wfa.measurement.protocol package.
  ComputationStage INT64 NOT NULL,

  -- The attempt number for this stage for this computation.
  -- Ideally this number would always be one.
  --
  -- When inserting a new row this should be set to
  --   ComputationStages.NextAttempt and
  --   ComputationStages.NextAttempt should be incremented.
  Attempt INT64 NOT NULL,

  -- When the attempt of the stage began.
  BeginTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  -- When the attempt finished.
  EndTime TIMESTAMP OPTIONS (allow_commit_timestamp = true),

  -- Serialized bytes of a proto3 protobuf with details about the attempt of
  --  a computation stage which do not need to be indexed by the database.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationStageAttemptDetails Proto
  Details BYTES(MAX) NOT NULL,

  -- Serialized bytes of a proto3 protobuf with details about the attempt of
  --  a computation stage which do not need to be indexed by the database.
  --
  -- See the wfa.measurement.internal.db.gcp.ComputationStageAttemptDetails Proto
  DetailsJSON STRING(MAX) NOT NULL,
) PRIMARY KEY (ComputationId, ComputationStage, Attempt),
  INTERLEAVE IN PARENT ComputationStages ON DELETE CASCADE;


-- ComputationStats
--   At a high level this table is for a string MetricName, int64 Value pair
--   for metrics at various attempts of the stages of a computation at the duchy.
--   The table is meant to allow for non-structured metrics (e.g. crypto_cpu_time_millis,
--   computation_elapsed_time_millis, etc.) to be collected across all the stages.
--   The structure in this table is for queriability  across stages of the computation.
--   But depending on the question being asked, the consumer of this table may need
--   to know particular names of metrics for a stage as they are not baked into the schema.
--   These names will be consistent across stages but could differ slightly
--   (e.g. there may be crypto_cpu_time_millis for all crypto stages, but not all
--   of the stages have crypto operations).
--
--   Job crashes, transient network connectivity issues, or other factors could lead to
--   some missing entries in this table.
CREATE TABLE ComputationStats (
  -- The local identifier of the computation.
  ComputationId INT64 NOT NULL,

  -- The stage the computation was in.
  ComputationStage INT64 NOT NULL,

  -- The attempt number for this stage for this computation.
  -- Ideally this number would always be one.
  Attempt INT64 NOT NULL,

  -- Name of the metric that is being measured e.g. crypto_cpu_time_millis.
  MetricName STRING(MAX) NOT NULL,

  -- Time when the row was inserted.
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  -- Numerical value of the measurement.
  MetricValue INT64 NOT NULL,

) PRIMARY KEY (ComputationId, ComputationStage, Attempt, MetricName),
  INTERLEAVE IN PARENT ComputationStageAttempts ON DELETE CASCADE;

RUN BATCH;
