-- liquibase formatted sql

-- Copyright 2023 The Cross-Media Measurement Authors
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


-- changeset yuhongwang:1 dbms:postgresql

-- Postgres database schema for the Duchy system.
--
-- Table hierarchy:
--  Root
--  └── Computations
--      ├── Requisitions
--      └── ComputationStages
--          ├── ComputationBlobReferences
--          └── ComputationStageAttempts
--              └── ComputationStats
--  └── HeraldContinuationTokens
--
-- A Computation is the 'C' in MPC so it represents the full end-to-end unit of
-- work done by the across all the worker nodes in the system. There are
-- multiple rounds to a computation and each of those rounds requires some
-- amount of work from each of the MPC Nodes this work is a stage.
-- There is a one-to-many mapping of rounds to stages. MPC Nodes track the
-- progress of a computation through various stages. Most of which require
-- work on BLOBs of encrypted data. These BLOBs are not stored directly in
-- the postgres database, but are referenced here. So worker processes within
-- an MPC Node know where to find them.


-- Computations
--   Contains records of computations operated on by this duchy.
--   A Computation in this sense is the unit of work done across all
--   MPC Nodes. There are multiple stages to each computation at each
--   Node. This table keeps track of when work is being done and/or
--   needs to be done to move a computation along.
CREATE TABLE Computations (
  -- The local identifier of the computation.
  ComputationId bigint NOT NULL,

  -- The protocol the computation is following. This column is immutable.
  --
  -- See the wfa.measurement.internal.duchy.ComputationTypeEnum proto
  Protocol integer NOT NULL,

  -- The current stage of the computation as known to this Node.
  -- This does not reflect the stage to all Nodes.
  --
  -- Numeric value of a wfa.measurement.internal.duchy.ComputationStage protobuf enum.
  ComputationStage integer NOT NULL,

  -- Last time the stage was modified.
  UpdateTime timestamp NOT NULL,

  -- The global identifier of the computation that was given when starting
  -- the computation. The global id is provided to the duchy by the kingdom
  -- e.g., it is how the kingdom references this computation.
  GlobalComputationId text NOT NULL,

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
  LockOwner text,

  -- Time the lock on the computation expires. After such a time the
  -- computation may be acquired by another worker process in the MPC Node,
  --
  -- A NULL value in this field indicates the computation does not need to be
  -- assigned to a worker. This is usually the case when the computation is
  -- waiting on inputs from another MPC node. A NULL value here should imply
  -- a NULL value in LockOwner.
  LockExpirationTime timestamp,

  -- Serialized wfa.measurement.internal.duchy.ComputationDetails protobuf message
  ComputationDetails bytea NOT NULL,

  -- Human-readable copy of the ComputationDetails column for debugging only.
  ComputationDetailsJSON jsonb NOT NULL,

  -- The time the Computation was created.
  CreationTime timestamp NOT NULL,

  PRIMARY KEY (ComputationId)
);

-- Query computations ready for work. All computations that can be worked on
-- have a non null LockExpirationTime.
CREATE INDEX ComputationsByLockExpirationTime
  ON Computations(Protocol, LockExpirationTime ASC, UpdateTime ASC)
  WHERE LockExpirationTime IS NOT NULL;

-- Enforce uniqueness of GlobalComputationId.
CREATE UNIQUE INDEX ComputationsByGlobalId ON Computations(GlobalComputationId);

-- Requisitions
--   All requisitions used in a particular computation. The data is store in
--   the BLOB store, and the path to the BLOB is recorded in this table.
--   If the path is set, the requisition is fulfilled at this duchy. Otherwise,
--   it is not fulfilled yet or fulfilled at other duchies.
CREATE TABLE Requisitions (
  -- The local identifier of the parent computation
  ComputationId bigint NOT NULL,

  -- The local identifier of this requisition.
  RequisitionId bigint NOT NULL,

  -- The external requisition id.
  ExternalRequisitionId text NOT NULL,
  RequisitionFingerprint bytea NOT NULL,

  -- A reference to the BLOB. If empty, then the requisition is unfulfilled
  PathToBlob text,

  -- Serialized wfa.measurement.internal.RequisitionDetails protobuf message.
  RequisitionDetails bytea NOT NULL,

  -- Human-readable copy of the RequisitionDetails column for debugging only.
  RequisitionDetailsJSON jsonb NOT NULL,

  -- The time the Requisition was created.
  CreationTime timestamp NOT NULL,

  -- Last time the Requisition was modified.
  UpdateTime timestamp NOT NULL,

  PRIMARY KEY (ComputationId, RequisitionId),
  FOREIGN KEY (ComputationId)
    REFERENCES Computations(ComputationId) ON DELETE CASCADE
);

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
  ComputationId bigint NOT NULL,

  -- Numeric value of a wfa.measurement.internal.duchy.ComputationStage protobuf enum.
  ComputationStage integer NOT NULL,

  -- The time the computation stage was created. This is strictly
  -- less than or equal to the ComputationStageAttempts.BeginTime
  -- as the stage is created at or before the time of its attempt.
  CreationTime timestamp NOT NULL,

  -- The number of the next attempt of this stage.
  --
  -- This should be used as Attempt when inserting a row in
  -- ComputationStageAttempts.
  NextAttempt bigint NOT NULL,

  -- The time the computation left the stage.
  EndTime timestamp,

  -- Serialized wfa.measurement.internal.duchy.ComputationStage protobuf message
  -- that stores the stage this computation was in before entering this stage.
  PreviousStage integer,

  -- Serialized wfa.measurement.internal.duchy.ComputationStage protobuf message
  -- that stores the stage this computation transitioned into after leaving this stage.
  FollowingStage integer,

  -- Serialized wfa.measurement.internal.duchy.ComputationStageDetails protobuf message
  Details bytea NOT NULL,

  -- Human-readable copy of the Details column for debugging only.
  DetailsJSON jsonb NOT NULL,

  PRIMARY KEY (ComputationId, ComputationStage),
  FOREIGN KEY (ComputationId)
    REFERENCES Computations(ComputationId) ON DELETE CASCADE
);


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
  ComputationId bigint NOT NULL,

  -- Numeric value of a wfa.measurement.internal.duchy.ComputationStage protobuf enum.
  ComputationStage integer NOT NULL,

  -- A unique identifier for the BLOB.
  BlobId bigint NOT NULL,

  -- A reference to the BLOB. This is the way which the BLOB is accessed
  -- via a Mill job. This is a globally unique name starting with
  -- StoredDataPrefix from the parent table.
  PathToBlob text,

-- Numeric value of a wfa.measurement.internal.duchy.ComputationBlobDependency protobuf enum.
  DependencyType integer NOT NULL,

  PRIMARY KEY (ComputationId, ComputationStage, BlobId),
  FOREIGN KEY (ComputationId, ComputationStage)
    REFERENCES ComputationStages(ComputationId, ComputationStage) ON DELETE CASCADE
);


-- ComputationStageAttempts
--   Running history of attempts for running a stage of a computation.
--   It is generally expected that each stage will be attempted once,
--   but it is possible for Mill to fail mid operation.
CREATE TABLE ComputationStageAttempts (
  -- The local identifier of the computation.
  ComputationId bigint NOT NULL,

  -- Numeric value of a wfa.measurement.internal.duchy.ComputationStage protobuf enum.
  ComputationStage integer NOT NULL,

  -- The attempt number for this stage for this computation.
  -- Ideally this number would always be one.
  --
  -- When inserting a new row this should be set to
  --   ComputationStages.NextAttempt and
  --   ComputationStages.NextAttempt should be incremented.
  Attempt integer NOT NULL,

  -- When the attempt of the stage began.
  BeginTime timestamp NOT NULL,

  -- When the attempt finished.
  EndTime timestamp,

  -- Serialized wfa.measurement.internal.duchy.ComputationStageAttemptDetails protobuf message
  Details bytea,

  -- Human-readable copy of the Details column for debugging only.
  DetailsJSON jsonb,

  PRIMARY KEY (ComputationId, ComputationStage, Attempt),
  FOREIGN KEY (ComputationId, ComputationStage)
    REFERENCES ComputationStages(ComputationId, ComputationStage) ON DELETE CASCADE
);


-- ComputationStats
--   At a high level this table is for a string MetricName, int64 Value pair
--   for metrics at various attempts of the stages of a computation at the duchy.
--   The table is meant to allow for non-structured metrics (e.g. crypto_cpu_time_millis,
--   computation_elapsed_time_millis, etc.) to be collected across all the stages.
--   The structure in this table is for queriability across stages of the computation.
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
  ComputationId bigint NOT NULL,

  -- The stage the computation was in.
  ComputationStage integer NOT NULL,

  -- The attempt number for this stage for this computation.
  -- Ideally this number would always be one.
  Attempt integer NOT NULL,

  -- Name of the metric that is being measured e.g. crypto_cpu_time_millis.
  MetricName text NOT NULL,

  -- Time when the row was inserted.
  CreationTime timestamp NOT NULL,

  -- Numerical value of the measurement.
  MetricValue bigint NOT NULL,

  PRIMARY KEY (ComputationId, ComputationStage, Attempt, MetricName),
  FOREIGN KEY (ComputationId, ComputationStage, Attempt)
    REFERENCES ComputationStageAttempts(ComputationId, ComputationStage, Attempt) ON DELETE CASCADE
);

-- HeraldContinuationTokens
--   A ContinuationToken contains information to locate the progress of
--   Computation streaming for the Herald daemon in a Duchy. The Kingdom uses
--   the content of continuation token as a filter to query Computations and
--   also returns a new continuation token to mark the progress. Duchy stores
--   the latest continuation token in the database every time it receives one.
--   When restart happens, herald retrieve the continuation token to continue
--   streaming.
CREATE TABLE HeraldContinuationTokens (
  Presence bool NOT NULL,

  -- The content of the latest ContinuationToken
  ContinuationToken text NOT NULL,

  -- Last time the ContinuationToken was modified.
  UpdateTime timestamp NOT NULL,

  CONSTRAINT presence_set CHECK(Presence),

  PRIMARY KEY (Presence)
);
