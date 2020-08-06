// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

/**
 * Information about a computation needed to edit a computation.
 */
data class ComputationStorageEditToken<StageT>(
  /** The identifier for the computation used locally. */
  val localId: Long,
  /** The stage of the computation when the token was created. */
  val stage: StageT,
  /** The number of the current attempt of this stage for this computation. */
  val attempt: Int,
  /**
   * The version number of the last known edit to the computation.
   * The version is a monotonically increasing number used as a guardrail to protect against
   * concurrent edits to the same computation.
   */
  val editVersion: Long
)

/**
 * Specifies what to do with the lock on a computation after transitioning to a new stage.
 *
 * @see[ComputationsRelationalDb.updateComputationStage]
 */
enum class AfterTransition {
  /** Retain and extend the lock for the current owner. */
  CONTINUE_WORKING,

  /**
   * Add the computation to the work queue, but in an unclaimed stage for some
   * worker to claim at a later time.
   */
  ADD_UNCLAIMED_TO_QUEUE,

  /**
   * Do not add to the work queue, and release any lock on the computation.
   * There is no work to be done on the computation at this time.
   * Examples for when to set this include the computation finished or
   * input from another source is required before continuing.
   */
  DO_NOT_ADD_TO_QUEUE
}

/**
 * Specifies why a computation has ended.
 *
 * @see[ComputationsRelationalDb.endComputation]
 */
enum class EndComputationReason {
  /** Computation went the expected execution and succeeded. */
  SUCCEEDED,

  /** Computation failed and will not be retried again. */
  FAILED,

  /**
   * The computation was canceled. There were not known issues when it was ended, but results
   * will not be obtained.
   */
  CANCELED
}

/**
 * Performs read operations on a relational database of computations.
 */
interface ReadOnlyComputationsRelationalDb {

  /** Gets a [ComputationToken] for the current state of a computation. */
  suspend fun readComputationToken(globalId: Long): ComputationToken

  /**
   * Gets a collection of all the global computation ids for a computation in the database
   * which are in a one of the provided stages.
   */
  suspend fun readGlobalComputationIds(stages: Set<ComputationStage>): Set<Long>
}

/**
 * Relational database for keeping track of Computations of a single protocol type as they
 * progress through stages of a Computation within a Duchy.
 *
 * The database must have strong consistency guarantees as it is used to
 * coordinate assignment of work by pulling jobs.
 *
 * @param StageT Object represent a stage of a computation protocol.
 */
interface ComputationsRelationalDb<StageT> {

  /**
   * Inserts a new computation for the global identifier.
   *
   * The computation is not added to the queue.
   */
  suspend fun insertComputation(globalId: Long, initialStage: StageT)

  /**
   * Adds a computation to the work queue, saying it can be worked on by a worker job.
   *
   * This will release any ownership and locks associated with the computation.
   */
  suspend fun enqueue(token: ComputationStorageEditToken<StageT>)

  /**
   * Query for Computations with tasks ready for processing, and claim one for an owner.
   *
   * @param[ownerId] The identifier of the worker process that will own the lock.
   * @return global computation id of work that was claimed. When null, no work was claimed.
   */
  suspend fun claimTask(ownerId: String): Long?

  /**
   * Transitions a computation to a new stage.
   *
   * @param[token] The token for the computation
   * @param[nextStage] Stage this computation should transition to.
   * @param[inputBlobPaths] References to BLOBs that are inputs to this computation stage, all
   *    inputs should be written on transition and should not change.
   * @param[outputBlobs] Number of BLOBs this computation outputs. These are created as
   *    part of the computation so they do not have a reference to the real storage location.
   * @param[afterTransition] The work to be do with the computation after a successful transition.
   */
  suspend fun updateComputationStage(
    token: ComputationStorageEditToken<StageT>,
    nextStage: StageT,
    inputBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition
  )

  /** Moves a computation to a terminal state and records the reason why it ended. */
  suspend fun endComputation(
    token: ComputationStorageEditToken<StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason
  )

  /** Writes the reference to a BLOB needed for [BlobDependencyType.OUTPUT] from a stage. */
  suspend fun writeOutputBlobReference(token: ComputationStorageEditToken<StageT>, blobRef: BlobRef)
}

/**
 * Grouping of a database reader and writer to interact with a database for one type of computation
 * through [ComputationStage]s.
 */
interface SingleProtocolDatabase :
  ReadOnlyComputationsRelationalDb,
  ComputationsRelationalDb<ComputationStage>,
  ProtocolStageEnumHelper<ComputationStage> {

  val computationType: ComputationType
}

/**
 * Reference to a BLOB's storage location (key).
 *
 * @param idInRelationalDatabase identifier of the blob as stored in the [ComputationsRelationalDb]
 * @param key object key of the the blob which can be used to retrieve it from the BLOB storage.
 */
data class BlobRef(val idInRelationalDatabase: Long, val key: String)

/** BLOBs storage used by a computation. */
interface ComputationsBlobDb<StageT> {

  /** Reads and returns a BLOB from storage */
  suspend fun read(reference: BlobRef): ByteArray

  /** Write a BLOB and ensure it is fully written before returning. */
  suspend fun blockingWrite(blob: BlobRef, bytes: ByteArray) = blockingWrite(blob.key, bytes)

  /** Write a BLOB and ensure it is fully written before returning. */
  suspend fun blockingWrite(path: String, bytes: ByteArray)

  /** Deletes a BLOB */
  suspend fun delete(reference: BlobRef)
}

/** The way in which a stage depends upon a BLOB. */
enum class BlobDependencyType {
  /** BLOB is used as an input to the computation stage. */
  INPUT,

  /** BLOB is an output of the computation stage. */
  OUTPUT,

  /**
   * ONLY makes sense when reading blobs from the database. Read all blobs
   * no mater the type.
   */
  ANY;

  fun toComputationStageBlobDependencyType(): ComputationBlobDependency =
    when (this) {
      INPUT -> ComputationBlobDependency.INPUT
      OUTPUT -> ComputationBlobDependency.OUTPUT
      else -> error("Conversion of $this to  ComputationBlobDependency::class not supported.")
    }
}
