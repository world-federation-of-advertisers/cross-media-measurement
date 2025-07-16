// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.db.computation

import java.time.Duration
import java.time.Instant
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionEntry

/**
 * Grouping of a read only view ([ComputationsDatabaseReader]) and a writer (
 * [ComputationsDatabaseTransactor]) to interact with a database for all types of computations.
 */
interface ComputationsDatabase :
  ComputationsDatabaseReader,
  ComputationsDatabaseTransactor<
    ComputationType,
    ComputationStage,
    ComputationStageDetails,
    ComputationDetails,
  >,
  ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>

/** Performs read operations on a relational database of computations. */
interface ComputationsDatabaseReader {

  /** Gets a [ComputationToken] for the current state of a computation if it exists. */
  suspend fun readComputationToken(globalId: String): ComputationToken?

  /**
   * Gets a [ComputationToken] for the current state of a computation who owns a particular
   * requisition if it exists.
   */
  suspend fun readComputationToken(
    externalRequisitionKey: ExternalRequisitionKey
  ): ComputationToken?

  /**
   * Gets a collection of all the global computation ids for a computation in the database which are
   * in a one of the provided stages and before the updated time if specified.
   */
  suspend fun readGlobalComputationIds(
    stages: Set<ComputationStage>,
    updatedBefore: Instant? = null,
  ): Set<String>

  /** Gets all blobKeys of a Computation's data */
  suspend fun readComputationBlobKeys(localId: Long): List<String>

  /** Gets all blobKeys of a Computation's requisitions */
  suspend fun readRequisitionBlobKeys(localId: Long): List<String>
}

/**
 * Relational database for keeping track of Computations of a single protocol type as they progress
 * through stages of a Computation within a Duchy.
 *
 * The database must have strong consistency guarantees as it is used to coordinate assignment of
 * work by pulling jobs.
 *
 * @param ProtocolT Object representing different protocols (computation types)
 * @param StageT Object representing stages of the computation protocol.
 * @param StageDetailsT Object representing details specific to a stage of a computation.
 * @param ComputationDetailsT Object representing details specific to a computation.
 */
interface ComputationsDatabaseTransactor<ProtocolT, StageT, StageDetailsT, ComputationDetailsT> {

  /**
   * Inserts a new computation for the global identifier.
   *
   * The computation is added to the queue immediately.
   */
  suspend fun insertComputation(
    globalId: String,
    protocol: ProtocolT,
    initialStage: StageT,
    stageDetails: StageDetailsT,
    computationDetails: ComputationDetailsT,
    requisitions: List<RequisitionEntry> = listOf(),
  )

  /**
   * Delete a computation for the local identifier.
   *
   * All related records in other tables are also removed.
   */
  suspend fun deleteComputation(localId: Long)

  /**
   * Adds a computation to the work queue to be worked on after a delay.
   *
   * This releases the lock on the computation, making it available for another worker to claim
   * after the specified delay.
   *
   * @param token the token for the computation to enqueue
   * @param delaySecond the number of seconds to wait before the computation is available to be
   *   claimed
   * @param expectedOwner the ID of the mill that is expected to be the current owner of the
   *   computation's lock. This is used as a precondition to prevent race conditions. The request
   *   will fail if this ID does not match the current lock owner.
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException if the
   *   Computation is not found
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
   *   if the [token] version does not match
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationLockOwnerMismatchException if
   *   the [expectedOwner] does not match the current lock owner
   */
  suspend fun enqueue(
    token: ComputationEditToken<ProtocolT, StageT>,
    delaySecond: Int,
    expectedOwner: String,
  )

  /**
   * Query for Computations with tasks ready for processing, and claim one for an owner.
   *
   * @param protocol The protocol of the task to claim
   * @param ownerId The identifier of the worker process that will own the lock.
   * @param lockDuration The time-to-live of the lock ownership.
   * @param prioritizedStages Stages that have the priority to be claimed.
   * @return global computation id of work that was claimed. When null, no work was claimed.
   */
  suspend fun claimTask(
    protocol: ProtocolT,
    ownerId: String,
    lockDuration: Duration,
    prioritizedStages: List<StageT> = listOf(),
  ): String?

  /**
   * Transitions a computation to a new stage.
   *
   * @param token The token for the computation
   * @param nextStage Stage this computation should transition to.
   * @param inputBlobPaths References to blobs that are inputs to this computation stage, all inputs
   *   should be written on transition and should not change.
   * @param passThroughBlobPaths References to blobs that are outputs of this computation stage, but
   *   were written before the start of the computation stage.
   * @param outputBlobs Number of blobs this computation outputs. These are created as part of the
   *   computation so they do not have a reference to the real storage location.
   * @param afterTransition The work to be do with the computation after a successful transition.
   * @param nextStageDetails Details specific to the next stage.
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException if the
   *   Computation is not found
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
   *   if the [token] version does not match
   */
  suspend fun updateComputationStage(
    token: ComputationEditToken<ProtocolT, StageT>,
    nextStage: StageT,
    inputBlobPaths: List<String>,
    passThroughBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition,
    nextStageDetails: StageDetailsT,
    lockExtension: Duration?,
  )

  /**
   * Moves a computation to a terminal state and records the reason why it ended.
   *
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException if the
   *   Computation is not found
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
   *   if the [token] version does not match
   */
  suspend fun endComputation(
    token: ComputationEditToken<ProtocolT, StageT>,
    endingStage: StageT,
    endComputationReason: EndComputationReason,
    computationDetails: ComputationDetailsT,
  )

  /**
   * Overrides the computationDetails of the computation using the given value.
   *
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException if the
   *   Computation is not found
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
   *   if the [token] version does not match
   */
  suspend fun updateComputationDetails(
    token: ComputationEditToken<ProtocolT, StageT>,
    computationDetails: ComputationDetailsT,
    requisitions: List<RequisitionEntry> = listOf(),
  )

  /**
   * Writes the reference to a blob needed for an output blob from a stage.
   *
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException if the
   *   Computation is not found
   * @throws org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
   *   if the [token] version does not match
   */
  suspend fun writeOutputBlobReference(
    token: ComputationEditToken<ProtocolT, StageT>,
    blobRef: BlobRef,
  )

  /**
   * Writes the blob path of requisition from a fulfillment.
   *
   * If secretSeedCiphertext is specified, also writes the secretSeedCiphertext.
   */
  suspend fun writeRequisitionBlobPath(
    token: ComputationEditToken<ProtocolT, StageT>,
    externalRequisitionKey: ExternalRequisitionKey,
    pathToBlob: String,
    publicApiVersion: String,
    protocol: RequisitionDetails.RequisitionProtocol? = null,
  )

  /** Inserts the specified [ComputationStatMetric] into the database. */
  suspend fun insertComputationStat(
    localId: Long,
    stage: StageT,
    attempt: Long,
    metric: ComputationStatMetric,
  )
}

/**
 * Reference to a blob's storage location (key).
 *
 * @param idInRelationalDatabase identifier of the blob as stored in the
 *   [ComputationsDatabaseTransactor]
 * @param key object key of the the blob which can be used to retrieve it from the blob storage.
 */
data class BlobRef(val idInRelationalDatabase: Long, val key: String)

/**
 * Reference to a ComputationStat metric that is captured in the Mill.
 *
 * @param name identifier of the metric.
 * @param value numerical value of the metric.
 */
data class ComputationStatMetric(val name: String, val value: Long)

/**
 * Specifies what to do with the lock on a computation after transitioning to a new stage.
 *
 * @see[ComputationsDatabaseTransactor.updateComputationStage]
 */
enum class AfterTransition {
  /** Retain and extend the lock for the current owner. */
  CONTINUE_WORKING,

  /**
   * Add the computation to the work queue, but in an unclaimed stage for some worker to claim at a
   * later time.
   */
  ADD_UNCLAIMED_TO_QUEUE,

  /**
   * Do not add to the work queue, and release any lock on the computation. There is no work to be
   * done on the computation at this time. Examples for when to set this include the computation
   * finished or input from another source is required before continuing.
   */
  DO_NOT_ADD_TO_QUEUE,
}

/**
 * Specifies why a computation has ended.
 *
 * @see[ComputationsDatabaseTransactor.endComputation]
 */
enum class EndComputationReason {
  /** Computation went the expected execution and succeeded. */
  SUCCEEDED,

  /** Computation failed and will not be retried again. */
  FAILED,

  /**
   * The computation was canceled. There were not known issues when it was ended, but results will
   * not be obtained.
   */
  CANCELED,
}
