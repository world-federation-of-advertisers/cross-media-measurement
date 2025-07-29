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

package org.wfanet.measurement.duchy.db.computation.testing

import io.grpc.Status
import java.time.Duration
import java.time.Instant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.db.computation.toCompletedReason
import org.wfanet.measurement.duchy.service.internal.ComputationLockOwnerMismatchException
import org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computations.newPassThroughBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTokenKt
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionEntry
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.requisitionMetadata

/** In-memory [ComputationsDatabase] */
class FakeComputationsDatabase
private constructor(
  /** Map of local computation ID to [ComputationToken]. */
  private val tokens: MutableMap<Long, ComputationToken>,
  /** Map of [ExternalRequisitionKey] to local computation ID. */
  private val requisitionMap: MutableMap<ExternalRequisitionKey, Long>,
) :
  Map<Long, ComputationToken> by tokens,
  ComputationsDatabase,
  ComputationProtocolStagesEnumHelper<
    ComputationType,
    ComputationStage,
  > by ComputationProtocolStages {

  constructor() : this(tokens = mutableMapOf(), requisitionMap = mutableMapOf())

  /** Map of global computation ID to the owner of the lock. */
  val claimedComputations = mutableMapOf<String, String>()

  fun remove(localId: Long) = tokens.remove(localId)

  override suspend fun insertComputation(
    globalId: String,
    protocol: ComputationType,
    initialStage: ComputationStage,
    stageDetails: ComputationStageDetails,
    computationDetails: ComputationDetails,
    requisitions: List<RequisitionEntry>,
  ) {
    if (globalId.toLong() in tokens) {
      throw Status.fromCode(Status.Code.ALREADY_EXISTS).asRuntimeException()
    }
    addComputation(
      globalId = globalId,
      stage = initialStage,
      computationDetails = computationDetails,
      stageDetails = stageDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0L)),
      requisitions =
        requisitions.map {
          requisitionMetadata {
            externalKey = it.key
            details = it.value
          }
        },
    )
  }

  override suspend fun deleteComputation(localId: Long) {
    remove(localId)
  }

  /** Adds a fake computation to the [tokens] map. */
  fun addComputation(
    localId: Long,
    stage: ComputationStage,
    computationDetails: ComputationDetails,
    blobs: List<ComputationStageBlobMetadata> = listOf(),
    stageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance(),
    requisitions: List<RequisitionMetadata> = listOf(),
  ) {
    require(localId !in tokens) { "Cannot add multiple computations with the same id. $localId" }
    require(blobs.distinctBy { it.blobId }.size == blobs.size) { "Blobs must have distinct IDs" }

    tokens[localId] =
      newPartialToken(localId, stage)
        .apply {
          setComputationDetails(computationDetails)
          addAllBlobs(blobs)
          if (stageDetails !== ComputationStageDetails.getDefaultInstance()) {
            stageSpecificDetails = stageDetails
          }
          addAllRequisitions(requisitions)
        }
        .build()
    requisitions.forEach { requisitionMap[it.externalKey] = localId }
  }

  /** @see addComputation */
  fun addComputation(
    globalId: String,
    stage: ComputationStage,
    computationDetails: ComputationDetails,
    blobs: List<ComputationStageBlobMetadata> = listOf(),
    stageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance(),
    requisitions: List<RequisitionMetadata> = listOf(),
  ) {
    addComputation(
      // For the purpose of a fake it is fine to assume that the globalId can be parsed as Long
      // and use the Long value for the localId.
      localId = globalId.toLong(),
      stage = stage,
      computationDetails = computationDetails,
      blobs = blobs,
      stageDetails = stageDetails,
      requisitions = requisitions,
    )
  }

  /**
   * Changes the token for a computation to a new one and increments the lastUpdateTime. Blob
   * references are unchanged.
   *
   * @param tokenToUpdate token of the computation that will be changed.
   * @param fillUpdatedToken function which fills the token to replace [tokenToUpdate]. The version
   *   of the token is always incremented.
   */
  private inline fun updateToken(
    tokenToUpdate: ComputationEditToken<ComputationType, ComputationStage>,
    fillUpdatedToken: ComputationTokenKt.Dsl.() -> Unit,
  ) {
    val current = requireTokenFromCurrent(tokenToUpdate)
    tokens[tokenToUpdate.localId] =
      current.copy {
        fillUpdatedToken()
        version = tokenToUpdate.editVersion + 1
      }
  }

  private fun requireTokenFromCurrent(
    token: ComputationEditToken<ComputationType, ComputationStage>
  ): ComputationToken {
    val current = getNonNull(token.localId)
    // Just the last update time is checked because it mimics the way in which a relational database
    // will check the version of the update.
    if (current.version != token.editVersion) {
      throw ComputationTokenVersionMismatchException(
        computationId = token.localId,
        version = current.version,
        tokenVersion = token.editVersion,
        message = "Token provided $token != current token $current",
      )
    }
    return current
  }

  private fun getNonNull(globalId: Long): ComputationToken =
    tokens[globalId] ?: error("No computation for $globalId")

  override suspend fun updateComputationStage(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    nextStage: ComputationStage,
    inputBlobPaths: List<String>,
    passThroughBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition,
    nextStageDetails: ComputationStageDetails,
    lockExtension: Duration?,
  ) {
    updateToken(token) {
      require(validTransition(computationStage, nextStage))
      // The next stage token will be a variant of the current token for the computation.
      computationStage = nextStage

      clearStageSpecificDetails()
      if (nextStageDetails != ComputationStageDetails.getDefaultInstance()) {
        stageSpecificDetails = nextStageDetails
      }

      // The blob metadata will always be different.
      blobs.clear()
      // Add input blob metadata to token.
      blobs +=
        inputBlobPaths.mapIndexed { idx, objectKey ->
          newInputBlobMetadata(id = idx.toLong(), key = objectKey)
        }
      // Add input blob metadata to token.
      blobs +=
        passThroughBlobPaths.mapIndexed { idx, objectKey ->
          newPassThroughBlobMetadata(id = idx.toLong() + inputBlobPaths.size, key = objectKey)
        }
      // Add output blob metadata to token.
      blobs +=
        (0 until outputBlobs).map { idx ->
          newEmptyOutputBlobMetadata(idx.toLong() + inputBlobPaths.size + passThroughBlobPaths.size)
        }
      // Set attempt number and presence in the queue.
      when (afterTransition) {
        AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> {
          attempt = 0
          claimedComputations.remove(globalComputationId)
        }
        AfterTransition.DO_NOT_ADD_TO_QUEUE -> {
          attempt = 1
          claimedComputations.remove(globalComputationId)
        }
        AfterTransition.CONTINUE_WORKING -> {
          attempt = 1
        }
      }
    }
  }

  override suspend fun updateComputationDetails(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    computationDetails: ComputationDetails,
    requisitions: List<RequisitionEntry>,
  ) {
    updateToken(token) {
      this.computationDetails = computationDetails

      val requisitionMetadataByKey =
        this.requisitions.associateByTo(mutableMapOf()) { it.externalKey }
      for (requisition in requisitions) {
        val existingMetadata =
          requisitionMetadataByKey[requisition.key]
            ?: error("Requisition not found: ${requisition.key.toJson()}")
        requisitionMetadataByKey[requisition.key] =
          existingMetadata.copy { details = requisition.value }
      }

      this.requisitions.clear()
      this.requisitions += requisitionMetadataByKey.values
    }
  }

  override suspend fun endComputation(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    endingStage: ComputationStage,
    endComputationReason: EndComputationReason,
    computationDetails: ComputationDetails,
  ) {
    require(validTerminalStage(token.protocol, endingStage))
    updateToken(token) {
      claimedComputations.remove(globalComputationId)
      computationStage = endingStage
      this.computationDetails =
        computationDetails.copy { endingState = endComputationReason.toCompletedReason() }
      blobs.clear()
      clearStageSpecificDetails()
    }
  }

  override suspend fun writeOutputBlobReference(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    blobRef: BlobRef,
  ) {
    updateToken(token) {
      val existingBlobInToken = newEmptyOutputBlobMetadata(blobRef.idInRelationalDatabase)
      val blobs: MutableSet<ComputationStageBlobMetadata> =
        getNonNull(localComputationId).blobsList.toMutableSet()
      // Replace the blob metadata in the token.
      check(blobs.remove(existingBlobInToken)) { "$existingBlobInToken not in $blobs" }
      blobs.add(existingBlobInToken.copy { path = blobRef.key })

      this.blobs.clear()
      this.blobs += blobs
    }
  }

  override suspend fun writeRequisitionBlobPath(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    externalRequisitionKey: ExternalRequisitionKey,
    pathToBlob: String,
    publicApiVersion: String,
    protocol: RequisitionDetails.RequisitionProtocol?,
  ) {
    updateToken(token) {
      val requisitionIndex = requisitions.indexOfFirst { it.externalKey == externalRequisitionKey }
      if (requisitionIndex < 0) {
        error("Requisition not found: ${externalRequisitionKey.toJson()}")
      }
      requisitions[requisitionIndex] =
        requisitions[requisitionIndex].copy {
          path = pathToBlob
          details =
            details.copy {
              this.publicApiVersion = publicApiVersion
              if (protocol != null) {
                this.protocol = protocol
              }
            }
        }
    }
  }

  override suspend fun enqueue(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    delaySecond: Int,
    expectedOwner: String,
  ) {
    val currentOwner = claimedComputations[token.globalId]
    if (currentOwner != null && currentOwner != expectedOwner) {
      throw ComputationLockOwnerMismatchException(token.localId, expectedOwner, currentOwner)
    }
    // ignore the delaySecond in the fake
    updateToken(token) { claimedComputations.remove(globalComputationId) }
  }

  override suspend fun claimTask(
    protocol: ComputationType,
    ownerId: String,
    lockDuration: Duration,
    prioritizedStages: List<ComputationStage>,
  ): String? {
    val editTokens =
      tokens.values
        .asSequence()
        .filter { !claimedComputations.containsKey(it.globalComputationId) }
        .map {
          ComputationEditToken(
            localId = it.localComputationId,
            protocol =
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (it.computationStage.stageCase) {
                ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
                  ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
                ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
                  ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
                ComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
                  ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE
                ComputationStage.StageCase.TRUS_TEE -> ComputationType.TRUS_TEE
                ComputationStage.StageCase.STAGE_NOT_SET ->
                  error("Computation type for $it is unknown")
              },
            stage = it.computationStage,
            attempt = it.attempt,
            editVersion = it.version,
            globalId = it.globalComputationId,
          )
        }
    val prioritizedComputations = editTokens.filter { it.stage in prioritizedStages }
    val claimed =
      if (!prioritizedComputations.none()) {
        prioritizedComputations.first()
      } else {
        editTokens.firstOrNull() ?: return null
      }

    updateToken(claimed) {
      claimedComputations[globalComputationId] = ownerId
      this.attempt = claimed.attempt + 1
    }
    return claimed.localId.toString()
  }

  override suspend fun readComputationToken(globalId: String): ComputationToken? =
    tokens[globalId.toLong()]

  override suspend fun readComputationToken(
    externalRequisitionKey: ExternalRequisitionKey
  ): ComputationToken? = tokens[requisitionMap[externalRequisitionKey]]

  override suspend fun readGlobalComputationIds(
    stages: Set<ComputationStage>,
    updatedBefore: Instant?,
  ): Set<String> =
    tokens.filterValues { it.computationStage in stages }.map { it.key.toString() }.toSet()

  /** For testing purposes, doesn't do anything useful. */
  override suspend fun insertComputationStat(
    localId: Long,
    stage: ComputationStage,
    attempt: Long,
    metric: ComputationStatMetric,
  ) {
    require(metric.name.isNotEmpty())
  }

  override suspend fun readComputationBlobKeys(localId: Long): List<String> {
    return listOf("${localId}_1", "${localId}_2")
  }

  override suspend fun readRequisitionBlobKeys(localId: Long): List<String> {
    return listOf("${localId}_1", "${localId}_2")
  }

  companion object {
    fun newPartialToken(localId: Long, stage: ComputationStage): ComputationToken.Builder {
      return ComputationToken.newBuilder().apply {
        globalComputationId = localId.toString()
        localComputationId = localId
        computationStage = stage
        version = 0
        attempt = 0
      }
    }
  }
}
