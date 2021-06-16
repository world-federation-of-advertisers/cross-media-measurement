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
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStages
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.db.computation.ExternalRequisitionKey
import org.wfanet.measurement.duchy.db.computation.RequisitionDetailUpdate
import org.wfanet.measurement.duchy.db.computation.toCompletedReason
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newInputBlobMetadata
import org.wfanet.measurement.duchy.service.internal.computation.newPassThroughBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.RequisitionMetadata

/** In-memory [ComputationsDatabase] */
class FakeComputationsDatabase
private constructor(
  /** Map of local computation ID to [ComputationToken]. */
  private val tokens: MutableMap<Long, ComputationToken>,
  /**
   * Map of external_requisition_id to local computation id. External_data_provider_id is ignored in
   * this fake implementation.
   */
  private val requisitionMap: MutableMap<String, Long>
) :
  Map<Long, ComputationToken> by tokens,
  ComputationsDatabase,
  ComputationProtocolStagesEnumHelper<
    ComputationType, ComputationStage> by ComputationProtocolStages {

  constructor() : this(tokens = mutableMapOf(), requisitionMap = mutableMapOf())

  val claimedComputationIds = mutableSetOf<String>()

  fun remove(localId: Long) = tokens.remove(localId)

  override suspend fun insertComputation(
    globalId: String,
    protocol: ComputationType,
    initialStage: ComputationStage,
    stageDetails: ComputationStageDetails,
    computationDetails: ComputationDetails,
    requisitions: List<ExternalRequisitionKey>
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
          RequisitionMetadata.newBuilder()
            .apply {
              externalDataProviderId = it.externalDataProviderId
              externalRequisitionId = it.externalRequisitionId
            }
            .build()
        }
    )
  }

  /** Adds a fake computation to the [tokens] map. */
  fun addComputation(
    localId: Long,
    stage: ComputationStage,
    computationDetails: ComputationDetails,
    blobs: List<ComputationStageBlobMetadata> = listOf(),
    stageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance(),
    requisitions: List<RequisitionMetadata> = listOf()
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
    requisitions.forEach {
      // externalDataProviderId is ignored in this fake implementation.
      requisitionMap[it.externalRequisitionId] = localId
    }
  }

  /** @see addComputation */
  fun addComputation(
    globalId: String,
    stage: ComputationStage,
    computationDetails: ComputationDetails,
    blobs: List<ComputationStageBlobMetadata> = listOf(),
    stageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance(),
    requisitions: List<RequisitionMetadata> = listOf()
  ) {
    addComputation(
      // For the purpose of a fake it is fine to assume that the globalId can be parsed as Long and
      // use the Long value for the localId.
      localId = globalId.toLong(),
      stage = stage,
      computationDetails = computationDetails,
      blobs = blobs,
      stageDetails = stageDetails,
      requisitions = requisitions
    )
  }

  /**
   * Changes the token for a computation to a new one and increments the lastUpdateTime. Blob
   * references are unchanged.
   *
   * @param tokenToUpdate token of the computation that will be changed.
   * @param changedTokenBuilderFunc function which returns a [ComputationToken.Builder] used to
   * replace the [tokenToUpdate]. The version of the token is always incremented.
   */
  private fun updateToken(
    tokenToUpdate: ComputationEditToken<ComputationType, ComputationStage>,
    changedTokenBuilderFunc: (ComputationToken) -> ComputationToken.Builder
  ) {
    val current = requireTokenFromCurrent(tokenToUpdate)
    tokens[tokenToUpdate.localId] =
      changedTokenBuilderFunc(current).setVersion(tokenToUpdate.editVersion + 1).build()
  }

  private fun requireTokenFromCurrent(
    token: ComputationEditToken<ComputationType, ComputationStage>
  ): ComputationToken {
    val current = getNonNull(token.localId)
    // Just the last update time is checked because it mimics the way in which a relational database
    // will check the version of the update.
    require(current.version == token.editVersion) {
      "Token provided $token != current token $current"
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
    nextStageDetails: ComputationStageDetails
  ) {
    updateToken(token) { existing ->
      require(validTransition(existing.computationStage, nextStage))
      // The next stage token will be a variant of the current token for the computation.
      existing.toBuilder().apply {
        computationStage = nextStage

        clearStageSpecificDetails()
        if (nextStageDetails != ComputationStageDetails.getDefaultInstance()) {
          stageSpecificDetails = nextStageDetails
        }

        // The blob metadata will always be different.
        clearBlobs()
        // Add input blob metadata to token.
        addAllBlobs(
          inputBlobPaths.mapIndexed { idx, objectKey ->
            newInputBlobMetadata(id = idx.toLong(), key = objectKey)
          }
        )
        // Add input blob metadata to token.
        addAllBlobs(
          passThroughBlobPaths.mapIndexed { idx, objectKey ->
            newPassThroughBlobMetadata(id = idx.toLong() + inputBlobPaths.size, key = objectKey)
          }
        )
        // Add output blob metadata to token.
        addAllBlobs(
          (0 until outputBlobs).map { idx ->
            newEmptyOutputBlobMetadata(
              idx.toLong() + inputBlobPaths.size + passThroughBlobPaths.size
            )
          }
        )
        // Set attempt number and presence in the queue.
        when (afterTransition) {
          AfterTransition.ADD_UNCLAIMED_TO_QUEUE -> {
            attempt = 0
            claimedComputationIds.remove(existing.globalComputationId)
          }
          AfterTransition.DO_NOT_ADD_TO_QUEUE -> {
            attempt = 1
            claimedComputationIds.remove(existing.globalComputationId)
          }
          AfterTransition.CONTINUE_WORKING -> {
            attempt = 1
            claimedComputationIds.add(existing.globalComputationId)
          }
        }
      }
    }
  }

  override suspend fun updateComputationDetails(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    computationDetails: ComputationDetails,
    requisitionDetailUpdates: List<RequisitionDetailUpdate>
  ) {
    updateToken(token) { existing ->
      val tokenBuilder = existing.toBuilder().setComputationDetails(computationDetails)
      val requisitionBuildersByExternalId =
        tokenBuilder.requisitionsBuilderList.associateBy { it.externalRequisitionId }
      for (update in requisitionDetailUpdates) {
        requisitionBuildersByExternalId[update.key.externalRequisitionId]?.setDetails(update.detail)
          ?: error("requisition not found")
      }
      tokenBuilder
    }
  }

  override suspend fun endComputation(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    endingStage: ComputationStage,
    endComputationReason: EndComputationReason
  ) {
    require(validTerminalStage(token.protocol, endingStage))
    updateToken(token) { existing ->
      claimedComputationIds.remove(existing.globalComputationId)
      existing.toBuilder().also {
        it.computationStage = endingStage
        it.computationDetailsBuilder.endingState = endComputationReason.toCompletedReason()
        it.clearBlobs()
        it.clearStageSpecificDetails()
      }
    }
  }

  override suspend fun writeOutputBlobReference(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    blobRef: BlobRef
  ) {
    updateToken(token) { existing ->
      val existingBlobInToken = newEmptyOutputBlobMetadata(blobRef.idInRelationalDatabase)
      val blobs: MutableSet<ComputationStageBlobMetadata> =
        getNonNull(existing.localComputationId).blobsList.toMutableSet()
      // Replace the blob metadata in the token.
      check(blobs.remove(existingBlobInToken)) { "$existingBlobInToken not in $blobs" }
      blobs.add(existingBlobInToken.toBuilder().setPath(blobRef.key).build())
      existing.toBuilder().clearBlobs().addAllBlobs(blobs)
    }
  }

  override suspend fun writeRequisitionBlobPath(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    externalRequisitionKey: ExternalRequisitionKey,
    pathToBlob: String
  ) {
    updateToken(token) { existing ->
      val tokenBuilder = existing.toBuilder()
      tokenBuilder
        .requisitionsBuilderList
        .find { it.externalRequisitionId == externalRequisitionKey.externalRequisitionId }
        ?.setPath(pathToBlob)
        ?: error("requisition not found")
      tokenBuilder
    }
  }

  override suspend fun enqueue(
    token: ComputationEditToken<ComputationType, ComputationStage>,
    delaySecond: Int
  ) {
    // ignore the delaySecond in the fake
    updateToken(token) { existing ->
      claimedComputationIds.remove(existing.globalComputationId)
      existing.toBuilder()
    }
  }

  override suspend fun claimTask(protocol: ComputationType, ownerId: String): String? {
    val claimed =
      tokens
        .values
        .asSequence()
        .filter { it.globalComputationId !in claimedComputationIds }
        .map {
          ComputationEditToken(
            localId = it.localComputationId,
            protocol =
              when (it.computationStage.stageCase) {
                ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
                  ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
                else -> error("Computation type for $it is unknown")
              },
            stage = it.computationStage,
            attempt = it.attempt,
            editVersion = it.version
          )
        }
        .firstOrNull()
        ?: return null

    updateToken(claimed) { existing ->
      claimedComputationIds.add(existing.globalComputationId)
      existing.toBuilder().setAttempt(claimed.attempt + 1)
    }
    return claimed.localId.toString()
  }

  override suspend fun readComputationToken(globalId: String): ComputationToken? =
    tokens[globalId.toLong()]

  override suspend fun readComputationToken(
    externalRequisitionKey: ExternalRequisitionKey
  ): ComputationToken? = tokens[requisitionMap[externalRequisitionKey.externalRequisitionId]]

  override suspend fun readGlobalComputationIds(stages: Set<ComputationStage>): Set<String> =
    tokens.filterValues { it.computationStage in stages }.map { it.key.toString() }.toSet()

  /** For testing purposes, doesn't do anything useful. */
  override suspend fun insertComputationStat(
    localId: Long,
    stage: ComputationStage,
    attempt: Long,
    metric: ComputationStatMetric
  ) {
    require(metric.name.isNotEmpty())
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
