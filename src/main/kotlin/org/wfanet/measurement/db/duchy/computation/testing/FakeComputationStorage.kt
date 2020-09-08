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

package org.wfanet.measurement.db.duchy.computation.testing

import io.grpc.Status
import org.wfanet.measurement.db.duchy.computation.AfterTransition
import org.wfanet.measurement.db.duchy.computation.BlobRef
import org.wfanet.measurement.db.duchy.computation.ComputationStorageEditToken
import org.wfanet.measurement.db.duchy.computation.EndComputationReason
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.db.duchy.computation.ProtocolStageEnumHelper
import org.wfanet.measurement.db.duchy.computation.SingleProtocolDatabase
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newInputBlobMetadata

/** In memory mapping of computation ids to [ComputationToken]s. */
class FakeComputationStorage(
  private val otherDuchies: List<String>
) : MutableMap<Long, ComputationToken> by mutableMapOf(),
  SingleProtocolDatabase,
  ProtocolStageEnumHelper<ComputationStage> by
  LiquidLegionsSketchAggregationProtocol.ComputationStages {
  companion object {
    const val NEXT_WORKER = "NEXT_WORKER"
    const val PRIMARY_WORKER = "PRIMARY_WORKER"
  }

  override val computationType: ComputationType =
    ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
  val claimedComputationIds = mutableSetOf<String>()

  override suspend fun insertComputation(
    globalId: String,
    initialStage: ComputationStage,
    stageDetails: ComputationStageDetails
  ) {
    if (globalId.toLong() in this) {
      throw Status.fromCode(Status.Code.ALREADY_EXISTS).asRuntimeException()
    }
    val role =
      if ((globalId.toLong() % 2) == 0L) RoleInComputation.PRIMARY
      else RoleInComputation.SECONDARY
    addComputation(
      globalId = globalId,
      stage = initialStage,
      role = role,
      stageDetails = stageDetails,
      blobs = listOf(newEmptyOutputBlobMetadata(id = 0L))
    )
  }

  /** Adds a fake computation to the fake computation storage. */
  fun addComputation(
    globalId: String,
    stage: ComputationStage,
    role: RoleInComputation,
    blobs: List<ComputationStageBlobMetadata>,
    stageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance()
  ) {
    val localId = globalId.toLong()
    require(localId !in this) {
      "Cannot add multiple computations with the same id. $localId"
    }
    this[localId] = ComputationToken.newBuilder().apply {
      globalComputationId = globalId
      // For the purpose of a fake it is fine to assume that the globalId can be parsed as Long and
      // use the Long value for the localId.
      localComputationId = localId
      computationStage = stage
      version = 0
      setRole(role)
      nextDuchy = NEXT_WORKER
      primaryDuchy = PRIMARY_WORKER
      attempt = 0
      addAllBlobs(blobs)
      if (stageDetails != ComputationStageDetails.getDefaultInstance()) {
        stageSpecificDetails = stageDetails
      }
    }.build()
  }

  /**
   * Changes the token for a computation to a new one and increments the lastUpdateTime.
   * Blob references are unchanged.
   *
   * @param tokenToUpdate token of the computation that will be changed.
   * @param changedTokenBuilderFunc function which returns a [ComputationToken.Builder] used to
   *   replace the [tokenToUpdate]. The version of the token is always incremented.
   */
  private fun updateToken(
    tokenToUpdate: ComputationStorageEditToken<ComputationStage>,
    changedTokenBuilderFunc: (ComputationToken) -> ComputationToken.Builder
  ) {
    val current = requireTokenFromCurrent(tokenToUpdate)
    this[tokenToUpdate.localId] =
      changedTokenBuilderFunc(current).setVersion(tokenToUpdate.editVersion + 1).build()
  }

  private fun requireTokenFromCurrent(
    token: ComputationStorageEditToken<ComputationStage>
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
    this[globalId] ?: error("No computation for $globalId")

  override suspend fun updateComputationStage(
    token: ComputationStorageEditToken<ComputationStage>,
    nextStage: ComputationStage,
    inputBlobPaths: List<String>,
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
        // Add output blob metadata to token.
        addAllBlobs(
          (0 until outputBlobs).map { idx ->
            newEmptyOutputBlobMetadata(idx.toLong() + inputBlobPaths.size)
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
          else -> error("Unknown $afterTransition")
        }
      }
    }
  }

  override suspend fun endComputation(
    token: ComputationStorageEditToken<ComputationStage>,
    endingStage: ComputationStage,
    endComputationReason: EndComputationReason
  ) {
    require(validTerminalStage(endingStage))
    updateToken(token) { existing ->
      claimedComputationIds.remove(existing.globalComputationId)
      existing.toBuilder().setComputationStage(endingStage).clearBlobs().clearStageSpecificDetails()
    }
  }

  override suspend fun writeOutputBlobReference(
    token: ComputationStorageEditToken<ComputationStage>,
    blobRef: BlobRef
  ) {
    updateToken(token) { existing ->
      val existingBlobInToken = newEmptyOutputBlobMetadata(blobRef.idInRelationalDatabase)
      val blobs: MutableSet<ComputationStageBlobMetadata> =
        getNonNull(existing.localComputationId).blobsList.toMutableSet()
      // Replace the blob metadata in the token.
      check(blobs.remove(existingBlobInToken)) { "$existingBlobInToken not in $blobs" }
      blobs.add(existingBlobInToken.toBuilder().setPath(blobRef.key).build())
      existing.toBuilder()
        .clearBlobs()
        .addAllBlobs(blobs)
    }
  }

  override suspend fun enqueue(token: ComputationStorageEditToken<ComputationStage>) {
    updateToken(token) { existing ->
      claimedComputationIds.remove(existing.globalComputationId)
      existing.toBuilder()
    }
  }

  override suspend fun claimTask(ownerId: String): String? {
    val claimed = values.asSequence()
      .filter { it.globalComputationId !in claimedComputationIds }
      .map {
        ComputationStorageEditToken(
          localId = it.localComputationId,
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

  override suspend fun readComputationToken(globalId: String): ComputationToken =
    getNonNull(globalId.toLong())

  override suspend fun readGlobalComputationIds(stages: Set<ComputationStage>): Set<String> =
    filterValues { it.computationStage in stages }.map { it.key.toString() }.toSet()
}
