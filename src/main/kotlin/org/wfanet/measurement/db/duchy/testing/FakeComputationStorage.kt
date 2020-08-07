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

package org.wfanet.measurement.db.duchy.testing

import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest.AfterTransition
import org.wfanet.measurement.db.duchy.LiquidLegionsSketchAggregationProtocol
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStage.StageCase
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.service.internal.duchy.computation.storage.newEmptyOutputBlobMetadata
import org.wfanet.measurement.service.internal.duchy.computation.storage.newInputBlobMetadata

/** In memory mapping of computation ids to [ComputationToken]s. */
class FakeComputationStorage(
  private val otherDuchies: List<String>
) : MutableMap<Long, ComputationToken> by mutableMapOf() {
  companion object {
    const val NEXT_WORKER = "NEXT_WORKER"
  }

  val claimedComputationIds = mutableSetOf<Long>()

  private fun validateStageChange(stage: ComputationStage, nextStage: ComputationStage): Boolean {
    return when (stage.stageCase) {
      StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION -> {
        LiquidLegionsSketchAggregationProtocol.ComputationStages.validTransition(
          stage,
          nextStage
        )
      }
      else -> error("Unsupported computation protocol with stage $stage.")
    }
  }

  private fun validEndingStage(stage: ComputationStage): Boolean {
    return when (stage.stageCase) {
      StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION -> {
        LiquidLegionsSketchAggregationProtocol.ComputationStages.validTerminalStage(stage)
      }
      else -> error("Unsupported computation protocol with stage $stage.")
    }
  }

  private fun stageDetails(stage: ComputationStage): ComputationStageDetails {
    return when (stage.stageCase) {
      StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION -> {
        LiquidLegionsSketchAggregationProtocol.ComputationStages.Details(otherDuchies)
          .detailsFor(stage)
      }
      else -> error("Unsupported computation protocol with stage $stage.")
    }
  }

  /** Adds a fake computation to the fake computation storage. */
  fun addComputation(
    id: Long,
    stage: ComputationStage,
    role: RoleInComputation,
    blobs: List<ComputationStageBlobMetadata>,
    stageDetails: ComputationStageDetails = ComputationStageDetails.getDefaultInstance()
  ) {
    this[id] = ComputationToken.newBuilder().apply {
      globalComputationId = id
      // For the purpose of a fake it is fine to use the same id for both local and global ids
      localComputationId = id
      computationStage = stage
      version = 0
      setRole(role)
      nextDuchy = NEXT_WORKER
      attempt = 0
      addAllBlobs(blobs)
      if (stageDetails != ComputationStageDetails.getDefaultInstance())
        stageSpecificDetails = stageDetails
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
    tokenToUpdate: ComputationToken,
    changedTokenBuilderFunc: () -> ComputationToken.Builder
  ) {
    requireTokenFromCurrent(tokenToUpdate)
    this[tokenToUpdate.localComputationId] =
      changedTokenBuilderFunc().setVersion(tokenToUpdate.version + 1).build()
  }

  private fun requireTokenFromCurrent(token: ComputationToken) {
    val current = this[token.localComputationId]!!
    // Just the last update time is checked because it mimics the way in which a relational database
    // will check the version of the update.
    require(current.version == token.version) {
      "Token provided $token != current token $current"
    }
  }

  fun getNonNull(globalId: Long): ComputationToken =
    this[globalId] ?: error("No computation for $globalId")

  fun advanceComputationStage(
    token: ComputationToken,
    nextStage: ComputationStage,
    inputBlobPaths: List<String>,
    outputBlobs: Int,
    afterTransition: AfterTransition
  ) {
    require(validateStageChange(token.computationStage, nextStage))
    updateToken(token) {
      // The next stage token will be a variant of the current token for the computation.
      token.toBuilder().apply {
        computationStage = nextStage

        clearStageSpecificDetails()
        val details = stageDetails(nextStage)
        if (details != ComputationStageDetails.getDefaultInstance()) {
          stageSpecificDetails = details
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
            claimedComputationIds.remove(token.globalComputationId)
          }
          AfterTransition.DO_NOT_ADD_TO_QUEUE -> {
            attempt = 1
            claimedComputationIds.remove(token.globalComputationId)
          }
          AfterTransition.RETAIN_AND_EXTEND_LOCK -> {
            attempt = 1
            claimedComputationIds.add(token.globalComputationId)
          }
          else -> error("Unknown $afterTransition")
        }
      }
    }
  }

  fun endComputation(
    token: ComputationToken,
    endingStage: ComputationStage
  ) {
    require(validEndingStage(endingStage))
    updateToken(token) {
      claimedComputationIds.remove(token.globalComputationId)
      token.toBuilder().setComputationStage(endingStage)
    }
  }

  fun writeOutputBlobReference(
    token: ComputationToken,
    blobId: Long,
    path: String
  ) {
    updateToken(token) {
      val existing = newEmptyOutputBlobMetadata(blobId)
      val blobs: MutableSet<ComputationStageBlobMetadata> =
        getNonNull(token.globalComputationId).blobsList.toMutableSet()
      // Replace the blob metadata in the token.
      check(blobs.remove(existing)) { "$existing not in $blobs" }
      blobs.add(existing.toBuilder().setPath(path).build())
      token.toBuilder()
        .clearBlobs()
        .addAllBlobs(blobs)
    }
  }

  fun enqueue(id: Long) {
    val token = getNonNull(id)
    updateToken(token) {
      claimedComputationIds.remove(id)
      token.toBuilder().setVersion(token.version + 1)
    }
  }

  fun claimTask(): ComputationToken? {
    val claimed = values.asSequence()
      .filter { it.globalComputationId !in claimedComputationIds }
      .firstOrNull()
      ?: return null

    updateToken(claimed) {
      claimedComputationIds.add(claimed.globalComputationId)
      claimed.toBuilder().setAttempt(claimed.attempt + 1)
    }
    return getNonNull(claimed.globalComputationId)
  }
}
