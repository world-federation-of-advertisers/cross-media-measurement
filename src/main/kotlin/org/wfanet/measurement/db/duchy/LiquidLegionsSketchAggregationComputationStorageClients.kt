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

import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.SketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.SketchAggregationStage.CREATED
import org.wfanet.measurement.internal.SketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationStage.UNKNOWN
import org.wfanet.measurement.internal.SketchAggregationStage.UNRECOGNIZED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest.AfterTransition
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails.RoleInComputation
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toBlobPath
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toProtocolStage

/**
 *
 * Storage clients specific to running the Privacy-Preserving Secure Cardinality and
 * Frequency Estimation protocol using sparse representation of
 * Liquid Legions Cardinality Estimator sketches.
 */
class LiquidLegionsSketchAggregationComputationStorageClients(
  val computationStorageClient: ComputationStorageServiceCoroutineStub,
  private val blobDatabase: ComputationsBlobDb<SketchAggregationStage>,
  otherDuchies: List<String>
) {

  val liquidLegionsStageDetails: LiquidLegionsSketchAggregationProtocol.EnumStages.Details =
    LiquidLegionsSketchAggregationProtocol.EnumStages.Details(otherDuchies)

  private val otherDuchiesInComputation: Int = otherDuchies.size

  /**
   * Calls AdvanceComputationStage to move to a new stage in a consistent way.
   *
   * The assumption is this will only be called by a job that is executing the stage of a
   * computation, which will have knowledge of all the data needed as input to the next stage.
   * Most of the time [inputsToNextStage] is the list of outputs of the currently running stage.
   */
  suspend fun transitionComputationToStage(
    computationToken: ComputationToken,
    inputsToNextStage: List<String> = listOf(),
    stage: SketchAggregationStage
  ): ComputationToken {
    requireValidRoleForStage(stage, computationToken.role)
    val advanceStageRequestBuilder = AdvanceComputationStageRequest.newBuilder().apply {
      token = computationToken
      nextComputationStage = stage.toProtocolStage()
      addAllInputBlobs(inputsToNextStage)
      outputBlobs = 1
      // TODO: Pass request.stageDetails to updateComputationStage for it to write. Currently the
      //   details are set to a default value based on the stage, which is not so flexible to the
      //   caller. What to write is in the request but is being ignored.
      // stageDetails = liquidLegionsStageDetails.detailsFor(stage)
    }
    val request: AdvanceComputationStageRequest = when (stage) {
      // Stages of computation creating a single output without any input blobs.
      TO_ADD_NOISE ->
        advanceStageRequestBuilder.apply {
          outputBlobs = 1
          afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }.build()
      // Stages of computation mapping some number of inputs to single output.
      TO_APPEND_SKETCHES_AND_ADD_NOISE,
      TO_BLIND_POSITIONS,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
        advanceStageRequestBuilder.apply {
          requireNotEmpty(inputBlobsList)
          outputBlobs = 1
          afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        }.build()
      // The primary duchy is waiting for input from all the other duchies. This is a special case
      // of the other wait stages as it has n-1 outputs.
      WAIT_SKETCHES ->
        advanceStageRequestBuilder.apply {
          // The output contains otherDuchiesInComputation sketches from the other duchies.
          outputBlobs = otherDuchiesInComputation
          afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
        }.build()
      // Stages were the duchy is waiting for a single input from the predecessor duchy.
      WAIT_CONCATENATED,
      WAIT_FLAG_COUNTS ->
        advanceStageRequestBuilder.apply {
          requireNotEmpty(inputBlobsList)
          // Requires an output to be written e.g., the sketch sent by the predecessor duchy.
          outputBlobs = 1
          // Mill have nothing to do for this stage.
          afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
        }.build()
      COMPLETED -> error("Computation should be ended with call to endComputation(...)")
      // Stages that we can't transition to ever.
      UNRECOGNIZED, UNKNOWN, CREATED -> error("Cannot make transition function to stage $stage")
    }
    return computationStorageClient.advanceComputationStage(request).token
  }

  private fun requireValidRoleForStage(stage: SketchAggregationStage, role: RoleInComputation) {
    when (stage) {
      WAIT_SKETCHES,
      TO_APPEND_SKETCHES_AND_ADD_NOISE,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS -> require(role == RoleInComputation.PRIMARY) {
        "$stage may only be executed by the primary MPC worker."
      }
      TO_ADD_NOISE,
      TO_BLIND_POSITIONS,
      TO_DECRYPT_FLAG_COUNTS -> require(role == RoleInComputation.SECONDARY) {
        "$stage may only be executed by a non-primary MPC worker."
      }
      else -> { /* Stage can be executed at either primary or non-primary */ }
    }
  }

  /**
   * Writes the concatenated sketch as a blob and update the stage's output blobref to point to
   * the sketch.
   *
   * When the concatenated sketch already exists, no blob is written, but the path to the blob
   * is returned.
   *
   * @return Pair of token after updating blob reference, and path to written blob.
   */
  suspend fun writeReceivedConcatenatedSketch(
    computationToken: ComputationToken,
    sketch: ByteArray
  ): Pair<ComputationToken, String> {
    val onlyOutputBlob =
      computationToken.blobsList.single { it.dependencyType == ComputationBlobDependency.OUTPUT }
    return writeExpectedBlobIfNotPresent(
      requiredStage = WAIT_CONCATENATED,
      nameForBlob = "concatenated_sketch",
      storageToken = computationToken,
      bytes = sketch,
      blobId = onlyOutputBlob.blobId,
      existingPath = onlyOutputBlob.path
    )
  }

  suspend fun writeReceivedFlagsAndCounts(
    computationToken: ComputationToken,
    encryptedFlagCounts: ByteArray
  ): Pair<ComputationToken, String> {
    val onlyOutputBlob =
      computationToken.blobsList.single { it.dependencyType == ComputationBlobDependency.OUTPUT }
    return writeExpectedBlobIfNotPresent(
      requiredStage = WAIT_FLAG_COUNTS,
      nameForBlob = "encrypted_flag_counts",
      storageToken = computationToken,
      bytes = encryptedFlagCounts,
      blobId = onlyOutputBlob.blobId,
      existingPath = onlyOutputBlob.path
    )
  }

  suspend fun writeReceivedNoisedSketch(
    computationToken: ComputationToken,
    sketch: ByteArray,
    sender: String
  ): ComputationToken {
    // Get the blob id by looking up the sender in the stage specific details.
    val stageDetails = computationToken.stageSpecificDetails.waitSketchStageDetails
    val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[sender])
    val outputBlob = computationToken.blobsList.single {
      it.dependencyType == ComputationBlobDependency.OUTPUT &&
        it.blobId == blobId
    }
    val (newToken, _) = writeExpectedBlobIfNotPresent(
      requiredStage = WAIT_SKETCHES,
      nameForBlob = "noised_sketch_$sender",
      storageToken = computationToken,
      bytes = sketch,
      blobId = blobId,
      existingPath = outputBlob.path
    )
    return newToken
  }

  private suspend fun writeExpectedBlobIfNotPresent(
    requiredStage: SketchAggregationStage,
    nameForBlob: String,
    storageToken: ComputationToken,
    bytes: ByteArray,
    blobId: Long,
    existingPath: String
  ): Pair<ComputationToken, String> {
    require(storageToken.computationStage.liquidLegionsSketchAggregation == requiredStage) {
      "Cannot accept $nameForBlob while in stage ${storageToken.computationStage}"
    }
    // Return the path to the already written blob if one exists.
    if (existingPath.isNotEmpty()) return Pair(storageToken, existingPath)

    // Write the blob to a new path if there is not already a reference saved for it in
    // the relational database.
    val newPath = storageToken.toBlobPath(nameForBlob)
    blobDatabase.blockingWrite(newPath, bytes)
    computationStorageClient.recordOutputBlobPath(
      RecordOutputBlobPathRequest.newBuilder().apply {
        token = storageToken
        outputBlobId = blobId
        blobPath = newPath
      }.build()
    )
    val newToken =
      computationStorageClient.getComputationToken(
        storageToken.globalComputationId.toGetTokenRequest()
      ).token
    return Pair(newToken, newPath)
  }

  suspend fun readInputBlobs(token: ComputationToken): Map<ComputationStageBlobMetadata, ByteArray> =
    token.blobsList.filter { it.dependencyType == ComputationBlobDependency.INPUT }
      .map { it to blobDatabase.read(BlobRef(it.blobId, it.path)) }
      .toMap()
}

private fun requireNotEmpty(paths: List<String>): List<String> {
  require(paths.isNotEmpty()) { "Passed paths to input blobs is empty" }
  return paths
}
