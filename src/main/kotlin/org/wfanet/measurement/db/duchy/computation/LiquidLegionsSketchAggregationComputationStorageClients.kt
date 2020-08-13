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

package org.wfanet.measurement.db.duchy.computation

import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.SKETCH_AGGREGATION_STAGE_UNKNOWN
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_APPEND_SKETCHES_AND_ADD_NOISE
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.UNRECOGNIZED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_SKETCHES
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage.WAIT_TO_START
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
  private val blobDatabase: ComputationsBlobDb<LiquidLegionsSketchAggregationStage>,
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
    inputsToNextStage: List<String>,
    stage: LiquidLegionsSketchAggregationStage
  ): ComputationToken {
    requireValidRoleForStage(stage, computationToken.role)
    requireNotEmpty(inputsToNextStage)
    val request: AdvanceComputationStageRequest =
      AdvanceComputationStageRequest.newBuilder().apply {
        token = computationToken
        nextComputationStage = stage.toProtocolStage()
        addAllInputBlobs(inputsToNextStage)
        stageDetails = liquidLegionsStageDetails.detailsFor(stage)
        afterTransition = afterTransitionForStage(stage)
        outputBlobs = outputBlobsForStage(stage)
      }.build()
    return computationStorageClient.advanceComputationStage(request).token
  }

  private fun outputBlobsForStage(stage: LiquidLegionsSketchAggregationStage): Int =
    when (stage) {
      WAIT_TO_START ->
        // There is no output in this stage, the input is forwarded to the next stage as input.
        0
      WAIT_CONCATENATED,
      WAIT_FLAG_COUNTS,
      TO_ADD_NOISE,
      TO_APPEND_SKETCHES_AND_ADD_NOISE,
      TO_BLIND_POSITIONS,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
        // The output is the intermediate computation result either received from another duchy
        // or computed locally.
        1
      WAIT_SKETCHES ->
        // The output contains otherDuchiesInComputation sketches from the other duchies.
        otherDuchiesInComputation
      // Mill have nothing to do for this stage.
      COMPLETED -> error("Computation should be ended with call to endComputation(...)")
      // Stages that we can't transition to ever.
      UNRECOGNIZED, SKETCH_AGGREGATION_STAGE_UNKNOWN, TO_CONFIRM_REQUISITIONS ->
        error("Cannot make transition function to stage $stage")
    }

  private fun afterTransitionForStage(stage: LiquidLegionsSketchAggregationStage): AfterTransition =
    when (stage) {
      // Stages of computation mapping some number of inputs to single output.
      TO_ADD_NOISE,
      TO_APPEND_SKETCHES_AND_ADD_NOISE,
      TO_BLIND_POSITIONS,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS ->
        AfterTransition.ADD_UNCLAIMED_TO_QUEUE
      WAIT_TO_START,
      WAIT_SKETCHES,
      WAIT_CONCATENATED,
      WAIT_FLAG_COUNTS ->
        AfterTransition.DO_NOT_ADD_TO_QUEUE
      COMPLETED -> error("Computation should be ended with call to endComputation(...)")
      // Stages that we can't transition to ever.
      UNRECOGNIZED, SKETCH_AGGREGATION_STAGE_UNKNOWN, TO_CONFIRM_REQUISITIONS ->
        error("Cannot make transition function to stage $stage")
    }

  private fun requireValidRoleForStage(
    stage: LiquidLegionsSketchAggregationStage,
    role: RoleInComputation
  ) {
    when (stage) {
      WAIT_SKETCHES,
      TO_APPEND_SKETCHES_AND_ADD_NOISE,
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS -> require(role == RoleInComputation.PRIMARY) {
        "$stage may only be executed by the primary MPC worker."
      }
      WAIT_TO_START,
      TO_ADD_NOISE,
      TO_BLIND_POSITIONS,
      TO_DECRYPT_FLAG_COUNTS -> require(role == RoleInComputation.SECONDARY) {
        "$stage may only be executed by a non-primary MPC worker."
      }
      else -> { /* Stage can be executed at either primary or non-primary */
      }
    }
  }

  /**
   * Writes the encrypted sketch with added noise from another duchy as an output blob to the
   * current stage.
   *
   * @return [ComputationToken] after updating blob reference. When the output already exists,
   * no blob is written, but the returned token will have a path to the previously written blob.
   */
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
    return writeExpectedBlobIfNotPresent(
      requiredStage = WAIT_SKETCHES,
      nameForBlob = "noised_sketch_$sender",
      computationToken = computationToken,
      bytes = sketch,
      blobId = blobId,
      existingPath = outputBlob.path
    )
  }

  /**
   * Writes the data as a single output blob to the current stage.
   *
   * @return [ComputationToken] after updating blob reference. When the output already exists,
   * no blob is written, but the returned token will have a path to the previously written blob.
   */
  suspend fun writeSingleOutputBlob(
    computationToken: ComputationToken,
    data: ByteArray
  ): ComputationToken {
    val onlyOutputBlob = computationToken.singleOutputBlobMetadata()
    return writeExpectedBlobIfNotPresent(
      requiredStage = computationToken.computationStage.liquidLegionsSketchAggregation,
      nameForBlob = "output",
      computationToken = computationToken,
      bytes = data,
      blobId = onlyOutputBlob.blobId,
      existingPath = onlyOutputBlob.path
    )
  }

  private suspend fun writeExpectedBlobIfNotPresent(
    requiredStage: LiquidLegionsSketchAggregationStage,
    nameForBlob: String,
    computationToken: ComputationToken,
    bytes: ByteArray,
    blobId: Long,
    existingPath: String
  ): ComputationToken {
    require(computationToken.computationStage.liquidLegionsSketchAggregation == requiredStage) {
      "Cannot accept $nameForBlob while in stage ${computationToken.computationStage}"
    }
    // Return the path to the already written blob if one exists.
    if (existingPath.isNotEmpty()) return computationToken

    // Write the blob to a new path if there is not already a reference saved for it in
    // the relational database.
    val newPath = computationToken.toBlobPath(nameForBlob)
    blobDatabase.blockingWrite(newPath, bytes)
    computationStorageClient.recordOutputBlobPath(
      RecordOutputBlobPathRequest.newBuilder().apply {
        token = computationToken
        outputBlobId = blobId
        blobPath = newPath
      }.build()
    )
    return computationStorageClient.getComputationToken(
      computationToken.globalComputationId.toGetTokenRequest()
    ).token
  }

  /**
   * Returns a map of [ComputationStageBlobMetadata] to the actual bytes of the BLOB for all inputs
   * to the stage.
   */
  suspend fun readInputBlobs(
    token: ComputationToken
  ): Map<ComputationStageBlobMetadata, ByteArray> =
    token.blobsList.filter { it.dependencyType == ComputationBlobDependency.INPUT }
      .map { it to blobDatabase.read(BlobRef(it.blobId, it.path)) }
      .toMap()
}

private fun requireNotEmpty(paths: List<String>): List<String> {
  require(paths.isNotEmpty()) { "Passed paths to input blobs is empty" }
  return paths
}

/**
 * Returns the single [ComputationStageBlobMetadata] of type output from a token. Throws an
 * error if there are not any output blobs or if there are more than one.
 *
 * The returned [ComputationStageBlobMetadata] may be for a yet to be written blob. In such a
 * case the path will be empty.
 */
fun ComputationToken.singleOutputBlobMetadata(): ComputationStageBlobMetadata =
  blobsList.single { it.dependencyType == ComputationBlobDependency.OUTPUT }
