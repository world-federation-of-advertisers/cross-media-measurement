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
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.service.internal.duchy.computation.storage.toBlobPath
import org.wfanet.measurement.service.internal.duchy.computation.storage.toGetTokenRequest

/**
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
  ): ComputationToken =
    computationStorageClient
      .advanceLiquidLegionsComputationStage(
        computationToken,
        inputsToNextStage,
        stage,
        liquidLegionsStageDetails
      )

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
    val outputBlob = computationToken.toNoisedSketchBlobMetadataFor(sender)
    return writeExpectedBlobIfNotPresent(
      nameForBlob = "noised_sketch_$sender",
      computationToken = computationToken,
      bytes = sketch,
      blobId = outputBlob.blobId,
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
      nameForBlob = "output",
      computationToken = computationToken,
      bytes = data,
      blobId = onlyOutputBlob.blobId,
      existingPath = onlyOutputBlob.path
    )
  }

  private suspend fun writeExpectedBlobIfNotPresent(
    nameForBlob: String,
    computationToken: ComputationToken,
    bytes: ByteArray,
    blobId: Long,
    existingPath: String
  ): ComputationToken {
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

  /**
   * Returns the bytes of the single output blob of the stage, or null if the output path doesn't
   * exist yet.
   */
  suspend fun readSingleOutputBlob(
    token: ComputationToken
  ): ByteArray? {
    val singleOutputBlob = token.singleOutputBlobMetadata()
    return if (singleOutputBlob.path.isEmpty()) null
    else blobDatabase.read(BlobRef(singleOutputBlob.blobId, singleOutputBlob.path))
  }
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

/**
 * Returns the [ComputationStageBlobMetadata] for the output blob that should hold data sent by
 * the [sender].
 *
 * The returned [ComputationStageBlobMetadata] may be for a yet to be written blob. In such a
 * case the path will be empty.
 */
fun ComputationToken.toNoisedSketchBlobMetadataFor(
  sender: String
): ComputationStageBlobMetadata {
  // Get the blob id by looking up the sender in the stage specific details.
  val stageDetails = stageSpecificDetails.waitSketchStageDetails
  val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[sender])
  return blobsList.single {
    it.dependencyType == ComputationBlobDependency.OUTPUT &&
      it.blobId == blobId
  }
}
