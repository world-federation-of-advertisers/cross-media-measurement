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

package org.wfanet.measurement.duchy.db.computation

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.read

/**
 * Storage clients providing access to the ComputationsService and ComputationStore.
 */
class ComputationDataClients private constructor(
  val computationsClient: ComputationsCoroutineStub,
  private val computationStore: ComputationStore,
  otherDuchies: List<String>
) {

  constructor(
    computationStorageClient: ComputationsCoroutineStub,
    storageClient: StorageClient,
    otherDuchies: List<String>
  ) : this(computationStorageClient, ComputationStore(storageClient), otherDuchies)

  val computationProtocolStageDetails = ComputationProtocolStageDetails(otherDuchies)

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
    passThroughBlobs: List<String> = listOf(),
    stage: ComputationStage
  ): ComputationToken =
    computationsClient
      .advanceComputationStage(
        computationToken = computationToken,
        inputsToNextStage = inputsToNextStage,
        passThroughBlobs = passThroughBlobs,
        stage = stage,
        computationProtocolStageDetails = computationProtocolStageDetails
      )

  /**
   * Writes the content as a single output blob to the current stage if no
   * output blob has yet been written.
   *
   * @return the resulting [ComputationToken] after updating blob reference, or
   *     [computationToken] if no blob was written
   */
  suspend fun writeSingleOutputBlob(
    computationToken: ComputationToken,
    content: Flow<ByteString>
  ): ComputationToken {
    return writeBlobIfNotPresent(computationToken, computationToken.singleOutputBlobMetadata()) {
      computationStore.write(it, content)
    }
  }

  /** @see writeSingleOutputBlob */
  suspend fun writeSingleOutputBlob(
    computationToken: ComputationToken,
    content: ByteString
  ): ComputationToken {
    return writeBlobIfNotPresent(computationToken, computationToken.singleOutputBlobMetadata()) {
      computationStore.write(it, content)
    }
  }

  /**
   * Writes the blob content using [writeContent] if no blob key is present in
   * [metadata].
   *
   * @param metadata [ComputationStageBlobMetadata] for the blob
   * @param writeContent function which writes bound blob content, to be called
   *     with [computationToken]
   * @return resulting [ComputationToken] from write, or [computationToken] if
   *     no write was performed
   */
  private suspend fun writeBlobIfNotPresent(
    computationToken: ComputationToken,
    metadata: ComputationStageBlobMetadata,
    writeContent: suspend (ComputationToken) -> ComputationStore.Blob
  ): ComputationToken {
    if (metadata.path.isNotEmpty()) {
      return computationToken
    }

    val blob = writeContent(computationToken)
    val response = computationsClient.recordOutputBlobPath(
      RecordOutputBlobPathRequest.newBuilder().apply {
        token = computationToken
        outputBlobId = metadata.blobId
        blobPath = blob.blobKey
      }.build()
    )
    return response.token
  }

  /**
   * Returns a map of [BlobRef]s to the actual bytes of the BLOB for all inputs
   * to the stage.
   */
  suspend fun readInputBlobs(token: ComputationToken): Map<BlobRef, ByteString> {
    return token.blobsList
      .filter { it.dependencyType == ComputationBlobDependency.INPUT }
      .map { it.toBlobRef() }
      .associateWith { getBlob(it).read().flatten() }
  }

  /**
   * Returns the content of the single output blob of the stage, or
   * `null` if it hasn't yet been written.
   */
  fun readSingleOutputBlob(token: ComputationToken): Flow<ByteString>? {
    val blobRef = token.singleOutputBlobMetadata().toBlobRef()
    if (blobRef.key.isEmpty()) {
      return null
    }

    return getBlob(blobRef).read()
  }

  private fun getBlob(ref: BlobRef): ComputationStore.Blob {
    return checkNotNull(computationStore.get(ref.key)) {
      "Failed to read content for computation blob ${ref.idInRelationalDatabase}: " +
        "Blob with key ${ref.key} not found"
    }
  }

  companion object {
    fun forTesting(
      computationStorageClient: ComputationsCoroutineStub,
      computationStore: ComputationStore,
      otherDuchies: List<String>
    ): ComputationDataClients {
      return ComputationDataClients(
        computationStorageClient,
        computationStore,
        otherDuchies
      )
    }
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
  allOutputBlobMetadataList().single()

/**
 * Returns all [ComputationStageBlobMetadata]s which are of type output from a token.
 *
 * The returned [ComputationStageBlobMetadata] may be for a yet to be written blob. In such a
 * case the path will be empty.
 */
fun ComputationToken.allOutputBlobMetadataList(): List<ComputationStageBlobMetadata> =
  blobsList.filter {
    it.dependencyType == ComputationBlobDependency.OUTPUT ||
      it.dependencyType == ComputationBlobDependency.PASS_THROUGH
  }

/**
 * Returns the [ComputationStageBlobMetadata] for the output blob that should hold data sent by
 * the [sender].
 *
 * The returned [ComputationStageBlobMetadata] may be for a yet to be written blob. In such a
 * case the path will be empty.
 */
// TODO: replace with something generic.
fun ComputationToken.toNoisedSketchBlobMetadataFor(
  sender: String
): ComputationStageBlobMetadata {
  // Get the blob id by looking up the sender in the stage specific details.
  val stageDetails = stageSpecificDetails.liquidLegionsV1.waitSketchStageDetails
  val blobId = checkNotNull(stageDetails.externalDuchyLocalBlobIdMap[sender])
  return blobsList.single {
    it.dependencyType == ComputationBlobDependency.OUTPUT &&
      it.blobId == blobId
  }
}
