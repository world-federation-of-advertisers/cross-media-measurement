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

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.duchy.storage.ComputationBlobContext
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageException
import org.wfanet.measurement.storage.Store.Blob

/** Storage clients providing access to the ComputationsService and ComputationStore. */
class ComputationDataClients
private constructor(
  val computationsClient: ComputationsCoroutineStub,
  private val computationStore: ComputationStore,
  private val requisitionStore: RequisitionStore,
) {

  constructor(
    computationStorageClient: ComputationsCoroutineStub,
    storageClient: StorageClient,
  ) : this(
    computationStorageClient,
    ComputationStore(storageClient),
    RequisitionStore(storageClient),
  )

  /**
   * Calls AdvanceComputationStage to move to a new stage in a consistent way.
   *
   * The assumption is this will only be called by a job that is executing the stage of a
   * computation, which will have knowledge of all the data needed as input to the next stage. Most
   * of the time [inputsToNextStage] is the list of outputs of the currently running stage.
   */
  suspend fun transitionComputationToStage(
    computationToken: ComputationToken,
    inputsToNextStage: List<String> = listOf(),
    passThroughBlobs: List<String> = listOf(),
    stage: ComputationStage,
  ): ComputationToken {
    return try {
      computationsClient.advanceComputationStage(
        computationToken = computationToken,
        inputsToNextStage = inputsToNextStage,
        passThroughBlobs = passThroughBlobs,
        stage = stage,
      )
    } catch (e: StatusException) {
      val message = "Error advancing computation stage"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.ABORTED -> TransientErrorException(message, e)
        else -> PermanentErrorException(message, e)
      }
    }
  }

  /**
   * Writes the content as a single output blob to the current stage if no output blob has yet been
   * written.
   *
   * @return the resulting [ComputationToken] after updating blob reference, or [computationToken]
   *   if no blob was written
   */
  private suspend fun writeSingleOutputBlob(
    computationToken: ComputationToken,
    content: Flow<ByteString>,
  ): ComputationToken {
    return writeBlobIfNotPresent(
      computationToken,
      computationToken.singleOutputBlobMetadata(),
      content,
    )
  }

  /** @see writeSingleOutputBlob */
  suspend fun writeSingleOutputBlob(
    computationToken: ComputationToken,
    content: ByteString,
  ): ComputationToken {
    val blobMetadata = computationToken.singleOutputBlobMetadata()
    return writeBlobIfNotPresent(computationToken, blobMetadata, flowOf(content))
  }

  /**
   * Writes the blob content if no blob key is present in [metadata].
   *
   * @param metadata [ComputationStageBlobMetadata] for the blob
   * @param content blob content to write
   * @return resulting [ComputationToken] from write, or [computationToken] if no write was
   *   performed
   */
  private suspend fun writeBlobIfNotPresent(
    computationToken: ComputationToken,
    metadata: ComputationStageBlobMetadata,
    content: Flow<ByteString>,
  ): ComputationToken {
    if (metadata.path.isNotEmpty()) {
      return computationToken
    }

    val blob =
      try {
        computationStore.write(
          ComputationBlobContext.fromToken(computationToken, metadata),
          content,
        )
      } catch (e: StorageException) {
        // Storage client is possible to raise non-retryable exceptions, which are actually
        // retryable. To avoid to fail Computation in this case, mark all StorageExceptions as
        // transient so the mill can start another attempt. This is only for LLv2 protocol because
        // HMSS does not need to write intermediate result into storage.
        //
        // See github issue:
        // https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/1733
        //
        // TODO(@renjiez): Handle the exception case by case after the issue fixed.
        throw TransientErrorException("Error writing blob to storage.", e)
      }

    val response: RecordOutputBlobPathResponse =
      try {
        computationsClient.recordOutputBlobPath(
          RecordOutputBlobPathRequest.newBuilder()
            .apply {
              token = computationToken
              outputBlobId = metadata.blobId
              blobPath = blob.blobKey
            }
            .build()
        )
      } catch (e: StatusException) {
        val message = "Error recording blob output path"
        throw when (e.status.code) {
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED -> TransientErrorException(message, e)
          else -> PermanentErrorException(message, e)
        }
      }
    return response.token
  }

  /** Reads and combines all requisition blobs fulfilled at this duchy. */
  suspend fun readAllRequisitionBlobs(token: ComputationToken, duchyId: String): ByteString {
    return token.requisitionsList
      .filter { it.details.externalFulfillingDuchyId == duchyId }
      .map {
        checkNotNull(requisitionStore.get(it.path)) { "Blob with key ${it.path} not found" }
          .read()
          .flatten()
      }
      .flatten()
  }

  /** Reads and returns fulfilled requisition blobs associated by requisition ids. */
  suspend fun readRequisitionBlobs(token: ComputationToken): Map<String, ByteString> {
    return token.requisitionsList
      .filter { it.path.isNotBlank() }
      .associate {
        val blob =
          checkNotNull(requisitionStore.get(it.path)) { "Blob with key ${it.path} not found" }
        it.externalKey.externalRequisitionId to blob.read().flatten()
      }
  }

  /** Reads and returns a single fulfilled requisition blob. */
  suspend fun readSingleRequisitionBlob(requisition: RequisitionMetadata): ByteString? {
    val path = requisition.path
    val blob: Blob = requisitionStore.get(path) ?: return null
    return blob.read().flatten()
  }

  /** Returns a map of [BlobRef]s to the actual bytes of the blob for all inputs to the stage. */
  suspend fun readInputBlobs(token: ComputationToken): Map<BlobRef, ByteString> {
    return token.blobsList
      .filter { it.dependencyType == ComputationBlobDependency.INPUT }
      .map { it.toBlobRef() }
      .associateWith { getBlob(it).read().flatten() }
  }

  /**
   * Returns the content of the single output blob of the stage, or `null` if it hasn't yet been
   * written.
   */
  fun readSingleOutputBlob(token: ComputationToken): Flow<ByteString>? {
    val blobRef = token.singleOutputBlobMetadata().toBlobRef()
    if (blobRef.key.isEmpty()) {
      return null
    }

    return flow { emitAll(getBlob(blobRef).read()) }
  }

  private suspend fun getBlob(ref: BlobRef): Blob {
    return checkNotNull(computationStore.get(ref.key)) {
      "Failed to read content for computation blob ${ref.idInRelationalDatabase}: " +
        "Blob with key ${ref.key} not found"
    }
  }

  companion object {
    fun forTesting(
      computationStorageClient: ComputationsCoroutineStub,
      computationStore: ComputationStore,
      requisitionStore: RequisitionStore,
    ): ComputationDataClients {
      return ComputationDataClients(computationStorageClient, computationStore, requisitionStore)
    }
  }

  /**
   * [Exception] which indicates a transient computation error, i.e. one that can be retried on in a
   * future attempt to process the computation.
   */
  class TransientErrorException(message: String? = null, cause: Throwable? = null) :
    Exception(message, cause)

  /**
   * [Exception] which indicates a permanent computation error, i.e. one that should result in
   * immediate failure of the Computation.
   */
  class PermanentErrorException(message: String? = null, cause: Throwable? = null) :
    Exception(message, cause)
}

/**
 * Returns the single [ComputationStageBlobMetadata] of type output from a token. Throws an error if
 * there are not any output blobs or if there are more than one.
 *
 * The returned [ComputationStageBlobMetadata] may be for a yet to be written blob. In such a case
 * the path will be empty.
 */
fun ComputationToken.singleOutputBlobMetadata(): ComputationStageBlobMetadata =
  allOutputBlobMetadataList().single()

/**
 * Returns all [ComputationStageBlobMetadata]s which are of type output from a token.
 *
 * The returned [ComputationStageBlobMetadata] may be for a yet to be written blob. In such a case
 * the path will be empty.
 */
fun ComputationToken.allOutputBlobMetadataList(): List<ComputationStageBlobMetadata> =
  blobsList.filter {
    it.dependencyType == ComputationBlobDependency.OUTPUT ||
      it.dependencyType == ComputationBlobDependency.PASS_THROUGH
  }

private fun ComputationStageBlobMetadata.toBlobRef() = BlobRef(blobId, path)
