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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationResponse
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineImplBase as AsyncComputationControlCoroutineService
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.GetOutputBlobMetadataRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest

/** Implementation of the internal Async Computation Control Service. */
class AsyncComputationControlService(private val computationsClient: ComputationsCoroutineStub) :
  AsyncComputationControlCoroutineService() {

  override suspend fun advanceComputation(
    request: AdvanceComputationRequest
  ): AdvanceComputationResponse {
    logger.info("[id=${request.globalComputationId}]: Received blob ${request.blobPath}.")
    val stages =
      ProtocolStages.forStageType(request.computationStage.stageCase)
        ?: failGrpc { "Unexpected stage type ${request.computationStage.stageCase}" }
    val tokenForRecordingPath =
      getComputationToken(request.globalComputationId)
        ?: throw Status.NOT_FOUND.withDescription(
            "Computation with global ID ${request.globalComputationId} not found"
          )
          .asRuntimeException()
    val computationStage = tokenForRecordingPath.computationStage
    if (computationStage != request.computationStage) {
      if (computationStage == stages.nextStage(request.computationStage)) {
        // request is not an error but is no longer relevant.
        return AdvanceComputationResponse.getDefaultInstance()
      }
      failGrpc(Status.FAILED_PRECONDITION) {
        "Actual stage from computation ($computationStage) did not match the expected " +
          "stage from request (${request.computationStage})."
      }
    }

    // Record the key provided as the path to the output blob. If this
    // causes an edit to the
    // computations database the original token not valid, so a new token is used for advancing.
    val outputBlob =
      tokenForRecordingPath.blobsList.firstOrNull {
        it.blobId == request.blobId && it.dependencyType == ComputationBlobDependency.OUTPUT
      }
        ?: failGrpc(Status.FAILED_PRECONDITION) { "No output blob with ID ${request.blobId}" }
    val tokenForAdvancingStage =
      recordOutputBlobPath(tokenForRecordingPath, outputBlob, request.blobPath)

    // Advance the computation to next stage if all blob paths are present.
    advanceIfAllOutputsPresent(stages, tokenForAdvancingStage)
    return AdvanceComputationResponse.getDefaultInstance()
  }

  override suspend fun getOutputBlobMetadata(
    request: GetOutputBlobMetadataRequest
  ): ComputationStageBlobMetadata {
    val currentToken =
      getComputationToken(request.globalComputationId)
        ?: throw Status.NOT_FOUND.withDescription(
            "Computation with global ID ${request.globalComputationId} not found"
          )
          .asRuntimeException()
    val stageType = currentToken.computationStage.stageCase
    val stages =
      ProtocolStages.forStageType(stageType) ?: failGrpc { "Unexpected stage type $stageType" }

    try {
      return stages.outputBlob(currentToken, request.dataOrigin)
    } catch (e: IllegalStageException) {
      throw Status.FAILED_PRECONDITION.withCause(e)
        .withDescription("Computation in unexpected stage ${e.computationStage}")
        .asRuntimeException()
    }
  }

  /**
   * Retrieves a [ComputationToken] from the Computations service.
   *
   * @return the retrieved token, or [null] if not found
   */
  private suspend fun getComputationToken(globalComputationId: String): ComputationToken? {
    val response =
      try {
        computationsClient.getComputationToken(
          getComputationTokenRequest { this.globalComputationId = globalComputationId }
        )
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.NOT_FOUND) {
          throw Exception("Unable to retrieve token for $globalComputationId.", e)
        }
        null
      }
    return response?.token
  }

  /**
   * Records the blob key for the output blob of a stage for a [LiquidLegionsV2Stages]
   *
   * @return the [ComputationToken] after recording the output path. If the output was already
   *   recorded in the token, then the token itself is returned.
   */
  private suspend fun recordOutputBlobPath(
    token: ComputationToken,
    blob: ComputationStageBlobMetadata,
    blobPath: String
  ): ComputationToken {

    if (blob.path.isNotEmpty()) return token
    return computationsClient
      .recordOutputBlobPath(
        RecordOutputBlobPathRequest.newBuilder()
          .apply {
            setToken(token)
            outputBlobId = blob.blobId
            this.blobPath = blobPath
          }
          .build()
      )
      .token
  }

  private suspend fun advanceIfAllOutputsPresent(stages: ProtocolStages, token: ComputationToken) {
    if (token.outputPathList().any(String::isEmpty)) {
      return
    }
    computationsClient.advanceComputationStage(
      computationToken = token,
      inputsToNextStage = token.outputPathList(),
      stage = stages.nextStage(token.computationStage)
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
