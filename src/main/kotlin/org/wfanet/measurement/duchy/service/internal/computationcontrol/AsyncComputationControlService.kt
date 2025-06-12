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
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.time.delay
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.service.internal.computations.role
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationResponse
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineImplBase as AsyncComputationControlCoroutineService
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.GetOutputBlobMetadataRequest
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.internal.duchy.recordOutputBlobPathRequest

/** Implementation of the internal Async Computation Control Service. */
class AsyncComputationControlService(
  private val computationsClient: ComputationsCoroutineStub,
  /** Maximum number of attempts for [advanceComputation]. */
  private val maxAdvanceAttempts: Int,
  /** Retry backoff for [advanceComputation]. */
  private val advanceRetryBackoff: ExponentialBackoff = ExponentialBackoff(),
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : AsyncComputationControlCoroutineService(coroutineContext) {
  init {
    require(maxAdvanceAttempts >= 1) { "maxAdvanceAttempts must be at least 1" }
  }

  override suspend fun advanceComputation(
    request: AdvanceComputationRequest
  ): AdvanceComputationResponse {
    val globalComputationId = request.globalComputationId

    var attempt = 1
    while (coroutineContext.isActive) {
      try {
        return advanceComputationInternal(request)
      } catch (e: RetryableException) {
        if (attempt < maxAdvanceAttempts) {
          logger.log(Level.WARNING, e) {
            "[id=$globalComputationId]: advanceComputation attempt #$attempt failed; retrying"
          }
          delay(advanceRetryBackoff.durationForAttempt(attempt))
          attempt++
          continue
        }
        throw e
      }
    }

    error("Should be unreachable")
  }

  private suspend fun advanceComputationInternal(
    request: AdvanceComputationRequest
  ): AdvanceComputationResponse {
    val globalComputationId = request.globalComputationId
    logger.info("[id=$globalComputationId]: Received blob ${request.blobPath}.")
    val stages =
      ProtocolStages.forStageType(request.computationStage.stageCase)
        ?: failGrpc { "Unexpected stage type ${request.computationStage.stageCase}" }
    var token =
      getComputationToken(globalComputationId)
        ?: throw Status.NOT_FOUND.withDescription(
            "Computation with global ID $globalComputationId not found"
          )
          .asRuntimeException()

    val computationStage = token.computationStage
    val role = token.role()

    // Tolerant certain stage mismatches.
    if (computationStage != request.computationStage) {
      // Ignore if stage in the request is one step behind.
      if (computationStage == stages.nextStage(request.computationStage, role)) {
        // This is technically an error, but it should be safe to treat as a no-op.
        logger.warning {
          "[id=$globalComputationId]: Computation stage has already been advanced. " +
            "computation_stage=$computationStage, request_stage=${request.computationStage}"
        }
        return AdvanceComputationResponse.getDefaultInstance()
      }
      // Catch up if stage in the request is one step ahead.
      if (stages.nextStage(computationStage, role) == request.computationStage) {
        logger.warning {
          "[id=$globalComputationId]: Computation stage catching up... " +
            "computation_stage=$computationStage, request_stage=${request.computationStage}"
        }
        if (token.outputPathList().any(String::isEmpty)) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "Output blob has not been set. Unable to advance the stage."
            )
            .asRuntimeException()
        }
        token =
          computationsClient.advanceComputationStage(
            computationToken = token,
            inputsToNextStage = token.outputPathList(),
            stage = stages.nextStage(token.computationStage, role),
          )
      } else {
        // When stage mismatch is not tolerable.
        throw Status.ABORTED.withDescription(
            "Stage mismatch cannot be tolerate. computation_stage=$computationStage, " +
              "request_stage=${request.computationStage}"
          )
          .asRuntimeException()
      }
    }

    val outputBlob =
      token.blobsList.firstOrNull {
        it.blobId == request.blobId && it.dependencyType == ComputationBlobDependency.OUTPUT
      } ?: failGrpc(Status.FAILED_PRECONDITION) { "No output blob with ID ${request.blobId}" }
    if (outputBlob.path.isNotEmpty()) {
      if (outputBlob.path != request.blobPath) {
        throw Status.FAILED_PRECONDITION.withDescription(
            "Output blob ${outputBlob.blobId} already has a different path recorded"
          )
          .asRuntimeException()
      }
      logger.info {
        "[id=$globalComputationId]: Path already recorded for output blob ${outputBlob.blobId}"
      }
    } else {
      val response =
        try {
          computationsClient.recordOutputBlobPath(
            recordOutputBlobPathRequest {
              this.token = token
              outputBlobId = outputBlob.blobId
              blobPath = request.blobPath
            }
          )
        } catch (e: StatusException) {
          throw when (e.status.code) {
            Status.Code.UNAVAILABLE,
            Status.Code.ABORTED -> RetryableException(e)
            else -> Status.UNKNOWN.withCause(e).asRuntimeException()
          }
        }

      // Computation has changed, so use the new token.
      token = response.token
    }

    // Advance the computation to next stage if all blob paths are present.
    if (!token.outputPathList().any(String::isEmpty)) {
      try {
        computationsClient.advanceComputationStage(
          computationToken = token,
          inputsToNextStage = token.outputPathList(),
          stage = stages.nextStage(token.computationStage, role),
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.UNAVAILABLE,
          Status.Code.ABORTED -> RetryableException(e)
          else -> Status.UNKNOWN.withCause(e).asRuntimeException()
        }
      }
    }

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
   * @return the retrieved token, or `null` if not found
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

  private class RetryableException(message: String? = null, cause: Throwable? = null) :
    Exception(message, cause) {
    constructor(cause: Throwable) : this(null, cause)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
