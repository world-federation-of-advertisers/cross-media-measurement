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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationResponse
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineImplBase as AsyncComputationControlCoroutineService
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
import org.wfanet.measurement.internal.duchy.ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest

typealias ComputationTypeDetails =
  ComputationProtocolStageDetailsHelper<
    ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails>

/** Implementation of the internal Async Computation Control Service. */
class AsyncComputationControlService(
  private val computationsClient: ComputationsCoroutineStub,
  private val computationTypeDetails: ComputationTypeDetails
) : AsyncComputationControlCoroutineService() {

  override suspend fun advanceComputation(
    request: AdvanceComputationRequest
  ): AdvanceComputationResponse {
    logger.info(
      "[id=${request.globalComputationId}]: Received input from from ${request.dataOrigin}."
    )
    val context: SingleRequestContext = when (val type = request.computationStage.stageCase) {
      LIQUID_LEGIONS_SKETCH_AGGREGATION_V1 -> LiquidLegionsSketchAggregationV1Context(request)
      LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> LiquidLegionsSketchAggregationV2Context(request)
      else -> failGrpc { "Unrecognized computation type: $type" }
    }
    val tokenForRecordingPath = getComputationToken(context).checkStageIn(context)
      // token is null if the request is not an error but is no longer relevant.
      ?: return AdvanceComputationResponse.getDefaultInstance()
    // Record the key provided as the path to the output blob. If this causes an edit to the
    // computations database the original token not valid, so a new token is used for advancing.
    val tokenForAdvancingStage = recordOutputBlobPath(context, tokenForRecordingPath)
    // Advance the computation to next stage if all blob paths are present.
    advanceIfAllOutputsPresent(context, tokenForAdvancingStage)
    return AdvanceComputationResponse.getDefaultInstance()
  }

  /**
   * Retrieves a [ComputationToken] from the Computations service.
   *
   * @throws StatusException if the computation doesn't exist.
   */
  private suspend fun getComputationToken(
    context: SingleRequestContext
  ): ComputationToken = with(context) {
    val getTokenRequest = GetComputationTokenRequest.newBuilder()
      .setGlobalComputationId(request.globalComputationId)
      .build()

    val getTokenResponse = try {
      computationsClient.getComputationToken(getTokenRequest)
    } catch (e: StatusException) {
      val status = e.status.withCause(e).apply {
        if (code != Status.Code.NOT_FOUND) {
          withDescription("Unable to retrieve token for ${request.globalComputationId}.")
        }
      }
      throw status.asRuntimeException()
    }
    return getTokenResponse.token
  }

  /**
   * Checks the stage of a [ComputationToken] against expected stage of a [context].
   *
   * @return original computation token when there is work to be done on the cmoputation,
   *   or null if the message should be acked.
   *
   * @throws StatusException if the payload for [context] is unexpected based on the current token.
   */
  private fun ComputationToken.checkStageIn(
    context: SingleRequestContext
  ): ComputationToken? = with(context) {
    if (computationStage != request.computationStage) {
      // If the computation is in the next stage, return a null token value which means the
      // rpc should be acked.
      if (computationStage == nextStage(computationDetails, request.computationStage)) return null
      failGrpc {
        val err =
          "Actual stage from computation ($computationStage) did not match the expected " +
            "stage from request (${request.computationStage})."
        logger.info("[id=$globalComputationId]: $err.")
        err
      }
    }
    return this@checkStageIn
  }

  /**
   * Records the blob key for the output blob of a stage for a [SingleRequestContext]
   *
   * @return the [ComputationToken] after recording the output path. If the output was already
   *   recorded in the token, then the token itself is returned.
   */
  private suspend fun recordOutputBlobPath(
    context: SingleRequestContext,
    token: ComputationToken
  ): ComputationToken = with(context) {
    val blob = outputBlob(token)
    if (blob.path.isNotEmpty()) return token
    return computationsClient.recordOutputBlobPath(
      RecordOutputBlobPathRequest.newBuilder().apply {
        setToken(token)
        outputBlobId = blob.blobId
        blobPath = request.blobPath
      }.build()
    ).token
  }

  private suspend fun advanceIfAllOutputsPresent(
    context: SingleRequestContext,
    token: ComputationToken
  ): Unit =
    with(context) {
      if (token.outputPathList().count { it.isEmpty() } == 0) {
        computationsClient.advanceComputationStage(
          computationToken = token,
          inputsToNextStage = token.outputPathList(),
          stage = nextStage(token.computationDetails, token.computationStage),
          computationProtocolStageDetails = computationTypeDetails
        )
      }
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
