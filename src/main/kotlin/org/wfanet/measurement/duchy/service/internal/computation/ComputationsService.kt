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

package org.wfanet.measurement.duchy.service.internal.computation

import io.grpc.Status
import java.time.Clock
import java.util.logging.Logger
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.mpcAlgorithm
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse
import org.wfanet.measurement.system.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub

/** Implementation of the Computations service. */
class ComputationsService(
  private val computationsDatabase: ComputationsDatabase,
  private val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val duchyName: String,
  private val clock: Clock = Clock.systemUTC()
) : ComputationsCoroutineImplBase() {

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    val claimed = computationsDatabase.claimTask(request.computationType, request.owner)
    return if (claimed != null) {
      val token = computationsDatabase.readComputationToken(claimed)!!
      sendStatusUpdateToKingdom(
        newStatusUpdateRequest(
          token.globalComputationId,
          token.computationStage,
          token.attempt.toLong()
        )
      )
      token.toClaimWorkResponse()
    } else ClaimWorkResponse.getDefaultInstance()
  }

  override suspend fun createComputation(
    request: CreateComputationRequest
  ): CreateComputationResponse {
    // TODO: validate CreateComputationRequest

    if (computationsDatabase.readComputationToken(request.globalComputationId) != null) {
      throw Status.fromCode(Status.Code.ALREADY_EXISTS).asRuntimeException()
    }

    computationsDatabase.insertComputation(
      request.globalComputationId,
      request.computationType,
      computationsDatabase.getValidInitialStage(request.computationType).first(),
      request.stageDetails,
      request.computationDetails
    )

    sendStatusUpdateToKingdom(
      newStatusUpdateRequest(
        request.globalComputationId,
        computationsDatabase.getValidInitialStage(request.computationType).first()
      )
    )

    return computationsDatabase.readComputationToken(request.globalComputationId)!!
      .toCreateComputationResponse()
  }

  override suspend fun finishComputation(
    request: FinishComputationRequest
  ): FinishComputationResponse {
    computationsDatabase.endComputation(
      request.token.toDatabaseEditToken(),
      request.endingComputationStage,
      when (val it = request.reason) {
        ComputationDetails.CompletedReason.SUCCEEDED -> EndComputationReason.SUCCEEDED
        ComputationDetails.CompletedReason.FAILED -> EndComputationReason.FAILED
        ComputationDetails.CompletedReason.CANCELED -> EndComputationReason.CANCELED
        else -> error("Unknown CompletedReason $it")
      }
    )

    sendStatusUpdateToKingdom(
      newStatusUpdateRequest(
        request.token.globalComputationId,
        request.endingComputationStage
      )
    )

    return computationsDatabase.readComputationToken(request.token.globalComputationId)!!
      .toFinishComputationResponse()
  }

  override suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    val computationToken = computationsDatabase.readComputationToken(request.globalComputationId)
      ?: throw Status.NOT_FOUND.asRuntimeException()
    return computationToken.toGetComputationTokenResponse()
  }

  override suspend fun updateComputationDetails(
    request: UpdateComputationDetailsRequest
  ): UpdateComputationDetailsResponse {
    require(request.token.computationDetails.detailsCase == request.details.detailsCase) {
      "The protocol type cannot change."
    }
    computationsDatabase.updateComputationDetails(
      request.token.toDatabaseEditToken(),
      request.details
    )
    return computationsDatabase.readComputationToken(request.token.globalComputationId)!!
      .toUpdateComputationDetailsResponse()
  }

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {
    computationsDatabase.writeOutputBlobReference(
      request.token.toDatabaseEditToken(),
      BlobRef(
        request.outputBlobId,
        request.blobPath
      )
    )
    return computationsDatabase.readComputationToken(request.token.globalComputationId)!!
      .toRecordOutputBlobPathResponse()
  }

  override suspend fun advanceComputationStage(
    request: AdvanceComputationStageRequest
  ): AdvanceComputationStageResponse {
    computationsDatabase.updateComputationStage(
      request.token.toDatabaseEditToken(),
      request.nextComputationStage,
      request.inputBlobsList,
      request.passThroughBlobsList,
      request.outputBlobs,
      when (val it = request.afterTransition) {
        AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
          AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE ->
          AfterTransition.DO_NOT_ADD_TO_QUEUE
        AdvanceComputationStageRequest.AfterTransition.RETAIN_AND_EXTEND_LOCK ->
          AfterTransition.CONTINUE_WORKING
        else -> error("Unsupported AdvanceComputationStageRequest.AfterTransition '$it'. ")
      },
      request.stageDetails
    )

    sendStatusUpdateToKingdom(
      newStatusUpdateRequest(
        request.token.globalComputationId,
        request.nextComputationStage
      )
    )
    return computationsDatabase.readComputationToken(request.token.globalComputationId)!!
      .toAdvanceComputationStageResponse()
  }

  override suspend fun getComputationIds(
    request: GetComputationIdsRequest
  ): GetComputationIdsResponse {
    val ids = computationsDatabase.readGlobalComputationIds(request.stagesList.toSet())
    return GetComputationIdsResponse.newBuilder()
      .addAllGlobalIds(ids).build()
  }

  override suspend fun enqueueComputation(
    request: EnqueueComputationRequest
  ): EnqueueComputationResponse {
    grpcRequire(request.delaySecond >= 0) {
      "DelaySecond ${request.delaySecond} should be non-negative."
    }
    computationsDatabase.enqueue(request.token.toDatabaseEditToken(), request.delaySecond)
    return EnqueueComputationResponse.getDefaultInstance()
  }

  private fun newStatusUpdateRequest(
    globalId: String,
    computationStage: ComputationStage,
    attempt: Long = 0L
  ): CreateGlobalComputationStatusUpdateRequest {
    return CreateGlobalComputationStatusUpdateRequest.newBuilder().apply {
      parentBuilder.globalComputationId = globalId
      statusUpdateBuilder.apply {
        selfReportedIdentifier = duchyName
        stageDetailsBuilder.apply {
          algorithm = computationStage.mpcAlgorithm
          stageNumber = computationStage.number.toLong()
          stageName = computationStage.name
          start = clock.protoTimestamp()
          attemptNumber = attempt
        }
        updateMessage = "Computation $globalId at stage ${computationStage.name}, " +
          "attempt $attempt"
      }
    }.build()
  }

  private suspend fun sendStatusUpdateToKingdom(
    request: CreateGlobalComputationStatusUpdateRequest
  ) {
    try {
      globalComputationsClient.createGlobalComputationStatusUpdate(request)
    } catch (ignored: Exception) {
      logger.warning("Failed to update status change to the kingdom. $ignored")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}

private fun ComputationToken.toDatabaseEditToken():
  ComputationEditToken<ComputationType, ComputationStage> =
    ComputationEditToken(
      localId = localComputationId,
      protocol = computationStage.toComputationType(),
      stage = computationStage,
      attempt = attempt,
      editVersion = version
    )

private fun ComputationStage.toComputationType() =
  when (stageCase) {
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1 ->
      ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V1
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
      ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
    else -> failGrpc { "Computation type for $this is unknown" }
  }
