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

package org.wfanet.measurement.service.internal.duchy.computation.storage

import io.grpc.Status
import org.wfanet.measurement.api.v1alpha.CreateGlobalComputationStatusUpdateRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.db.duchy.computation.AfterTransition
import org.wfanet.measurement.db.duchy.computation.BlobRef
import org.wfanet.measurement.db.duchy.computation.ComputationStorageEditToken
import org.wfanet.measurement.db.duchy.computation.EndComputationReason
import org.wfanet.measurement.db.duchy.computation.SingleProtocolDatabase
import org.wfanet.measurement.db.gcp.gcpTimestamp
import org.wfanet.measurement.duchy.mpcAlgorithm
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationToken
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
import org.wfanet.measurement.service.v1alpha.common.grpcRequire
import java.time.Clock
import java.util.logging.Logger

/** Implementation of the Computation Storage Service. */
class ComputationStorageServiceImpl(
  private val computationsDatabase: SingleProtocolDatabase,
  private val globalComputationsClient: GlobalComputationsCoroutineStub,
  private val duchyName: String,
  private val clock: Clock = Clock.systemUTC()
) : ComputationStorageServiceCoroutineImplBase() {

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    grpcRequire(computationsDatabase.computationType == request.computationType) {
      "May only claim work for ${computationsDatabase.computationType} computations. " +
        "${request.computationType} is not supported."
    }
    val claimed = computationsDatabase.claimTask(request.owner)
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
    if (computationsDatabase.readComputationToken(request.globalComputationId) != null) {
      throw Status.fromCode(Status.Code.ALREADY_EXISTS).asRuntimeException()
    }

    grpcRequire(computationsDatabase.computationType == request.computationType) {
      "May only create ${computationsDatabase.computationType} computations. " +
        "${request.computationType} is not supported."
    }
    computationsDatabase.insertComputation(
      request.globalComputationId,
      computationsDatabase.validInitialStages.first(),
      request.stageDetails
    )

    sendStatusUpdateToKingdom(
      newStatusUpdateRequest(
        request.globalComputationId,
        computationsDatabase.validInitialStages.first()
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
    grpcRequire(computationsDatabase.computationType == request.computationType) {
      "May only read tokens for type ${computationsDatabase.computationType}. " +
        "${request.computationType} is not supported"
    }

    val computationToken = computationsDatabase.readComputationToken(request.globalComputationId)
      ?: throw Status.NOT_FOUND.asRuntimeException()
    return computationToken.toGetComputationTokenResponse()
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
    computationsDatabase.enqueue(request.token.toDatabaseEditToken())
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
          start = clock.gcpTimestamp().toProto()
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

fun ComputationToken.toDatabaseEditToken(): ComputationStorageEditToken<ComputationStage> =
  ComputationStorageEditToken(
    localId = localComputationId,
    stage = computationStage,
    attempt = attempt,
    editVersion = version
  )
