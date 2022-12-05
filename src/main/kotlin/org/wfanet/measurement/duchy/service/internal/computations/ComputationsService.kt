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

package org.wfanet.measurement.duchy.service.internal.computations

import io.grpc.Status
import java.time.Clock
import java.util.logging.Logger
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
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
import org.wfanet.measurement.internal.duchy.DeleteComputationRequest
import org.wfanet.measurement.internal.duchy.DeleteComputationResponse
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest.KeyCase
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest

/** Implementation of the Computations service. */
class ComputationsService(
  private val computationsDatabase: ComputationsDatabase,
  private val computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  private val computationStorageClient: ComputationStore,
  private val requisitionStorageClient: RequisitionStore,
  private val duchyName: String,
  private val clock: Clock = Clock.systemUTC()
) : ComputationsCoroutineImplBase() {

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    val claimed = computationsDatabase.claimTask(request.computationType, request.owner)
    return if (claimed != null) {
      val token = computationsDatabase.readComputationToken(claimed)!!
      sendStatusUpdateToKingdom(
        newCreateComputationLogEntryRequest(
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
      request.computationDetails,
      request.requisitionsList
    )

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.globalComputationId,
        computationsDatabase.getValidInitialStage(request.computationType).first()
      )
    )

    return computationsDatabase
      .readComputationToken(request.globalComputationId)!!
      .toCreateComputationResponse()
  }

  override suspend fun deleteComputation(
    request: DeleteComputationRequest
  ): DeleteComputationResponse {
    val computationBlobKeys =
      computationsDatabase.readComputationBlobKeys(request.localComputationId)
    for (blobKey in computationBlobKeys) {
      computationStorageClient.get(blobKey)?.delete()
    }

    val requisitionBlobKeys =
      computationsDatabase.readRequisitionBlobKeys(request.localComputationId)
    for (blobKey in requisitionBlobKeys) {
      requisitionStorageClient.get(blobKey)?.delete()
    }
    computationsDatabase.deleteComputation(request.localComputationId)

    return DeleteComputationResponse.getDefaultInstance()
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
      },
      request.token.computationDetails
    )

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.endingComputationStage
      )
    )

    return computationsDatabase
      .readComputationToken(request.token.globalComputationId)!!
      .toFinishComputationResponse()
  }

  override suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    val computationToken =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (request.keyCase) {
        KeyCase.GLOBAL_COMPUTATION_ID ->
          computationsDatabase.readComputationToken(request.globalComputationId)
        KeyCase.REQUISITION_KEY -> computationsDatabase.readComputationToken(request.requisitionKey)
        KeyCase.KEY_NOT_SET ->
          throw Status.INVALID_ARGUMENT.withDescription("key not set").asRuntimeException()
      }
        ?: throw Status.NOT_FOUND.asRuntimeException()

    return computationToken.toGetComputationTokenResponse()
  }

  override suspend fun updateComputationDetails(
    request: UpdateComputationDetailsRequest
  ): UpdateComputationDetailsResponse {
    require(request.token.computationDetails.protocolCase == request.details.protocolCase) {
      "The protocol type cannot change."
    }
    computationsDatabase.updateComputationDetails(
      request.token.toDatabaseEditToken(),
      request.details,
      request.requisitionsList
    )
    return computationsDatabase
      .readComputationToken(request.token.globalComputationId)!!
      .toUpdateComputationDetailsResponse()
  }

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {
    computationsDatabase.writeOutputBlobReference(
      request.token.toDatabaseEditToken(),
      BlobRef(request.outputBlobId, request.blobPath)
    )
    return computationsDatabase
      .readComputationToken(request.token.globalComputationId)!!
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
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.nextComputationStage
      )
    )
    return computationsDatabase
      .readComputationToken(request.token.globalComputationId)!!
      .toAdvanceComputationStageResponse()
  }

  override suspend fun getComputationIds(
    request: GetComputationIdsRequest
  ): GetComputationIdsResponse {
    val ids = computationsDatabase.readGlobalComputationIds(request.stagesList.toSet())
    return GetComputationIdsResponse.newBuilder().addAllGlobalIds(ids).build()
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

  override suspend fun recordRequisitionBlobPath(
    request: RecordRequisitionBlobPathRequest
  ): RecordRequisitionBlobPathResponse {
    computationsDatabase.writeRequisitionBlobPath(
      request.token.toDatabaseEditToken(),
      request.key,
      request.blobPath
    )
    return checkNotNull(computationsDatabase.readComputationToken(request.key))
      .toRecordRequisitionBlobPathResponse()
  }

  private fun newCreateComputationLogEntryRequest(
    globalId: String,
    computationStage: ComputationStage,
    attempt: Long = 0L
  ): CreateComputationLogEntryRequest {
    return CreateComputationLogEntryRequest.newBuilder()
      .apply {
        parent = ComputationParticipantKey(globalId, duchyName).toName()
        computationLogEntryBuilder.apply {
          // TODO: maybe set participantChildReferenceId
          logMessage =
            "Computation $globalId at stage ${computationStage.name}, " + "attempt $attempt"
          stageAttemptBuilder.apply {
            stage = computationStage.number
            stageName = computationStage.name
            stageStartTime = clock.protoTimestamp()
            attemptNumber = attempt
          }
        }
      }
      .build()
  }

  private suspend fun sendStatusUpdateToKingdom(request: CreateComputationLogEntryRequest) {
    try {
      computationLogEntriesClient.createComputationLogEntry(request)
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
    ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
      ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
    else -> failGrpc { "Computation type for $this is unknown" }
  }
