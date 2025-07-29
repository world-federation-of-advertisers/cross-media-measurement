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

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.db.computation.toDatabaseEditToken
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.service.internal.ComputationLockOwnerMismatchException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.DeleteComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationRequest
import org.wfanet.measurement.internal.duchy.EnqueueComputationResponse
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationIdsRequest
import org.wfanet.measurement.internal.duchy.GetComputationIdsResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest.KeyCase
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.PurgeComputationsRequest
import org.wfanet.measurement.internal.duchy.PurgeComputationsResponse
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordOutputBlobPathResponse
import org.wfanet.measurement.internal.duchy.RecordRequisitionFulfillmentRequest
import org.wfanet.measurement.internal.duchy.RecordRequisitionFulfillmentResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse
import org.wfanet.measurement.internal.duchy.purgeComputationsResponse
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.computationLogEntry
import org.wfanet.measurement.system.v1alpha.createComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.stageAttempt

/** Implementation of the Computations service. */
class ComputationsService(
  private val computationsDatabase: ComputationsDatabase,
  private val computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  private val computationStore: ComputationStore,
  private val requisitionStore: RequisitionStore,
  private val duchyName: String,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val clock: Clock = Clock.systemUTC(),
  private val defaultLockDuration: Duration = Duration.ofMinutes(5),
) : ComputationsCoroutineImplBase(coroutineContext) {

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    grpcRequire(request.owner.isNotEmpty()) { "owner is not specified" }
    val lockDuration =
      if (request.hasLockDuration()) request.lockDuration.toDuration() else defaultLockDuration
    val claimed =
      computationsDatabase.claimTask(
        request.computationType,
        request.owner,
        lockDuration,
        request.prioritizedStagesList,
      )
    return if (claimed != null) {
      val token = computationsDatabase.readComputationToken(claimed)!!
      sendStatusUpdateToKingdom(
        newCreateComputationLogEntryRequest(
          token.globalComputationId,
          token.computationStage,
          attempt = token.attempt,
          callerInstanceId = request.owner,
        )
      )
      token.toClaimWorkResponse()
    } else {
      ClaimWorkResponse.getDefaultInstance()
    }
  }

  override suspend fun createComputation(
    request: CreateComputationRequest
  ): CreateComputationResponse {
    // TODO: validate CreateComputationRequest
    val token = computationsDatabase.readComputationToken(request.globalComputationId)
    if (token != null) {
      throw Status.fromCode(Status.Code.ALREADY_EXISTS)
        .withDescription(
          "Computation already exists. " +
            "computationId=${token.localComputationId}, " +
            "stage=${token.computationStage}, version=${token.version}"
        )
        .asRuntimeException()
    }

    val initialStage =
      if (request.hasComputationStage()) {
        request.computationStage
      } else {
        computationsDatabase.getValidInitialStage(request.computationType).first()
      }

    computationsDatabase.insertComputation(
      request.globalComputationId,
      request.computationType,
      initialStage,
      request.stageDetails,
      request.computationDetails,
      request.requisitionsList,
    )

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.globalComputationId,
        computationsDatabase.getValidInitialStage(request.computationType).first(),
      )
    )

    return computationsDatabase
      .readComputationToken(request.globalComputationId)!!
      .toCreateComputationResponse()
  }

  private suspend fun deleteComputation(localId: Long) {
    val computationBlobKeys = computationsDatabase.readComputationBlobKeys(localId)
    for (blobKey in computationBlobKeys) {
      computationStore.get(blobKey)?.delete()
    }
    val requisitionBlobKeys = computationsDatabase.readRequisitionBlobKeys(localId)
    for (blobKey in requisitionBlobKeys) {
      requisitionStore.get(blobKey)?.delete()
    }
    computationsDatabase.deleteComputation(localId)
  }

  override suspend fun deleteComputation(request: DeleteComputationRequest): Empty {
    deleteComputation(request.localComputationId)
    return Empty.getDefaultInstance()
  }

  override suspend fun purgeComputations(
    request: PurgeComputationsRequest
  ): PurgeComputationsResponse {
    grpcRequire(
      request.stagesList.all {
        computationsDatabase.validTerminalStage(computationsDatabase.stageToProtocol(it), it)
      }
    ) {
      "Requested stage list contains non terminal stage."
    }
    var deleted = 0
    try {
      val globalIds =
        computationsDatabase.readGlobalComputationIds(
          request.stagesList.toSet(),
          request.updatedBefore.toInstant(),
        )
      if (!request.force) {
        return purgeComputationsResponse {
          purgeCount = globalIds.size
          purgeSample += globalIds
        }
      }
      for (globalId in globalIds) {
        val token = computationsDatabase.readComputationToken(globalId) ?: continue
        deleteComputation(token.localComputationId)
        deleted += 1
      }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Exception during Computations cleaning. $e")
    }
    return purgeComputationsResponse { this.purgeCount = deleted }
  }

  override suspend fun finishComputation(
    request: FinishComputationRequest
  ): FinishComputationResponse {
    try {
      computationsDatabase.endComputation(
        request.token.toDatabaseEditToken(),
        request.endingComputationStage,
        when (val it = request.reason) {
          ComputationDetails.CompletedReason.SUCCEEDED -> EndComputationReason.SUCCEEDED
          ComputationDetails.CompletedReason.FAILED -> EndComputationReason.FAILED
          ComputationDetails.CompletedReason.CANCELED -> EndComputationReason.CANCELED
          else -> error("Unknown CompletedReason $it")
        },
        request.token.computationDetails,
      )
    } catch (e: ComputationNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ComputationTokenVersionMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.endingComputationStage,
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
      } ?: throw Status.NOT_FOUND.asRuntimeException()

    return computationToken.toGetComputationTokenResponse()
  }

  override suspend fun updateComputationDetails(
    request: UpdateComputationDetailsRequest
  ): UpdateComputationDetailsResponse {
    require(request.token.computationDetails.protocolCase == request.details.protocolCase) {
      "The protocol type cannot change."
    }
    try {
      computationsDatabase.updateComputationDetails(
        request.token.toDatabaseEditToken(),
        request.details,
        request.requisitionsList,
      )
    } catch (e: ComputationNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ComputationTokenVersionMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
    return computationsDatabase
      .readComputationToken(request.token.globalComputationId)!!
      .toUpdateComputationDetailsResponse()
  }

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {
    try {
      computationsDatabase.writeOutputBlobReference(
        request.token.toDatabaseEditToken(),
        BlobRef(request.outputBlobId, request.blobPath),
      )
    } catch (e: ComputationNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ComputationTokenVersionMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
    return computationsDatabase
      .readComputationToken(request.token.globalComputationId)!!
      .toRecordOutputBlobPathResponse()
  }

  override suspend fun advanceComputationStage(
    request: AdvanceComputationStageRequest
  ): AdvanceComputationStageResponse {
    val lockExtension: Duration =
      if (request.hasLockExtension()) request.lockExtension.toDuration() else defaultLockDuration
    try {
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
        request.stageDetails,
        lockExtension,
      )
    } catch (e: ComputationNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ComputationTokenVersionMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.nextComputationStage,
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
    try {
      computationsDatabase.enqueue(
        request.token.toDatabaseEditToken(),
        request.delaySecond,
        request.expectedOwner,
      )
    } catch (e: ComputationNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ComputationTokenVersionMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    } catch (e: ComputationLockOwnerMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }
    return EnqueueComputationResponse.getDefaultInstance()
  }

  override suspend fun recordRequisitionFulfillment(
    request: RecordRequisitionFulfillmentRequest
  ): RecordRequisitionFulfillmentResponse {
    computationsDatabase.writeRequisitionBlobPath(
      token = request.token.toDatabaseEditToken(),
      externalRequisitionKey = request.key,
      pathToBlob = request.blobPath,
      publicApiVersion = request.publicApiVersion,
      protocol = if (request.hasProtocolDetails()) request.protocolDetails else null,
    )
    return checkNotNull(computationsDatabase.readComputationToken(request.key))
      .toRecordRequisitionBlobPathResponse()
  }

  private fun newCreateComputationLogEntryRequest(
    globalId: String,
    computationStage: ComputationStage,
    attempt: Int = 0,
    callerInstanceId: String = "",
  ): CreateComputationLogEntryRequest = createComputationLogEntryRequest {
    parent = ComputationParticipantKey(globalId, duchyName).toName()
    computationLogEntry = computationLogEntry {
      participantChildReferenceId = callerInstanceId
      logMessage = "Computation $globalId at stage ${computationStage.name}, attempt $attempt"
      stageAttempt = stageAttempt {
        stage = computationStage.number
        stageName = computationStage.name
        stageStartTime = clock.protoTimestamp()
        attemptNumber = attempt.toLong()
      }
    }
  }

  /**
   * Sends a status update to the Kingdom (creates a Measurement log entry).
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#1682): Handle this in the calling
   *   component instead of having a connection from the Duchy internal API to the Kingdom system
   *   API.
   */
  private suspend fun sendStatusUpdateToKingdom(request: CreateComputationLogEntryRequest) {
    try {
      computationLogEntriesClient.createComputationLogEntry(request)
    } catch (e: StatusException) {
      logger.log(Level.WARNING, e) { "Failed to update status change to the kingdom." }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
