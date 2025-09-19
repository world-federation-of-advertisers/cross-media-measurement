// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.postgres

import com.google.protobuf.Empty
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.protoTimestamp
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.db.computation.toDatabaseEditToken
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationBlobReferenceReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.RequisitionReader
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.AdvanceComputationStage
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.ClaimWork
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.CreateComputation
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.DeleteComputation
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.EnqueueComputation
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.FinishComputation
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.PurgeComputations
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.RecordOutputBlobPath
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.RecordRequisitionData
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.UpdateComputationDetails
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.service.internal.ComputationAlreadyExistsException
import org.wfanet.measurement.duchy.service.internal.ComputationDetailsNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationInitialStageInvalidException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
import org.wfanet.measurement.duchy.service.internal.computations.toAdvanceComputationStageResponse
import org.wfanet.measurement.duchy.service.internal.computations.toClaimWorkResponse
import org.wfanet.measurement.duchy.service.internal.computations.toCreateComputationResponse
import org.wfanet.measurement.duchy.service.internal.computations.toFinishComputationResponse
import org.wfanet.measurement.duchy.service.internal.computations.toGetComputationTokenResponse
import org.wfanet.measurement.duchy.service.internal.computations.toRecordOutputBlobPathResponse
import org.wfanet.measurement.duchy.service.internal.computations.toRecordRequisitionBlobPathResponse
import org.wfanet.measurement.duchy.service.internal.computations.toUpdateComputationDetailsResponse
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
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
import org.wfanet.measurement.internal.duchy.getComputationIdsResponse
import org.wfanet.measurement.internal.duchy.purgeComputationsResponse
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.CreateComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.computationLogEntry
import org.wfanet.measurement.system.v1alpha.createComputationLogEntryRequest
import org.wfanet.measurement.system.v1alpha.stageAttempt

/** Implementation of the Computations service for Postgres database. */
class PostgresComputationsService(
  private val computationTypeEnumHelper: ComputationTypeEnumHelper<ComputationType>,
  private val protocolStagesEnumHelper:
    ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>,
  private val computationProtocolStageDetailsHelper:
    ComputationProtocolStageDetailsHelper<
      ComputationType,
      ComputationStage,
      ComputationStageDetails,
      ComputationDetails,
    >,
  private val client: DatabaseClient,
  private val idGenerator: IdGenerator,
  private val duchyName: String,
  private val computationStore: ComputationStore,
  private val requisitionStore: RequisitionStore,
  private val computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val clock: Clock = Clock.systemUTC(),
  private val defaultLockDuration: Duration = Duration.ofMinutes(5),
) : ComputationsCoroutineImplBase(coroutineContext) {

  private val computationReader = ComputationReader(protocolStagesEnumHelper)
  private val computationBlobReferenceReader = ComputationBlobReferenceReader()
  private val requisitionReader = RequisitionReader()

  override suspend fun createComputation(
    request: CreateComputationRequest
  ): CreateComputationResponse {
    grpcRequire(request.globalComputationId.isNotEmpty()) {
      "global_computation_id is not specified."
    }

    val token =
      try {
        CreateComputation(
            request.globalComputationId,
            request.computationType,
            protocolStagesEnumHelper.getValidInitialStage(request.computationType).first(),
            request.stageDetails,
            request.computationDetails,
            request.requisitionsList,
            clock,
            computationTypeEnumHelper,
            protocolStagesEnumHelper,
            computationProtocolStageDetailsHelper,
            computationReader,
          )
          .execute(client, idGenerator)
      } catch (ex: ComputationInitialStageInvalidException) {
        throw ex.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      } catch (ex: ComputationAlreadyExistsException) {
        throw ex.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      }

    return token.toCreateComputationResponse()
  }

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    grpcRequire(request.owner.isNotEmpty()) { "owner is not specified." }

    val lockDuration =
      if (request.hasLockDuration()) request.lockDuration.toDuration() else defaultLockDuration
    val claimedToken =
      try {
        ClaimWork(
            request.computationType,
            request.prioritizedStagesList,
            request.owner,
            lockDuration,
            clock,
            computationTypeEnumHelper,
            protocolStagesEnumHelper,
            computationReader,
          )
          .execute(client, idGenerator)
      } catch (e: ComputationNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: ComputationDetailsNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }

    if (claimedToken != null) {
      sendStatusUpdateToKingdom(
        newCreateComputationLogEntryRequest(
          claimedToken.globalComputationId,
          claimedToken.computationStage,
          attempt = claimedToken.attempt,
          callerInstanceId = request.owner,
        )
      )
      return claimedToken.toClaimWorkResponse()
    }

    return ClaimWorkResponse.getDefaultInstance()
  }

  override suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    val token =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (request.keyCase) {
        KeyCase.GLOBAL_COMPUTATION_ID ->
          computationReader.readComputationToken(client, request.globalComputationId)
        KeyCase.REQUISITION_KEY ->
          computationReader.readComputationToken(client, request.requisitionKey)
        KeyCase.KEY_NOT_SET -> failGrpc(Status.INVALID_ARGUMENT) { "key not set" }
      } ?: failGrpc(Status.NOT_FOUND) { "Computation not found" }

    return token.toGetComputationTokenResponse()
  }

  override suspend fun deleteComputation(request: DeleteComputationRequest): Empty {
    val computationBlobKeys =
      computationBlobReferenceReader.readComputationBlobKeys(
        client.singleUse(),
        request.localComputationId,
      )
    for (blobKey in computationBlobKeys) {
      computationStore.get(blobKey)?.delete()
    }

    val requisitionBlobKeys =
      requisitionReader.readRequisitionBlobKeys(client.singleUse(), request.localComputationId)
    for (blobKey in requisitionBlobKeys) {
      requisitionStore.get(blobKey)?.delete()
    }

    DeleteComputation(request.localComputationId).execute(client, idGenerator)
    return Empty.getDefaultInstance()
  }

  override suspend fun purgeComputations(
    request: PurgeComputationsRequest
  ): PurgeComputationsResponse {
    grpcRequire(
      request.stagesList.all {
        protocolStagesEnumHelper.validTerminalStage(
          protocolStagesEnumHelper.stageToProtocol(it),
          it,
        )
      }
    ) {
      "Requested stage list contains non terminal stage."
    }
    val purgeResult =
      PurgeComputations(
          request.stagesList,
          request.updatedBefore.toInstant(),
          request.force,
          computationReader,
        )
        .execute(client, idGenerator)

    return purgeComputationsResponse {
      purgeCount = purgeResult.purgeCount
      purgeSample += purgeResult.purgeSamples
    }
  }

  override suspend fun finishComputation(
    request: FinishComputationRequest
  ): FinishComputationResponse {
    val writer =
      FinishComputation(
        request.token.toDatabaseEditToken(),
        endingStage = request.endingComputationStage,
        endComputationReason =
          when (request.reason) {
            ComputationDetails.CompletedReason.SUCCEEDED -> EndComputationReason.SUCCEEDED
            ComputationDetails.CompletedReason.FAILED -> EndComputationReason.FAILED
            ComputationDetails.CompletedReason.CANCELED -> EndComputationReason.CANCELED
            else -> error("Unknown CompletedReason ${request.reason}")
          },
        computationDetails = request.token.computationDetails,
        clock = clock,
        protocolStagesEnumHelper = protocolStagesEnumHelper,
        protocolStageDetailsHelper = computationProtocolStageDetailsHelper,
        computationReader = computationReader,
      )
    val token: ComputationToken =
      try {
        writer.execute(client, idGenerator)
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

    return token.toFinishComputationResponse()
  }

  override suspend fun updateComputationDetails(
    request: UpdateComputationDetailsRequest
  ): UpdateComputationDetailsResponse {
    require(request.token.computationDetails.protocolCase == request.details.protocolCase) {
      "The protocol type cannot change."
    }

    val writer =
      UpdateComputationDetails(
        token = request.token.toDatabaseEditToken(),
        clock = clock,
        computationDetails = request.details,
        requisitionEntries = request.requisitionsList,
        computationReader = computationReader,
      )
    val token: ComputationToken =
      try {
        writer.execute(client, idGenerator)
      } catch (e: ComputationNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: ComputationTokenVersionMismatchException) {
        throw e.asStatusRuntimeException(Status.Code.ABORTED)
      }

    return token.toUpdateComputationDetailsResponse()
  }

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {
    val writer =
      RecordOutputBlobPath(
        token = request.token.toDatabaseEditToken(),
        clock = clock,
        blobRef = BlobRef(request.outputBlobId, request.blobPath),
        protocolStagesEnumHelper = protocolStagesEnumHelper,
        computationReader = computationReader,
      )
    val token: ComputationToken =
      try {
        writer.execute(client, idGenerator)
      } catch (e: ComputationNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: ComputationTokenVersionMismatchException) {
        throw e.asStatusRuntimeException(Status.Code.ABORTED)
      }

    return token.toRecordOutputBlobPathResponse()
  }

  override suspend fun advanceComputationStage(
    request: AdvanceComputationStageRequest
  ): AdvanceComputationStageResponse {
    val lockExtension: Duration =
      if (request.hasLockExtension()) request.lockExtension.toDuration() else defaultLockDuration
    val afterTransition =
      when (request.afterTransition) {
        AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
          AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE ->
          AfterTransition.DO_NOT_ADD_TO_QUEUE
        AdvanceComputationStageRequest.AfterTransition.RETAIN_AND_EXTEND_LOCK ->
          AfterTransition.CONTINUE_WORKING
        else ->
          error(
            "Unsupported AdvanceComputationStageRequest.AfterTransition '${request.afterTransition}'. "
          )
      }

    val writer =
      AdvanceComputationStage(
        request.token.toDatabaseEditToken(),
        nextStage = request.nextComputationStage,
        nextStageDetails = request.stageDetails,
        inputBlobPaths = request.inputBlobsList,
        passThroughBlobPaths = request.passThroughBlobsList,
        outputBlobs = request.outputBlobs,
        afterTransition = afterTransition,
        lockExtension = lockExtension,
        clock = clock,
        protocolStagesEnumHelper = protocolStagesEnumHelper,
        computationReader = computationReader,
      )
    val token: ComputationToken =
      try {
        writer.execute(client, idGenerator)
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

    return token.toAdvanceComputationStageResponse()
  }

  override suspend fun getComputationIds(
    request: GetComputationIdsRequest
  ): GetComputationIdsResponse {
    val ids = computationReader.readGlobalComputationIds(client.singleUse(), request.stagesList)
    return getComputationIdsResponse { globalIds += ids }
  }

  override suspend fun enqueueComputation(
    request: EnqueueComputationRequest
  ): EnqueueComputationResponse {
    grpcRequire(request.delaySecond >= 0) {
      "DelaySecond ${request.delaySecond} should be non-negative."
    }
    val writer =
      EnqueueComputation(
        request.token.localComputationId,
        request.token.version,
        request.delaySecond.toLong(),
        clock,
      )
    try {
      writer.execute(client, idGenerator)
    } catch (e: ComputationNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ComputationTokenVersionMismatchException) {
      throw e.asStatusRuntimeException(Status.Code.ABORTED)
    }

    return EnqueueComputationResponse.getDefaultInstance()
  }

  override suspend fun recordRequisitionFulfillment(
    request: RecordRequisitionFulfillmentRequest
  ): RecordRequisitionFulfillmentResponse {
    val token =
      RecordRequisitionData(
          clock = clock,
          localId = request.token.localComputationId,
          externalRequisitionKey = request.key,
          pathToBlob = request.blobPath,
          publicApiVersion = request.publicApiVersion,
          protocolDetails = if (request.hasProtocolDetails()) request.protocolDetails else null,
          computationReader = computationReader,
        )
        .execute(client, idGenerator)

    return token.toRecordRequisitionBlobPathResponse()
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

  private suspend fun sendStatusUpdateToKingdom(request: CreateComputationLogEntryRequest) {
    try {
      computationLogEntriesClient.createComputationLogEntry(request)
    } catch (ignored: Exception) {
      logger.log(Level.WARNING, "Failed to update status change to the kingdom. $ignored")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
