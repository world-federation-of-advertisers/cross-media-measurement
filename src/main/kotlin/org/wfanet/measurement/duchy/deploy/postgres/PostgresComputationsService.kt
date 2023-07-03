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

package org.wfanet.measurement.duchy.deploy.postgres

import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
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
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.deploy.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.postgres.writers.AdvanceComputationStage
import org.wfanet.measurement.duchy.deploy.postgres.writers.ClaimWork
import org.wfanet.measurement.duchy.deploy.postgres.writers.CreateComputation
import org.wfanet.measurement.duchy.deploy.postgres.writers.DeleteComputation
import org.wfanet.measurement.duchy.deploy.postgres.writers.EndComputation
import org.wfanet.measurement.duchy.deploy.postgres.writers.EnqueueComputation
import org.wfanet.measurement.duchy.deploy.postgres.writers.RecordOutputBlobPath
import org.wfanet.measurement.duchy.deploy.postgres.writers.RecordRequisitionBlobPath
import org.wfanet.measurement.duchy.deploy.postgres.writers.UpdateComputationDetails
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.service.internal.ComputationAlreadyExistsException
import org.wfanet.measurement.duchy.service.internal.ComputationDetailsNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationInitialStageInvalidException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
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
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageRequest
import org.wfanet.measurement.internal.duchy.AdvanceComputationStageResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
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
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathRequest
import org.wfanet.measurement.internal.duchy.RecordRequisitionBlobPathResponse
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsRequest
import org.wfanet.measurement.internal.duchy.UpdateComputationDetailsResponse
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
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
      ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
    >,
  private val client: DatabaseClient,
  private val idGenerator: IdGenerator,
  private val duchyName: String,
  private val computationStorageClient: ComputationStore,
  private val requisitionStorageClient: RequisitionStore,
  private val computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
  private val defaultLockDuration: Duration = Duration.ofMinutes(5),
) : ComputationsCoroutineImplBase() {

  private val computationReader = ComputationReader(protocolStagesEnumHelper)

  override suspend fun createComputation(
    request: CreateComputationRequest
  ): CreateComputationResponse {
    grpcRequire(request.globalComputationId.isNotEmpty()) {
      "global_computation_id is not specified."
    }

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
          computationProtocolStageDetailsHelper
        )
        .execute(client, idGenerator)
    } catch (ex: ComputationInitialStageInvalidException) {
      throw ex.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    } catch (ex: ComputationAlreadyExistsException) {
      throw ex.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val token =
      computationReader.readComputationToken(client, request.globalComputationId)
        ?: failGrpc(Status.INTERNAL) { "Created computation not found." }
    return token.toCreateComputationResponse()
  }

  override suspend fun claimWork(request: ClaimWorkRequest): ClaimWorkResponse {
    grpcRequire(request.owner.isNotEmpty()) { "owner is not specified." }

    val lockDuration =
      if (request.hasLockDuration()) request.lockDuration.toDuration() else defaultLockDuration
    val claimed =
      try {
        ClaimWork(
            request.computationType,
            request.owner,
            lockDuration,
            clock,
            computationTypeEnumHelper,
            protocolStagesEnumHelper,
          )
          .execute(client, idGenerator)
      } catch (e: ComputationNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: ComputationDetailsNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }

    return if (claimed != null) {
      val token =
        ComputationReader(protocolStagesEnumHelper).readComputationToken(client, claimed)
          ?: failGrpc(Status.INTERNAL) { "Claimed computation $claimed not found." }

      sendStatusUpdateToKingdom(
        newCreateComputationLogEntryRequest(
          token.globalComputationId,
          token.computationStage,
          token.attempt.toLong()
        )
      )
      token.toClaimWorkResponse()
    } else {
      ClaimWorkResponse.getDefaultInstance()
    }
  }

  override suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    val reader = ComputationReader(protocolStagesEnumHelper)
    val token =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (request.keyCase) {
        KeyCase.GLOBAL_COMPUTATION_ID ->
          reader.readComputationToken(client, request.globalComputationId)
        KeyCase.REQUISITION_KEY -> reader.readComputationToken(client, request.requisitionKey)
        KeyCase.KEY_NOT_SET -> failGrpc(Status.INVALID_ARGUMENT) { "key not set" }
      }
        ?: failGrpc(Status.NOT_FOUND) { "Computation not found" }

    return token.toGetComputationTokenResponse()
  }

  override suspend fun deleteComputation(request: DeleteComputationRequest): Empty {
    val reader = ComputationReader(protocolStagesEnumHelper)

    val computationBlobKeys =
      reader.readComputationBlobKeys(client.singleUse(), request.localComputationId)
    for (blobKey in computationBlobKeys) {
      try {
        computationStorageClient.get(blobKey)?.delete()
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.NOT_FOUND) {
          throw e
        }
      }
    }

    val requisitionBlobKeys =
      reader.readRequisitionBlobKeys(client.singleUse(), request.localComputationId)
    for (blobKey in requisitionBlobKeys) {
      try {
        requisitionStorageClient.get(blobKey)?.delete()
      } catch (e: StatusException) {
        if (e.status.code != Status.NOT_FOUND.code) {
          throw e
        }
      }
    }

    DeleteComputation(localComputationId = request.localComputationId).execute(client, idGenerator)
    return Empty.getDefaultInstance()
  }

  override suspend fun purgeComputations(
    request: PurgeComputationsRequest
  ): PurgeComputationsResponse {
    var deleted = 0
    try {
      val globalIds: Set<String> =
        computationReader.readGlobalComputationIds(
          client.singleUse(),
          request.stagesList,
          request.updatedBefore.toInstant()
        )
      if (!request.force) {
        return purgeComputationsResponse {
          purgeCount = globalIds.size
          purgeSample += globalIds
        }
      }
      for (globalId in globalIds) {
        // TODO: move this to a writer
        val computation: ComputationReader.Computation =
          computationReader.readComputation(client.singleUse(), globalId) ?: continue
        val computationStageEnum =
          protocolStagesEnumHelper.longValuesToComputationStageEnum(
            ComputationStageLongValues(computation.protocol, computation.computationStage)
          )
        val protocolEnum = computationTypeEnumHelper.longToProtocolEnum(computation.protocol)
        val endComputationStage = getEndingComputationStage(computationStageEnum)

        if (!isTerminated(computationStageEnum)) {
          EndComputation(
              localComputationId = computation.localComputationId,
              editVersion = computation.version,
              protocol = protocolEnum,
              currentAttempt = computation.nextAttempt.toLong(),
              currentStage = computationStageEnum,
              endingStage = endComputationStage,
              endComputationReason = EndComputationReason.FAILED,
              computationDetails = computation.computationDetails,
              clock = clock,
              protocolStagesEnumHelper = protocolStagesEnumHelper,
              protocolStageDetailsHelper = computationProtocolStageDetailsHelper,
            )
            .execute(client, idGenerator)
          sendStatusUpdateToKingdom(
            newCreateComputationLogEntryRequest(
              computation.globalComputationId,
              endComputationStage,
            )
          )
        }
        DeleteComputation(computation.localComputationId).execute(client, idGenerator)
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
    EndComputation(
        localComputationId = request.token.localComputationId,
        editVersion = request.token.version,
        protocol = protocolStagesEnumHelper.stageToProtocol(request.token.computationStage),
        currentAttempt = request.token.attempt.toLong(),
        currentStage = request.token.computationStage,
        endingStage = request.endingComputationStage,
        endComputationReason =
          when (val it = request.reason) {
            ComputationDetails.CompletedReason.SUCCEEDED -> EndComputationReason.SUCCEEDED
            ComputationDetails.CompletedReason.FAILED -> EndComputationReason.FAILED
            ComputationDetails.CompletedReason.CANCELED -> EndComputationReason.CANCELED
            else -> error("Unknown CompletedReason $it")
          },
        computationDetails = request.token.computationDetails,
        clock = clock,
        protocolStagesEnumHelper = protocolStagesEnumHelper,
        protocolStageDetailsHelper = computationProtocolStageDetailsHelper,
      )
      .execute(client, idGenerator)

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.endingComputationStage
      )
    )

    val token =
      computationReader.readComputationToken(client, request.token.globalComputationId)
        ?: failGrpc(Status.INTERNAL) {
          "Finished computation ${request.token.globalComputationId} not found."
        }

    return token.toFinishComputationResponse()
  }

  override suspend fun updateComputationDetails(
    request: UpdateComputationDetailsRequest
  ): UpdateComputationDetailsResponse {
    require(request.token.computationDetails.protocolCase == request.details.protocolCase) {
      "The protocol type cannot change."
    }
    UpdateComputationDetails(
        clock = clock,
        localId = request.token.localComputationId,
        editVersion = request.token.version,
        computationDetails = request.details,
        requisitionEntries = request.requisitionsList
      )
      .execute(client, idGenerator)

    val token =
      computationReader.readComputationToken(client, request.token.globalComputationId)
        ?: failGrpc(Status.INTERNAL) {
          "Updated computation ${request.token.globalComputationId} not found."
        }
    return token.toUpdateComputationDetailsResponse()
  }

  override suspend fun recordOutputBlobPath(
    request: RecordOutputBlobPathRequest
  ): RecordOutputBlobPathResponse {

    RecordOutputBlobPath(
        clock = clock,
        localId = request.token.localComputationId,
        editVersion = request.token.version,
        stage = request.token.computationStage,
        blobRef = BlobRef(request.outputBlobId, request.blobPath),
        protocolStagesEnumHelper = protocolStagesEnumHelper
      )
      .execute(client, idGenerator)

    val token =
      computationReader.readComputationToken(client, request.token.globalComputationId)
        ?: failGrpc(Status.INTERNAL) {
          "Computation ${request.token.globalComputationId} not found."
        }
    return token.toRecordOutputBlobPathResponse()
  }

  override suspend fun advanceComputationStage(
    request: AdvanceComputationStageRequest
  ): AdvanceComputationStageResponse {
    val lockExtension: Duration =
      if (request.hasLockExtension()) request.lockExtension.toDuration() else defaultLockDuration
    val afterTransition =
      when (val it = request.afterTransition) {
        AdvanceComputationStageRequest.AfterTransition.ADD_UNCLAIMED_TO_QUEUE ->
          AfterTransition.ADD_UNCLAIMED_TO_QUEUE
        AdvanceComputationStageRequest.AfterTransition.DO_NOT_ADD_TO_QUEUE ->
          AfterTransition.DO_NOT_ADD_TO_QUEUE
        AdvanceComputationStageRequest.AfterTransition.RETAIN_AND_EXTEND_LOCK ->
          AfterTransition.CONTINUE_WORKING
        else -> error("Unsupported AdvanceComputationStageRequest.AfterTransition '$it'. ")
      }

    AdvanceComputationStage(
        clock,
        localComputationId = request.token.localComputationId,
        currentStage = request.token.computationStage,
        attempt = request.token.attempt.toLong(),
        editVersion = request.token.version,
        nextStage = request.nextComputationStage,
        inputBlobPaths = request.inputBlobsList,
        passThroughBlobPaths = request.passThroughBlobsList,
        outputBlobs = request.outputBlobs,
        afterTransition = afterTransition,
        nextStageDetails = request.stageDetails,
        lockExtension = lockExtension,
        protocolStagesEnumHelper = protocolStagesEnumHelper
      )
      .execute(client, idGenerator)

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.nextComputationStage
      )
    )

    val token =
      computationReader.readComputationToken(client, request.token.globalComputationId)
        ?: failGrpc(Status.INTERNAL) {
          "Computation ${request.token.globalComputationId} not found."
        }
    return token.toAdvanceComputationStageResponse()
  }

  override suspend fun getComputationIds(
    request: GetComputationIdsRequest
  ): GetComputationIdsResponse {
    val ids = computationReader.readGlobalComputationIds(client.singleUse(), request.stagesList)
    return GetComputationIdsResponse.newBuilder().addAllGlobalIds(ids).build()
  }

  override suspend fun enqueueComputation(
    request: EnqueueComputationRequest
  ): EnqueueComputationResponse {
    grpcRequire(request.delaySecond >= 0) {
      "DelaySecond ${request.delaySecond} should be non-negative."
    }
    EnqueueComputation(
        clock,
        request.token.localComputationId,
        request.token.version,
        request.delaySecond.toLong()
      )
      .execute(client, idGenerator)
    return EnqueueComputationResponse.getDefaultInstance()
  }

  override suspend fun recordRequisitionBlobPath(
    request: RecordRequisitionBlobPathRequest
  ): RecordRequisitionBlobPathResponse {
    RecordRequisitionBlobPath(
        clock = clock,
        localComputationId = request.token.localComputationId,
        externalRequisitionKey = request.key,
        pathToBlob = request.blobPath
      )
      .execute(client, idGenerator)

    val token =
      computationReader.readComputationToken(client, request.token.globalComputationId)
        ?: failGrpc(Status.INTERNAL) {
          "Computation ${request.token.globalComputationId} not found."
        }
    return token.toRecordRequisitionBlobPathResponse()
  }

  private fun newCreateComputationLogEntryRequest(
    globalId: String,
    computationStage: ComputationStage,
    attempt: Long = 0L
  ): CreateComputationLogEntryRequest {
    return createComputationLogEntryRequest {
      parent = ComputationParticipantKey(globalId, duchyName).toName()
      computationLogEntry {
        // TODO: maybe set participantChildReferenceId
        logMessage =
          "Computation $globalId at stage ${computationStage.name}, " + "attempt $attempt"
        stageAttempt {
          stage = computationStage.number
          stageName = computationStage.name
          stageStartTime = clock.protoTimestamp()
          attemptNumber = attempt
        }
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

  private fun isTerminated(computationStage: ComputationStage): Boolean {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (computationStage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        computationStage.liquidLegionsSketchAggregationV2 ==
          LiquidLegionsSketchAggregationV2.Stage.COMPLETE
      ComputationStage.StageCase.STAGE_NOT_SET -> false
    }
  }

  private fun getEndingComputationStage(computationStage: ComputationStage): ComputationStage {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    return when (computationStage.stageCase) {
      ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
        LiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage()
      ComputationStage.StageCase.STAGE_NOT_SET -> error("protocol not set")
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
