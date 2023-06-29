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
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.deploy.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.postgres.writers.ClaimWork
import org.wfanet.measurement.duchy.deploy.postgres.writers.CreateComputation
import org.wfanet.measurement.duchy.deploy.postgres.writers.DeleteComputation
import org.wfanet.measurement.duchy.deploy.postgres.writers.EndComputation
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.service.internal.ComputationDetailsNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationInitialStageInvalidException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.computations.toClaimWorkResponse
import org.wfanet.measurement.duchy.service.internal.computations.toFinishComputationResponse
import org.wfanet.measurement.duchy.service.internal.computations.toGetComputationTokenResponse
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.toProtocolStage
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
import org.wfanet.measurement.internal.duchy.FinishComputationRequest
import org.wfanet.measurement.internal.duchy.FinishComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest.KeyCase
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.PurgeComputationsRequest
import org.wfanet.measurement.internal.duchy.PurgeComputationsResponse
import org.wfanet.measurement.internal.duchy.createComputationResponse
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
  private val protocolStageEnumHelper:
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

  private val computationReader = ComputationReader(protocolStageEnumHelper)

  override suspend fun createComputation(
    request: CreateComputationRequest
  ): CreateComputationResponse {
    grpcRequire(request.globalComputationId.isNotEmpty()) {
      "global_computation_id is not specified."
    }

    val computationToken =
      try {
        CreateComputation(
          request.globalComputationId,
          request.computationType,
          protocolStageEnumHelper.getValidInitialStage(request.computationType).first(),
          request.stageDetails,
          request.computationDetails,
          request.requisitionsList,
          clock,
          computationTypeEnumHelper,
          protocolStageEnumHelper,
          computationProtocolStageDetailsHelper
        )
          .execute(client, idGenerator)

        computationReader
          .readComputationToken(client, request.globalComputationId)
          ?: failGrpc(Status.INTERNAL) { "Created computation not found." }
      } catch (ex: ComputationInitialStageInvalidException) {
        throw ex.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    return createComputationResponse { token = computationToken }
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
          protocolStageEnumHelper,
        )
          .execute(client, idGenerator)
      } catch (e: ComputationNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: ComputationDetailsNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }

    return if (claimed != null) {
      val token =
        ComputationReader(protocolStageEnumHelper).readComputationToken(client, claimed)
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
    val reader = ComputationReader(protocolStageEnumHelper)
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
    val reader = ComputationReader(protocolStageEnumHelper)

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
    val reader = ComputationReader(protocolStageEnumHelper)

    val stages =
      request.stagesList.map { protocolStageEnumHelper.computationStageEnumToLongValues(it) }
    val computationTypes = stages.map { it.protocol }.distinct()
    grpcRequire(computationTypes.count() == 1) {
      "All stages should have the same ComputationType."
    }

    var deleted = 0
    try {
      val globalIds: Set<String> =
        reader.readGlobalComputationIds(
          client.singleUse(),
          stages.map { it.stage },
          computationTypes[0],
          request.updatedBefore.toInstant()
        )
      if (!request.force) {
        return purgeComputationsResponse {
          purgeCount = globalIds.size
          purgeSample += globalIds
        }
      }
      for (globalId in globalIds) {
        val computation: ComputationReader.Computation =
          reader.readComputation(client.singleUse(), globalId) ?: continue
        val computationStageEnum =
          protocolStageEnumHelper.longValuesToComputationStageEnum(
            ComputationStageLongValues(computation.protocol, computation.computationStage)
          )
        val endComputationStage = getEndingComputationStage(computationStageEnum)
        val endComputationDetails =
          computationProtocolStageDetailsHelper.detailsFor(
            endComputationStage,
            computation.computationDetails
          )
        if (!isTerminated(computationStageEnum)) {
          EndComputation(
            localComputationId = computation.localComputationId,
            editVersion = computation.version,
            protocol = computation.protocol,
            currentAttempt = computation.nextAttempt.toLong(),
            currentStage = computation.computationStage,
            endingStage =
            protocolStageEnumHelper.computationStageEnumToLongValues(endComputationStage).stage,
            endComputationReason = EndComputationReason.FAILED,
            endComputationDetails = endComputationDetails.toByteArray(),
            endComputationDetailsJson = endComputationDetails.toJson(),
            clock = clock
          )
            .execute(client, idGenerator)
          sendStatusUpdateToKingdom(
            newCreateComputationLogEntryRequest(
              computation.globalComputationId,
              endComputationStage,
            )
          )
        }
        DeleteComputation(computation.localComputationId)
        deleted += 1
      }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Exception during Computations cleaning. $e")
    }
    return purgeComputationsResponse { this.purgeCount = deleted }
  }

  override suspend fun finishComputation(request: FinishComputationRequest): FinishComputationResponse {
    val stageLongValues = protocolStageEnumHelper.computationStageEnumToLongValues(request.token.computationStage)
    EndComputation(
      localComputationId = request.token.localComputationId,
      editVersion = request.token.version,
      protocol = stageLongValues.protocol,
      currentAttempt = request.token.attempt.toLong(),
      currentStage = stageLongValues.stage,
      endingStage = protocolStageEnumHelper.computationStageEnumToLongValues(request.endingComputationStage).stage,
      endComputationReason = when (val it = request.reason) {
        ComputationDetails.CompletedReason.SUCCEEDED -> EndComputationReason.SUCCEEDED
        ComputationDetails.CompletedReason.FAILED -> EndComputationReason.FAILED
        ComputationDetails.CompletedReason.CANCELED -> EndComputationReason.CANCELED
        else -> error("Unknown CompletedReason $it")
      },
      endComputationDetails = request.token.computationDetails.toByteArray(),
      endComputationDetailsJson = request.token.computationDetails.toJson(),
      clock = clock
    )

    sendStatusUpdateToKingdom(
      newCreateComputationLogEntryRequest(
        request.token.globalComputationId,
        request.endingComputationStage
      )
    )

    return computationReader
      .readComputationToken(client, request.token.globalComputationId)!!
      .toFinishComputationResponse()
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
