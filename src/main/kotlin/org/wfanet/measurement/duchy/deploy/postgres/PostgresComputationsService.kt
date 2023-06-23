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

import io.grpc.Status
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
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.deploy.postgres.readers.ComputationReader
import org.wfanet.measurement.duchy.deploy.postgres.writers.ClaimWork
import org.wfanet.measurement.duchy.deploy.postgres.writers.CreateComputation
import org.wfanet.measurement.duchy.name
import org.wfanet.measurement.duchy.number
import org.wfanet.measurement.duchy.service.internal.ComputationDetailsNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationInitialStageInvalidException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.computations.toClaimWorkResponse
import org.wfanet.measurement.duchy.service.internal.computations.toGetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.ClaimWorkRequest
import org.wfanet.measurement.internal.duchy.ClaimWorkResponse
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.CreateComputationResponse
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest.KeyCase
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.createComputationResponse
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
  private val computationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  private val clock: Clock = Clock.systemUTC(),
  private val defaultLockDuration: Duration = Duration.ofMinutes(5),
) : ComputationsCoroutineImplBase() {

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

        ComputationReader(protocolStageEnumHelper)
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
        ClaimWork<ComputationType, ComputationStageDetails, ComputationStage, ComputationDetails>(
            request.computationType,
            request.owner,
            lockDuration,
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
          ?: failGrpc(Status.UNKNOWN) { "Claimed computation $claimed not found." }

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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
