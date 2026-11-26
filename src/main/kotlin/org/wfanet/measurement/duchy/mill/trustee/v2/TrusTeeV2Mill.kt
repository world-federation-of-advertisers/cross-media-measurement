/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.Measurement.Result.MultimeasurementResult
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients.PermanentErrorException
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.trustee.TrusTeeRequisitionDecryptor
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2.Stage
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt

/**
 * The Mill of the TrusTEE multimeasurement protocol.
 *
 * It reads the base vector of every fulfilled requisition, evaluates every query of the computation
 * over them with [MultimeasurementEvaluator], and sets the result on the Kingdom's `Computation` as
 * an unsigned, unencrypted `MultimeasurementResult`.
 *
 * @param requisitionDecryptor reads, and where encrypted decrypts, each requisition's vector
 * @param privacyParams the noise mechanism and thresholds fixed in the attested image
 * @param fallbackEventDescriptor the event descriptor to resolve population attributes against when
 *   a computation carries none, or `null` to fail such a computation
 */
class TrusTeeV2Mill(
  millId: String,
  duchyId: String,
  signingKey: SigningKeyHandle,
  consentSignalCert: Certificate,
  dataClients: ComputationDataClients,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  systemComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  systemComputationLogEntriesClient: ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
  computationStatsClient: ComputationStatsGrpcKt.ComputationStatsCoroutineStub,
  workLockDuration: Duration,
  private val requisitionDecryptor: TrusTeeRequisitionDecryptor,
  private val privacyParams: CellPrivacyParams,
  private val fallbackEventDescriptor: TrusTeeV2.EventDescriptor?,
  requestChunkSizeBytes: Int = 1024 * 32,
  maximumAttempts: Int = 10,
  clock: Clock = Clock.systemUTC(),
) :
  MillBase(
    millId = millId,
    duchyId = duchyId,
    signingKey = signingKey,
    consentSignalCert = consentSignalCert,
    dataClients = dataClients,
    systemComputationParticipantsClient = systemComputationParticipantsClient,
    systemComputationsClient = systemComputationsClient,
    systemComputationLogEntriesClient = systemComputationLogEntriesClient,
    computationStatsClient = computationStatsClient,
    computationType = ComputationType.TRUS_TEE_V2,
    workLockDuration = workLockDuration,
    requestChunkSizeBytes = requestChunkSizeBytes,
    maximumAttempts = maximumAttempts,
    clock = clock,
  ) {
  override val endingStage = Stage.COMPLETE.toProtocolStage()

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasTrusTeeV2()) {
      "Only TrusTEE v2 computations are supported in this mill."
    }
    val stage: Stage = token.computationStage.trusTeeV2
    val updatedToken: ComputationToken =
      when (stage) {
        Stage.INITIALIZED -> initializationPhase(token)
        Stage.COMPUTING -> computingPhase(token)
        Stage.WAIT_TO_START,
        Stage.COMPLETE,
        Stage.STAGE_UNSPECIFIED,
        Stage.UNRECOGNIZED -> error("Unexpected stage $stage.")
      }

    val globalId = token.globalComputationId
    val updatedStage = updatedToken.computationStage.trusTeeV2
    logger.info("$globalId@$millId: Stage transitioned from $stage to $updatedStage")
  }

  private suspend fun initializationPhase(token: ComputationToken): ComputationToken {
    // A TrusTEE v2 computation sends the same empty parameters as TrusTEE.
    val requisitionParams =
      ComputationParticipantKt.requisitionParams {
        duchyCertificate = consentSignalCert.name
        trusTee = ComputationParticipant.RequisitionParams.TrusTee.getDefaultInstance()
      }
    sendRequisitionParamsToKingdom(token, requisitionParams)

    return dataClients.transitionComputationToStage(
      token,
      stage = Stage.WAIT_TO_START.toProtocolStage(),
    )
  }

  private suspend fun computingPhase(token: ComputationToken): ComputationToken {
    val details: TrusTeeV2.ComputationDetails = token.computationDetails.trusTeeV2
    val eventDescriptor: TrusTeeV2.EventDescriptor = resolveEventDescriptor(details)

    val vectors: List<BaseVector> = token.requisitionsList.map { readBaseVector(it) }
    QueryEvaluator.requireIndexesCoverRange(vectors.map { it.index })
    val vectorsByIndex: List<BaseVector> = vectors.sortedBy { it.index }

    val attributes = SubpopulationAttributes.build(details.populationSpec, eventDescriptor)
    val subpopulations =
      HashRankSubpopulationIndex.build(
        details.populationSpec,
        attributes,
        details.vidSamplingInterval.start,
        details.vidSamplingInterval.width,
      )
    for (vector in vectorsByIndex) {
      require(vector.counts.size == subpopulations.vectorSize) {
        "Base vector ${vector.index} has ${vector.counts.size} positions, expected " +
          "${subpopulations.vectorSize} for the PopulationSpec and VID sampling interval"
      }
    }

    // TODO(world-federation-of-advertisers/cross-media-measurement#4532): Remove the slices of a
    //  DataProvider whose reported impression count is below min_users before the first pass, as
    //  docs/trustee/v2-mill-evaluation.md describes. The slice test removes them regardless.
    val multimeasurementResult: MultimeasurementResult =
      MultimeasurementEvaluator(
          queries = details.queriesList,
          dataProviders = vectorsByIndex.map { it.dataProvider },
          subpopulations = subpopulations,
          vidSamplingIntervalWidth = details.vidSamplingInterval.width.toDouble(),
          privacyParams = privacyParams,
          measurementName = token.computationDetails.kingdomComputation.measurement,
        )
        .evaluate(vectorsByIndex.map { it.counts })

    sendUnencryptedResultToKingdom(
      token,
      MeasurementKt.result { this.multimeasurementResult = multimeasurementResult },
    )

    return completeComputation(token, ComputationDetails.CompletedReason.SUCCEEDED)
  }

  /**
   * Returns the event descriptor of [details], or [fallbackEventDescriptor] if it carries none.
   *
   * @throws PermanentErrorException if neither is present
   */
  private fun resolveEventDescriptor(
    details: TrusTeeV2.ComputationDetails
  ): TrusTeeV2.EventDescriptor {
    if (details.eventDescriptor.eventMessageType.isNotEmpty()) {
      return details.eventDescriptor
    }
    return fallbackEventDescriptor
      ?: throw PermanentErrorException(
        "Computation has no event_descriptor and this Mill has no fallback event descriptor, so " +
          "population attributes cannot be resolved"
      )
  }

  /** Reads the base vector [requisition] was fulfilled with. */
  private suspend fun readBaseVector(requisition: RequisitionMetadata): BaseVector {
    val details: RequisitionDetails = requisition.details
    val requisitionId: String = requisition.externalKey.externalRequisitionId
    require(details.hasVectorIndex()) { "Requisition $requisitionId has no vector_index" }
    require(details.protocol.hasTrusTeeV2()) {
      "Requisition $requisitionId was not fulfilled for TrusTEE v2"
    }
    val protocol: RequisitionDetails.RequisitionProtocol.TrusTeeV2 = details.protocol.trusTeeV2
    require(protocol.dataProviderResourceId.isNotEmpty()) {
      "Requisition $requisitionId has no DataProvider"
    }
    val data: ByteString =
      dataClients.readSingleRequisitionBlob(requisition)
        ?: throw PermanentErrorException("Requisition data not found for $requisitionId")
    return BaseVector(
      index = details.vectorIndex,
      dataProvider = protocol.dataProviderResourceId,
      counts = requisitionDecryptor.readFrequencyVector(protocol.trusTee, data),
    )
  }

  /**
   * A base vector of the computation.
   *
   * @property dataProvider resource ID of the `DataProvider` that fulfilled it
   * @property counts one unsigned count per position
   */
  private class BaseVector(val index: Int, val dataProvider: String, val counts: ByteArray)

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
