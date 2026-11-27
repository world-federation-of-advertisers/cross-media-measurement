// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee

import java.nio.ByteBuffer
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeParams
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeProcessor
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeReachAndFrequencyParams
import org.wfanet.measurement.duchy.mill.trustee.processor.TrusTeeReachParams
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.protocol.TrusTee
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.ComputationDetails as TrusTeeDetails
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt

class TrusTeeMill(
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
  private val trusTeeProcessorFactory: TrusTeeProcessor.Factory,
  private val requisitionDecryptor: TrusTeeRequisitionDecryptor,
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
    computationType = ComputationType.TRUS_TEE,
    workLockDuration = workLockDuration,
    requestChunkSizeBytes = requestChunkSizeBytes,
    maximumAttempts = maximumAttempts,
    clock = clock,
  ) {

  override val endingStage = Stage.COMPLETE.toProtocolStage()

  override suspend fun processComputationImpl(token: ComputationToken) {
    require(token.computationDetails.hasTrusTee()) {
      "Only TrusTEE computation is supported in this mill."
    }
    val stage = token.computationStage.trusTee
    val action = actions[stage] ?: error("Unexpected stage $stage.")
    val updatedToken = action(token)

    val globalId = token.globalComputationId
    val updatedStage = updatedToken.computationStage.trusTee
    logger.info("$globalId@$millId: Stage transitioned from $stage to $updatedStage")
  }

  private val actions =
    mapOf(Stage.INITIALIZED to ::initializationPhase, Stage.COMPUTING to ::computingPhase)

  private suspend fun initializationPhase(token: ComputationToken): ComputationToken {
    sendTrusTeeRequisitionParamsToKingdom(token)

    return dataClients.transitionComputationToStage(
      token,
      stage = Stage.WAIT_TO_START.toProtocolStage(),
    )
  }

  private suspend fun computingPhase(token: ComputationToken): ComputationToken {
    val trusTeeDetails: TrusTeeDetails = token.computationDetails.trusTee
    val trusTeeParams = trusTeeDetails.toTrusTeeParams()

    val processor: TrusTeeProcessor = trusTeeProcessorFactory.create(trusTeeParams)

    for (requisition: RequisitionMetadata in token.requisitionsList) {
      // TODO(world-federation-of-advertisers/cross-media-measurement#2800): Use
      //  StreamingAeadStorage instead to read and decrypt requisition data.
      processor.addFrequencyVector(
        requisitionDecryptor.readFrequencyVector(
          requisition.details.protocol.trusTee,
          readRequisitionData(requisition),
        )
      )
    }

    val computationResult = processor.computeResult()

    sendResultToKingdom(token, computationResult)

    return completeComputation(token, ComputationDetails.CompletedReason.SUCCEEDED)
  }

  /** Converts internal [TrusTee.ComputationDetails] to the internal [TrusTeeParams]. */
  fun TrusTeeDetails.toTrusTeeParams(): TrusTeeParams {
    val isNoNoise = parameters.noiseMechanism == NoiseMechanism.NONE

    val resultMinimumThresholds: ResultMinimumThresholds? =
      if (parameters.hasResultMinimumThresholds()) {
        require(parameters.resultMinimumThresholds.minUsers > 0) {
          "small-cell suppression minUsers must be greater than 0, got ${parameters.resultMinimumThresholds.minUsers}"
        }
        require(parameters.resultMinimumThresholds.minImpressions > 0) {
          "small-cell suppression minImpressions must be greater than 0, got ${parameters.resultMinimumThresholds.minImpressions}"
        }
        ResultMinimumThresholds(
          minUsers = parameters.resultMinimumThresholds.minUsers,
          minImpressions = parameters.resultMinimumThresholds.minImpressions,
        )
      } else {
        null
      }

    return when (type) {
      TrusTeeDetails.Type.REACH -> {
        if (!isNoNoise) {
          require(parameters.hasReachDpParams()) {
            "Reach DP params are required for a Reach-only TrusTee computation."
          }
        }
        TrusTeeReachParams(
          parameters.vidSamplingIntervalWidth.toDouble(),
          if (isNoNoise) null else parameters.reachDpParams,
          resultMinimumThresholds,
          parameters.noiseMechanism,
        )
      }
      TrusTeeDetails.Type.REACH_AND_FREQUENCY -> {
        if (!isNoNoise) {
          require(parameters.hasReachDpParams()) {
            "Reach DP params are required for a Reach-and-Frequency TrusTee computation."
          }
          require(parameters.hasFrequencyDpParams()) {
            "Frequency DP params are required for a Reach-and-Frequency TrusTee computation."
          }
        }
        TrusTeeReachAndFrequencyParams(
          parameters.maximumFrequency,
          parameters.vidSamplingIntervalWidth.toDouble(),
          if (isNoNoise) null else parameters.reachDpParams,
          if (isNoNoise) null else parameters.frequencyDpParams,
          resultMinimumThresholds,
          parameters.noiseMechanism,
        )
      }
      TrusTeeDetails.Type.TYPE_UNSPECIFIED,
      TrusTeeDetails.Type.UNRECOGNIZED ->
        throw IllegalArgumentException(
          "Unsupported or unspecified TrusTEE computation type in ComputationDetails: $type"
        )
    }
  }

  private fun toIntArray(bytes: ByteArray): IntArray {
    require(bytes.size % 4 == 0) { "Input ByteArray size (${bytes.size}) must be a multiple of 4." }

    val intArray = IntArray(bytes.size / 4)
    val buffer = ByteBuffer.wrap(bytes)
    buffer.asIntBuffer().get(intArray)

    return intArray
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
