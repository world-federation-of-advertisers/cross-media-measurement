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

package org.wfanet.measurement.duchy.daemon.herald

import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.sha1Hash
import org.wfanet.measurement.duchy.daemon.utils.toDuchyDifferentialPrivacyParams
import org.wfanet.measurement.duchy.daemon.utils.toDuchyElGamalPublicKey
import org.wfanet.measurement.duchy.daemon.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.daemon.utils.toRequisitionEntries
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.Parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.updateComputationDetailsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationParticipant as SystemComputationParticipant

private const val MIN_REACH_EPSILON = 0.00001
private const val MIN_FREQUENCY_EPSILON = 0.00001

object LiquidLegionsV2Starter {

  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = Stage.COMPLETE.toProtocolStage()

  suspend fun createComputation(
    computationStorageClient: ComputationsCoroutineStub,
    systemComputation: Computation,
    liquidLegionsV2SetupConfig: LiquidLegionsV2SetupConfig,
    blobStorageBucket: String
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val initialComputationDetails =
      ComputationDetails.newBuilder()
        .apply {
          blobsStoragePrefix = "$blobStorageBucket/$globalId"
          kingdomComputation = systemComputation.toKingdomComputationDetails()
          liquidLegionsV2Builder.apply {
            role = liquidLegionsV2SetupConfig.role
            parameters = systemComputation.toLiquidLegionsV2Parameters()
          }
        }
        .build()
    val requisitions =
      systemComputation.requisitionsList.toRequisitionEntries(systemComputation.measurementSpec)

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
        globalComputationId = globalId
        computationDetails = initialComputationDetails
        this.requisitions += requisitions
      }
    )
  }

  /**
   * Orders the list of computation participants by their roles in the computation. The
   * non-aggregators are shuffled by the sha1Hash of their elgamal public keys and the global
   * computation id, the aggregator is placed at the end of the list. This return order is also the
   * order of all participants in the MPC ring structure.
   */
  private fun List<ComputationParticipant>.orderByRoles(
    globalComputationId: String,
    aggregatorId: String
  ): List<ComputationParticipant> {
    val aggregator =
      this.find { it.duchyId == aggregatorId }
        ?: error("Aggregator duchy is missing from the participants.")
    val nonAggregators = this.filter { it.duchyId != aggregatorId }
    return nonAggregators.sortedBy {
      sha1Hash(it.elGamalPublicKey.toStringUtf8() + globalComputationId)
    } + aggregator
  }

  private suspend fun updateRequisitionsAndKeySetsInternal(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub,
    systemComputation: Computation,
    aggregatorId: String
  ) {
    val updatedDetails =
      token.computationDetails
        .toBuilder()
        .apply {
          liquidLegionsV2Builder
            .clearParticipant()
            .addAllParticipant(
              systemComputation.computationParticipantsList
                .map { it.toDuchyComputationParticipant(systemComputation.publicApiVersion) }
                .orderByRoles(token.globalComputationId, aggregatorId)
            )
        }
        .build()
    val requisitions =
      systemComputation.requisitionsList.toRequisitionEntries(systemComputation.measurementSpec)
    val updateComputationDetailsRequest = updateComputationDetailsRequest {
      this.token = token
      details = updatedDetails
      this.requisitions += requisitions
    }

    val newToken =
      computationStorageClient.updateComputationDetails(updateComputationDetailsRequest).token
    logger.info(
      "[id=${token.globalComputationId}] " + "Requisitions and Duchy Elgamal Keys are now updated."
    )

    computationStorageClient.advanceComputationStage(
      computationToken = newToken,
      stage = Stage.CONFIRMATION_PHASE.toProtocolStage()
    )
  }

  suspend fun updateRequisitionsAndKeySets(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub,
    systemComputation: Computation,
    aggregatorId: String,
  ) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Liquid Legions V2 computation required"
    }

    val stage = token.computationStage.liquidLegionsSketchAggregationV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_REQUISITIONS_AND_KEY_SET.
      Stage.WAIT_REQUISITIONS_AND_KEY_SET -> {
        updateRequisitionsAndKeySetsInternal(
          token,
          computationStorageClient,
          systemComputation,
          aggregatorId
        )
        return
      }

      // For past stages, we throw.
      Stage.INITIALIZATION_PHASE -> {
        error(
          "[id=${token.globalComputationId}]: cannot update requisitions and key sets for " +
            "computation still in state ${stage.name}"
        )
      }

      // For future stages, we log and exit.
      Stage.WAIT_TO_START,
      Stage.CONFIRMATION_PHASE,
      Stage.WAIT_SETUP_PHASE_INPUTS,
      Stage.SETUP_PHASE,
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      Stage.EXECUTION_PHASE_ONE,
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      Stage.EXECUTION_PHASE_TWO,
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS,
      Stage.EXECUTION_PHASE_THREE,
      Stage.COMPLETE -> {
        logger.info(
          "[id=${token.globalComputationId}]: not updating," +
            " stage '$stage' is after WAIT_REQUISITIONS_AND_KEY_SET"
        )
        return
      }

      // For weird stages, we throw.
      Stage.UNRECOGNIZED,
      Stage.STAGE_UNKNOWN -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }

  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub
  ) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Liquid Legions V2 computation required"
    }

    val stage = token.computationStage.liquidLegionsSketchAggregationV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_TO_START.
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          inputsToNextStage = token.outputPathList(),
          stage = Stage.SETUP_PHASE.toProtocolStage()
        )
        logger.info("[id=${token.globalComputationId}] Computation is now started")
        return
      }

      // For past stages, we throw.
      Stage.INITIALIZATION_PHASE,
      Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      Stage.CONFIRMATION_PHASE -> {
        error(
          "[id=${token.globalComputationId}]: cannot start a computation still" +
            " in state ${stage.name}"
        )
      }

      // For future stages, we log and exit.
      Stage.WAIT_SETUP_PHASE_INPUTS,
      Stage.SETUP_PHASE,
      Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
      Stage.EXECUTION_PHASE_ONE,
      Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
      Stage.EXECUTION_PHASE_TWO,
      Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS,
      Stage.EXECUTION_PHASE_THREE,
      Stage.COMPLETE -> {
        logger.info(
          "[id=${token.globalComputationId}]: not starting," +
            " stage '$stage' is after WAIT_TO_START"
        )
        return
      }

      // For weird stages, we throw.
      Stage.UNRECOGNIZED,
      Stage.STAGE_UNKNOWN -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }

  private fun SystemComputationParticipant.toDuchyComputationParticipant(
    publicApiVersion: String
  ): ComputationParticipant {
    require(requisitionParams.hasLiquidLegionsV2()) {
      "Missing liquid legions v2 requisition params."
    }
    return ComputationParticipant.newBuilder()
      .also {
        it.duchyId = key.duchyId
        it.publicKey =
          requisitionParams.liquidLegionsV2.elGamalPublicKey.toDuchyElGamalPublicKey(
            Version.fromString(publicApiVersion)
          )
        it.elGamalPublicKey = requisitionParams.liquidLegionsV2.elGamalPublicKey
        it.elGamalPublicKeySignature = requisitionParams.liquidLegionsV2.elGamalPublicKeySignature
        it.duchyCertificateDer = requisitionParams.duchyCertificateDer
      }
      .build()
  }

  /** Creates a liquid legions v2 `Parameters` from the system Api computation. */
  private fun Computation.toLiquidLegionsV2Parameters(): Parameters {
    require(mpcProtocolConfig.hasLiquidLegionsV2()) {
      "Missing liquidLegionV2 in the duchy protocol config."
    }
    val llv2Config = mpcProtocolConfig.liquidLegionsV2

    return Parameters.newBuilder()
      .also {
        it.maximumFrequency = llv2Config.maximumFrequency
        it.liquidLegionsSketchBuilder.apply {
          decayRate = llv2Config.sketchParams.decayRate
          size = llv2Config.sketchParams.maxSize
        }
        it.noiseBuilder.apply {
          reachNoiseConfigBuilder.apply {
            val mpcNoise = llv2Config.mpcNoise
            blindHistogramNoise = mpcNoise.blindedHistogramNoise.toDuchyDifferentialPrivacyParams()
            noiseForPublisherNoise =
              mpcNoise.noiseForPublisherNoise.toDuchyDifferentialPrivacyParams()
          }
          when (Version.fromString(publicApiVersion)) {
            Version.V2_ALPHA -> {
              val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)
              require(measurementSpec.hasReachAndFrequency()) {
                "Missing ReachAndFrequency in the measurementSpec."
              }
              val reachAndFrequency = measurementSpec.reachAndFrequency
              require(reachAndFrequency.reachPrivacyParams.delta > 0) {
                "LLv2 requires that reach_privacy_params.delta be greater than 0"
              }
              require(reachAndFrequency.reachPrivacyParams.epsilon > MIN_REACH_EPSILON) {
                "LLv2 requires that reach_privacy_params.epsilon be greater than $MIN_REACH_EPSILON"
              }
              require(reachAndFrequency.frequencyPrivacyParams.delta > 0) {
                "LLv2 requires that frequency_privacy_params.delta be greater than 0"
              }
              require(reachAndFrequency.frequencyPrivacyParams.epsilon > MIN_FREQUENCY_EPSILON) {
                "LLv2 requires that frequency_privacy_params.epsilon be greater than " +
                  "$MIN_FREQUENCY_EPSILON"
              }
              reachNoiseConfigBuilder.globalReachDpNoise =
                reachAndFrequency.reachPrivacyParams.toDuchyDifferentialPrivacyParams()
              frequencyNoiseConfig =
                reachAndFrequency.frequencyPrivacyParams.toDuchyDifferentialPrivacyParams()
            }
            Version.VERSION_UNSPECIFIED -> error("Public api version is invalid or unspecified.")
          }
        }
        it.ellipticCurveId = llv2Config.ellipticCurveId
      }
      .build()
  }
}
