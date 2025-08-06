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

package org.wfanet.measurement.duchy.herald

import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.key
import org.wfanet.measurement.duchy.utils.sha1Hash
import org.wfanet.measurement.duchy.utils.toDuchyDifferentialPrivacyParams
import org.wfanet.measurement.duchy.utils.toDuchyElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.utils.toRequisitionEntries
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.Parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.ParametersKt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.computationDetails as llv2Details
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfigKt.reachNoiseConfig
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsV2NoiseConfig
import org.wfanet.measurement.internal.duchy.updateComputationDetailsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.Computation.MpcProtocolConfig.NoiseMechanism as SystemNoiseMechanism
import org.wfanet.measurement.system.v1alpha.ComputationParticipant as SystemComputationParticipant

/**
 * Minimum epsilon value for reach noise.
 *
 * This value is chosen due to memory constraints.
 */
private const val MIN_REACH_EPSILON = 0.00001

/**
 * Minimum epsilon value for frequency noise.
 *
 * This value is chosen due to memory constraints.
 */
private const val MIN_FREQUENCY_EPSILON = 0.00001

object LiquidLegionsV2Starter {

  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = Stage.COMPLETE.toProtocolStage()

  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsCoroutineStub,
    systemComputation: Computation,
    liquidLegionsV2SetupConfig: LiquidLegionsV2SetupConfig,
    blobStorageBucket: String,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId

    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$duchyId/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      liquidLegionsV2 = llv2Details {
        role = liquidLegionsV2SetupConfig.role
        parameters = systemComputation.toLiquidLegionsV2Parameters()
      }
    }
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
    aggregatorId: String,
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
    aggregatorId: String,
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
      stage = Stage.CONFIRMATION_PHASE.toProtocolStage(),
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
          aggregatorId,
        )
        return
      }

      // For INITIALIZATION_PHASE, it could be caused by an interrupted execution. If the ElGamal
      // keys have been set, skip the stage to catch up to the Measurement state.
      Stage.INITIALIZATION_PHASE -> {
        if (!isInitialized(token)) {
          error(
            "[id=${token.globalComputationId}]:cannot update Requisitions and keySets while " +
              "Computation details not initialized"
          )
        }
        logger.log(
          Level.WARNING,
          "[id=${token.globalComputationId}] skipping " + "INITIALIZATION_PHASE to catch up.",
        )
        val updatedToken =
          computationStorageClient.advanceComputationStage(
            token,
            stage = Stage.WAIT_REQUISITIONS_AND_KEY_SET.toProtocolStage(),
          )
        updateRequisitionsAndKeySetsInternal(
          updatedToken,
          computationStorageClient,
          systemComputation,
          aggregatorId,
        )
        return
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
        logger.warning(
          "[id=${token.globalComputationId}]: not updating," +
            " stage '$stage' is after WAIT_REQUISITIONS_AND_KEY_SET"
        )
        return
      }

      // For weird stages, we throw.
      Stage.UNRECOGNIZED,
      Stage.STAGE_UNSPECIFIED -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }

  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub,
  ) {
    require(token.computationDetails.hasLiquidLegionsV2()) {
      "Liquid Legions V2 computation required"
    }

    if (token.computationDetails.liquidLegionsV2.role == RoleInComputation.AGGREGATOR) {
      // Aggregator is waiting for input from non-aggregator to enter SETUP_PHASE instead of
      // triggered by the herald.
      logger.log(
        Level.INFO,
        "[id=${token.globalComputationId}] ignore startComputation" + "for aggregator.",
      )
      return
    }

    val stage = token.computationStage.liquidLegionsSketchAggregationV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_TO_START.
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
        )
        logger.info("[id=${token.globalComputationId}] Computation is now started")
        return
      }
      // For CONFIRMATION_PHASE, it could be caused by an interrupted execution. Skip the stage to
      // catch up to the Measurement state.
      Stage.CONFIRMATION_PHASE -> {
        logger.log(
          Level.WARNING,
          "[id=${token.globalComputationId}] skipping " + "CONFIRMATION_PHASE to catch up.",
        )
        val updatedToken =
          computationStorageClient.advanceComputationStage(
            token,
            stage = Stage.WAIT_TO_START.toProtocolStage(),
          )
        computationStorageClient.advanceComputationStage(
          computationToken = updatedToken,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
        )
        return
      }
      // For past stages, we throw.
      Stage.INITIALIZATION_PHASE,
      Stage.WAIT_REQUISITIONS_AND_KEY_SET -> {
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
        logger.warning(
          "[id=${token.globalComputationId}]: not starting," +
            " stage '$stage' is after WAIT_TO_START"
        )
        return
      }

      // For weird stages, we throw.
      Stage.UNRECOGNIZED,
      Stage.STAGE_UNSPECIFIED -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }

  private fun isInitialized(token: ComputationToken): Boolean =
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (token.computationDetails.protocolCase) {
      ComputationDetails.ProtocolCase.LIQUID_LEGIONS_V2 -> {
        token.computationDetails.liquidLegionsV2.hasLocalElgamalKey()
      }
      ComputationDetails.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 -> {
        token.computationDetails.reachOnlyLiquidLegionsV2.hasLocalElgamalKey()
      }
      ComputationDetails.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
      ComputationDetails.ProtocolCase.TRUS_TEE,
      ComputationDetails.ProtocolCase.PROTOCOL_NOT_SET -> {
        error("Invalid Protocol type in ComputationDetails.")
      }
    }

  private fun SystemComputationParticipant.toDuchyComputationParticipant(
    publicApiVersion: String
  ): ComputationParticipant {
    require(requisitionParams.hasLiquidLegionsV2()) {
      "Missing liquid legions v2 requisition params."
    }
    return computationParticipant {
      duchyId = key.duchyId
      publicKey =
        requisitionParams.liquidLegionsV2.elGamalPublicKey.toDuchyElGamalPublicKey(
          Version.fromString(publicApiVersion)
        )
      elGamalPublicKey = requisitionParams.liquidLegionsV2.elGamalPublicKey
      elGamalPublicKeySignature = requisitionParams.liquidLegionsV2.elGamalPublicKeySignature
      elGamalPublicKeySignatureAlgorithmOid =
        requisitionParams.liquidLegionsV2.elGamalPublicKeySignatureAlgorithmOid
      duchyCertificateDer = requisitionParams.duchyCertificateDer
    }
  }

  private fun SystemNoiseMechanism.toInternalNoiseMechanism(): NoiseMechanism {
    return when (this) {
      SystemNoiseMechanism.GEOMETRIC -> NoiseMechanism.GEOMETRIC
      SystemNoiseMechanism.DISCRETE_GAUSSIAN -> NoiseMechanism.DISCRETE_GAUSSIAN
      SystemNoiseMechanism.CONTINUOUS_GAUSSIAN -> NoiseMechanism.CONTINUOUS_GAUSSIAN
      SystemNoiseMechanism.UNRECOGNIZED,
      SystemNoiseMechanism.NOISE_MECHANISM_UNSPECIFIED -> error("Invalid system NoiseMechanism")
    }
  }

  /** Creates a liquid legions v2 `Parameters` from the system Api computation. */
  private fun Computation.toLiquidLegionsV2Parameters(): Parameters {
    check(mpcProtocolConfig.hasLiquidLegionsV2()) {
      "Missing liquidLegionV2 in the duchy protocol config."
    }
    val llv2Config = mpcProtocolConfig.liquidLegionsV2

    val apiVersion = Version.fromString(publicApiVersion)
    check(apiVersion == Version.V2_ALPHA) { "Unsupported API version $apiVersion" }
    val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)

    when (val measurementType = measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY,
      MeasurementSpec.MeasurementTypeCase.REACH -> {}
      MeasurementSpec.MeasurementTypeCase.IMPRESSION,
      MeasurementSpec.MeasurementTypeCase.DURATION,
      MeasurementSpec.MeasurementTypeCase.POPULATION,
      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
        error("Unsupported measurement type $measurementType")
    }

    return parameters { populate(llv2Config, measurementSpec) }
  }

  private fun ParametersKt.Dsl.populate(
    protocolConfig: Computation.MpcProtocolConfig.LiquidLegionsV2,
    measurementSpec: MeasurementSpec,
  ) {
    sketchParameters = liquidLegionsSketchParameters {
      decayRate = protocolConfig.sketchParams.decayRate
      size = protocolConfig.sketchParams.maxSize
    }
    ellipticCurveId = protocolConfig.ellipticCurveId
    if (measurementSpec.hasReachAndFrequency()) {
      maximumFrequency = measurementSpec.reachAndFrequency.maximumFrequency
      if (maximumFrequency == 0) {
        @Suppress("DEPRECATION") // For legacy Computations.
        maximumFrequency = protocolConfig.maximumFrequency
      }
      require(maximumFrequency > 1) { "Maximum frequency must be greater than 1" }
    }

    noise = liquidLegionsV2NoiseConfig {
      noiseMechanism = protocolConfig.noiseMechanism.toInternalNoiseMechanism()

      reachNoiseConfig = reachNoiseConfig {
        val mpcNoise = protocolConfig.mpcNoise
        blindHistogramNoise = mpcNoise.blindedHistogramNoise.toDuchyDifferentialPrivacyParams()
        noiseForPublisherNoise = mpcNoise.publisherNoise.toDuchyDifferentialPrivacyParams()

        val reachPrivacyParams: DifferentialPrivacyParams =
          if (measurementSpec.hasReachAndFrequency()) {
            measurementSpec.reachAndFrequency.reachPrivacyParams
          } else {
            measurementSpec.reach.privacyParams
          }
        require(reachPrivacyParams.delta > 0) { "Reach privacy delta must be greater than 0" }
        require(reachPrivacyParams.epsilon >= MIN_REACH_EPSILON) {
          "Reach privacy epsilon must be greater than or equal to $MIN_REACH_EPSILON"
        }
        globalReachDpNoise = reachPrivacyParams.toDuchyDifferentialPrivacyParams()
      }

      if (measurementSpec.hasReachAndFrequency()) {
        val frequencyPrivacyParams = measurementSpec.reachAndFrequency.frequencyPrivacyParams
        require(frequencyPrivacyParams.delta > 0) {
          "Frequency privacy delta must be be greater than 0"
        }
        require(frequencyPrivacyParams.epsilon >= MIN_FREQUENCY_EPSILON) {
          "Frequency privacy epsilon must be greater than or equal to $MIN_FREQUENCY_EPSILON"
        }
        frequencyNoiseConfig = frequencyPrivacyParams.toDuchyDifferentialPrivacyParams()
      }
    }
  }
}
