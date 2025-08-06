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

package org.wfanet.measurement.duchy.herald

import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computations.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.key
import org.wfanet.measurement.duchy.utils.sha1Hash
import org.wfanet.measurement.duchy.utils.toDuchyDifferentialPrivacyParams
import org.wfanet.measurement.duchy.utils.toDuchyElGamalPublicKey
import org.wfanet.measurement.duchy.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.utils.toRequisitionEntries
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfigKt
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt
import org.wfanet.measurement.internal.duchy.protocol.copy
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsSketchParameters
import org.wfanet.measurement.internal.duchy.protocol.liquidLegionsV2NoiseConfig
import org.wfanet.measurement.internal.duchy.updateComputationDetailsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationParticipant

/**
 * Minimum epsilon value for reach noise.
 *
 * This value is chosen due to memory constraints.
 */
private const val MIN_REACH_EPSILON = 0.00001

object ReachOnlyLiquidLegionsV2Starter {

  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE.toProtocolStage()

  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    reachOnlyLiquidLegionsV2SetupConfig: LiquidLegionsV2SetupConfig,
    blobStorageBucket: String,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId

    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$duchyId/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      reachOnlyLiquidLegionsV2 =
        ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = reachOnlyLiquidLegionsV2SetupConfig.role
          parameters = systemComputation.toReachOnlyLiquidLegionsV2Parameters()
        }
    }
    val requisitions =
      systemComputation.requisitionsList.toRequisitionEntries(systemComputation.measurementSpec)

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType =
          ComputationTypeEnum.ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
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
  private fun List<LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant>
    .orderByRoles(
    globalComputationId: String,
    aggregatorId: String,
  ): List<LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant> {
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
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    aggregatorId: String,
  ) {
    val updatedDetails =
      token.computationDetails.copy {
        reachOnlyLiquidLegionsV2 =
          reachOnlyLiquidLegionsV2.copy {
            participant.clear()
            participant +=
              systemComputation.computationParticipantsList
                .map { it.toDuchyComputationParticipant(systemComputation.publicApiVersion) }
                .orderByRoles(token.globalComputationId, aggregatorId)
          }
      }
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
      stage = ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE.toProtocolStage(),
    )
  }

  suspend fun updateRequisitionsAndKeySets(
    token: ComputationToken,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    aggregatorId: String,
  ) {
    require(token.computationDetails.hasReachOnlyLiquidLegionsV2()) {
      "Reach Only Liquid Legions V2 ComputationDetails required"
    }

    val stage = token.computationStage.reachOnlyLiquidLegionsSketchAggregationV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_REQUISITIONS_AND_KEY_SET.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET -> {
        updateRequisitionsAndKeySetsInternal(
          token,
          computationStorageClient,
          systemComputation,
          aggregatorId,
        )
        return
      }

      // For past stages, we throw.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE -> {
        error(
          "[id=${token.globalComputationId}]: cannot update requisitions and key sets for " +
            "computation still in state ${stage.name}"
        )
      }

      // For future stages, we log and exit.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE -> {
        logger.warning(
          "[id=${token.globalComputationId}]: not updating," +
            " stage '$stage' is after WAIT_REQUISITIONS_AND_KEY_SET"
        )
        return
      }

      // For weird stages, we throw.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }

  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  ) {
    require(token.computationDetails.hasReachOnlyLiquidLegionsV2()) {
      "Reach-Only Liquid Legions V2 computation required"
    }

    val stage = token.computationStage.reachOnlyLiquidLegionsSketchAggregationV2
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // We expect stage WAIT_TO_START.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          inputsToNextStage = token.outputPathList(),
          stage = ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE.toProtocolStage(),
        )
        logger.info("[id=${token.globalComputationId}] Computation is now started")
        return
      }

      // For past stages, we throw.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_REQUISITIONS_AND_KEY_SET,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE -> {
        error(
          "[id=${token.globalComputationId}]: cannot start a computation still" +
            " in state ${stage.name}"
        )
      }

      // For future stages, we log and exit.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_EXECUTION_PHASE_INPUTS,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE -> {
        logger.warning(
          "[id=${token.globalComputationId}]: not starting," +
            " stage '$stage' is after WAIT_TO_START"
        )
        return
      }

      // For weird stages, we throw.
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage '$stage'")
      }
    }
  }

  private fun ComputationParticipant.toDuchyComputationParticipant(
    publicApiVersion: String
  ): LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant {
    require(requisitionParams.hasReachOnlyLiquidLegionsV2()) {
      "Missing reach-only liquid legions v2 requisition params."
    }
    return LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant {
      duchyId = key.duchyId
      publicKey =
        requisitionParams.reachOnlyLiquidLegionsV2.elGamalPublicKey.toDuchyElGamalPublicKey(
          Version.fromString(publicApiVersion)
        )
      elGamalPublicKey = requisitionParams.reachOnlyLiquidLegionsV2.elGamalPublicKey
      elGamalPublicKeySignature =
        requisitionParams.reachOnlyLiquidLegionsV2.elGamalPublicKeySignature
      elGamalPublicKeySignatureAlgorithmOid =
        requisitionParams.reachOnlyLiquidLegionsV2.elGamalPublicKeySignatureAlgorithmOid
      duchyCertificateDer = requisitionParams.duchyCertificateDer
    }
  }

  private fun Computation.MpcProtocolConfig.NoiseMechanism.toInternalNoiseMechanism():
    NoiseMechanism {
    return when (this) {
      Computation.MpcProtocolConfig.NoiseMechanism.GEOMETRIC -> NoiseMechanism.GEOMETRIC
      Computation.MpcProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN ->
        NoiseMechanism.DISCRETE_GAUSSIAN
      Computation.MpcProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN ->
        NoiseMechanism.CONTINUOUS_GAUSSIAN
      Computation.MpcProtocolConfig.NoiseMechanism.UNRECOGNIZED,
      Computation.MpcProtocolConfig.NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED ->
        error("Invalid system NoiseMechanism")
    }
  }

  /** Creates a reach-only liquid legions v2 `Parameters` from the system Api computation. */
  private fun Computation.toReachOnlyLiquidLegionsV2Parameters():
    ReachOnlyLiquidLegionsSketchAggregationV2.ComputationDetails.Parameters {
    require(mpcProtocolConfig.hasReachOnlyLiquidLegionsV2()) {
      "Missing reachOnlyLiquidLegionV2 in the duchy protocol config."
    }

    return ReachOnlyLiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.parameters {
      sketchParameters = liquidLegionsSketchParameters {
        decayRate = mpcProtocolConfig.reachOnlyLiquidLegionsV2.sketchParams.decayRate
        size = mpcProtocolConfig.reachOnlyLiquidLegionsV2.sketchParams.maxSize
      }
      ellipticCurveId = mpcProtocolConfig.reachOnlyLiquidLegionsV2.ellipticCurveId
      noise = liquidLegionsV2NoiseConfig {
        noiseMechanism =
          mpcProtocolConfig.reachOnlyLiquidLegionsV2.noiseMechanism.toInternalNoiseMechanism()
        reachNoiseConfig =
          LiquidLegionsV2NoiseConfigKt.reachNoiseConfig {
            val mpcNoise = mpcProtocolConfig.reachOnlyLiquidLegionsV2.mpcNoise
            blindHistogramNoise = mpcNoise.blindedHistogramNoise.toDuchyDifferentialPrivacyParams()
            noiseForPublisherNoise = mpcNoise.publisherNoise.toDuchyDifferentialPrivacyParams()

            when (Version.fromString(publicApiVersion)) {
              Version.V2_ALPHA -> {
                val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
                when (measurementSpec.measurementTypeCase) {
                  MeasurementSpec.MeasurementTypeCase.REACH -> {
                    val reach = measurementSpec.reach
                    require(reach.privacyParams.delta > 0) {
                      "RoLLv2 requires that privacy_params.delta be greater than 0"
                    }
                    require(reach.privacyParams.epsilon >= MIN_REACH_EPSILON) {
                      "RoLLv2 requires that privacy_params.epsilon be greater than or equal to $MIN_REACH_EPSILON"
                    }
                    globalReachDpNoise = reach.privacyParams.toDuchyDifferentialPrivacyParams()
                  }
                  MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY,
                  MeasurementSpec.MeasurementTypeCase.IMPRESSION,
                  MeasurementSpec.MeasurementTypeCase.DURATION,
                  MeasurementSpec.MeasurementTypeCase.POPULATION,
                  MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> {
                    throw IllegalArgumentException("Missing Reach in the measurementSpec.")
                  }
                }
              }
            }
          }
      }
    }
  }
}
