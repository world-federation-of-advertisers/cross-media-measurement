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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.duchy.daemon.utils.PublicApiVersion
import org.wfanet.measurement.duchy.daemon.utils.toPublicApiVersion
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.service.internal.computation.outputPathList
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.CreateComputationRequest
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.Parameters
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.system.v1alpha.Computation

object LiquidLegionsV2Starter {

  suspend fun createComputation(
    computationStorageClient: ComputationsCoroutineStub,
    globalComputation: Computation,
    liquidLegionsV2SetupConfig: LiquidLegionsV2SetupConfig,
    configMaps: Map<String, ProtocolConfig>,
    blobStorageBucket: String
  ) {
    liquidLegionsV2SetupConfig.role
    val globalId: String = checkNotNull(globalComputation.key?.computationId)
    val initialComputationDetails =
      ComputationDetails.newBuilder()
        .apply {
          blobsStoragePrefix = "$blobStorageBucket/$globalId"
          kingdomComputation = globalComputation.toKingdomComputationDetails()
          liquidLegionsV2Builder.apply {
            role = liquidLegionsV2SetupConfig.role
            parameters = globalComputation.toLiquidLegionsV2Parameters(configMaps)
          }
        }
        .build()
    val requisitions =
      globalComputation.requisitionsList.map {
        ExternalRequisitionKey.newBuilder()
          .apply {
            externalDataProviderId = it.dataProviderId
            externalRequisitionId = it.key.requisitionId
          }
          .build()
      }

    computationStorageClient.createComputation(
      CreateComputationRequest.newBuilder()
        .apply {
          computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
          globalComputationId = globalId
          computationDetails = initialComputationDetails
          addAllRequisitions(requisitions)
        }
        .build()
    )
  }

  suspend fun updateRequisitionsAndKeySets() {
    TODO("not implemented")
  }

  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsCoroutineStub,
    computationProtocolStageDetails: ComputationProtocolStageDetails,
    logger: Logger
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
          stage = Stage.SETUP_PHASE.toProtocolStage(),
          computationProtocolStageDetails = computationProtocolStageDetails
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

  /** Creates a liquid legions v2 `Parameters` from the system Api computation. */
  private fun Computation.toLiquidLegionsV2Parameters(
    configMaps: Map<String, ProtocolConfig>
  ): Parameters {
    val publicProtocolConfig =
      configMaps[protocolConfigId] ?: error("ProtocolConfig $protocolConfigId not found.")
    require(publicProtocolConfig.hasLiquidLegionsV2()) {
      "Missing liquidLegionV2 in the public API protocol config."
    }
    require(duchyProtocolConfig.hasLiquidLegionsV2()) {
      "Missing liquidLegionV2 in the duchy protocol config."
    }

    return Parameters.newBuilder()
      .also {
        it.maximumFrequency = duchyProtocolConfig.liquidLegionsV2.maximumFrequency
        it.liquidLegionsSketchBuilder.apply {
          decayRate = publicProtocolConfig.liquidLegionsV2.sketchParams.decayRate
          size = publicProtocolConfig.liquidLegionsV2.sketchParams.maxSize
        }
        it.noiseBuilder.apply {
          reachNoiseConfigBuilder.apply {
            val mpcNoise = duchyProtocolConfig.liquidLegionsV2.mpcNoise
            blindHistogramNoise = mpcNoise.blindedHistogramNoise.toDuchyDifferentialPrivacyParams()
            noiseForPublisherNoise =
              mpcNoise.noiseForPublisherNoise.toDuchyDifferentialPrivacyParams()
          }
          when (publicApiVersion.toPublicApiVersion()) {
            PublicApiVersion.V2_ALPHA -> {
              val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)
              require(measurementSpec.hasReachAndFrequency()) {
                "Missing ReachAndFrequency in the measurementSpec."
              }
              val reachAndFrequency = measurementSpec.reachAndFrequency
              reachNoiseConfigBuilder.globalReachDpNoise =
                reachAndFrequency.reachPrivacyParams.toDuchyDifferentialPrivacyParams()
              frequencyNoiseConfig =
                reachAndFrequency.frequencyPrivacyParams.toDuchyDifferentialPrivacyParams()
            }
          }
        }
        it.ellipticCurveId = publicProtocolConfig.liquidLegionsV2.ellipticCurveId
      }
      .build()
  }
}
