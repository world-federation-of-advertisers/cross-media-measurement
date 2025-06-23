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

package org.wfanet.measurement.duchy.herald

import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.crypto.PrivateKeyStore
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.duchy.utils.key
import org.wfanet.measurement.duchy.utils.toDuchyDifferentialPrivacyParams
import org.wfanet.measurement.duchy.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.utils.toRequisitionEntries
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.config.TrusTeeSetupConfig
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.protocol.TrusTee
import org.wfanet.measurement.internal.duchy.protocol.TrusTee.Stage
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeKt
import org.wfanet.measurement.system.v1alpha.Computation

/**
 * Minimum epsilon value for reach noise.
 *
 * This value is chosen due to memory constraints.
 */
private const val MIN_REACH_EPSILON = 0.000001

/**
 * Minimum epsilon value for frequency noise.
 *
 * This value is chosen due to memory constraints.
 */
private const val MIN_FREQUENCY_EPSILON = 0.000001

object TrusTeeStarter {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  /** Create a TrusTEE Computation populated with randomness seed and tink encryption key pairs. */
  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    protocolSetupConfig: TrusTeeSetupConfig,
    blobStorageBucket: String,
    privateKeyStore: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>? = null,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val role = protocolSetupConfig.role

    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$duchyId/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      trusTee =
        TrusTeeKt.computationDetails {
          this.role = role
          require(role == RoleInComputation.AGGREGATOR) { "Invalid role for TrusTEE: role" }
          parameters = systemComputation.toTrusTeeParameters()
        }
    }

    val requisitions =
      systemComputation.requisitionsList.toRequisitionEntries(systemComputation.measurementSpec)

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType = ComputationTypeEnum.ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE
        globalComputationId = globalId
        computationStage =
          org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
            .INITIALIZED
            .toProtocolStage()
        computationDetails = initialComputationDetails
        this.requisitions += requisitions
      }
    )
  }

  /** Start the Computation. */
  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  ) {
    require(token.computationDetails.hasTrusTee()) { "TrusTEE computation required." }

    val stage = token.computationStage.trusTee
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // Expect WAIT_TO_START for the first non-aggregator duchy.
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          stage = Stage.INITIALIZED.toProtocolStage(),
        )
        logger.log(Level.INFO) { "[id=${token.globalComputationId}] Computation starts." }
      }
      // Stage that should not be reached.
      Stage.INITIALIZED ->
        logger.log(Level.WARNING) {
          "[id=${token.globalComputationId}]: Computation is not ready to start. stage=$stage."
        }
      // Log and skip for future Stages.
      Stage.COMPUTING,
      Stage.COMPLETE -> {
        logger.log(Level.WARNING) {
          "[id=${token.globalComputationId}]: Computation has started. Skip start request. stage=$stage."
        }
      }
      Stage.STAGE_UNSPECIFIED,
      Stage.UNRECOGNIZED -> {
        error("[id=${token.globalComputationId}]: Unrecognized stage $stage")
      }
    }
  }

  private fun Computation.toTrusTeeParameters(): TrusTee.ComputationDetails.Parameters {
    require(mpcProtocolConfig.hasHonestMajorityShareShuffle()) {
      "Missing honestMajorityShareShuffle in the duchy protocol config."
    }

    val apiVersion = Version.fromString(publicApiVersion)
    require(apiVersion == Version.V2_ALPHA) { "Unsupported API version $apiVersion" }
    val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)

    return TrusTeeKt.ComputationDetailsKt.parameters {
      if (measurementSpec.hasReachAndFrequency()) {
        maximumFrequency = measurementSpec.reachAndFrequency.maximumFrequency
        require(maximumFrequency > 1) { "Maximum frequency must be greater than 1" }
        reachDpParams =
          measurementSpec.reachAndFrequency.reachPrivacyParams.toDuchyDifferentialPrivacyParams()
        require(reachDpParams.delta > 0) { "Reach privacy delta must be greater than 0" }
        require(reachDpParams.epsilon >= MIN_REACH_EPSILON) {
          "Reach privacy epsilon must be greater than or equal to $MIN_REACH_EPSILON"
        }
        frequencyDpParams =
          measurementSpec.reachAndFrequency.frequencyPrivacyParams
            .toDuchyDifferentialPrivacyParams()
        require(frequencyDpParams.delta > 0) { "Frequency privacy delta must be be greater than 0" }
        require(frequencyDpParams.epsilon >= MIN_FREQUENCY_EPSILON) {
          "Frequency privacy epsilon must be greater than or equal to $MIN_FREQUENCY_EPSILON"
        }
      } else {
        maximumFrequency = 1
        reachDpParams = measurementSpec.reach.privacyParams.toDuchyDifferentialPrivacyParams()
      }
      noiseMechanism = mpcProtocolConfig.trusTee.noiseMechanism.toInternalNoiseMechanism()
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
}
