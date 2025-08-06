// Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.security.SecureRandom
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
import org.wfanet.measurement.internal.duchy.EncryptionPublicKey
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.HonestMajorityShareShuffleSetupConfig
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.encryptionKeyPair
import org.wfanet.measurement.internal.duchy.encryptionPublicKey
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.system.v1alpha.Computation

private const val RANDOM_SEED_LENGTH_IN_BYTES = 48

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

object HonestMajorityShareShuffleStarter {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = Stage.COMPLETE.toProtocolStage()

  /**
   * Create a HonestMajority Computation populated with randomness seed and tink encryption key
   * pairs.
   */
  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    protocolSetupConfig: HonestMajorityShareShuffleSetupConfig,
    blobStorageBucket: String,
    privateKeyStore: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>? = null,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val role = protocolSetupConfig.role

    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$duchyId/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      honestMajorityShareShuffle =
        HonestMajorityShareShuffleKt.computationDetails {
          this.role = role
          parameters = systemComputation.toHonestMajorityShareShuffleParameters()
          nonAggregators += getNonAggregators(protocolSetupConfig)
          if (role != RoleInComputation.AGGREGATOR) {
            requireNotNull(privateKeyStore) { "privateKeyStore cannot be null" }

            randomSeed = generateRandomSeed()

            val privateKeyHandle = TinkPrivateKeyHandle.generateHpke()
            val privateKeyId = storePrivateKey(privateKeyStore, privateKeyHandle)
            encryptionKeyPair = encryptionKeyPair {
              this.privateKeyId = privateKeyId
              publicKey = encryptionPublicKey {
                format = EncryptionPublicKey.Format.TINK_KEYSET
                data = privateKeyHandle.publicKey.toByteString()
              }
            }
          }
        }
    }

    val requisitions =
      systemComputation.requisitionsList.toRequisitionEntries(systemComputation.measurementSpec)

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType = ComputationTypeEnum.ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE
        globalComputationId = globalId
        computationStage = Stage.INITIALIZED.toProtocolStage()
        computationDetails = initialComputationDetails
        this.requisitions += requisitions
      }
    )
  }

  /** Start the Computation for FIRST_NON_AGGREGATOR. */
  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  ) {
    require(token.computationDetails.hasHonestMajorityShareShuffle()) {
      "Honest Majority Share Shuffle computation required."
    }

    if (
      token.computationDetails.honestMajorityShareShuffle.role !=
        RoleInComputation.FIRST_NON_AGGREGATOR
    ) {
      // Only FIRST_NON_AGGREGATOR is triggered by herald to start the computation.
      logger.log(Level.INFO, "[id=${token.globalComputationId}] ignore startComputation.")
      return
    }

    val stage = token.computationStage.honestMajorityShareShuffle
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
    when (stage) {
      // Expect WAIT_TO_START for the first non-aggregator duchy.
      Stage.WAIT_TO_START -> {
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
        )
        logger.log(Level.INFO) { "[id=${token.globalComputationId}] Computation starts." }
      }
      // For INITIALIZED, it could be caused by an interrupted execution. If the encryption
      // key has been set, skip the stage to catch up to the Measurement state.
      Stage.INITIALIZED -> {
        if (!isInitialized(token)) {
          error(
            "[id=${token.globalComputationId}]:cannot start computation while Computation " +
              "details not initialized"
          )
        }
        logger.log(
          Level.WARNING,
          "[id=${token.globalComputationId}] skipping " + "INITIALIZED to catch up.",
        )
        computationStorageClient.advanceComputationStage(
          token,
          stage = Stage.WAIT_TO_START.toProtocolStage(),
        )
        computationStorageClient.advanceComputationStage(
          computationToken = token,
          stage = Stage.SETUP_PHASE.toProtocolStage(),
        )
        return
      }
      // throw exception for unreachable Stages.
      Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
      Stage.WAIT_ON_AGGREGATION_INPUT -> {
        error("[id=${token.globalComputationId}]: unreachable stage $stage")
      }
      // Log and skip for future Stages.
      Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO,
      Stage.SETUP_PHASE,
      Stage.SHUFFLE_PHASE,
      Stage.AGGREGATION_PHASE,
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

  private fun isInitialized(token: ComputationToken): Boolean =
    token.computationDetails.honestMajorityShareShuffle.hasEncryptionKeyPair()

  private fun Computation.toHonestMajorityShareShuffleParameters():
    HonestMajorityShareShuffle.ComputationDetails.Parameters {
    require(mpcProtocolConfig.hasHonestMajorityShareShuffle()) {
      "Missing honestMajorityShareShuffle in the duchy protocol config."
    }

    val hmssConfig = mpcProtocolConfig.honestMajorityShareShuffle

    val apiVersion = Version.fromString(publicApiVersion)
    require(apiVersion == Version.V2_ALPHA) { "Unsupported API version $apiVersion" }
    val measurementSpec = MeasurementSpec.parseFrom(measurementSpec)

    return HonestMajorityShareShuffleKt.ComputationDetailsKt.parameters {
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
        ringModulus = hmssConfig.reachAndFrequencyRingModulus
      } else {
        maximumFrequency = 1
        reachDpParams = measurementSpec.reach.privacyParams.toDuchyDifferentialPrivacyParams()
        ringModulus = hmssConfig.reachRingModulus
      }
      noiseMechanism = hmssConfig.noiseMechanism.toInternalNoiseMechanism()
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

  private fun getNonAggregators(setupConfig: HonestMajorityShareShuffleSetupConfig): List<String> {
    return listOf(setupConfig.firstNonAggregatorDuchyId, setupConfig.secondNonAggregatorDuchyId)
  }

  private fun generateRandomSeed(): ByteString {
    val secureRandom = SecureRandom()
    return secureRandom.generateSeed(RANDOM_SEED_LENGTH_IN_BYTES).toByteString()
  }

  private suspend fun storePrivateKey(
    client: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>,
    privateKeyHandle: TinkPrivateKeyHandle,
  ): String {
    return client.write(privateKeyHandle)
  }
}
