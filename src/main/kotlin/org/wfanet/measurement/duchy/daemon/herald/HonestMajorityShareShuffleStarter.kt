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

package org.wfanet.measurement.duchy.daemon.herald

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
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toDuchyDifferentialPrivacyParams
import org.wfanet.measurement.duchy.daemon.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.daemon.utils.toRequisitionEntries
import org.wfanet.measurement.duchy.db.computation.advanceComputationStage
import org.wfanet.measurement.duchy.toProtocolStage
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
import org.wfanet.measurement.internal.duchy.protocol.shareShuffleSketchParams
import org.wfanet.measurement.system.v1alpha.Computation

private const val RANDOM_SEED_LENGTH_IN_BYTES = 48

object HonestMajorityShareShuffleStarter {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = Stage.COMPLETE.toProtocolStage()

  /**
   * Create a HonestMajority Computation populated with randomness seed and tink encryption key
   * pairs.
   */
  suspend fun createComputation(
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    honestMajorityShareShuffleSetupConfig: HonestMajorityShareShuffleSetupConfig,
    blobStorageBucket: String,
    privateKeyStore: PrivateKeyStore<TinkKeyId, TinkPrivateKeyHandle>,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val role = honestMajorityShareShuffleSetupConfig.role

    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      honestMajorityShareShuffle =
        HonestMajorityShareShuffleKt.computationDetails {
          this.role = role
          parameters = systemComputation.toHonestMajorityShareShuffleParameters()
          participants += systemComputation.computationParticipantsList.map { it.key.duchyId }
          if (role != RoleInComputation.AGGREGATOR) {
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

  suspend fun startComputation(
    token: ComputationToken,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  ) {
    require(token.computationDetails.hasHonestMajorityShareShuffle()) {
      "Honest Majority Share Shuffle computation required."
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
      // Skip for the second non-aggregator and the aggregator.
      Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
      Stage.WAIT_ON_AGGREGATION_INPUT -> {}
      // Throw error when Computation is not ready to start.
      Stage.INITIALIZED -> {
        error("[id=${token.globalComputationId}]: Computation is not ready to start. stage=$stage.")
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
        dpParams =
          measurementSpec.reachAndFrequency.frequencyPrivacyParams
            .toDuchyDifferentialPrivacyParams()
      } else {
        maximumFrequency = 1
        dpParams = measurementSpec.reach.privacyParams.toDuchyDifferentialPrivacyParams()
      }

      sketchParams = shareShuffleSketchParams {
        registerCount = hmssConfig.sketchParams.registerCount
        bytesPerRegister = hmssConfig.sketchParams.bytesPerRegister
        maximumCombinedFrequency =
          measurementSpec.reachAndFrequency.maximumFrequency * measurementSpec.nonceHashesCount
        this.ringModulus = hmssConfig.sketchParams.ringModulus
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
      Computation.MpcProtocolConfig.NoiseMechanism.UNRECOGNIZED,
      Computation.MpcProtocolConfig.NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED ->
        error("Invalid system NoiseMechanism")
    }
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
