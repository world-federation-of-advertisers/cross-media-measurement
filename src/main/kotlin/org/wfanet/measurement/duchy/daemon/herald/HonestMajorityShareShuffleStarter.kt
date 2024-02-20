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

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.duchy.daemon.utils.getComputationParticipantKey
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.daemon.utils.toRequisitionEntries
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.NoiseMechanism
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.HonestMajorityShareShuffleSetupConfig
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.internal.duchy.protocol.shareShuffleSketchParams
import org.wfanet.measurement.system.v1alpha.Computation

object HonestMajorityShareShuffleStarter {
  /**
   * Create a HonestMajority Computation populated with randomness seed and tink encryption key
   * pairs.
   */
  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    honestMajorityShareShuffleSetupConfig: HonestMajorityShareShuffleSetupConfig,
    blobStorageBucket: String,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      honestMajorityShareShuffle =
        HonestMajorityShareShuffleKt.computationDetails {
          role = honestMajorityShareShuffleSetupConfig.role
          parameters = systemComputation.toHonestMajorityShareShuffleParameters()
          participants += systemComputation.computationParticipantsList.map { it.key.duchyId }
        }
    }

    val requisitions =
      systemComputation.requisitionsList
        .filter {
          require(it.fulfillingComputationParticipant.isNotBlank()) {
            "Fulfilling computation participant cannot be blank for HMSS requisition"
          }
          getComputationParticipantKey(it.fulfillingComputationParticipant).duchyId == duchyId
        }
        .toRequisitionEntries(systemComputation.measurementSpec)

    // TODO(@renjiez): Sample random seed and tink key pair.

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
        globalComputationId = globalId
        computationDetails = initialComputationDetails
        this.requisitions += requisitions
      }
    )
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
      } else {
        maximumFrequency = 1
      }

      sketchParams = shareShuffleSketchParams {
        registerCount = hmssConfig.sketchParams.registerCount
        bytesPerRegister = hmssConfig.sketchParams.bytesPerRegister
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
}
