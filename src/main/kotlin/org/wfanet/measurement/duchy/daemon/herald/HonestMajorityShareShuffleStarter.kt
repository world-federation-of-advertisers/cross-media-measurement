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

import java.util.logging.Logger
import org.wfanet.measurement.duchy.daemon.utils.key
import org.wfanet.measurement.duchy.daemon.utils.toKingdomComputationDetails
import org.wfanet.measurement.duchy.daemon.utils.toRequisitionEntries
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.config.HonestMajorityShareShuffleSetupConfig
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey

object HonestMajorityShareShuffleStarter {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  val TERMINAL_STAGE = Stage.COMPLETE.toProtocolStage()

  suspend fun createComputation(
    duchyId: String,
    computationStorageClient: ComputationsGrpcKt.ComputationsCoroutineStub,
    systemComputation: Computation,
    protocolSetupConfig: HonestMajorityShareShuffleSetupConfig,
    blobStorageBucket: String,
  ) {
    require(systemComputation.name.isNotEmpty()) { "Resource name not specified" }
    val globalId: String = systemComputation.key.computationId
    val initialComputationDetails = computationDetails {
      blobsStoragePrefix = "$blobStorageBucket/$globalId"
      kingdomComputation = systemComputation.toKingdomComputationDetails()
      honestMajorityShareShuffle =
        HonestMajorityShareShuffleKt.computationDetails {
          role = protocolSetupConfig.role
          parameters = systemComputation.toHonestMajorityShareShuffleParameters()
        }
    }
    val requisitions =
      systemComputation.requisitionsList
        .filter {
          val participantKey =
            ComputationParticipantKey.fromName(it.fulfillingComputationParticipant)
          participantKey != null && participantKey.duchyId == duchyId
        }
        .toRequisitionEntries(systemComputation.measurementSpec)

    computationStorageClient.createComputation(
      createComputationRequest {
        computationType = ComputationTypeEnum.ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE
        globalComputationId = globalId
        computationDetails = initialComputationDetails
        this.requisitions += requisitions
      }
    )
  }
}
