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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.HonestMajorityShareShuffleProtocol
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.computationStageBlobMetadata
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.computationStageInput
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.ComputationDetailsKt.randomSeeds
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.waitOnAggregationInputDetails
import org.wfanet.measurement.internal.duchy.requisitionMetadata

private val SEED = "my seed".toByteStringUtf8()
private val PEER_SEED = "peer seed".toByteStringUtf8()
private val SHUFFLE_PHASE_INPUT = computationStageInput {
  honestMajorityShareShuffleShufflePhaseInput =
    HonestMajorityShareShuffleKt.shufflePhaseInput { this.peerRandomSeed = PEER_SEED }
}

private val WAIT_ON_SHUFFLE_INPUT_TOKEN = computationToken {
  computationStage = computationStage { honestMajorityShareShuffle = Stage.WAIT_ON_SHUFFLE_INPUT }
  computationDetails = computationDetails {
    honestMajorityShareShuffle =
      HonestMajorityShareShuffleKt.computationDetails {
        seeds = randomSeeds { this.commonRandomSeed = SEED }
      }
  }
  requisitions += requisitionMetadata {}
  requisitions += requisitionMetadata {}
}

private val UPDATED_WAIT_ON_SHUFFLE_INPUT_TOKEN =
  WAIT_ON_SHUFFLE_INPUT_TOKEN.copy {
    computationDetails = computationDetails {
      honestMajorityShareShuffle =
        HonestMajorityShareShuffleKt.computationDetails {
          seeds = randomSeeds {
            this.commonRandomSeed = SEED
            this.commonRandomSeedFromPeer = PEER_SEED
          }
        }
    }
  }

private val FULFILLED_WAIT_ON_SHUFFLE_INPUT_TOKEN =
  WAIT_ON_SHUFFLE_INPUT_TOKEN.copy {
    requisitions.clear()
    requisitions += requisitionMetadata {
      seed = "seed_1".toByteStringUtf8()
      path = "path_1"
    }
    requisitions += requisitionMetadata {
      seed = "seed_2".toByteStringUtf8()
      path = "path_2"
    }
  }

private val READY_WAIT_ON_SHUFFLE_INPUT_TOKEN =
  UPDATED_WAIT_ON_SHUFFLE_INPUT_TOKEN.copy {
    requisitions.clear()
    requisitions += requisitionMetadata {
      seed = "seed_1".toByteStringUtf8()
      path = "path_1"
    }
    requisitions += requisitionMetadata {
      seed = "seed_2".toByteStringUtf8()
      path = "path_2"
    }
  }

private val WAIT_ON_AGGREGATION_INPUT_TOKEN = computationToken {
  computationStage = computationStage {
    honestMajorityShareShuffle = Stage.WAIT_ON_AGGREGATION_INPUT
  }
  blobs += computationStageBlobMetadata {
    dependencyType = ComputationBlobDependency.OUTPUT
    blobId = 1
  }
  blobs += computationStageBlobMetadata {
    dependencyType = ComputationBlobDependency.OUTPUT
    blobId = 2
  }
}

private val READY_WAIT_ON_AGGREGATION_INPUT_TOKEN =
  WAIT_ON_AGGREGATION_INPUT_TOKEN.copy {
    blobs.clear()
    blobs += computationStageBlobMetadata {
      dependencyType = ComputationBlobDependency.OUTPUT
      path = "path_1"
      blobId = 1
    }
    blobs += computationStageBlobMetadata {
      dependencyType = ComputationBlobDependency.OUTPUT
      path = "path_2"
      blobId = 2
    }
  }

@RunWith(JUnit4::class)
class HonestMajorityShareShuffleStagesTest {
  private val stages = HonestMajorityShareShuffleStages()

  private fun assertContextThrowsErrorWhenCallingNextStage(stage: Stage) {
    if (stage == Stage.UNRECOGNIZED) {
      return
    }
    val token = computationToken {
      computationStage = computationStage { honestMajorityShareShuffle = stage }
      blobs += newEmptyOutputBlobMetadata(1L)
    }
    assertFailsWith<IllegalArgumentException> { stages.outputBlob(token, "Buck") }
  }

  @Test
  fun `next stages are valid for WAIT_ON_AGGREGATION_INPUT and WAIT_ON_SHUFFLE_INPUT`() {
    for (stage in Stage.values()) {
      when (stage) {
        Stage.WAIT_ON_AGGREGATION_INPUT,
        Stage.WAIT_ON_SHUFFLE_INPUT -> {
          val next = stages.nextStage(stage.toProtocolStage()).honestMajorityShareShuffle
          assertThat(HonestMajorityShareShuffleProtocol.EnumStages.validTransition(stage, next))
            .isTrue()
        }
        else -> {
          assertContextThrowsErrorWhenCallingNextStage(stage)
        }
      }
    }
  }

  @Test
  fun `outputBlob returns BlobMeta for WAIT_ON_AGGREGATION_INPUT`() {
    val token = computationToken {
      computationStage = Stage.WAIT_ON_AGGREGATION_INPUT.toProtocolStage()
      blobs += newEmptyOutputBlobMetadata(1L)
      blobs += newEmptyOutputBlobMetadata(21L)

      stageSpecificDetails = computationStageDetails {
        honestMajorityShareShuffle = stageDetails {
          waitOnAggregationInputDetails = waitOnAggregationInputDetails {
            externalDuchyLocalBlobId["alice"] = 21L
            externalDuchyLocalBlobId["bob"] = 1L
          }
        }
      }
    }

    assertThat(stages.outputBlob(token, "alice")).isEqualTo(newEmptyOutputBlobMetadata(21L))
    assertThat(stages.outputBlob(token, "bob")).isEqualTo(newEmptyOutputBlobMetadata(1L))
    assertFailsWith<IllegalStateException> { stages.outputBlob(token, "unknown-sender") }
  }

  @Test
  fun `isValidStage returns correct boolean`() {
    for (currentStage in Stage.values()) {
      for (requestStage in Stage.values()) {
        if (currentStage == Stage.UNRECOGNIZED || requestStage == Stage.UNRECOGNIZED) {
          continue
        }
        when (requestStage) {
          Stage.WAIT_ON_SHUFFLE_INPUT -> {
            if (
              currentStage == Stage.INITIALIZED ||
                currentStage == Stage.SETUP_PHASE ||
                currentStage == Stage.WAIT_ON_SHUFFLE_INPUT
            ) {
              assertThat(
                  stages.isValidStage(
                    currentStage.toProtocolStage(),
                    requestStage.toProtocolStage(),
                  )
                )
                .isTrue()
            } else {
              assertThat(
                  stages.isValidStage(
                    currentStage.toProtocolStage(),
                    requestStage.toProtocolStage(),
                  )
                )
                .isFalse()
            }
          }
          else -> {
            if (currentStage == requestStage) {
              assertThat(
                  stages.isValidStage(
                    currentStage.toProtocolStage(),
                    requestStage.toProtocolStage(),
                  )
                )
                .isTrue()
            } else {
              assertThat(
                  stages.isValidStage(
                    currentStage.toProtocolStage(),
                    requestStage.toProtocolStage(),
                  )
                )
                .isFalse()
            }
          }
        }
      }
    }
  }

  @Test
  fun `expectStageInput returns correct booleans`() {
    assertThat(stages.expectStageInput(WAIT_ON_SHUFFLE_INPUT_TOKEN)).isTrue()
    assertThat(stages.expectStageInput(UPDATED_WAIT_ON_SHUFFLE_INPUT_TOKEN)).isFalse()
  }

  @Test
  fun `updateComputationDetails returns updated computation details`() {
    val updatedDetails =
      stages.updateComputationDetails(
        WAIT_ON_SHUFFLE_INPUT_TOKEN.computationDetails,
        SHUFFLE_PHASE_INPUT,
      )

    assertThat(updatedDetails).isEqualTo(UPDATED_WAIT_ON_SHUFFLE_INPUT_TOKEN.computationDetails)
  }

  @Test
  fun `readyForNextStage returns correct boolean`() {
    assertThat(stages.readyForNextStage(WAIT_ON_SHUFFLE_INPUT_TOKEN)).isFalse()
    assertThat(stages.readyForNextStage(UPDATED_WAIT_ON_SHUFFLE_INPUT_TOKEN)).isFalse()
    assertThat(stages.readyForNextStage(FULFILLED_WAIT_ON_SHUFFLE_INPUT_TOKEN)).isFalse()
    assertThat(stages.readyForNextStage(READY_WAIT_ON_SHUFFLE_INPUT_TOKEN)).isTrue()

    val shufflePhaseToken =
      READY_WAIT_ON_SHUFFLE_INPUT_TOKEN.copy {
        computationStage = computationStage { honestMajorityShareShuffle = Stage.SHUFFLE_PHASE }
      }
    assertThat(stages.readyForNextStage(shufflePhaseToken)).isFalse()

    assertThat(stages.readyForNextStage(WAIT_ON_AGGREGATION_INPUT_TOKEN)).isFalse()
    assertThat(stages.readyForNextStage(READY_WAIT_ON_AGGREGATION_INPUT_TOKEN)).isTrue()

    val aggregationPhaseToken =
      READY_WAIT_ON_SHUFFLE_INPUT_TOKEN.copy {
        computationStage = computationStage { honestMajorityShareShuffle = Stage.AGGREGATION_PHASE }
      }
    assertThat(stages.readyForNextStage(aggregationPhaseToken)).isFalse()
  }
}
