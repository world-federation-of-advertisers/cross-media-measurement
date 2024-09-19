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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.HonestMajorityShareShuffleProtocol
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle.Stage
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.waitOnAggregationInputDetails

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
  fun `next stages are valid for waiting stages`() {
    for (stage in Stage.values()) {
      when (stage) {
        Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE -> {
          val next =
            stages
              .nextStage(stage.toProtocolStage(), RoleInComputation.SECOND_NON_AGGREGATOR)
              .honestMajorityShareShuffle
          assertThat(HonestMajorityShareShuffleProtocol.EnumStages.validTransition(stage, next))
            .isTrue()
        }
        Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO -> {
          val next =
            stages
              .nextStage(stage.toProtocolStage(), RoleInComputation.FIRST_NON_AGGREGATOR)
              .honestMajorityShareShuffle
          assertThat(HonestMajorityShareShuffleProtocol.EnumStages.validTransition(stage, next))
            .isTrue()
        }
        Stage.WAIT_ON_AGGREGATION_INPUT -> {
          val next =
            stages
              .nextStage(stage.toProtocolStage(), RoleInComputation.AGGREGATOR)
              .honestMajorityShareShuffle
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
  fun `outputBlob returns BlobMetadata for WAIT_ON_AGGREGATION_INPUT`() {
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
}
