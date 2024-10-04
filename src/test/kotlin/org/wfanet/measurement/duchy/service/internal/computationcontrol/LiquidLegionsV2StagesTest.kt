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

package org.wfanet.measurement.duchy.service.internal.computationcontrol

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationV2Protocol
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage

@RunWith(JUnit4::class)
class LiquidLegionsV2StagesTest {
  private val stages = LiquidLegionsV2Stages()

  @Test
  fun `next stages are valid`() {
    fun assertContextThrowsErrorWhenCallingNextStage(stage: Stage) {
      val token =
        ComputationToken.newBuilder()
          .apply {
            computationStage =
              ComputationStage.newBuilder()
                .setLiquidLegionsSketchAggregationV2Value(stage.ordinal)
                .build()
            addBlobs(newEmptyOutputBlobMetadata(1L))
          }
          .build()
      assertFailsWith<IllegalArgumentException> { stages.outputBlob(token, "Buck") }
    }

    for (stage in Stage.values()) {
      when (stage) {
        Stage.WAIT_SETUP_PHASE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> {
          for (role in listOf(RoleInComputation.AGGREGATOR, RoleInComputation.NON_AGGREGATOR)) {
            val next =
              stages.nextStage(stage.toProtocolStage(), role).liquidLegionsSketchAggregationV2
            assertTrue("$next is not a valid successor of $stage") {
              LiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(stage, next)
            }
          }
        }
        else -> assertContextThrowsErrorWhenCallingNextStage(stage)
      }
    }
  }

  @Test
  fun `output blob for wait sketches`() {
    val token =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
          addBlobs(newEmptyOutputBlobMetadata(1L))
          addBlobs(newEmptyOutputBlobMetadata(21L))
          stageSpecificDetailsBuilder.apply {
            liquidLegionsV2Builder.apply {
              waitSetupPhaseInputsDetailsBuilder
                .putExternalDuchyLocalBlobId("alice", 21L)
                .putExternalDuchyLocalBlobId("bob", 1L)
            }
          }
        }
        .build()

    assertThat(stages.outputBlob(token, "alice")).isEqualTo(newEmptyOutputBlobMetadata(21L))
    assertThat(stages.outputBlob(token, "bob")).isEqualTo(newEmptyOutputBlobMetadata(1L))
    assertFailsWith<IllegalStateException> { stages.outputBlob(token, "unknown-sender") }
  }

  @Test
  fun `output blob for wait reach estimation inputs`() {
    val token =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
          addBlobs(newEmptyOutputBlobMetadata(1L))
        }
        .build()

    assertThat(stages.outputBlob(token, "Buck")).isEqualTo(newEmptyOutputBlobMetadata(1L))
  }

  @Test
  fun `output blob for wait filtering phase inputs`() {
    val token =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
          addBlobs(newEmptyOutputBlobMetadata(100L))
        }
        .build()

    assertThat(stages.outputBlob(token, "Buck")).isEqualTo(newEmptyOutputBlobMetadata(100L))
  }

  @Test
  fun `output blob for wait frequency inputs`() {
    val token =
      ComputationToken.newBuilder()
        .apply {
          computationStage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
          addBlobs(newEmptyOutputBlobMetadata(120L))
        }
        .build()

    assertThat(stages.outputBlob(token, "Buck")).isEqualTo(newEmptyOutputBlobMetadata(120L))
  }

  @Test
  fun `output blob for unsupported stages throw`() {
    fun assertContextThrowsErrorWhenGettingBlob(stage: Stage) {
      val token =
        ComputationToken.newBuilder()
          .apply {
            computationStage =
              ComputationStage.newBuilder()
                .setLiquidLegionsSketchAggregationV2Value(stage.ordinal)
                .build()
            addBlobs(newEmptyOutputBlobMetadata(1L))
          }
          .build()
      assertFailsWith<IllegalArgumentException> { stages.outputBlob(token, "Buck") }
    }

    for (stage in Stage.values()) {
      when (stage) {
        // Skip all the supported stages, they are tested elsewhere.
        Stage.WAIT_SETUP_PHASE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> {}
        else -> assertContextThrowsErrorWhenGettingBlob(stage)
      }
    }
  }
}
