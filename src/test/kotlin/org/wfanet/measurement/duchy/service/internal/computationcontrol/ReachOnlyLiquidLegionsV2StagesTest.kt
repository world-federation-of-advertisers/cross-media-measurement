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
import org.wfanet.measurement.duchy.db.computation.ReachOnlyLiquidLegionsSketchAggregationV2Protocol
import org.wfanet.measurement.duchy.service.internal.computations.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.computationToken
import org.wfanet.measurement.internal.duchy.config.RoleInComputation
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2.Stage
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.waitSetupPhaseInputsDetails

@RunWith(JUnit4::class)
class ReachOnlyLiquidLegionsV2StagesTest {
  private val stages = ReachOnlyLiquidLegionsV2Stages()

  @Test
  fun `next stages are valid`() {
    fun assertContextThrowsErrorWhenCallingNextStage(stage: Stage) {
      if (stage == Stage.UNRECOGNIZED) {
        return
      }
      val token = computationToken {
        computationStage = computationStage { reachOnlyLiquidLegionsSketchAggregationV2 = stage }
        blobs += newEmptyOutputBlobMetadata(1L)
      }
      assertFailsWith<IllegalArgumentException> { stages.outputBlob(token, "Buck") }
    }

    for (stage in Stage.values()) {
      when (stage) {
        Stage.WAIT_SETUP_PHASE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_INPUTS -> {
          for (role in listOf(RoleInComputation.AGGREGATOR, RoleInComputation.NON_AGGREGATOR)) {
            val next =
              stages
                .nextStage(stage.toProtocolStage(), role)
                .reachOnlyLiquidLegionsSketchAggregationV2
            assertTrue("$next is not a valid successor of $stage") {
              ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
                stage,
                next,
              )
            }
          }
        }
        else -> assertContextThrowsErrorWhenCallingNextStage(stage)
      }
    }
  }

  @Test
  fun `output blob for wait sketches`() {
    val token = computationToken {
      computationStage = Stage.WAIT_SETUP_PHASE_INPUTS.toProtocolStage()
      blobs += newEmptyOutputBlobMetadata(1L)
      blobs += newEmptyOutputBlobMetadata(21L)

      stageSpecificDetails = computationStageDetails {
        reachOnlyLiquidLegionsV2 = stageDetails {
          waitSetupPhaseInputsDetails = waitSetupPhaseInputsDetails {
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
  fun `output blob for execution phase inputs`() {
    val token = computationToken {
      computationStage = Stage.WAIT_EXECUTION_PHASE_INPUTS.toProtocolStage()
      blobs += newEmptyOutputBlobMetadata(1L)
    }

    assertThat(stages.outputBlob(token, "Buck")).isEqualTo(newEmptyOutputBlobMetadata(1L))
  }

  @Test
  fun `output blob for unsupported stages throws`() {
    fun assertContextThrowsErrorWhenGettingBlob(stage: Stage) {
      if (stage == Stage.UNRECOGNIZED) {
        return
      }

      val token = computationToken {
        computationStage = computationStage { reachOnlyLiquidLegionsSketchAggregationV2 = stage }
        blobs += newEmptyOutputBlobMetadata(1L)
      }

      assertFailsWith<IllegalArgumentException> { stages.outputBlob(token, "Buck") }
    }

    for (stage in Stage.values()) {
      when (stage) {
        // Skip all the supported stages, they are tested elsewhere.
        Stage.WAIT_SETUP_PHASE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_INPUTS -> {}
        else -> assertContextThrowsErrorWhenGettingBlob(stage)
      }
    }
  }
}
