// Copyright 2020 The Measurement System Authors
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
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationV2Protocol
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2.Stage

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationV2ContextTest {

  @Test
  fun `next stages are valid`() {
    val context =
      LiquidLegionsSketchAggregationV2Context(AdvanceComputationRequest.newBuilder().build())
    fun assertContextThrowsErrorWhenCallingNextStage(stage: Stage) {
      val token = ComputationToken.newBuilder().apply {
        computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV2Value(stage.ordinal)
          .build()
        addBlobs(newEmptyOutputBlobMetadata(1L))
      }.build()
      assertFailsWith<StatusRuntimeException> {
        println(stage)
        context.outputBlob(token)
      }
    }

    for (stage in Stage.values()) {
      when (stage) {
        Stage.WAIT_SETUP_PHASE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> {
          val next =
            context
              .nextStage(ComputationDetails.getDefaultInstance(), stage.toProtocolStage())
              .liquidLegionsSketchAggregationV2
          assertTrue("$next is not a valid successor of $stage") {
            LiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(stage, next)
          }
        }
        else -> assertContextThrowsErrorWhenCallingNextStage(stage)
      }
    }
  }

  @Test
  fun `output blob for wait sketches`() {
    val token = ComputationToken.newBuilder().apply {
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
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV2Context(
        AdvanceComputationRequest.newBuilder().setDataOrigin("alice").build()
      ).outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(21L))
    assertThat(
      LiquidLegionsSketchAggregationV2Context(
        AdvanceComputationRequest.newBuilder().setDataOrigin("bob").build()
      ).outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(1L))
    assertFailsWith<IllegalStateException> {
      LiquidLegionsSketchAggregationV2Context(
        AdvanceComputationRequest.newBuilder().setDataOrigin("unknown-sender").build()
      ).outputBlob(token)
    }
  }

  @Test
  fun `output blob for wait reach estimation inputs`() {
    val token = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS.toProtocolStage()
      addBlobs(newEmptyOutputBlobMetadata(1L))
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV2Context(AdvanceComputationRequest.newBuilder().build())
        .outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(1L))
  }

  @Test
  fun `output blob for wait filtering phase inputs`() {
    val token = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS.toProtocolStage()
      addBlobs(newEmptyOutputBlobMetadata(100L))
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV2Context(AdvanceComputationRequest.newBuilder().build())
        .outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(100L))
  }

  @Test
  fun `output blob for wait frequency inputs`() {
    val token = ComputationToken.newBuilder().apply {
      computationStage = Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS.toProtocolStage()
      addBlobs(newEmptyOutputBlobMetadata(120L))
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV2Context(AdvanceComputationRequest.newBuilder().build())
        .outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(120L))
  }

  @Test
  fun `output blob for unsupported stages throw`() {
    val context =
      LiquidLegionsSketchAggregationV2Context(AdvanceComputationRequest.newBuilder().build())
    fun assertContextThrowsErrorWhenGettingBlob(stage: Stage) {
      val token = ComputationToken.newBuilder().apply {
        computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV2Value(stage.ordinal)
          .build()
        addBlobs(newEmptyOutputBlobMetadata(1L))
      }.build()
      assertFailsWith<StatusRuntimeException> {
        context.outputBlob(token)
      }
    }

    for (stage in Stage.values()) {
      println(stage)
      when (stage) {
        // Skip all the supported stages, they are tested elsewhere.
        Stage.WAIT_SETUP_PHASE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_ONE_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_TWO_INPUTS,
        Stage.WAIT_EXECUTION_PHASE_THREE_INPUTS -> { }
        else -> assertContextThrowsErrorWhenGettingBlob(stage)
      }
    }
  }
}
