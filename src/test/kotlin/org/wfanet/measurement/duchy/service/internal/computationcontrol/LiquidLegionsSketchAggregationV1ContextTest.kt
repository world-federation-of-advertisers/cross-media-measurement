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
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationV1Protocol
import org.wfanet.measurement.duchy.service.internal.computation.newEmptyOutputBlobMetadata
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.AdvanceComputationRequest
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1.ComputationDetails.RoleInComputation

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationV1ContextTest {

  @Test
  fun `next stages are valid`() {
    val detailsHelper = LiquidLegionsSketchAggregationV1Protocol.EnumStages.Details(listOf())
    assertThat(LiquidLegionsSketchAggregationV1Context.NEXT_STAGE_BY_ROLE.keys)
      .containsExactly(RoleInComputation.PRIMARY, RoleInComputation.SECONDARY)
    for ((role, successorMap) in LiquidLegionsSketchAggregationV1Context.NEXT_STAGE_BY_ROLE) {
      for ((curr, next) in successorMap) {
        assertTrue("$next is not a valid successor of $curr for role $role") {
          LiquidLegionsSketchAggregationV1Protocol.EnumStages.validTransition(curr, next)
        }
        // Ensure the stage is acceptable for the worker's role in the computation
        val detailsWrappingRole =
          LiquidLegionsSketchAggregationV1.ComputationDetails.newBuilder().setRole(role).build()
        assertTrue("$curr not valid stage for $detailsWrappingRole") {
          detailsHelper.validateRoleForStage(curr, detailsWrappingRole)
        }
        assertTrue("$next not valid stage for $detailsWrappingRole") {
          detailsHelper.validateRoleForStage(next, detailsWrappingRole)
        }
      }
    }
  }

  @Test
  fun `output blob for wait sketches`() {
    val token = ComputationToken.newBuilder().apply {
      computationStage = LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES.toProtocolStage()
      addBlobs(newEmptyOutputBlobMetadata(1L))
      addBlobs(newEmptyOutputBlobMetadata(21L))
      stageSpecificDetailsBuilder.apply {
        liquidLegionsV1Builder.apply {
          waitSketchStageDetailsBuilder.putExternalDuchyLocalBlobId("alice", 21L)
          waitSketchStageDetailsBuilder.putExternalDuchyLocalBlobId("bob", 1L)
        }
      }
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV1Context(
        AdvanceComputationRequest.newBuilder().setDataOrigin("alice").build()
      ).outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(21L))
    assertThat(
      LiquidLegionsSketchAggregationV1Context(
        AdvanceComputationRequest.newBuilder().setDataOrigin("bob").build()
      ).outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(1L))
    assertFailsWith<IllegalStateException> {
      LiquidLegionsSketchAggregationV1Context(
        AdvanceComputationRequest.newBuilder().setDataOrigin("unknown-sender").build()
      ).outputBlob(token)
    }
  }

  @Test
  fun `output blob for wait concatenated`() {
    val token = ComputationToken.newBuilder().apply {
      computationStage = LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED.toProtocolStage()
      addBlobs(newEmptyOutputBlobMetadata(1L))
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV1Context(AdvanceComputationRequest.newBuilder().build())
        .outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(1L))
  }

  @Test
  fun `output blob for wait flag counts`() {
    val token = ComputationToken.newBuilder().apply {
      computationStage = LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS.toProtocolStage()
      addBlobs(newEmptyOutputBlobMetadata(100L))
    }.build()

    assertThat(
      LiquidLegionsSketchAggregationV1Context(AdvanceComputationRequest.newBuilder().build())
        .outputBlob(token)
    ).isEqualTo(newEmptyOutputBlobMetadata(100L))
  }

  @Test
  fun `output blob for unsupported stages thorw`() {
    val context =
      LiquidLegionsSketchAggregationV1Context(AdvanceComputationRequest.newBuilder().build())
    fun assertContextThrowsErrorWhenGettingBlob(stage: LiquidLegionsSketchAggregationV1.Stage) {
      val token = ComputationToken.newBuilder().apply {
        computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV1Value(stage.ordinal)
          .build()
        addBlobs(newEmptyOutputBlobMetadata(1L))
      }.build()
      assertFailsWith<StatusRuntimeException> {
        context.outputBlob(token)
      }
    }

    for (stage in LiquidLegionsSketchAggregationV1.Stage.values()) {
      println(stage)
      when (stage) {
        // Skip all the supported stages.
        LiquidLegionsSketchAggregationV1.Stage.WAIT_FLAG_COUNTS,
        LiquidLegionsSketchAggregationV1.Stage.WAIT_CONCATENATED,
        LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES -> { }
        else -> assertContextThrowsErrorWhenGettingBlob(stage)
      }
    }
  }
}
