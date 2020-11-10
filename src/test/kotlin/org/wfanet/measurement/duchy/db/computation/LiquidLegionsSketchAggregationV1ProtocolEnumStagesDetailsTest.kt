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

package org.wfanet.measurement.duchy.db.computation

import com.google.common.truth.extensions.proto.ProtoTruth
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationV1ProtocolEnumStagesDetailsTest {

  @Test
  fun `stage defaults and conversions`() {
    val d = LiquidLegionsSketchAggregationV1Protocol.EnumStages.Details(listOf("A", "B", "C"))
    for (stage in LiquidLegionsSketchAggregationV1.Stage.values()) {
      val expected =
        when (stage) {
          LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES ->
            ComputationStageDetails.newBuilder().apply {
              liquidLegionsV1Builder.waitSketchStageDetailsBuilder.apply {
                putExternalDuchyLocalBlobId("A", 1L)
                putExternalDuchyLocalBlobId("B", 2L)
                putExternalDuchyLocalBlobId("C", 3L)
              }
            }.build()
          else -> ComputationStageDetails.getDefaultInstance()
        }
      val stageProto = d.detailsFor(stage)
      ProtoTruth.assertThat(stageProto).isEqualTo(expected)
      ProtoTruth.assertThat(d.parseDetails(stageProto.toByteArray())).isEqualTo(stageProto)
    }
  }
}
