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

package org.wfanet.measurement.duchy.db.computation

import com.google.common.truth.extensions.proto.ProtoTruth
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationV2Protocol.EnumStages.Details
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationV2ProtocolEnumStagesDetailsTest {

  @Test
  fun `stage defaults and conversions`() {
    for (stage in LiquidLegionsSketchAggregationV2.Stage.values()) {
      val expected =
        when (stage) {
          LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS ->
            ComputationStageDetails.newBuilder()
              .apply {
                liquidLegionsV2Builder.waitSetupPhaseInputsDetailsBuilder.apply {
                  putExternalDuchyLocalBlobId("A", 0L)
                  putExternalDuchyLocalBlobId("B", 1L)
                  putExternalDuchyLocalBlobId("C", 2L)
                }
              }
              .build()
          else -> ComputationStageDetails.getDefaultInstance()
        }
      val stageProto =
        Details.detailsFor(
          stage,
          LiquidLegionsSketchAggregationV2.ComputationDetails.newBuilder()
            .apply {
              addParticipantBuilder().apply { duchyId = "A" }
              addParticipantBuilder().apply { duchyId = "B" }
              addParticipantBuilder().apply { duchyId = "C" }
              addParticipantBuilder().apply { duchyId = "D" }
            }
            .build(),
        )
      ProtoTruth.assertThat(stageProto).isEqualTo(expected)
      ProtoTruth.assertThat(Details.parseDetails(stageProto.toByteArray())).isEqualTo(stageProto)
    }
  }
}
