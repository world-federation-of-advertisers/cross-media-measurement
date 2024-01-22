// Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.duchy.db.computation.ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.Details
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt.ComputationDetailsKt.computationParticipant
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.computationDetails
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2Kt.waitSetupPhaseInputsDetails

@RunWith(JUnit4::class)
class ReachOnlyLiquidLegionsSketchAggregationV2ProtocolEnumStagesDetailsTest {

  @Test
  fun `stage defaults and conversions`() {
    for (stage in ReachOnlyLiquidLegionsSketchAggregationV2.Stage.values()) {
      val expected =
        when (stage) {
          ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS ->
            computationStageDetails {
              reachOnlyLiquidLegionsV2 = stageDetails {
                waitSetupPhaseInputsDetails = waitSetupPhaseInputsDetails {
                  externalDuchyLocalBlobId["A"] = 0L
                  externalDuchyLocalBlobId["B"] = 1L
                  externalDuchyLocalBlobId["C"] = 2L
                }
              }
            }
          else -> ComputationStageDetails.getDefaultInstance()
        }
      val stageProto =
        Details.detailsFor(
          stage,
          computationDetails {
            participant += computationParticipant { duchyId = "A" }
            participant += computationParticipant { duchyId = "B" }
            participant += computationParticipant { duchyId = "C" }
            participant += computationParticipant { duchyId = "D" }
          },
        )
      ProtoTruth.assertThat(stageProto).isEqualTo(expected)
      ProtoTruth.assertThat(Details.parseDetails(stageProto.toByteArray())).isEqualTo(stageProto)
    }
  }
}
