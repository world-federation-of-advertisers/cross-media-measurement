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
import org.wfanet.measurement.duchy.db.computation.HonestMajorityShareShuffleProtocol.EnumStages.Details
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.computationStageDetails
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.computationDetails
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.stageDetails
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffleKt.waitOnAggregationInputDetails

@RunWith(JUnit4::class)
class HonestMajorityShareShuffleProtocolEnumStagesDetailsTest {

  @Test
  fun `stage defaults and conversions`() {
    for (stage in HonestMajorityShareShuffle.Stage.values()) {
      val expected =
        when (stage) {
          HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT ->
            computationStageDetails {
              honestMajorityShareShuffle = stageDetails {
                waitOnAggregationInputDetails = waitOnAggregationInputDetails {
                  externalDuchyLocalBlobId["worker_1"] = 0L
                  externalDuchyLocalBlobId["worker_2"] = 1L
                }
              }
            }
          else -> ComputationStageDetails.getDefaultInstance()
        }
      val stageProto =
        Details.detailsFor(
          stage,
          computationDetails {
            nonAggregators += "worker_1"
            nonAggregators += "worker_2"
          },
        )
      ProtoTruth.assertThat(stageProto).isEqualTo(expected)
      ProtoTruth.assertThat(Details.parseDetails(stageProto.toByteArray())).isEqualTo(stageProto)
    }
  }
}
