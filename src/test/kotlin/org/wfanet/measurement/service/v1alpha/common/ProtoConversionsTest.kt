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

package org.wfanet.measurement.service.v1alpha.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

@RunWith(JUnit4::class)
class ProtoConversionsTest {
  @Test
  fun `convert Requisition to apiProto`() {
    val requisition = Requisition.newBuilder().apply {
      externalDataProviderId = ExternalId(1).value
      externalCampaignId = ExternalId(2).value
      externalRequisitionId = ExternalId(3).value

      state = RequisitionState.FULFILLED
      windowStartTime = Instant.MIN.toProtoTime()
      windowEndTime = Instant.MAX.toProtoTime()
    }.build()

    assertThat(requisition.toV1Api())
      .isEqualTo(
        MetricRequisition.newBuilder().apply {
          keyBuilder.apply {
            dataProviderId = ExternalId(1).apiId.value
            campaignId = ExternalId(2).apiId.value
            metricRequisitionId = ExternalId(3).apiId.value
          }

          state = MetricRequisition.State.FULFILLED
        }.build()
      )
  }

  @Test
  fun `convert RequisitionState to apiProto`() {
    assertThat(RequisitionState.FULFILLED.toV1Api())
      .isEqualTo(MetricRequisition.State.FULFILLED)
    assertThat(RequisitionState.UNFULFILLED.toV1Api())
      .isEqualTo(MetricRequisition.State.UNFULFILLED)
  }

  @Test
  fun `convert MetricRequisition State to RequisitionState`() {
    assertThat(MetricRequisition.State.FULFILLED.toRequisitionState())
      .isEqualTo(RequisitionState.FULFILLED)
    assertThat(MetricRequisition.State.UNFULFILLED.toRequisitionState())
      .isEqualTo(RequisitionState.UNFULFILLED)
    assertFails {
      MetricRequisition.State.STATE_UNSPECIFIED.toRequisitionState()
    }
    assertFails {
      MetricRequisition.State.UNRECOGNIZED.toRequisitionState()
    }
  }
}
