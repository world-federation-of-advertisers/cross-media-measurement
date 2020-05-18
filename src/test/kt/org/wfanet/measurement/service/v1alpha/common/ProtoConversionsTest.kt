package org.wfanet.measurement.service.v1alpha.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.Requisition
import org.wfanet.measurement.db.RequisitionExternalKey
import org.wfanet.measurement.db.RequisitionState
import java.time.Instant
import kotlin.test.assertFails

@RunWith(JUnit4::class)
class ProtoConversionsTest {
  @Test
  fun `convert Requisition to apiProto`() {
    val requisition = Requisition(
      externalKey = RequisitionExternalKey(ExternalId(1), ExternalId(2), ExternalId(3)),
      state = RequisitionState.FULFILLED,
      windowStartTime = Instant.MIN,
      windowEndTime = Instant.MAX
    )

    assertThat(requisition.toV1Api())
      .isEqualTo(MetricRequisition.newBuilder().apply {
        keyBuilder.apply {
          dataProviderId = ExternalId(1).apiId.value
          campaignId = ExternalId(2).apiId.value
          metricRequisitionId = ExternalId(3).apiId.value
        }

        state = MetricRequisition.State.FULFILLED
      }.build())
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
