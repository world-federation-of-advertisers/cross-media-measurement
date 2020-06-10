package org.wfanet.measurement.db.duchy

import com.google.common.truth.extensions.proto.ProtoTruth
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.WaitSketchesStageDetails

@RunWith(JUnit4::class)
class SketchAggregationStageDetailsTest {

  @Test
  fun `stage defaults and conversions`() {
    val d = SketchAggregationStageDetails(listOf("A", "B", "C"))
    for (stage in SketchAggregationState.values()) {
      val expected =
        when (stage) {
          SketchAggregationState.WAIT_SKETCHES -> ComputationStageDetails.newBuilder()
            .setWaitSketchStageDetails(
              WaitSketchesStageDetails.newBuilder()
                .putExternalDuchyLocalBlobId("A", 0L)
                .putExternalDuchyLocalBlobId("B", 1L)
                .putExternalDuchyLocalBlobId("C", 2L)
            )
            .build()
          else -> ComputationStageDetails.getDefaultInstance()
        }
      val stageProto = d.detailsFor(stage)
      ProtoTruth.assertThat(stageProto).isEqualTo(expected)
      ProtoTruth.assertThat(d.parseDetails(stageProto.toByteArray())).isEqualTo(stageProto)
    }
  }
}
