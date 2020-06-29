package org.wfanet.measurement.db.duchy

import com.google.common.truth.extensions.proto.ProtoTruth
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.WaitSketchesStageDetails

@RunWith(JUnit4::class)
class SketchAggregationStageDetailsTest {

  @Test
  fun `stage defaults and conversions`() {
    val d = SketchAggregationStageDetails(listOf("A", "B", "C"))
    for (stage in SketchAggregationStage.values()) {
      val expected =
        when (stage) {
          SketchAggregationStage.WAIT_SKETCHES ->
            ComputationStageDetails.newBuilder()
              .setWaitSketchStageDetails(
                WaitSketchesStageDetails.newBuilder()
                  .putExternalDuchyLocalBlobId("A", 1L)
                  .putExternalDuchyLocalBlobId("B", 2L)
                  .putExternalDuchyLocalBlobId("C", 3L)
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
