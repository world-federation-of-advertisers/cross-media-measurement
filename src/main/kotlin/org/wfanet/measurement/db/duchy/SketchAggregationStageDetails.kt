package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.WaitSketchesStageDetails

class SketchAggregationStageDetails(private val otherDuchies: List<String>) :
  ProtocolStageDetails<SketchAggregationState, ComputationStageDetails> {
  override fun detailsFor(stage: SketchAggregationState): ComputationStageDetails {
    return when (stage) {
      SketchAggregationState.WAIT_SKETCHES ->
        ComputationStageDetails.newBuilder()
          .setWaitSketchStageDetails(
            WaitSketchesStageDetails.newBuilder()
              .putAllExternalDuchyLocalBlobId(
                otherDuchies.mapIndexed { idx, duchy -> duchy to idx.toLong() }.toMap()
              )
          )
          .build()
      else -> ComputationStageDetails.getDefaultInstance()
    }
  }

  override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
    ComputationStageDetails.parseFrom(bytes)
}
