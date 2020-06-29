package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.WaitSketchesStageDetails

class SketchAggregationStageDetails(private val otherDuchies: List<String>) :
  ProtocolStageDetails<SketchAggregationStage, ComputationStageDetails> {
  override fun detailsFor(stage: SketchAggregationStage): ComputationStageDetails {
    return when (stage) {
      SketchAggregationStage.WAIT_SKETCHES ->
        ComputationStageDetails.newBuilder()
          .setWaitSketchStageDetails(
            WaitSketchesStageDetails.newBuilder()
              // The WAIT_SKETCHES stage has exactly one input which is the noised sketches from
              // the primary duchy running the wait operation. It is not an output of the stage
              // because it is a result of a locally running stage.
              .putAllExternalDuchyLocalBlobId(
                otherDuchies.mapIndexed { idx, duchy -> duchy to (idx + 1).toLong() }.toMap()
              )
          )
          .build()
      else -> ComputationStageDetails.getDefaultInstance()
    }
  }

  override fun parseDetails(bytes: ByteArray): ComputationStageDetails =
    ComputationStageDetails.parseFrom(bytes)
}
