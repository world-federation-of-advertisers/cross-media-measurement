package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.internal.SketchAggregationStage
import org.wfanet.measurement.internal.SketchAggregationStage.COMPLETED
import org.wfanet.measurement.internal.SketchAggregationStage.CREATED
import org.wfanet.measurement.internal.SketchAggregationStage.TO_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationStage.TO_APPEND_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationStage.UNRECOGNIZED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationStage.WAIT_SKETCHES

/**
 * Implementation of [ProtocolStageEnumHelper] for [SketchAggregationStage].
 */
object SketchAggregationStages :
  ProtocolStageEnumHelper<SketchAggregationStage> {
  override val validInitialStages = setOf(CREATED)
  override val validTerminalStages = setOf(COMPLETED)

  override val validSuccessors =
    mapOf(
      CREATED to setOf(TO_ADD_NOISE),
      TO_ADD_NOISE to setOf(WAIT_SKETCHES, WAIT_CONCATENATED),
      WAIT_SKETCHES to setOf(TO_APPEND_SKETCHES),
      TO_APPEND_SKETCHES to setOf(WAIT_CONCATENATED),
      WAIT_CONCATENATED to setOf(
        TO_BLIND_POSITIONS,
        TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
      ),
      TO_BLIND_POSITIONS to setOf(WAIT_FLAG_COUNTS, COMPLETED),
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS to setOf(WAIT_FLAG_COUNTS),
      WAIT_FLAG_COUNTS to setOf(
        TO_DECRYPT_FLAG_COUNTS,
        TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
      ),
      TO_DECRYPT_FLAG_COUNTS to setOf(),
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS to setOf()
    ).withDefault { setOf() }

  override fun enumToLong(value: SketchAggregationStage): Long {
    return value.numberAsLong
  }

  override fun longToEnum(value: Long): SketchAggregationStage {
    // forNumber() returns null for unrecognized enum values for the proto.
    return SketchAggregationStage.forNumber(value.toInt()) ?: UNRECOGNIZED
  }
}
