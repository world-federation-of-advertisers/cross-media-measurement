package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.SketchAggregationState.COMPLETED
import org.wfanet.measurement.internal.SketchAggregationState.CREATED
import org.wfanet.measurement.internal.SketchAggregationState.TO_ADD_NOISE
import org.wfanet.measurement.internal.SketchAggregationState.TO_APPEND_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.TO_BLIND_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationState.TO_BLIND_POSITIONS_AND_JOIN_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationState.TO_DECRYPT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS
import org.wfanet.measurement.internal.SketchAggregationState.UNRECOGNIZED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_SKETCHES

/**
 * Implementation of [ProtocolStateEnumHelper] for [SketchAggregationState].
 */
object SketchAggregationStates :
  ProtocolStateEnumHelper<SketchAggregationState> {
  override val validInitialStates = setOf(CREATED)

  override val validSuccessors =
    mapOf(
      CREATED to setOf(TO_ADD_NOISE, COMPLETED),
      TO_ADD_NOISE to setOf(WAIT_SKETCHES, WAIT_CONCATENATED, COMPLETED),
      WAIT_SKETCHES to setOf(TO_APPEND_SKETCHES, COMPLETED),
      TO_APPEND_SKETCHES to setOf(WAIT_CONCATENATED, COMPLETED),
      WAIT_CONCATENATED to setOf(
        TO_BLIND_POSITIONS,
        TO_BLIND_POSITIONS_AND_JOIN_REGISTERS,
        COMPLETED
      ),
      TO_BLIND_POSITIONS to setOf(WAIT_FLAG_COUNTS, COMPLETED),
      TO_BLIND_POSITIONS_AND_JOIN_REGISTERS to setOf(WAIT_FLAG_COUNTS, COMPLETED),
      WAIT_FLAG_COUNTS to setOf(
        TO_DECRYPT_FLAG_COUNTS,
        TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS,
        COMPLETED
      ),
      TO_DECRYPT_FLAG_COUNTS to setOf(COMPLETED),
      TO_DECRYPT_FLAG_COUNTS_AND_COMPUTE_METRICS to setOf(COMPLETED)
    ).withDefault { setOf() }

  override fun enumToLong(value: SketchAggregationState): Long {
    return value.numberAsLong
  }

  override fun longToEnum(value: Long): SketchAggregationState {
    // forNumber() returns null for unrecognized enum values for the proto.
    return SketchAggregationState.forNumber(value.toInt()) ?: UNRECOGNIZED
  }
}
