package org.wfanet.measurement.db.duchy

import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.internal.SketchAggregationState
import org.wfanet.measurement.internal.SketchAggregationState.ADDING_NOISE
import org.wfanet.measurement.internal.SketchAggregationState.BLINDING_AND_JOINING_REGISTERS
import org.wfanet.measurement.internal.SketchAggregationState.BLINDING_POSITIONS
import org.wfanet.measurement.internal.SketchAggregationState.CLEAN_UP
import org.wfanet.measurement.internal.SketchAggregationState.COMPUTING_METRICS
import org.wfanet.measurement.internal.SketchAggregationState.DECRYPTING_FLAG_COUNTS
import org.wfanet.measurement.internal.SketchAggregationState.FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.GATHERING_LOCAL_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.RECEIVED_SKETCHES
import org.wfanet.measurement.internal.SketchAggregationState.STARTING
import org.wfanet.measurement.internal.SketchAggregationState.UNRECOGNIZED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_CONCATENATED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_FINISHED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_JOINED
import org.wfanet.measurement.internal.SketchAggregationState.WAIT_SKETCHES

/**
 * Implementation of [ProtocolStateEnumHelper] for [SketchAggregationState].
 */
object SketchAggregationStates :
  ProtocolStateEnumHelper<SketchAggregationState> {
  override val validInitialStates = setOf(STARTING)

  override val validSuccessors =
    mapOf(
      STARTING to setOf(GATHERING_LOCAL_SKETCHES, CLEAN_UP),
      GATHERING_LOCAL_SKETCHES to setOf(ADDING_NOISE, CLEAN_UP),
      ADDING_NOISE to setOf(WAIT_SKETCHES, WAIT_CONCATENATED, CLEAN_UP),
      WAIT_SKETCHES to setOf(RECEIVED_SKETCHES, CLEAN_UP),
      RECEIVED_SKETCHES to setOf(WAIT_CONCATENATED, CLEAN_UP),
      WAIT_CONCATENATED to setOf(RECEIVED_CONCATENATED, CLEAN_UP),
      RECEIVED_CONCATENATED to setOf(BLINDING_POSITIONS, BLINDING_AND_JOINING_REGISTERS, CLEAN_UP),
      BLINDING_POSITIONS to setOf(WAIT_JOINED, CLEAN_UP),
      BLINDING_AND_JOINING_REGISTERS to setOf(WAIT_JOINED, CLEAN_UP),
      WAIT_JOINED to setOf(RECEIVED_JOINED, CLEAN_UP),
      RECEIVED_JOINED to setOf(DECRYPTING_FLAG_COUNTS, CLEAN_UP),
      DECRYPTING_FLAG_COUNTS to setOf(COMPUTING_METRICS, WAIT_FINISHED, CLEAN_UP),
      COMPUTING_METRICS to setOf(WAIT_FINISHED, CLEAN_UP),
      WAIT_FINISHED to setOf(CLEAN_UP),
      CLEAN_UP to setOf(FINISHED)
    ).withDefault { setOf() }

  override fun enumToLong(value: SketchAggregationState): Long {
    return value.numberAsLong
  }

  override fun longToEnum(value: Long): SketchAggregationState {
    // forNumber() returns null for unrecognized enum values for the proto.
    return SketchAggregationState.forNumber(value.toInt()) ?: UNRECOGNIZED
  }
}
