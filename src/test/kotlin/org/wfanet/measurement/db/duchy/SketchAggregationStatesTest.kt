package org.wfanet.measurement.db.duchy

import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.SketchAggregationState

@RunWith(JUnit4::class)
class SketchAggregationStatesTest {
  @Test
  fun `verify initial state`() {
    assertTrue { SketchAggregationStates.validInitialState(SketchAggregationState.STARTING) }
    assertFalse { SketchAggregationStates.validInitialState(SketchAggregationState.WAIT_SKETCHES) }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (state in SketchAggregationState.values()) {
      assertEquals(
        state,
        SketchAggregationStates.longToEnum(SketchAggregationStates.enumToLong(state)),
        "enumToLong and longToEnum were not inverses for $state"
      )
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(SketchAggregationState.UNRECOGNIZED, SketchAggregationStates.longToEnum(-1))
    assertEquals(SketchAggregationState.UNRECOGNIZED, SketchAggregationStates.longToEnum(1000))
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      SketchAggregationStates.validTransition(
        SketchAggregationState.STARTING,
        SketchAggregationState.WAIT_SKETCHES
      )
    }

    assertFalse {
      SketchAggregationStates.validTransition(
        SketchAggregationState.STARTING,
        SketchAggregationState.COMBINING_REGISTERS
      )
    }

    assertFalse {
      SketchAggregationStates.validTransition(
        SketchAggregationState.UNKNOWN,
        SketchAggregationState.STARTING
      )
    }

    assertFalse {
      SketchAggregationStates.validTransition(
        SketchAggregationState.UNRECOGNIZED,
        SketchAggregationState.STARTING
      )
    }
  }
}
