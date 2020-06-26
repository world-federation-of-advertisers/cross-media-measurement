package org.wfanet.measurement.db.duchy

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.SketchAggregationState
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnit4::class)
class SketchAggregationStatesTest {
  @Test
  fun `verify initial state`() {
    assertTrue { SketchAggregationStates.validInitialState(SketchAggregationState.CREATED) }
    assertFalse { SketchAggregationStates.validInitialState(SketchAggregationState.WAIT_SKETCHES) }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (state in SketchAggregationState.values()) {
      if (state == SketchAggregationState.UNRECOGNIZED) {
        assertFails { SketchAggregationStates.enumToLong(state) }
      } else {
        assertEquals(
          state,
          SketchAggregationStates.longToEnum(SketchAggregationStates.enumToLong(state)),
          "enumToLong and longToEnum were not inverses for $state"
        )
      }
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
        SketchAggregationState.CREATED,
        SketchAggregationState.TO_ADD_NOISE
      )
    }

    assertFalse {
      SketchAggregationStates.validTransition(
        SketchAggregationState.CREATED,
        SketchAggregationState.WAIT_SKETCHES
      )
    }

    assertFalse {
      SketchAggregationStates.validTransition(
        SketchAggregationState.UNKNOWN,
        SketchAggregationState.CREATED
      )
    }

    assertFalse {
      SketchAggregationStates.validTransition(
        SketchAggregationState.UNRECOGNIZED,
        SketchAggregationState.CREATED
      )
    }
  }
}
