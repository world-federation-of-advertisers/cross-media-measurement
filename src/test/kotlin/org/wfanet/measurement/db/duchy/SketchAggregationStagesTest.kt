package org.wfanet.measurement.db.duchy

import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.SketchAggregationStage

@RunWith(JUnit4::class)
class SketchAggregationStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue { SketchAggregationStages.validInitialStage(SketchAggregationStage.CREATED) }
    assertFalse { SketchAggregationStages.validInitialStage(SketchAggregationStage.WAIT_SKETCHES) }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in SketchAggregationStage.values()) {
      if (stage == SketchAggregationStage.UNRECOGNIZED) {
        assertFails { SketchAggregationStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          SketchAggregationStages.longToEnum(SketchAggregationStages.enumToLong(stage)),
          "enumToLong and longToEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(SketchAggregationStage.UNRECOGNIZED, SketchAggregationStages.longToEnum(-1))
    assertEquals(SketchAggregationStage.UNRECOGNIZED, SketchAggregationStages.longToEnum(1000))
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      SketchAggregationStages.validTransition(
        SketchAggregationStage.CREATED,
        SketchAggregationStage.TO_ADD_NOISE
      )
    }

    assertFalse {
      SketchAggregationStages.validTransition(
        SketchAggregationStage.CREATED,
        SketchAggregationStage.WAIT_SKETCHES
      )
    }

    assertFalse {
      SketchAggregationStages.validTransition(
        SketchAggregationStage.UNKNOWN,
        SketchAggregationStage.CREATED
      )
    }

    assertFalse {
      SketchAggregationStages.validTransition(
        SketchAggregationStage.UNRECOGNIZED,
        SketchAggregationStage.CREATED
      )
    }
  }
}
