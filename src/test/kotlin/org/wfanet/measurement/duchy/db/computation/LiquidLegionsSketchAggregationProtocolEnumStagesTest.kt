// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.db.computation

import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validInitialStage(
        LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS
      )
    }
    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validInitialStage(
        LiquidLegionsSketchAggregationStage.WAIT_SKETCHES
      )
    }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in LiquidLegionsSketchAggregationStage.values()) {
      if (stage == LiquidLegionsSketchAggregationStage.UNRECOGNIZED) {
        assertFails { LiquidLegionsSketchAggregationProtocol.EnumStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          LiquidLegionsSketchAggregationProtocol.EnumStages.longToEnum(
            LiquidLegionsSketchAggregationProtocol.EnumStages.enumToLong(stage)
          ),
          "enumToLong and longToEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(
      LiquidLegionsSketchAggregationStage.UNRECOGNIZED,
      LiquidLegionsSketchAggregationProtocol.EnumStages.longToEnum(-1)
    )
    assertEquals(
      LiquidLegionsSketchAggregationStage.UNRECOGNIZED,
      LiquidLegionsSketchAggregationProtocol.EnumStages.longToEnum(1000)
    )
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationStage.WAIT_TO_START,
        LiquidLegionsSketchAggregationStage.TO_ADD_NOISE
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS,
        LiquidLegionsSketchAggregationStage.TO_BLIND_POSITIONS
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationStage.SKETCH_AGGREGATION_STAGE_UNKNOWN,
        LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationStage.UNRECOGNIZED,
        LiquidLegionsSketchAggregationStage.TO_CONFIRM_REQUISITIONS
      )
    }
  }
}
