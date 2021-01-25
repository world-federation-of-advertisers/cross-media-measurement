// Copyright 2020 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationV1ProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue {
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.validInitialStage(
        LiquidLegionsSketchAggregationV1.Stage.TO_CONFIRM_REQUISITIONS
      )
    }
    assertFalse {
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.validInitialStage(
        LiquidLegionsSketchAggregationV1.Stage.WAIT_SKETCHES
      )
    }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in LiquidLegionsSketchAggregationV1.Stage.values()) {
      if (stage == LiquidLegionsSketchAggregationV1.Stage.UNRECOGNIZED) {
        assertFails { LiquidLegionsSketchAggregationV1Protocol.EnumStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          LiquidLegionsSketchAggregationV1Protocol.EnumStages.longToEnum(
            LiquidLegionsSketchAggregationV1Protocol.EnumStages.enumToLong(stage)
          ),
          "enumToLong and longToEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(
      LiquidLegionsSketchAggregationV1.Stage.UNRECOGNIZED,
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.longToEnum(-1)
    )
    assertEquals(
      LiquidLegionsSketchAggregationV1.Stage.UNRECOGNIZED,
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.longToEnum(1000)
    )
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV1.Stage.WAIT_TO_START,
        LiquidLegionsSketchAggregationV1.Stage.TO_ADD_NOISE
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV1.Stage.TO_CONFIRM_REQUISITIONS,
        LiquidLegionsSketchAggregationV1.Stage.TO_BLIND_POSITIONS
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV1.Stage.STAGE_UNKNOWN,
        LiquidLegionsSketchAggregationV1.Stage.TO_CONFIRM_REQUISITIONS
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationV1Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV1.Stage.UNRECOGNIZED,
        LiquidLegionsSketchAggregationV1.Stage.TO_CONFIRM_REQUISITIONS
      )
    }
  }
}
