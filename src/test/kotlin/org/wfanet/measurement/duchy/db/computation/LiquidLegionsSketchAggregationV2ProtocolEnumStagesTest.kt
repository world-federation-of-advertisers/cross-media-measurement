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
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationV2ProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue {
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.validInitialStage(
        LiquidLegionsSketchAggregationV2.Stage.CONFIRM_REQUISITIONS_PHASE
      )
    }
    assertFalse {
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.validInitialStage(
        LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
      )
    }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in LiquidLegionsSketchAggregationV2.Stage.values()) {
      if (stage == LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED) {
        assertFails { LiquidLegionsSketchAggregationV2Protocol.EnumStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          LiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(
            LiquidLegionsSketchAggregationV2Protocol.EnumStages.enumToLong(stage)
          ),
          "enumToLong and longToEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(
      LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(-1)
    )
    assertEquals(
      LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(1000)
    )
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS,
        LiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS,
        LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_TWO
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV2.Stage.STAGE_UNKNOWN,
        LiquidLegionsSketchAggregationV2.Stage.CONFIRM_REQUISITIONS_PHASE
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
        LiquidLegionsSketchAggregationV2.Stage.CONFIRM_REQUISITIONS_PHASE
      )
    }
  }
}
