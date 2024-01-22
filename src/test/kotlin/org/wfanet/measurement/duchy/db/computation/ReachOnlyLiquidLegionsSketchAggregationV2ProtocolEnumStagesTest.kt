// Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.duchy.protocol.ReachOnlyLiquidLegionsSketchAggregationV2

@RunWith(JUnit4::class)
class ReachOnlyLiquidLegionsSketchAggregationV2ProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue {
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validInitialStage(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE
      )
    }
    assertFalse {
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validInitialStage(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS
      )
    }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in ReachOnlyLiquidLegionsSketchAggregationV2.Stage.values()) {
      if (stage == ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED) {
        assertFails {
          ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.enumToLong(stage)
        }
      } else {
        assertEquals(
          stage,
          ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(
            ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.enumToLong(stage)
          ),
          "enumToLong and longToEnum were not inverses for $stage",
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(-1),
    )
    assertEquals(
      ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.longToEnum(1000),
    )
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS,
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.SETUP_PHASE,
      )
    }

    assertFalse {
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.WAIT_SETUP_PHASE_INPUTS,
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.COMPLETE,
      )
    }

    assertFalse {
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.STAGE_UNSPECIFIED,
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      )
    }

    assertFalse {
      ReachOnlyLiquidLegionsSketchAggregationV2Protocol.EnumStages.validTransition(
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED,
        ReachOnlyLiquidLegionsSketchAggregationV2.Stage.CONFIRMATION_PHASE,
      )
    }
  }
}
