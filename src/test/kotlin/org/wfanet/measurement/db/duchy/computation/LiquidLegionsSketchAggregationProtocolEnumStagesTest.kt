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

package org.wfanet.measurement.db.duchy.computation

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.SketchAggregationStage
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@RunWith(JUnit4::class)
class LiquidLegionsSketchAggregationProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue { LiquidLegionsSketchAggregationProtocol.EnumStages.validInitialStage(SketchAggregationStage.CREATED) }
    assertFalse { LiquidLegionsSketchAggregationProtocol.EnumStages.validInitialStage(SketchAggregationStage.WAIT_SKETCHES) }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in SketchAggregationStage.values()) {
      if (stage == SketchAggregationStage.UNRECOGNIZED) {
        assertFails { LiquidLegionsSketchAggregationProtocol.EnumStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          LiquidLegionsSketchAggregationProtocol.EnumStages.longToEnum(LiquidLegionsSketchAggregationProtocol.EnumStages.enumToLong(stage)),
          "enumToLong and longToEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(SketchAggregationStage.UNRECOGNIZED, LiquidLegionsSketchAggregationProtocol.EnumStages.longToEnum(-1))
    assertEquals(SketchAggregationStage.UNRECOGNIZED, LiquidLegionsSketchAggregationProtocol.EnumStages.longToEnum(1000))
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        SketchAggregationStage.CREATED,
        SketchAggregationStage.TO_ADD_NOISE
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        SketchAggregationStage.CREATED,
        SketchAggregationStage.TO_BLIND_POSITIONS
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        SketchAggregationStage.UNKNOWN,
        SketchAggregationStage.CREATED
      )
    }

    assertFalse {
      LiquidLegionsSketchAggregationProtocol.EnumStages.validTransition(
        SketchAggregationStage.UNRECOGNIZED,
        SketchAggregationStage.CREATED
      )
    }
  }
}
