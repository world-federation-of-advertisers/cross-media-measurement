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
import org.junit.Test
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV1
import org.wfanet.measurement.protocol.LiquidLegionsSketchAggregationV2

class ComputationsEnumHelperTest {
  @Test
  fun `liquidLegionsSketchAggregationV1 round trip conversion should get the same stage`() {
    for (stage in LiquidLegionsSketchAggregationV1.Stage.values()) {
      if (stage != LiquidLegionsSketchAggregationV1.Stage.UNRECOGNIZED) {
        val computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV1(stage).build()
        assertEquals(
          computationStage,
          ComputationProtocolStages.longValuesToComputationStageEnum(
            ComputationProtocolStages.computationStageEnumToLongValues(
              computationStage
            )
          ),
          "protocolEnumToLong and longToProtocolEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `liquidLegionsSketchAggregationV2 round trip conversion should get the same stage`() {
    for (stage in LiquidLegionsSketchAggregationV2.Stage.values()) {
      if (stage != LiquidLegionsSketchAggregationV2.Stage.UNRECOGNIZED) {
        val computationStage = ComputationStage.newBuilder()
          .setLiquidLegionsSketchAggregationV2(stage).build()
        assertEquals(
          computationStage,
          ComputationProtocolStages.longValuesToComputationStageEnum(
            ComputationProtocolStages.computationStageEnumToLongValues(
              computationStage
            )
          ),
          "protocolEnumToLong and longToProtocolEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longValuesToComputationStageEnum with invalid numbers`() {
    assertFails {
      ComputationProtocolStages.longValuesToComputationStageEnum(
        ComputationStageLongValues(-1, -1)
      )
    }
  }
}
