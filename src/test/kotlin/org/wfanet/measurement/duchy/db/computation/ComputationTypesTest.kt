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
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

class ComputationTypesTest {
  @Test
  fun `protocolEnumToLong then longToProtocolEnum results in same enum value`() {
    for (stage in ComputationType.values()) {
      if (stage == ComputationType.UNRECOGNIZED) {
        assertFails { ComputationTypes.protocolEnumToLong(stage) }
      } else {
        assertEquals(
          stage,
          ComputationTypes.longToProtocolEnum(ComputationTypes.protocolEnumToLong(stage)),
          "protocolEnumToLong and longToProtocolEnum were not inverses for $stage"
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(ComputationType.UNRECOGNIZED, ComputationTypes.longToProtocolEnum(-1))
    assertEquals(ComputationType.UNRECOGNIZED, ComputationTypes.longToProtocolEnum(1000))
  }
}
