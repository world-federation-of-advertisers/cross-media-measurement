// Copyright 2025 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.duchy.protocol.TrusTee

@RunWith(JUnit4::class)
class TrusTeeProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue {
      TrusTeeProtocol.EnumStages.validInitialStage(
        TrusTee.Stage.INITIALIZED
      )
    }
    assertFalse {
      TrusTeeProtocol.EnumStages.validInitialStage(
        TrusTee.Stage.WAIT_TO_START
      )
    }
    assertFalse {
      TrusTeeProtocol.EnumStages.validInitialStage(
        TrusTee.Stage.COMPUTING
      )
    }
    assertFalse {
      TrusTeeProtocol.EnumStages.validInitialStage(
        TrusTee.Stage.COMPLETE
      )
    }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in TrusTee.Stage.values()) {
      if (stage == TrusTee.Stage.UNRECOGNIZED) {
        assertFails { TrusTeeProtocol.EnumStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          TrusTeeProtocol.EnumStages.longToEnum(
            TrusTeeProtocol.EnumStages.enumToLong(stage)
          ),
          "enumToLong and longToEnum were not inverses for $stage",
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(
      TrusTee.Stage.UNRECOGNIZED,
      TrusTeeProtocol.EnumStages.longToEnum(-1),
    )
    assertEquals(
      TrusTee.Stage.UNRECOGNIZED,
      TrusTeeProtocol.EnumStages.longToEnum(1000),
    )
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      TrusTeeProtocol.EnumStages.validTransition(
        TrusTee.Stage.INITIALIZED,
        TrusTee.Stage.WAIT_TO_START,
      )
    }

    assertTrue {
      TrusTeeProtocol.EnumStages.validTransition(
        TrusTee.Stage.WAIT_TO_START,
        TrusTee.Stage.COMPUTING,
      )
    }

    assertTrue {
      TrusTeeProtocol.EnumStages.validTransition(
        TrusTee.Stage.COMPUTING,
        TrusTee.Stage.COMPLETE,
      )
    }

    assertFalse {
      TrusTeeProtocol.EnumStages.validTransition(
        TrusTee.Stage.WAIT_TO_START,
        TrusTee.Stage.COMPLETE,
      )
    }

    assertFalse {
      TrusTeeProtocol.EnumStages.validTransition(
        TrusTee.Stage.INITIALIZED,
        TrusTee.Stage.COMPLETE,
      )
    }

    assertFalse {
      TrusTeeProtocol.EnumStages.validTransition(
        TrusTee.Stage.UNRECOGNIZED,
        TrusTee.Stage.INITIALIZED,
      )
    }
  }
}
