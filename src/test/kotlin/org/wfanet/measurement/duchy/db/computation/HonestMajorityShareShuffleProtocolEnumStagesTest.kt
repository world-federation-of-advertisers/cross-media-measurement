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
import org.wfanet.measurement.internal.duchy.protocol.HonestMajorityShareShuffle

@RunWith(JUnit4::class)
class HonestMajorityShareShuffleProtocolEnumStagesTest {
  @Test
  fun `verify initial stage`() {
    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validInitialStage(
        HonestMajorityShareShuffle.Stage.INITIALIZED
      )
    }
    assertFalse {
      HonestMajorityShareShuffleProtocol.EnumStages.validInitialStage(
        HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT
      )
    }
    assertFalse {
      HonestMajorityShareShuffleProtocol.EnumStages.validInitialStage(
        HonestMajorityShareShuffle.Stage.SETUP_PHASE
      )
    }
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in HonestMajorityShareShuffle.Stage.values()) {
      if (stage == HonestMajorityShareShuffle.Stage.UNRECOGNIZED) {
        assertFails { HonestMajorityShareShuffleProtocol.EnumStages.enumToLong(stage) }
      } else {
        assertEquals(
          stage,
          HonestMajorityShareShuffleProtocol.EnumStages.longToEnum(
            HonestMajorityShareShuffleProtocol.EnumStages.enumToLong(stage)
          ),
          "enumToLong and longToEnum were not inverses for $stage",
        )
      }
    }
  }

  @Test
  fun `longToEnum with invalid numbers`() {
    assertEquals(
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED,
      HonestMajorityShareShuffleProtocol.EnumStages.longToEnum(-1),
    )
    assertEquals(
      HonestMajorityShareShuffle.Stage.UNRECOGNIZED,
      HonestMajorityShareShuffleProtocol.EnumStages.longToEnum(1000),
    )
  }

  @Test
  fun `verify transistions`() {
    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.INITIALIZED,
        HonestMajorityShareShuffle.Stage.WAIT_TO_START,
      )
    }

    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.INITIALIZED,
        HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
      )
    }

    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.WAIT_TO_START,
        HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      )
    }

    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_ONE,
        HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      )
    }
    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.SETUP_PHASE,
        HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      )
    }

    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.WAIT_ON_SHUFFLE_INPUT_PHASE_TWO,
        HonestMajorityShareShuffle.Stage.SHUFFLE_PHASE,
      )
    }

    assertTrue {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT,
        HonestMajorityShareShuffle.Stage.AGGREGATION_PHASE,
      )
    }

    assertFalse {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.WAIT_ON_AGGREGATION_INPUT,
        HonestMajorityShareShuffle.Stage.COMPLETE,
      )
    }

    assertFalse {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.SETUP_PHASE,
        HonestMajorityShareShuffle.Stage.COMPLETE,
      )
    }

    assertFalse {
      HonestMajorityShareShuffleProtocol.EnumStages.validTransition(
        HonestMajorityShareShuffle.Stage.UNRECOGNIZED,
        HonestMajorityShareShuffle.Stage.SETUP_PHASE,
      )
    }
  }
}
