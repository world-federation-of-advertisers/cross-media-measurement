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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.protocol.TrusTee

@RunWith(JUnit4::class)
class TrusTeeProtocolEnumStagesTest {
  @Test
  fun `validInitialStage verifies initial stages correctly`() {
    assertThat(TrusTeeProtocol.EnumStages.validInitialStage(TrusTee.Stage.INITIALIZED)).isTrue()
    assertThat(TrusTeeProtocol.EnumStages.validInitialStage(TrusTee.Stage.WAIT_TO_START)).isFalse()
    assertThat(TrusTeeProtocol.EnumStages.validInitialStage(TrusTee.Stage.COMPUTING)).isFalse()
    assertThat(TrusTeeProtocol.EnumStages.validInitialStage(TrusTee.Stage.COMPLETE)).isFalse()
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in TrusTee.Stage.values()) {
      if (stage == TrusTee.Stage.UNRECOGNIZED) {
        assertFails { TrusTeeProtocol.EnumStages.enumToLong(stage) }
      } else {
        assertThat(
            TrusTeeProtocol.EnumStages.longToEnum(TrusTeeProtocol.EnumStages.enumToLong(stage))
          )
          .isEqualTo(stage)
      }
    }
  }

  @Test
  fun `longToEnum returned UNRECOGNIZED for invalid numbers`() {
    assertThat(TrusTeeProtocol.EnumStages.longToEnum(-1)).isEqualTo(TrusTee.Stage.UNRECOGNIZED)
    assertThat(TrusTeeProtocol.EnumStages.longToEnum(1000)).isEqualTo(TrusTee.Stage.UNRECOGNIZED)
  }

  @Test
  fun `validTransition verifies the transition correctly`() {
    assertThat(
        TrusTeeProtocol.EnumStages.validTransition(
          TrusTee.Stage.INITIALIZED,
          TrusTee.Stage.WAIT_TO_START,
        )
      )
      .isTrue()

    assertThat(
        TrusTeeProtocol.EnumStages.validTransition(
          TrusTee.Stage.WAIT_TO_START,
          TrusTee.Stage.COMPUTING,
        )
      )
      .isTrue()

    assertThat(
        TrusTeeProtocol.EnumStages.validTransition(TrusTee.Stage.COMPUTING, TrusTee.Stage.COMPLETE)
      )
      .isTrue()

    assertThat(
        TrusTeeProtocol.EnumStages.validTransition(
          TrusTee.Stage.WAIT_TO_START,
          TrusTee.Stage.COMPLETE,
        )
      )
      .isFalse()

    assertThat(
        TrusTeeProtocol.EnumStages.validTransition(
          TrusTee.Stage.INITIALIZED,
          TrusTee.Stage.COMPLETE,
        )
      )
      .isFalse()

    assertThat(
        TrusTeeProtocol.EnumStages.validTransition(
          TrusTee.Stage.UNRECOGNIZED,
          TrusTee.Stage.INITIALIZED,
        )
      )
      .isFalse()
  }
}
