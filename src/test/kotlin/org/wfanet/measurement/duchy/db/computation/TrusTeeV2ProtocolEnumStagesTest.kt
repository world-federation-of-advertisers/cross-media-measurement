// Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2

@RunWith(JUnit4::class)
class TrusTeeV2ProtocolEnumStagesTest {
  @Test
  fun `validInitialStage verifies initial stages correctly`() {
    assertThat(TrusTeeV2Protocol.EnumStages.validInitialStage(TrusTeeV2.Stage.INITIALIZED)).isTrue()
    assertThat(TrusTeeV2Protocol.EnumStages.validInitialStage(TrusTeeV2.Stage.WAIT_TO_START))
      .isFalse()
    assertThat(TrusTeeV2Protocol.EnumStages.validInitialStage(TrusTeeV2.Stage.COMPUTING)).isFalse()
    assertThat(TrusTeeV2Protocol.EnumStages.validInitialStage(TrusTeeV2.Stage.COMPLETE)).isFalse()
  }

  @Test
  fun `enumToLong then longToEnum results in same enum value`() {
    for (stage in TrusTeeV2.Stage.values()) {
      if (stage == TrusTeeV2.Stage.UNRECOGNIZED) {
        assertFails { TrusTeeV2Protocol.EnumStages.enumToLong(stage) }
      } else {
        assertThat(
            TrusTeeV2Protocol.EnumStages.longToEnum(TrusTeeV2Protocol.EnumStages.enumToLong(stage))
          )
          .isEqualTo(stage)
      }
    }
  }

  @Test
  fun `longToEnum returned UNRECOGNIZED for invalid numbers`() {
    assertThat(TrusTeeV2Protocol.EnumStages.longToEnum(-1)).isEqualTo(TrusTeeV2.Stage.UNRECOGNIZED)
    assertThat(TrusTeeV2Protocol.EnumStages.longToEnum(1000))
      .isEqualTo(TrusTeeV2.Stage.UNRECOGNIZED)
  }

  @Test
  fun `validTransition verifies the transition correctly`() {
    assertThat(
        TrusTeeV2Protocol.EnumStages.validTransition(
          TrusTeeV2.Stage.INITIALIZED,
          TrusTeeV2.Stage.WAIT_TO_START,
        )
      )
      .isTrue()

    assertThat(
        TrusTeeV2Protocol.EnumStages.validTransition(
          TrusTeeV2.Stage.WAIT_TO_START,
          TrusTeeV2.Stage.COMPUTING,
        )
      )
      .isTrue()

    assertThat(
        TrusTeeV2Protocol.EnumStages.validTransition(
          TrusTeeV2.Stage.COMPUTING,
          TrusTeeV2.Stage.COMPLETE,
        )
      )
      .isTrue()

    assertThat(
        TrusTeeV2Protocol.EnumStages.validTransition(
          TrusTeeV2.Stage.WAIT_TO_START,
          TrusTeeV2.Stage.COMPLETE,
        )
      )
      .isFalse()

    assertThat(
        TrusTeeV2Protocol.EnumStages.validTransition(
          TrusTeeV2.Stage.INITIALIZED,
          TrusTeeV2.Stage.COMPLETE,
        )
      )
      .isFalse()

    assertThat(
        TrusTeeV2Protocol.EnumStages.validTransition(
          TrusTeeV2.Stage.UNRECOGNIZED,
          TrusTeeV2.Stage.INITIALIZED,
        )
      )
      .isFalse()
  }
}
