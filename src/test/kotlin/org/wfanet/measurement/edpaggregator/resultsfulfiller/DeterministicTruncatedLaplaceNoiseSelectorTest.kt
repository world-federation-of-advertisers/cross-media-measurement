/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.dataprovider.RequisitionRefusalException
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DeterministicTruncatedLaplaceNoiseSelectorTest {

  @Test
  fun `selectNoiseMechanism returns DETERMINISTIC_TRUNCATED_LAPLACE when offered`() {
    val selected =
      DeterministicTruncatedLaplaceNoiseSelector()
        .selectNoiseMechanism(
          listOf(NoiseMechanism.CONTINUOUS_GAUSSIAN, NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
        )

    assertThat(selected).isEqualTo(DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
  }

  @Test
  fun `selectNoiseMechanism refuses when the mechanism is not offered`() {
    assertFailsWith<RequisitionRefusalException> {
      DeterministicTruncatedLaplaceNoiseSelector()
        .selectNoiseMechanism(listOf(NoiseMechanism.CONTINUOUS_GAUSSIAN))
    }
  }

  @Test
  fun `selectNoiseMechanism refuses an empty option list`() {
    assertFailsWith<RequisitionRefusalException> {
      DeterministicTruncatedLaplaceNoiseSelector().selectNoiseMechanism(emptyList())
    }
  }
}
