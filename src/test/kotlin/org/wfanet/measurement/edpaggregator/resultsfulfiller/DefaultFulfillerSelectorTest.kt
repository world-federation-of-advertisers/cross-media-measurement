/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.ImpressionCapMode

@RunWith(JUnit4::class)
class DefaultFulfillerSelectorTest {

  @Test
  fun `DYNAMIC keeps the vector uncapped whatever the configured cap`() {
    for (configured in listOf(null, -1, 1, 5, 127)) {
      assertThat(frequencyVectorCap(ImpressionCapMode.DYNAMIC, configured)).isEqualTo(UNCAPPED)
    }
  }

  @Test
  fun `other modes keep the configured cap`() {
    for (configured in listOf(null, -1, 1, 5, 127)) {
      assertThat(frequencyVectorCap(ImpressionCapMode.LEGACY_CAP_MODE, configured))
        .isEqualTo(configured)
    }
  }
}
