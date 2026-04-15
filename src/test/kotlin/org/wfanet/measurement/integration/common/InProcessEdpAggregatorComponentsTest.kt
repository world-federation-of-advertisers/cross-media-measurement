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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class InProcessEdpAggregatorComponentsTest {

  @Test
  fun `requireSingleModelLineName returns only configured model line`() {
    val modelLineName = "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA"

    assertThat(InProcessEdpAggregatorComponents.requireSingleModelLineName(setOf(modelLineName)))
      .isEqualTo(modelLineName)
  }

  @Test
  fun `requireSingleModelLineName throws when multiple model lines are configured`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        InProcessEdpAggregatorComponents.requireSingleModelLineName(
          setOf(
            "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineA",
            "modelProviders/provider1/modelSuites/suite1/modelLines/modelLineB",
          )
        )
      }

    assertThat(exception).hasMessageThat().contains("supports exactly one model line")
  }
}
