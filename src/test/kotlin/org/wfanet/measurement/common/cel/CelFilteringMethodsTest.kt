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

package org.wfanet.measurement.common.cel

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec

@RunWith(JUnit4::class)
class CelFilteringMethodsTest {
  @Test
  fun `filterList with Message-based env returns items matching a string-equality filter`() {
    // Regression for the buildCelEnvironment(Message) overload: it must preserve the reflectType
    // binding that ProtoTypeRegistry.registerMessage installs, so that filterList can convert
    // runtime MetricCalculationSpec values to CEL native values without falling back to the
    // DynamicMessage path. If the overload regresses to a descriptor-only registration the
    // expected match below either returns the wrong set or throws on conversion.
    val env = buildCelEnvironment(MetricCalculationSpec.getDefaultInstance())
    val items =
      listOf(
        metricCalculationSpec { displayName = "wanted" },
        metricCalculationSpec { displayName = "ignored" },
      )

    val matched = filterList(env, items, "display_name == 'wanted'")

    assertThat(matched).containsExactly(items[0])
  }

  @Test
  fun `filterList with Descriptor-based env returns items matching a string-equality filter`() {
    // Pin the Descriptor overload at the same call site so future changes to either branch are
    // exercised symmetrically.
    val env = buildCelEnvironment(MetricCalculationSpec.getDescriptor())
    val items =
      listOf(
        metricCalculationSpec { displayName = "wanted" },
        metricCalculationSpec { displayName = "ignored" },
      )

    val matched = filterList(env, items, "display_name == 'wanted'")

    assertThat(matched).containsExactly(items[0])
  }
}
