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

package org.wfanet.measurement.api.v2alpha

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

private const val PACKAGE_NAME = "org.wfanet.measurement.api.v2alpha.eventtemplates"

@RunWith(JUnit4::class)
class EventTemplateTypeRegistryTest {
  @Test
  fun `loadTemplate() contains correct fields`() {
    val typeRegistry = EventTemplateTypeRegistry(PACKAGE_NAME)

    assertThat(typeRegistry.getTemplates().keys)
      .containsExactly("TestVideoTemplate", "TestBannerTemplate")
    assertThat(typeRegistry.getTemplates()["TestVideoTemplate"]?.fields?.map { c -> c.toString() })
      .containsExactly(
        "org.wfa.measurement.api.v2alpha.eventtemplates.TestVideoTemplate.age",
        "org.wfa.measurement.api.v2alpha.eventtemplates.TestVideoTemplate.duration"
      )
  }

  @Test
  fun `loadTemplate() contains correct custom options`() {
    val typeRegistry = EventTemplateTypeRegistry(PACKAGE_NAME)

    assertThat(
      typeRegistry.getTemplates()["TestVideoTemplate"]
        ?.options
        .toString()
        .contains("display_name: \"Video Ad\"")
    )
    assertThat(
      typeRegistry.getTemplates()["TestBannerTemplate"]
        ?.options
        .toString()
        .contains("display_name: \"Banner Ad\"")
    )
  }
}
