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
  fun `loadTemplate() template contains correct fields`() {
    val typeRegistry = EventTemplateTypeRegistry(PACKAGE_NAME)

    assertThat(
        typeRegistry.getDescriptorForType(
            "org.wfa.measurement.api.v2alpha.eventtemplates.TestBannerTemplate"
          )
          .fields
      )
      .isEmpty()
    assertThat(
        typeRegistry.getDescriptorForType(
            "org.wfa.measurement.api.v2alpha.eventtemplates.TestVideoTemplate"
          )
          .fields
          ?.map { field -> field.name }
      )
      .containsExactly("age", "duration")
  }

  @Test
  fun `loadTemplate() template contains correct custom options`() {
    val typeRegistry = EventTemplateTypeRegistry(PACKAGE_NAME)

    assertThat(
        typeRegistry
          .getDescriptorForType("org.wfa.measurement.api.v2alpha.eventtemplates.TestBannerTemplate")
          .options
          .getExtension(EventAnnotations.eventTemplate)
          .displayName
      )
      .isEqualTo("Banner Ad")
    assertThat(
        typeRegistry
          .getDescriptorForType("org.wfa.measurement.api.v2alpha.eventtemplates.TestBannerTemplate")
          .options
          .getExtension(EventAnnotations.eventTemplate)
          .description
      )
      .isEqualTo("A simple Event Template for a banner ad.")
    assertThat(
        typeRegistry
          .getDescriptorForType("org.wfa.measurement.api.v2alpha.eventtemplates.TestVideoTemplate")
          .options
          .getExtension(EventAnnotations.eventTemplate)
          .displayName
      )
      .isEqualTo("Video Ad")
    assertThat(
        typeRegistry
          .getDescriptorForType("org.wfa.measurement.api.v2alpha.eventtemplates.TestVideoTemplate")
          .options
          .getExtension(EventAnnotations.eventTemplate)
          .description
      )
      .isEqualTo("A simple Event Template for a video ad.")
  }

  @Test
  fun `loadTemplate() loads subpackages`() {
    val typeRegistry = EventTemplateTypeRegistry("org.wfanet.measurement.api.v2alpha")

    assertThat(
        typeRegistry.getDescriptorForType(
          "org.wfa.measurement.api.v2alpha.eventtemplates.TestBannerTemplate"
        )
      )
      .isNotNull()
    assertThat(
        typeRegistry.getDescriptorForType(
          "org.wfa.measurement.api.v2alpha.eventtemplates.TestVideoTemplate"
        )
      )
      .isNotNull()
  }
}
