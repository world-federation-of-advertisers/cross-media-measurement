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

private const val PACKAGE_NAME = "org.wfanet.measurement.api.v2alpha.testing.event_templates"
private const val TEMPLATE_PREFIX = "org.wfa.measurement.api.v2alpha.testing.event_templates"
private const val BANNER_TEMPLATE_NAME = "$TEMPLATE_PREFIX.TestBannerTemplate"
private const val VIDEO_TEMPLATE_NAME = "$TEMPLATE_PREFIX.TestVideoTemplate"

@RunWith(JUnit4::class)
class EventTemplateTypeRegistryTest {
  @Test
  fun `loadTemplate() template contains correct fields`() {
    val typeRegistry = EventTemplateTypeRegistry.createRegistryForPackagePrefix(PACKAGE_NAME)

    assertThat(typeRegistry.getDescriptorForType(BANNER_TEMPLATE_NAME).fields).isEmpty()
    assertThat(
        typeRegistry.getDescriptorForType(VIDEO_TEMPLATE_NAME).fields?.map { field -> field.name }
      )
      .containsExactly("age", "duration")
  }

  @Test
  fun `loadTemplate() template contains correct custom options`() {
    val typeRegistry = EventTemplateTypeRegistry.createRegistryForPackagePrefix(PACKAGE_NAME)

    assertThat(typeRegistry.getEventTemplateForType(BANNER_TEMPLATE_NAME).displayName)
      .isEqualTo("Banner Ad")
    assertThat(typeRegistry.getEventTemplateForType(BANNER_TEMPLATE_NAME).description)
      .isEqualTo("A simple Event Template for a banner ad.")
    assertThat(typeRegistry.getEventTemplateForType(VIDEO_TEMPLATE_NAME).displayName)
      .isEqualTo("Video Ad")
    assertThat(typeRegistry.getEventTemplateForType(VIDEO_TEMPLATE_NAME).description)
      .isEqualTo("A simple Event Template for a video ad.")
  }

  @Test
  fun `loadTemplate() loads subpackages`() {
    val typeRegistry =
      EventTemplateTypeRegistry.createRegistryForPackagePrefix("org.wfanet.measurement.api.v2alpha")

    assertThat(typeRegistry.getDescriptorForType(BANNER_TEMPLATE_NAME)).isNotNull()
    assertThat(typeRegistry.getDescriptorForType(VIDEO_TEMPLATE_NAME)).isNotNull()
  }

  private fun EventTemplateTypeRegistry.getEventTemplateForType(messageType: String) =
    getDescriptorForType(messageType).options.getExtension(EventAnnotations.eventTemplate)
}
