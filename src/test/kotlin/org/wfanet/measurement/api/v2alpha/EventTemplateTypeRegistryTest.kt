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

private const val PACKAGE_NAME = "org.wfanet.measurement.api.v2alpha.event_templates.testing"
private const val CLASS_PATH = "wfa.measurement.api.v2alpha.event_templates.testing"
private const val BANNER_TEMPLATE_NAME = "TestBannerTemplate"
private const val VIDEO_TEMPLATE_NAME = "TestVideoTemplate"

@RunWith(JUnit4::class)
class EventTemplateTypeRegistryTest {

  @Test
  fun `loadTemplate() template contains correct fields`() {
    EventTemplateTypeRegistry.createRegistryForPackagePrefix(PACKAGE_NAME, CLASS_PATH)

    assertThat(templateFieldList(BANNER_TEMPLATE_NAME)).containsExactly("gender")
    assertThat(templateFieldList(VIDEO_TEMPLATE_NAME)).containsExactly("age", "duration")
  }

  @Test
  fun `loadTemplate() template contains correct custom options`() {
    EventTemplateTypeRegistry.createRegistryForPackagePrefix(PACKAGE_NAME, CLASS_PATH)

    assertThat(getEventTemplateForType(BANNER_TEMPLATE_NAME)?.displayName).isEqualTo("Banner Ad")
    assertThat(getEventTemplateForType(BANNER_TEMPLATE_NAME)?.description)
      .isEqualTo("A simple Event Template for a banner ad.")
    assertThat(getEventTemplateForType(VIDEO_TEMPLATE_NAME)?.displayName).isEqualTo("Video Ad")
    assertThat(getEventTemplateForType(VIDEO_TEMPLATE_NAME)?.description)
      .isEqualTo("A simple Event Template for a video ad.")
  }

  @Test
  fun `loadTemplate() loads subpackages`() {
    EventTemplateTypeRegistry.createRegistryForPackagePrefix(
      "org.wfanet.measurement.api.v2alpha",
      CLASS_PATH
    )

    assertThat(EventTemplateTypeRegistry.getDescriptorForType(BANNER_TEMPLATE_NAME)).isNotNull()
    assertThat(EventTemplateTypeRegistry.getDescriptorForType(VIDEO_TEMPLATE_NAME)).isNotNull()
  }

  private fun getEventTemplateForType(messageName: String) =
    EventTemplateTypeRegistry.getDescriptorForType(messageName)
      ?.options
      ?.getExtension(EventAnnotations.eventTemplate)

  private fun templateFieldList(templateName: String): List<String> =
    checkNotNull(
      EventTemplateTypeRegistry.getDescriptorForType(templateName)?.fields?.map { field ->
        field.name
      }
    )
}
