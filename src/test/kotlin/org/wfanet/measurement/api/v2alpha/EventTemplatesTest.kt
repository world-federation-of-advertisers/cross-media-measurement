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

private const val PROTO_PACKAGE = "wfa.measurement.api.v2alpha.event_templates.testing"
private const val BANNER_TEMPLATE_NAME = "$PROTO_PACKAGE.TestBannerTemplate"
private const val VIDEO_TEMPLATE_NAME = "$PROTO_PACKAGE.TestVideoTemplate"

@RunWith(JUnit4::class)
class EventTemplatesTest {

  @Test
  fun `initializeTypeRegistry() contains correct templates`() {
    assertThat(templateFieldList(BANNER_TEMPLATE_NAME)).containsExactly("Gender")
    assertThat(templateFieldList(VIDEO_TEMPLATE_NAME)).containsExactly("Age Range", "View Duration")
  }

  @Test
  fun `loadTemplate() template contains correct custom options`() {
    assertThat(getEventTemplateForType(BANNER_TEMPLATE_NAME)?.displayName).isEqualTo("Banner Ad")
    assertThat(getEventTemplateForType(BANNER_TEMPLATE_NAME)?.description)
      .isEqualTo("A simple Event Template for a banner ad.")
    assertThat(getEventTemplateForType(VIDEO_TEMPLATE_NAME)?.displayName).isEqualTo("Video Ad")
    assertThat(getEventTemplateForType(VIDEO_TEMPLATE_NAME)?.description)
      .isEqualTo("A simple Event Template for a video ad.")
  }

  private fun getEventTemplateForType(messageName: String) =
    EventTemplates.getEventTemplateForType(messageName)

  private fun templateFieldList(messageName: String): List<String> =
    checkNotNull(
      getEventTemplateForType(messageName)?.eventFields?.map { field -> field.displayName }
    )
}
