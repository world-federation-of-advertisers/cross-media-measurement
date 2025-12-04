/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.EventDescriptor
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig

@RunWith(JUnit4::class)
class ImpressionQualificationFilterMappingTest {
  @Test
  fun `processing valid config is successful`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters +=
          ImpressionQualificationFilterConfigKt.eventFilter {
            terms +=
              ImpressionQualificationFilterConfigKt.eventTemplateField {
                path = "banner_ad.viewable"
                value =
                  ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                    boolValue = false
                  }
              }
          }
      }
    }

    val mrcIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      impressionQualificationFilterId = 2
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters +=
          ImpressionQualificationFilterConfigKt.eventFilter {
            terms +=
              ImpressionQualificationFilterConfigKt.eventTemplateField {
                path = "banner_ad.viewable"
                value =
                  ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                    boolValue = true
                  }
              }
          }
      }
    }

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
      impressionQualificationFilters += mrcIqf
    }

    ImpressionQualificationFilterMapping(
      impressionQualificationFilterConfig,
      EventDescriptor(TestEvent.getDescriptor()),
    )

    assert(true)
  }

  @Test
  fun `processing config fails when spec is invalid`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters +=
          ImpressionQualificationFilterConfigKt.eventFilter {
            terms +=
              ImpressionQualificationFilterConfigKt.eventTemplateField {
                path = "banner_ad.viewable"
                value =
                  ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                    floatValue = 1.0f
                  }
              }
          }
      }
    }

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          EventDescriptor(TestEvent.getDescriptor()),
        )
      }

    assertThat(exception.message).contains("Invalid impression qualification filter spec")
  }
}
