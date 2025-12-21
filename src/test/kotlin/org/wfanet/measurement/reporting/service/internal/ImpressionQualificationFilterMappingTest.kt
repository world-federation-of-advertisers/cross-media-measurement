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
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig

@RunWith(JUnit4::class)
class ImpressionQualificationFilterMappingTest {
  @Test
  fun `processing valid config returns instance`() {
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
      TestEvent.getDescriptor(),
    )
  }

  @Test
  fun `processing config fails when missing external id`() {
    val amiIqf = impressionQualificationFilter {
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

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message)
      .contains("Invalid external impression qualification filter resource ID")
  }

  @Test
  fun `processing config fails when external id invalid`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ABC"
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

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message)
      .contains("Invalid external impression qualification filter resource ID")
  }

  @Test
  fun `processing config fails when there are duplicate external IDs`() {
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

    val amiIqf2 = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
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
                    boolValue = false
                  }
              }
          }
      }
    }

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
      impressionQualificationFilters += amiIqf2
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message)
      .contains("There are duplicate external ids of impressionQualificationFilters")
  }

  @Test
  fun `processing config fails when missing id`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
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

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Impression qualification filter ID must be positive")
  }

  @Test
  fun `processing config fails when there are duplicate IDs`() {
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

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
      impressionQualificationFilters += mrcIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message)
      .contains("There are duplicate internal ids of impressionQualificationFilters")
  }

  @Test
  fun `processing config fails when spec filter value is invalid`() {
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
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Invalid impression qualification filter spec")
  }

  @Test
  fun `processing config fails when spec filter field is not IQF field`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters +=
          ImpressionQualificationFilterConfigKt.eventFilter {
            terms +=
              ImpressionQualificationFilterConfigKt.eventTemplateField {
                path = "person.age_group"
                value =
                  ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                    enumValue = "YEARS_18_TO_34"
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
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Invalid impression qualification filter spec")
  }

  @Test
  fun `processing config fails when spec filter field doesn't exist`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters +=
          ImpressionQualificationFilterConfigKt.eventFilter {
            terms +=
              ImpressionQualificationFilterConfigKt.eventTemplateField {
                path = "person.age_group_non"
                value =
                  ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                    enumValue = "YEARS_18_TO_34"
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
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Invalid impression qualification filter spec")
  }

  @Test
  fun `processing config fails when spec media type missing`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec {
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

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Invalid impression qualification filter spec")
  }

  @Test
  fun `processing config fails when spec media type duplicated`() {
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

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Duplicate MediaType")
  }

  @Test
  fun `processing config fails when spec missing filters`() {
    val amiIqf = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
    }

    val impressionQualificationFilterConfig = impressionQualificationFilterConfig {
      impressionQualificationFilters += amiIqf
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        ImpressionQualificationFilterMapping(
          impressionQualificationFilterConfig,
          TestEvent.getDescriptor(),
        )
      }

    assertThat(exception.message).contains("Invalid impression qualification filter spec")
  }
}
