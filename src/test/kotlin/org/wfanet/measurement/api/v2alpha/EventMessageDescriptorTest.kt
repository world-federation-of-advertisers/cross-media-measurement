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

package org.wfanet.measurement.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Descriptors
import com.google.protobuf.TypeRegistry
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.MissingFieldAnnotationEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.MissingTemplateAnnotationEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.UnsupportedFieldTypeEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.UnsupportedRepeatedFieldEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.UnsupportedReportingFeatureEvent

@RunWith(JUnit4::class)
class EventMessageDescriptorTest {
  @Test
  fun `EventMessageDescriptor instantiation succeeds`() {
    val typeRegistry = TypeRegistry.newBuilder().add(listOf(TestEvent.getDescriptor())).build()
    val eventMessageDescriptor =
      EventMessageDescriptor(typeRegistry.find(TestEvent.getDescriptor().fullName))

    assertThat(eventMessageDescriptor.eventTemplateFieldsByPath).hasSize(6)
    assertThat(eventMessageDescriptor.eventTemplateFieldsByPath)
      .containsExactly(
        "person.gender",
        EventMessageDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.MEDIA_TYPE_UNSPECIFIED,
          isPopulationAttribute = true,
          supportedReportingFeatures =
            EventMessageDescriptor.SupportedReportingFeatures(
              groupable = true,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.ENUM,
          enumType = Person.Gender.getDescriptor(),
        ),
        "person.age_group",
        EventMessageDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.MEDIA_TYPE_UNSPECIFIED,
          isPopulationAttribute = true,
          supportedReportingFeatures =
            EventMessageDescriptor.SupportedReportingFeatures(
              groupable = true,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.ENUM,
          enumType = Person.AgeGroup.getDescriptor(),
        ),
        "person.social_grade_group",
        EventMessageDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.MEDIA_TYPE_UNSPECIFIED,
          isPopulationAttribute = true,
          supportedReportingFeatures =
            EventMessageDescriptor.SupportedReportingFeatures(
              groupable = true,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.ENUM,
          enumType = Person.SocialGradeGroup.getDescriptor(),
        ),
        "video_ad.length",
        EventMessageDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.VIDEO,
          isPopulationAttribute = false,
          supportedReportingFeatures =
            EventMessageDescriptor.SupportedReportingFeatures(
              groupable = false,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.MESSAGE,
          enumType = null,
        ),
        "video_ad.viewed_fraction",
        EventMessageDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.VIDEO,
          isPopulationAttribute = false,
          supportedReportingFeatures =
            EventMessageDescriptor.SupportedReportingFeatures(
              groupable = false,
              filterable = false,
              impressionQualification = true,
            ),
          type = Descriptors.FieldDescriptor.Type.DOUBLE,
          enumType = null,
        ),
        "banner_ad.viewable",
        EventMessageDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.DISPLAY,
          isPopulationAttribute = false,
          supportedReportingFeatures =
            EventMessageDescriptor.SupportedReportingFeatures(
              groupable = false,
              filterable = false,
              impressionQualification = true,
            ),
          type = Descriptors.FieldDescriptor.Type.BOOL,
          enumType = null,
        ),
      )
  }

  @Test
  fun `EventMessageDescriptor instantiation fails with exception when EventTemplate annotation missing`() {
    assertFailsWith<IllegalArgumentException> {
      EventMessageDescriptor(MissingTemplateAnnotationEvent.getDescriptor())
    }
  }

  @Test
  fun `EventMessageDescriptor instantiation fails with exception when field annotation missing`() {
    assertFailsWith<IllegalArgumentException> {
      EventMessageDescriptor(MissingFieldAnnotationEvent.getDescriptor())
    }
  }

  @Test
  fun `EventMessageDescriptor instantiation fails with exception when reporting feature invalid`() {
    assertFailsWith<IllegalArgumentException> {
      EventMessageDescriptor(UnsupportedReportingFeatureEvent.getDescriptor())
    }
  }

  @Test
  fun `EventMessageDescriptor instantiation fails with exception when field repeated`() {
    assertFailsWith<IllegalArgumentException> {
      EventMessageDescriptor(UnsupportedRepeatedFieldEvent.getDescriptor())
    }
  }

  @Test
  fun `EventMessageDescriptor instantiation fails with exception when field unsupported type`() {
    assertFailsWith<IllegalArgumentException> {
      EventMessageDescriptor(UnsupportedFieldTypeEvent.getDescriptor())
    }
  }
}
