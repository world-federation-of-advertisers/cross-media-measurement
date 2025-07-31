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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.event_templates.testing.MissingFieldAnnotationEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.MissingTemplateAnnotationEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.UnsupportedFieldTypeEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.UnsupportedRepeatedFieldEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.UnsupportedReportingFeatureEvent

@RunWith(JUnit4::class)
class EventDescriptorTest {
  @Test
  fun `EventDescriptor instantiation succeeds`() {
    val extensionRegistry =
      ExtensionRegistry.newInstance()
        .apply {
          add(EventAnnotationsProto.eventTemplate)
          add(EventAnnotationsProto.templateField)
        }
        .unmodifiable

    val typeRegistry =
      TypeRegistry.newBuilder()
        .add(
          listOf(
            TestEvent.parseFrom(TestEvent.getDefaultInstance().toByteString(), extensionRegistry)
              .descriptorForType
          )
        )
        .build()
    val eventDescriptor = EventDescriptor(typeRegistry.find(TestEvent.getDescriptor().fullName))

    assertThat(eventDescriptor.eventTemplateFieldsMap).hasSize(6)
    assertThat(eventDescriptor.eventTemplateFieldsMap)
      .containsExactly(
        "person.gender",
        EventDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.MEDIA_TYPE_UNSPECIFIED,
          isPopulationAttribute = true,
          supportedReportingFeatures =
            EventDescriptor.SupportedReportingFeatures(
              groupable = true,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.ENUM,
          messageTypeFullName = "",
          enumValuesMap = mapOf("GENDER_UNSPECIFIED" to 0, "MALE" to 1, "FEMALE" to 2),
        ),
        "person.age_group",
        EventDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.MEDIA_TYPE_UNSPECIFIED,
          isPopulationAttribute = true,
          supportedReportingFeatures =
            EventDescriptor.SupportedReportingFeatures(
              groupable = true,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.ENUM,
          messageTypeFullName = "",
          enumValuesMap =
            mapOf(
              "AGE_GROUP_UNSPECIFIED" to 0,
              "YEARS_18_TO_34" to 1,
              "YEARS_35_TO_54" to 2,
              "YEARS_55_PLUS" to 3,
            ),
        ),
        "person.social_grade_group",
        EventDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.MEDIA_TYPE_UNSPECIFIED,
          isPopulationAttribute = true,
          supportedReportingFeatures =
            EventDescriptor.SupportedReportingFeatures(
              groupable = true,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.ENUM,
          messageTypeFullName = "",
          enumValuesMap = mapOf("SOCIAL_GRADE_GROUP_UNSPECIFIED" to 0, "A_B_C1" to 1, "C2_D_E" to 2),
        ),
        "video_ad.length",
        EventDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.VIDEO,
          isPopulationAttribute = false,
          supportedReportingFeatures =
            EventDescriptor.SupportedReportingFeatures(
              groupable = false,
              filterable = true,
              impressionQualification = false,
            ),
          type = Descriptors.FieldDescriptor.Type.MESSAGE,
          messageTypeFullName = "google.protobuf.Duration",
          enumValuesMap = mapOf(),
        ),
        "video_ad.viewed_fraction",
        EventDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.VIDEO,
          isPopulationAttribute = false,
          supportedReportingFeatures =
            EventDescriptor.SupportedReportingFeatures(
              groupable = false,
              filterable = false,
              impressionQualification = true,
            ),
          type = Descriptors.FieldDescriptor.Type.DOUBLE,
          messageTypeFullName = "",
          enumValuesMap = mapOf(),
        ),
        "banner_ad.viewable",
        EventDescriptor.EventTemplateFieldInfo(
          mediaType = MediaType.DISPLAY,
          isPopulationAttribute = false,
          supportedReportingFeatures =
            EventDescriptor.SupportedReportingFeatures(
              groupable = false,
              filterable = false,
              impressionQualification = true,
            ),
          type = Descriptors.FieldDescriptor.Type.BOOL,
          messageTypeFullName = "",
          enumValuesMap = mapOf(),
        ),
      )
  }

  @Test
  fun `EventDescriptor instantiation fails with exception when EventTemplate annotation missing`() {
    assertFailsWith<IllegalArgumentException> {
      EventDescriptor(MissingTemplateAnnotationEvent.getDescriptor())
    }
  }

  @Test
  fun `EventDescriptor instantiation fails with exception when field annotation missing`() {
    assertFailsWith<IllegalArgumentException> {
      EventDescriptor(MissingFieldAnnotationEvent.getDescriptor())
    }
  }

  @Test
  fun `EventDescriptor instantiation fails with exception when reporting feature invalid`() {
    assertFailsWith<IllegalArgumentException> {
      EventDescriptor(UnsupportedReportingFeatureEvent.getDescriptor())
    }
  }

  @Test
  fun `EventDescriptor instantiation fails with exception when field repeated`() {
    assertFailsWith<IllegalArgumentException> {
      EventDescriptor(UnsupportedRepeatedFieldEvent.getDescriptor())
    }
  }

  @Test
  fun `EventDescriptor instantiation fails with exception when field unsupported type`() {
    assertFailsWith<IllegalArgumentException> {
      EventDescriptor(UnsupportedFieldTypeEvent.getDescriptor())
    }
  }
}
