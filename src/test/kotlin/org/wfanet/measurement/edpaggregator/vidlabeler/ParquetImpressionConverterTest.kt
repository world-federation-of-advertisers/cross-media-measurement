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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.DigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt.inputFileEntityKeys
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue

@RunWith(JUnit4::class)
class ParquetImpressionConverterTest {
  private val eventDescriptor = TestEvent.getDescriptor()

  private val config: VidLabelerParams.ModelLineConfig =
    VidLabelerParamsKt.modelLineConfig {
      labelerInputFieldMapping.put("event_id.id", "eid")
      labelerInputFieldMapping.put("timestamp_usec", "ts")
      eventTemplateFieldMapping.put("person.gender", "gender")
      eventTemplateFieldMapping.put("person.age_group", "age")
    }

  private val entityKeysByInputBlobUri =
    mapOf(
      INPUT_BLOB_URI to
        inputFileEntityKeys {
          eventGroupReferenceId = EVENT_GROUP
          entityKeys += entityKey {
            entityType = "creative"
            entityId = "c-1"
          }
          entityKeys += entityKey {
            entityType = "placement"
            entityId = "p-9"
          }
        }
    )

  private fun digestedEvent(row: Map<String, ParquetValue>): ParquetDigestedEvent =
    DigestedEvent(row, EventIdDigest(0L, 0))

  @Test
  fun `convert projects labeler input, event, event group, and entity keys`() {
    val converter = ParquetImpressionConverter(eventDescriptor, entityKeysByInputBlobUri)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "gender" to parquetValue { stringValue = "MALE" },
        "age" to parquetValue { stringValue = "YEARS_18_TO_34" },
      )

    val converted = converter.convert(digestedEvent(row), config, INPUT_BLOB_URI)

    assertThat(converted).isNotNull()
    assertThat(converted!!.labelerInput.eventId.id).isEqualTo("event-1")
    assertThat(converted.eventTimeMicros).isEqualTo(1_700_000_000_000_000L)
    assertThat(converted.eventGroupReferenceId).isEqualTo(EVENT_GROUP)

    val event = converted.event.unpack(TestEvent::class.java)
    assertThat(event.person.gender).isEqualTo(Person.Gender.MALE)
    assertThat(event.person.ageGroup).isEqualTo(Person.AgeGroup.YEARS_18_TO_34)

    assertThat(converted.entityKeys.map { it.entityType to it.entityId })
      .containsExactly("creative" to "c-1", "placement" to "p-9")
  }

  @Test
  fun `convert with empty event_template_field_mapping yields an empty event of the type`() {
    val emptyMappingConfig =
      VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping.put("event_id.id", "eid")
        labelerInputFieldMapping.put("timestamp_usec", "ts")
      }
    val converter = ParquetImpressionConverter(eventDescriptor, entityKeysByInputBlobUri)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-2" },
        "ts" to parquetValue { int64Value = 5L },
      )

    val converted = converter.convert(digestedEvent(row), emptyMappingConfig, INPUT_BLOB_URI)

    assertThat(converted).isNotNull()
    val event = converted!!.event.unpack(TestEvent::class.java)
    assertThat(event).isEqualTo(TestEvent.getDefaultInstance())
    assertThat(converted.event.typeUrl)
      .isEqualTo("type.googleapis.com/" + TestEvent.getDescriptor().fullName)
  }

  @Test
  fun `convert throws when there are no entity keys for the event group`() {
    val converter = ParquetImpressionConverter(eventDescriptor, entityKeysByInputBlobUri)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-3" },
        "ts" to parquetValue { int64Value = 7L },
        "gender" to parquetValue { stringValue = "FEMALE" },
      )

    val exception =
      assertFailsWith<IllegalArgumentException> {
        converter.convert(digestedEvent(row), config, "unknown-blob-uri")
      }
    assertThat(exception).hasMessageThat().contains("no entity keys for input file")
    assertThat(exception).hasMessageThat().contains("unknown-blob-uri")
  }

  companion object {
    private const val EVENT_GROUP = "event-group-ref-1"
    private const val INPUT_BLOB_URI = "file:///raw/file-1.parquet"
  }
}
