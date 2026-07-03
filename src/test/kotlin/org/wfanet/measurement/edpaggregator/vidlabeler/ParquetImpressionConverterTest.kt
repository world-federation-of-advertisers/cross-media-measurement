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
import com.google.protobuf.util.Timestamps
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.DigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.v1alpha.LabelerInputFieldMapping
import org.wfanet.measurement.edpaggregator.v1alpha.ScalarColumn
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue

@RunWith(JUnit4::class)
class ParquetImpressionConverterTest {
  private val eventDescriptor = TestEvent.getDescriptor()

  private val config: VidLabelerParams.ModelLineConfig =
    VidLabelerParamsKt.modelLineConfig {
      labelerInputFieldMapping +=
        LabelerInputFieldMapping.newBuilder()
          .setFieldPath("event_id.id")
          .setScalar(ScalarColumn.newBuilder().setColumn("eid"))
          .build()
      labelerInputFieldMapping +=
        LabelerInputFieldMapping.newBuilder()
          .setFieldPath("timestamp_usec")
          .setScalar(ScalarColumn.newBuilder().setColumn("ts"))
          .build()
      eventTemplateFieldMapping.put("person.gender", "gender")
      eventTemplateFieldMapping.put("person.age_group", "age")
      // Entity keys are read per row from these columns (no longer from the footer).
      optionalEntityKeyFieldMapping.put("creative", "cr_col")
      optionalEntityKeyFieldMapping.put("placement", "pl_col")
    }

  // The footer now carries only the event group reference id and event date.
  private val fileEntityKeys =
    FileEntityKeys(eventGroupReferenceId = EVENT_GROUP, eventDate = LocalDate.parse("2026-06-30"))

  private fun digestedEvent(row: Map<String, ParquetValue>): ParquetDigestedEvent =
    DigestedEvent(row, EventIdDigest(0L, 0))

  @Test
  fun `convert projects labeler input, event, event group, and entity keys`() {
    val converter = ParquetImpressionConverter(eventDescriptor)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "gender" to parquetValue { stringValue = "MALE" },
        "age" to parquetValue { stringValue = "YEARS_18_TO_34" },
        "cr_col" to parquetValue { stringValue = "c-1" },
        "pl_col" to parquetValue { stringValue = "p-9" },
      )

    val converted = converter.convert(digestedEvent(row), config, fileEntityKeys)

    assertThat(converted).isNotNull()
    assertThat(converted!!.labelerInput.eventId.id).isEqualTo("event-1")
    assertThat(converted.eventTime).isEqualTo(Timestamps.fromMicros(1_700_000_000_000_000L))
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
        labelerInputFieldMapping +=
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("event_id.id")
            .setScalar(ScalarColumn.newBuilder().setColumn("eid"))
            .build()
        labelerInputFieldMapping +=
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("timestamp_usec")
            .setScalar(ScalarColumn.newBuilder().setColumn("ts"))
            .build()
        optionalEntityKeyFieldMapping.put("creative", "cr_col")
      }
    val converter = ParquetImpressionConverter(eventDescriptor)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-2" },
        "ts" to parquetValue { int64Value = 5L },
        "cr_col" to parquetValue { stringValue = "c-1" },
      )

    val converted = converter.convert(digestedEvent(row), emptyMappingConfig, fileEntityKeys)

    assertThat(converted).isNotNull()
    val event = converted!!.event.unpack(TestEvent::class.java)
    assertThat(event).isEqualTo(TestEvent.getDefaultInstance())
    assertThat(converted.event.typeUrl)
      .isEqualTo("type.googleapis.com/" + TestEvent.getDescriptor().fullName)
  }

  @Test
  fun `convert throws when all entity-key columns are null`() {
    val converter = ParquetImpressionConverter(eventDescriptor)
    // No cr_col / pl_col columns -> every mapped entity column is unset.
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-3" },
        "ts" to parquetValue { int64Value = 5L },
        "gender" to parquetValue { stringValue = "MALE" },
        "age" to parquetValue { stringValue = "YEARS_18_TO_34" },
      )

    assertFailsWith<IllegalArgumentException> {
      converter.convert(digestedEvent(row), config, fileEntityKeys)
    }
  }

  companion object {
    private const val EVENT_GROUP = "event-group-ref-1"
  }
}
