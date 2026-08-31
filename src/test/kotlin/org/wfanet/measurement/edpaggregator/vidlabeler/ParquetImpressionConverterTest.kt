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
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.populationSpec
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
      // Only non-population fields may be mapped from the raw row; population attributes come
      // from the model line's PopulationSpec.
      eventTemplateFieldMapping.put("video_ad.viewed_fraction", "viewed_fraction")
      // Entity keys are read per row from these columns (no longer from the footer).
      optionalEntityKeyFieldMapping.put("creative", "cr_col")
      optionalEntityKeyFieldMapping.put("placement", "pl_col")
    }

  private fun digestedEvent(row: Map<String, ParquetValue>): ParquetDigestedEvent =
    DigestedEvent(row, EventIdDigest(0L, 0))

  private companion object {
    /**
     * Minimal spec covering the VIDs these tests never actually label; identity is what matters.
     */
    private val POPULATION_ATTRIBUTE_WRITER =
      PopulationAttributeWriter(
        TestEvent.getDescriptor(),
        populationSpec {
          subpopulations +=
            PopulationSpecKt.subPopulation {
              attributes +=
                com.google.protobuf.Any.pack(
                  person {
                    gender = Person.Gender.FEMALE
                    ageGroup = Person.AgeGroup.YEARS_35_TO_54
                    socialGradeGroup = Person.SocialGradeGroup.C2_D_E
                  }
                )
              vidRanges +=
                PopulationSpecKt.vidRange {
                  startVid = 1L
                  endVidInclusive = 1_000L
                }
            }
        },
      )
  }

  @Test
  fun `convert projects labeler input, event, and entity keys`() {
    val converter = ParquetImpressionConverter(eventDescriptor, POPULATION_ATTRIBUTE_WRITER)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "viewed_fraction" to parquetValue { doubleValue = 0.5 },
        "cr_col" to parquetValue { stringValue = "c-1" },
        "pl_col" to parquetValue { stringValue = "p-9" },
      )

    val converted = converter.convert(digestedEvent(row), config)

    assertThat(converted).isNotNull()
    assertThat(converted!!.labelerInput.eventId.id).isEqualTo("event-1")
    assertThat(converted.labelerInput.timestampUsec).isEqualTo(1_700_000_000_000_000L)

    val event = TestEvent.parseFrom(converted.buildEvent().toByteString())
    // Only non-population event fields come from the raw row; the assigned VID's population
    // attributes are written later by PopulationAttributeWriter.
    assertThat(event.videoAd.viewedFraction).isEqualTo(0.5)
    assertThat(event.person).isEqualTo(Person.getDefaultInstance())

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
    val converter = ParquetImpressionConverter(eventDescriptor, POPULATION_ATTRIBUTE_WRITER)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-2" },
        "ts" to parquetValue { int64Value = 5L },
        "cr_col" to parquetValue { stringValue = "c-1" },
      )

    val converted = converter.convert(digestedEvent(row), emptyMappingConfig)

    assertThat(converted).isNotNull()
    val event = TestEvent.parseFrom(converted!!.buildEvent().toByteString())
    assertThat(event).isEqualTo(TestEvent.getDefaultInstance())
    assertThat(converted.buildEvent().descriptorForType.fullName)
      .isEqualTo(TestEvent.getDescriptor().fullName)
  }

  @Test
  fun `constructing a converter rejects a mapping onto a population attribute`() {
    // A population attribute mapped from the raw row is silently dead -- the writer overwrites it
    // from the PopulationSpec -- so reject the config instead of accepting a mapping that does
    // nothing. Declared demographics belong in labeler_input_field_mapping.
    val badConfig =
      VidLabelerParamsKt.modelLineConfig {
        labelerInputFieldMapping +=
          LabelerInputFieldMapping.newBuilder()
            .setFieldPath("event_id.id")
            .setScalar(ScalarColumn.newBuilder().setColumn("eid"))
            .build()
        eventTemplateFieldMapping.put("person.gender", "gender")
        optionalEntityKeyFieldMapping.put("creative", "cr_col")
      }
    val converter = ParquetImpressionConverter(eventDescriptor, POPULATION_ATTRIBUTE_WRITER)
    val row = mapOf("eid" to parquetValue { stringValue = "e" })

    val exception =
      assertFailsWith<IllegalArgumentException> { converter.convert(digestedEvent(row), badConfig) }
    assertThat(exception).hasMessageThat().contains("person.gender")
  }

  @Test
  fun `convert propagates the population attribute writer to the sink`() {
    val converter = ParquetImpressionConverter(eventDescriptor, POPULATION_ATTRIBUTE_WRITER)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "cr_col" to parquetValue { stringValue = "creative-1" },
      )

    val converted = converter.convert(digestedEvent(row), config)

    // The sink can only write the assigned VID's demo if the converter hands the writer through.
    assertThat(converted!!.populationAttributeWriter).isSameInstanceAs(POPULATION_ATTRIBUTE_WRITER)
  }

  @Test
  fun `convert reuses cached mappers and is race-free under concurrent calls`() {
    val converter = ParquetImpressionConverter(eventDescriptor, POPULATION_ATTRIBUTE_WRITER)
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-1" },
        "ts" to parquetValue { int64Value = 1_700_000_000_000_000L },
        "gender" to parquetValue { stringValue = "MALE" },
        "age" to parquetValue { stringValue = "YEARS_18_TO_34" },
        "cr_col" to parquetValue { stringValue = "c-1" },
        "pl_col" to parquetValue { stringValue = "p-9" },
      )
    // Reference result from a warm-up convert (populates the per-config mapper cache).
    val reference = converter.convert(digestedEvent(row), config)!!

    val threads = 8
    val perThread = 200
    val pool = java.util.concurrent.Executors.newFixedThreadPool(threads)
    val results = java.util.concurrent.ConcurrentLinkedQueue<ConvertedImpression>()
    val errors = java.util.concurrent.ConcurrentLinkedQueue<Throwable>()
    try {
      (0 until threads)
        .map {
          pool.submit {
            try {
              repeat(perThread) { results.add(converter.convert(digestedEvent(row), config)!!) }
            } catch (t: Throwable) {
              errors.add(t)
            }
          }
        }
        .forEach { it.get() }
    } finally {
      pool.shutdown()
    }

    assertThat(errors).isEmpty()
    assertThat(results).hasSize(threads * perThread)
    // Every concurrent conversion of the same row+config yields an identical ConvertedImpression,
    // proving the lock-free per-config mapper cache is race-free. Compared field-by-field:
    // ConvertedImpression is not a data class, because its PopulationAttributeWriter has no value
    // semantics.
    assertThat(
        results.all {
          it.labelerInput == reference.labelerInput &&
            it.buildEvent() == reference.buildEvent() &&
            it.entityKeys == reference.entityKeys
        }
      )
      .isTrue()
  }

  @Test
  fun `convert throws when all entity-key columns are null`() {
    val converter = ParquetImpressionConverter(eventDescriptor, POPULATION_ATTRIBUTE_WRITER)
    // No cr_col / pl_col columns -> every mapped entity column is unset.
    val row =
      mapOf(
        "eid" to parquetValue { stringValue = "event-3" },
        "ts" to parquetValue { int64Value = 5L },
        "gender" to parquetValue { stringValue = "MALE" },
        "age" to parquetValue { stringValue = "YEARS_18_TO_34" },
      )

    assertFailsWith<IllegalArgumentException> { converter.convert(digestedEvent(row), config) }
  }
}
