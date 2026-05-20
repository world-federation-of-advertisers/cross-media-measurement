// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.interval
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toProtoTime

private const val EDP_ID = "someDataProvider"
private const val EDP_NAME = "dataProviders/$EDP_ID"

private val LAST_EVENT_DATE = LocalDate.now()
private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

private const val DUCHY_ID = "worker1"
private const val RANDOM_SEED: Long = 0

@RunWith(JUnit4::class)
class FrequencyVectorGeneratorTest {
  @Test
  fun `generate returns non-wrapped around sketch with correct range`() {
    // EventGroupSpecs for female 18_TO_34
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(1L..4L, FIRST_EVENT_DATE, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.FEMALE)
    val eventQueries = InMemoryEventQuery(allEvents)

    val salt = ByteString.copyFromUtf8("salt")
    val vidUniverse = (1L..10L).asSequence()
    val vidToIndexMap = mutableMapOf<Long, IndexedValue>()
    for (i in 1L..10L) {
      vidToIndexMap[i] = IndexedValue((i - 1).toInt(), 0.05 + 0.1 * (i - 1))
    }

    // The interval contains 4 vids {3, 4, 5, 6} of the vid universe.
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = 0.25f
        width = 0.30f
      }
    }

    val hmssSketch =
      FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)

    val expectedSketch = intArrayOf(1, 1, 0, 0)
    assertThat(hmssSketch).isEqualTo(expectedSketch)
  }

  @Test
  fun `generate returns wrapped around sketch with correct range`() {
    // EventGroupSpecs for female 18_TO_34
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(1L..4L, FIRST_EVENT_DATE, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.FEMALE)
    val eventQueries = InMemoryEventQuery(allEvents)

    val salt = ByteString.copyFromUtf8("salt")
    val vidUniverse = (1L..10L).asSequence()
    val vidToIndexMap = mutableMapOf<Long, IndexedValue>()
    for (i in 1L..10L) {
      vidToIndexMap[i] = IndexedValue((i - 1).toInt(), 0.05 + 0.1 * (i - 1))
    }

    // The interval contains 5 vids {8, 9, 10, 1, 2} of the vid universe.
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = 0.75f
        width = 0.45f
      }
    }

    val hmssSketch =
      FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)

    val expectedSketch = intArrayOf(0, 0, 0, 1, 1)
    assertThat(hmssSketch).isEqualTo(expectedSketch)
  }

  @Test
  fun `generate throws IllegalArgumentException when vid sampling interval is in the middle of the unit interval and contains no vid`() {
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(
        LongRange.EMPTY,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )

    val eventQueries = InMemoryEventQuery(allEvents)
    val vidUniverse = (0L..9L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")

    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)
    val sortedNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()

    // sortedNormalizedHashValues[5] = 0.2478
    // sortedNormalizedHashValues[6] = 0.4839
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = (sortedNormalizedHashValues[5] + 0.01).toFloat()
        width = (sortedNormalizedHashValues[6] - sortedNormalizedHashValues[5] - 0.02).toFloat()
      }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
          .generate(eventGroupSpecs)
      }

    assertThat(exception).hasMessageThat().contains("small")
  }

  @Test
  fun `generate throws IllegalArgumentException when vid sampling interval is at the beginning of the unit interval and contains no vid`() {
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(
        LongRange.EMPTY,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )

    val eventQueries = InMemoryEventQuery(allEvents)
    val vidUniverse = (0L..9L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")

    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)
    val sortedNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()

    // sortedNormalizedHashValues[0] = 0.0163
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = (sortedNormalizedHashValues[0] - 0.01).toFloat()
      }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
          .generate(eventGroupSpecs)
      }

    assertThat(exception).hasMessageThat().contains("small")
  }

  @Test
  fun `generate throws IllegalArgumentException when vid sampling interval is at the end of the unit interval and contains no vid`() {
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(
        LongRange.EMPTY,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )

    val eventQueries = InMemoryEventQuery(allEvents)
    val vidUniverse = (0L..9L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")

    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)
    val sortedNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()

    // sortedNormalizedHashValues[9] = 0.7261
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = (sortedNormalizedHashValues[9] + 0.01).toFloat()
        width = 0.2f
      }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
          .generate(eventGroupSpecs)
      }

    assertThat(exception).hasMessageThat().contains("small")
  }

  @Test
  fun `generate throws IllegalArgumentException when vid sampling interval wraps around the unit interval and contains no vid`() {
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(
        LongRange.EMPTY,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )

    val eventQueries = InMemoryEventQuery(allEvents)
    val vidUniverse = (0L..9L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")

    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)
    val sortedNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()

    // sortedNormalizedHashValues[0] = 0.0163
    // sortedNormalizedHashValues[9] = 0.7261
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = (sortedNormalizedHashValues[9] + 0.01).toFloat()
        width =
          (1.0 - sortedNormalizedHashValues[9] + sortedNormalizedHashValues[0] - 0.02).toFloat()
      }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
          .generate(eventGroupSpecs)
      }

    assertThat(exception).hasMessageThat().contains("small")
  }

  @Test
  fun `empty EventQuery generates sketch with all zero frequency count`() {
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(
        LongRange.EMPTY,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )

    val vidUniverse = (0L..30L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")
    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)
    val eventQueries = InMemoryEventQuery(allEvents)
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
    }

    val hmssSketch =
      FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)
    assertThat(hmssSketch.size).isEqualTo(31)
    for (x in hmssSketch) {
      assertThat(x).isEqualTo(0)
    }
  }

  @Test
  fun `empty salt is used to generate the vid map`() {
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    val allEvents =
      generateEvents(
        0L..10L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )

    val vidUniverse = (0L..30L).asSequence()
    val salt = ByteString.EMPTY
    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)
    val eventQueries = InMemoryEventQuery(allEvents)
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
    }

    val hmssSketch =
      FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)
    assertThat(hmssSketch.size).isEqualTo(31)
    var oneCount: Int = 0
    for (x in hmssSketch) {
      if (x == 1) {
        oneCount++
      }
    }

    // It is expected to have 5 registers with the frequency count of 2.
    assertThat(oneCount).isEqualTo(11)
  }

  @Test
  fun `share shuffle sketch with non-wrapped around interval succeeds`() {
    // EventGroupSpecs for female 18_TO_34
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    // All events, where the frequency count for vids for the above group spec is 2.
    val allEvents =
      generateEvents(
        1L..10L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      ) +
        generateEvents(
          1L..10L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE,
        )

    val vidUniverse = (0L..30L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")
    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)

    // The vids of the group event (FEMALE, YEARS_18_TO_34).
    val subUniverse = (1L..10L).asSequence()

    val fullUniverseNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()
    val subUniverseNormalizedHashValues =
      VidToIndexMapGenerator.generateMapping(subUniverse, salt)
        .values
        .toList()
        .map { it.value }
        .sorted()

    val eventQueries = InMemoryEventQuery(allEvents)

    // The interval contains 5 vids of the group event (FEMALE, YEARS_18_TO_34).
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = (subUniverseNormalizedHashValues[3]).toFloat()
        width = (subUniverseNormalizedHashValues[7] - subUniverseNormalizedHashValues[3]).toFloat()
      }
    }

    val hmssSketch =
      FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)

    // Computes the expected sketch size.
    var expectedSketchSize: Int = 0
    for (x in fullUniverseNormalizedHashValues) {
      if (
        x >= measurementSpec.vidSamplingInterval.start &&
          x <= measurementSpec.vidSamplingInterval.start + measurementSpec.vidSamplingInterval.width
      ) {
        expectedSketchSize += 1
      }
    }

    assertThat(hmssSketch.size).isEqualTo(expectedSketchSize)

    var twoCount: Int = 0
    for (x in hmssSketch) {
      if (x == 2) {
        twoCount++
      }
    }

    // It is expected to have 5 registers with the frequency count of 2.
    assertThat(twoCount).isEqualTo(5)
  }

  @Test
  fun `share shuffle sketch with wrapped around interval succeeds`() {
    // EventGroupSpecs for female 18_TO_34
    val eventGroupSpecs =
      REQUISITION_SPEC.events.eventGroupsList.map {
        EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
      }

    // All events, where the frequency count for vids for the above group spec is 2.
    val allEvents =
      generateEvents(
        1L..10L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      ) +
        generateEvents(
          1L..10L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE,
        )

    val vidUniverse = (0L..30L).asSequence()
    val salt = ByteString.copyFromUtf8("salt")
    val vidToIndexMap = VidToIndexMapGenerator.generateMapping(vidUniverse, salt)

    // The vids of the group event (FEMALE, YEARS_18_TO_34).
    val subUniverse = (1L..10L).asSequence()
    val fullUniverseNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()
    val subUniverseNormalizedHashValues =
      VidToIndexMapGenerator.generateMapping(subUniverse, salt)
        .values
        .toList()
        .map { it.value }
        .sorted()

    val eventQueries = InMemoryEventQuery(allEvents)

    // The interval contains 7 vids of the group event (FEMALE, YEARS_18_TO_34).
    val measurementSpec = measurementSpec {
      vidSamplingInterval = vidSamplingInterval {
        start = (subUniverseNormalizedHashValues[5]).toFloat()
        width =
          (1.0 - subUniverseNormalizedHashValues[5] + subUniverseNormalizedHashValues[1] + 0.001)
            .toFloat()
      }
    }

    val hmssSketch =
      FrequencyVectorGenerator(vidToIndexMap, eventQueries, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)

    val start = measurementSpec.vidSamplingInterval.start
    val end = measurementSpec.vidSamplingInterval.start + measurementSpec.vidSamplingInterval.width
    // Computes the expected sketch size.
    var expectedSketchSize: Int = 0
    for (x in fullUniverseNormalizedHashValues) {
      if ((x >= start && x <= end) || (x <= end - 1.0)) {
        expectedSketchSize += 1
      }
    }

    assertThat(hmssSketch.size).isEqualTo(expectedSketchSize)

    var twoCount: Int = 0
    for (x in hmssSketch) {
      if (x == 2) {
        twoCount++
      }
    }

    // It is expected to have 7 registers with the frequency count of 2.
    assertThat(twoCount).isEqualTo(7)
  }

  companion object {
    private fun generateEvents(
      vidRange: LongRange,
      date: LocalDate,
      ageGroup: Person.AgeGroup,
      gender: Person.Gender,
    ): List<LabeledTestEvent> {
      val timestamp = date.atStartOfDay().toInstant(ZoneOffset.UTC)
      val message = testEvent {
        person = person {
          this.ageGroup = ageGroup
          this.gender = gender
        }
      }
      return vidRange.map { vid -> LabeledTestEvent(timestamp, vid, message) }
    }

    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private val REQUISITION_SPEC = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups += eventGroupEntry {
            key = EVENT_GROUP_NAME
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = interval {
                  startTime = TIME_RANGE.start.toProtoTime()
                  endTime = TIME_RANGE.endExclusive.toProtoTime()
                }
                filter = eventFilter {
                  expression =
                    "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                      "person.gender == ${Person.Gender.FEMALE_VALUE}"
                }
              }
          }
        }
      nonce = Random.Default.nextLong()
    }
  }
}
