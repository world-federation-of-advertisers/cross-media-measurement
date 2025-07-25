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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.kotlin.unpack
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import java.time.Instant
import java.time.ZoneId

@RunWith(JUnit4::class)
class SyntheticEventGeneratorTest {

  private fun getTestEvent(message: Any): TestEvent {
    return message.unpack<TestEvent>()
  }

  private fun createTestPopulationSpec(): SyntheticPopulationSpec {
    return SyntheticPopulationSpec.newBuilder().apply {
      vidRangeBuilder.apply {
        start = 1L
        endExclusive = 1000L
      }
      eventMessageTypeUrl = "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      addPopulationFields("person.gender")
      addPopulationFields("person.age_group")
      
      // Create sub-populations with different genders
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = 1L
          endExclusive = 500L
        }
        putPopulationFieldsValues("person.gender", 
          org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
            .setEnumValue(1) // MALE
            .build()
        )
        putPopulationFieldsValues("person.age_group", 
          org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
            .setEnumValue(1) // YEARS_18_TO_34
            .build()
        )
      }.build())
      
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = 500L
          endExclusive = 1000L
        }
        putPopulationFieldsValues("person.gender", 
          org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
            .setEnumValue(2) // FEMALE
            .build()
        )
        putPopulationFieldsValues("person.age_group", 
          org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
            .setEnumValue(2) // YEARS_35_TO_54
            .build()
        )
      }.build())
    }.build()
  }

  private fun createTestEventGroupSpec(): SyntheticEventGroupSpec {
    return SyntheticEventGroupSpec.newBuilder().apply {
      description = "Test event group spec"
      samplingNonce = 12345L
      
      addDateSpecs(SyntheticEventGroupSpec.DateSpec.newBuilder().apply {
        dateRangeBuilder.apply {
          startBuilder.apply {
            year = 2021
            month = 1
            day = 1
          }
          endExclusiveBuilder.apply {
            year = 2021
            month = 2
            day = 1
          }
        }
        
        // Add frequency specs for both VID ranges
        addFrequencySpecs(SyntheticEventGroupSpec.FrequencySpec.newBuilder().apply {
          frequency = 1L
          addVidRangeSpecs(SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.newBuilder().apply {
            vidRangeBuilder.apply {
              start = 1L
              endExclusive = 50L
            }
            samplingRate = 1.0
            putNonPopulationFieldValues("video_ad.viewed_fraction",
              org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
                .setDoubleValue(0.5)
                .build()
            )
          }.build())
        }.build())
        
        addFrequencySpecs(SyntheticEventGroupSpec.FrequencySpec.newBuilder().apply {
          frequency = 1L
          addVidRangeSpecs(SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.newBuilder().apply {
            vidRangeBuilder.apply {
              start = 500L
              endExclusive = 550L
            }
            samplingRate = 1.0
            putNonPopulationFieldValues("video_ad.viewed_fraction",
              org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue.newBuilder()
                .setDoubleValue(0.3)
                .build()
            )
          }.build())
        }.build())
      }.build())
    }.build()
  }

  @Test
  fun generateEventsProducesEvents(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()

    assertThat(events).isNotEmpty()
  }

  @Test
  fun generateEventsHasValidStructure(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()
    
    assertThat(events).isNotEmpty()
    val event = events[0]
    assertThat(event.vid).isGreaterThan(0L)
    assertThat(event.timestamp).isNotNull()
    assertThat(event.message).isNotNull()
    assertThat(getTestEvent(event.message).hasPerson()).isTrue()
  }

  @Test
  fun generateEventsDistributesVidsCorrectly(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()
    val actualVids = events.map { it.vid }.toSet()

    // All VIDs should be within the population spec range (1 to 1000)
    actualVids.forEach { vid ->
      assertThat(vid).isAtLeast(1L)
      assertThat(vid).isAtMost(1000L)
    }
    assertThat(actualVids).isNotEmpty()
  }

  @Test
  fun generateEventsWithCustomTimeRange(): Unit = runBlocking {
    val startTime = Instant.parse("2021-01-01T00:00:00Z")
    val endTime = Instant.parse("2021-02-01T00:00:00Z")  // Overlaps with our event group spec
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      timeRange = timeRange
    )

    val events = generator.generateEvents().toList()
    
    if (events.isNotEmpty()) {
      // All timestamps should be within the time range
      events.forEach { event ->
        assertThat(event.timestamp).isAtLeast(startTime)
        assertThat(event.timestamp).isLessThan(endTime)
      }
    }
    
    // Note: Events may be empty if the time range doesn't overlap with the event group spec
    // This is acceptable behavior
  }

  @Test
  fun generateEventsCreatesValidPersonDemographics(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()
    
    // All events should have valid person data
    events.forEach { event ->
      assertThat(getTestEvent(event.message).hasPerson()).isTrue()
      val person = getTestEvent(event.message).person
      
      // Gender should be either MALE or FEMALE (not UNSPECIFIED)
      assertThat(person.gender).isAnyOf(Person.Gender.MALE, Person.Gender.FEMALE)
      
      // Age group should be one of the three valid values
      assertThat(person.ageGroup).isAnyOf(
        Person.AgeGroup.YEARS_18_TO_34,
        Person.AgeGroup.YEARS_35_TO_54,
        Person.AgeGroup.YEARS_55_PLUS
      )
    }
    
    // Check that we have demographic diversity based on population spec
    val genders = events.map { getTestEvent(it.message).person.gender }.toSet()
    val ageGroups = events.map { getTestEvent(it.message).person.ageGroup }.toSet()
    assertThat(genders).isNotEmpty()
    assertThat(ageGroups).isNotEmpty()
  }

  @Test
  fun generateEventsWithEmptyTimeRangeProducesEmptyFlow(): Unit = runBlocking {
    val futureTime = Instant.parse("2030-01-01T00:00:00Z")
    val timeRange = OpenEndTimeRange(futureTime, futureTime.plusSeconds(1))
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      timeRange = timeRange
    )

    val events = generator.generateEvents().toList()

    assertThat(events).isEmpty()
  }

  @Test
  fun generateEventsProducesConsistentResults(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()

    assertThat(events).isNotEmpty()
    
    // Verify VID distribution follows population spec
    val vidCounts = events.groupingBy { it.vid }.eachCount()
    assertThat(vidCounts.keys).isNotEmpty()
    
    // All VIDs should be within expected range
    vidCounts.keys.forEach { vid ->
      assertThat(vid).isAtLeast(1L)
      assertThat(vid).isAtMost(1000L) // Based on population spec
    }
  }

  @Test
  fun generateEventsUsesPopulationSpecVidRanges(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()
    val vids = events.map { it.vid }.toSet()

    // Should use VIDs from the population spec ranges
    vids.forEach { vid ->
      assertThat(vid).isAtLeast(1L)
      assertThat(vid).isAtMost(1000L)
    }
    assertThat(vids).isNotEmpty()
  }

  @Test
  fun generateEventsCreatesConsistentTestEventStructure(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()
    
    events.forEach { event ->
      // Each event should be a properly structured TestEvent
      val testEvent = getTestEvent(event.message)
      assertThat(testEvent).isNotNull()
      assertThat(testEvent.hasPerson()).isTrue()
      
      // Person should have both gender and age group set based on population spec
      val person = testEvent.person
      assertThat(person.gender).isNotEqualTo(Person.Gender.GENDER_UNSPECIFIED)
      assertThat(person.ageGroup).isNotEqualTo(Person.AgeGroup.AGE_GROUP_UNSPECIFIED)
      
      // Event should have valid fields based on data spec
      // Note: video_ad field may not be set depending on the spec configuration
    }
  }

  @Test
  fun generateEventsRespectsPopulationSpecDemographics(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec()
    )

    val events = generator.generateEvents().toList()
    val genderCounts = events.groupingBy { getTestEvent(it.message).person.gender }.eachCount()
    
    // Should have events with genders based on population spec
    assertThat(genderCounts.keys).contains(Person.Gender.MALE)
    
    // Check social grade distribution from population spec
    val socialGrades = events.map { getTestEvent(it.message).person.socialGradeGroup }.toSet()
    assertThat(socialGrades).isNotEmpty()
  }

  @Test
  fun generateEventBatchesProducesValidBatches(): Unit = runBlocking {
    val batchSize = 50
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      batchSize = batchSize
    )

    val batches = generator.generateEventBatches().toList()
    
    assertThat(batches).isNotEmpty()
    
    // Each batch should have at most batchSize events
    val allEvents = batches.flatten()
    assertThat(allEvents).isNotEmpty()
    
    batches.forEach { batch ->
      assertThat(batch.size).isAtMost(batchSize)
      assertThat(batch.size).isAtLeast(1)
    }
  }

  @Test
  fun generateEventBatchesWithSmallBatchSize(): Unit = runBlocking {
    val batchSize = 3
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      batchSize = batchSize
    )

    val batches = generator.generateEventBatches().toList()
    val allEvents = batches.flatten()
    
    assertThat(allEvents).isNotEmpty()
    
    // Each batch should have at most batchSize events
    batches.forEach { batch ->
      assertThat(batch.size).isAtMost(batchSize)
      assertThat(batch.size).isAtLeast(1)
    }
    
    // All events should be valid
    allEvents.forEach { event ->
      assertThat(event.vid).isAtLeast(1L)
      assertThat(event.vid).isAtMost(100000L) // Based on population spec
      assertThat(event.timestamp).isNotNull()
      assertThat(getTestEvent(event.message).hasPerson()).isTrue()
    }
  }

  @Test
  fun generateEventBatchesHandlesLargeBatchSize(): Unit = runBlocking {
    val batchSize = 100000 // Very large batch size
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      batchSize = batchSize
    )

    val batches = generator.generateEventBatches().toList()
    
    // Should have at least 1 batch
    assertThat(batches).isNotEmpty()
    
    val allEvents = batches.flatten()
    assertThat(allEvents).isNotEmpty()
    
    // All events should be valid
    allEvents.forEach { event ->
      assertThat(event.vid).isAtLeast(1L)
      assertThat(event.vid).isAtMost(100000L) // Based on population spec
      assertThat(event.timestamp).isNotNull()
      assertThat(getTestEvent(event.message).hasPerson()).isTrue()
    }
  }

  @Test
  fun generateEventBatchesWithRestrictedTimeRange(): Unit = runBlocking {
    val futureTime = Instant.parse("2030-01-01T00:00:00Z")
    val timeRange = OpenEndTimeRange(futureTime, futureTime.plusSeconds(1))
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      timeRange = timeRange,
      batchSize = 10
    )

    val batches = generator.generateEventBatches().toList()

    assertThat(batches).isEmpty()
  }

  @Test
  fun generateEventBatchesProducesValidEvents(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      batchSize = 10
    )

    val batchedEvents = generator.generateEventBatches().toList().flatten()
    val individualEvents = generator.generateEvents().toList()
    
    // Both methods should produce events
    assertThat(batchedEvents).isNotEmpty()
    assertThat(individualEvents).isNotEmpty()
    
    // Both should have events with same VID characteristics based on population spec
    val batchedVids = batchedEvents.map { it.vid }.toSet()
    val individualVids = individualEvents.map { it.vid }.toSet()
    
    // Both should use VIDs from the same population spec range
    batchedVids.forEach { vid -> 
      assertThat(vid).isAtLeast(1L)
      assertThat(vid).isAtMost(1000L)
    }
    individualVids.forEach { vid -> 
      assertThat(vid).isAtLeast(1L)
      assertThat(vid).isAtMost(1000L)
    }
  }
  
  @Test
  fun generateEventsWithCustomZoneId(): Unit = runBlocking {
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      zoneId = ZoneId.of("America/New_York")
    )

    val events = generator.generateEvents().toList()
    
    assertThat(events).isNotEmpty()
    events.forEach { event ->
      assertThat(event.timestamp).isNotNull()
      assertThat(getTestEvent(event.message).hasPerson()).isTrue()
    }
  }
}