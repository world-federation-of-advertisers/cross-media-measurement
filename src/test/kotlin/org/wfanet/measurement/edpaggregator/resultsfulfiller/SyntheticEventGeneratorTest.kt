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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.common.OpenEndTimeRange
import java.time.Instant

@RunWith(JUnit4::class)
class SyntheticEventGeneratorTest {

  @Test
  fun generateEventsProducesCorrectNumberOfEvents(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(3600) // 1 hour
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 100L,
      uniqueVids = 10,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()

    assertThat(events).hasSize(100)
  }

  @Test
  fun generateEventsWithSingleEvent(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(60)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 1L,
      uniqueVids = 1,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()

    assertThat(events).hasSize(1)
    val event = events[0]
    assertThat(event.vid).isEqualTo(1L)
    assertThat(event.timestamp).isAtLeast(startTime)
    assertThat(event.timestamp).isLessThan(endTime)
    assertThat(event.message).isNotNull()
    assertThat(event.message.hasPerson()).isTrue()
  }

  @Test
  fun generateEventsDistributesVidsCorrectly(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(3600)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val uniqueVids = 5
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 1000L,
      uniqueVids = uniqueVids,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()
    val actualVids = events.map { it.vid }.toSet()

    // All VIDs should be in range 1 to uniqueVids
    assertThat(actualVids).containsAtLeastElementsIn(1L..uniqueVids.toLong())
    actualVids.forEach { vid ->
      assertThat(vid).isAtLeast(1L)
      assertThat(vid).isAtMost(uniqueVids.toLong())
    }
  }

  @Test
  fun generateEventsDistributesTimestampsWithinTimeRange(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(7200) // 2 hours
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 500L,
      uniqueVids = 10,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()
    
    // All timestamps should be within the time range
    events.forEach { event ->
      assertThat(event.timestamp).isAtLeast(startTime)
      assertThat(event.timestamp).isLessThan(endTime)
    }
    
    // Verify reasonable distribution across time range
    val timestamps = events.map { it.timestamp }
    val minTimestamp = timestamps.minOrNull()!!
    val maxTimestamp = timestamps.maxOrNull()!!
    
    // Should cover reasonable portion of the time range
    val actualRange = java.time.Duration.between(minTimestamp, maxTimestamp)
    val totalRange = java.time.Duration.between(startTime, endTime)
    assertThat(actualRange.seconds).isAtLeast(totalRange.seconds / 10) // At least 10% coverage
  }

  @Test
  fun generateEventsCreatesValidPersonDemographics(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(60)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 200L,
      uniqueVids = 10,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()
    
    // All events should have valid person data
    events.forEach { event ->
      assertThat(event.message.hasPerson()).isTrue()
      val person = event.message.person
      
      // Gender should be either MALE or FEMALE (not UNSPECIFIED)
      assertThat(person.gender).isAnyOf(Person.Gender.MALE, Person.Gender.FEMALE)
      
      // Age group should be one of the three valid values
      assertThat(person.ageGroup).isAnyOf(
        Person.AgeGroup.YEARS_18_TO_34,
        Person.AgeGroup.YEARS_35_TO_54,
        Person.AgeGroup.YEARS_55_PLUS
      )
    }
    
    // Check gender distribution (should have both genders)
    val genders = events.map { it.message.person.gender }.toSet()
    assertThat(genders).containsAtLeast(Person.Gender.MALE, Person.Gender.FEMALE)
    
    // Check age group distribution (should have multiple age groups)
    val ageGroups = events.map { it.message.person.ageGroup }.toSet()
    assertThat(ageGroups.size).isAtLeast(2) // Should have at least 2 different age groups
  }

  @Test
  fun generateEventsWithZeroEventsProducesEmptyFlow(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(60)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 0L,
      uniqueVids = 1,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()

    assertThat(events).isEmpty()
  }

  @Test
  fun generateEventsHandlesLargeEventCount(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(3600)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 50000L,
      uniqueVids = 1000,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()

    assertThat(events).hasSize(50000)
    
    // Verify reasonable VID distribution
    val vidCounts = events.groupingBy { it.vid }.eachCount()
    assertThat(vidCounts.keys.size).isAtLeast(500) // Should use at least half of available VIDs
    
    // Each VID should have reasonable number of events
    val avgEventsPerVid = 50000.0 / vidCounts.keys.size
    vidCounts.values.forEach { count ->
      assertThat(count.toDouble()).isWithin(avgEventsPerVid * 0.5).of(avgEventsPerVid)
    }
  }

  @Test
  fun generateEventsWithSingleVidUsesOnlyThatVid(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(60)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 100L,
      uniqueVids = 1,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()
    val vids = events.map { it.vid }.toSet()

    assertThat(vids).containsExactly(1L)
  }

  @Test
  fun generateEventsCreatesConsistentTestEventStructure(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(60)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 50L,
      uniqueVids = 5,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()
    
    events.forEach { event ->
      // Each event should be a properly structured TestEvent
      val testEvent = event.message
      assertThat(testEvent).isNotNull()
      assertThat(testEvent.hasPerson()).isTrue()
      
      // Person should have both gender and age group set
      val person = testEvent.person
      assertThat(person.gender).isNotEqualTo(Person.Gender.GENDER_UNSPECIFIED)
      assertThat(person.ageGroup).isNotEqualTo(Person.AgeGroup.AGE_GROUP_UNSPECIFIED)
    }
  }

  @Test
  fun generateEventsRespectsGenderProbabilityDistribution(): Unit = runBlocking {
    val startTime = Instant.now()
    val endTime = startTime.plusSeconds(60)
    val timeRange = OpenEndTimeRange(startTime, endTime)
    
    val generator = SyntheticEventGenerator(
      timeRange = timeRange,
      totalEvents = 10000L, // Large sample for statistical validation
      uniqueVids = 100,
      dispatcher = Dispatchers.Default
    )

    val events = generator.generateEvents().toList()
    val genderCounts = events.groupingBy { it.message.person.gender }.eachCount()
    
    val maleCount = genderCounts[Person.Gender.MALE] ?: 0
    val femaleCount = genderCounts[Person.Gender.FEMALE] ?: 0
    val totalCount = maleCount + femaleCount
    
    val maleRatio = maleCount.toDouble() / totalCount
    
    // Should be close to GENDER_MALE_PROBABILITY (0.6) with some tolerance for randomness
    assertThat(maleRatio).isWithin(0.05).of(0.6)
    assertThat(maleCount).isGreaterThan(femaleCount) // Males should be more frequent
  }
}