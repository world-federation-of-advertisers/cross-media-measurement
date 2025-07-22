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

import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

/**
 * Generates synthetic events for testing and load evaluation.
 * 
 * This generator creates events with:
 * - Configurable total event count
 * - Configurable number of unique VIDs
 * - Uniform distribution across time range
 * - Random gender and age group attributes
 */
class SyntheticEventGenerator(
  private val timeRange: OpenEndTimeRange,
  private val totalEvents: Long,
  private val uniqueVids: Int,
  private val dispatcher: CoroutineDispatcher
) {
  
  companion object {
    private val logger = Logger.getLogger(SyntheticEventGenerator::class.java.name)
    
    private val GENDERS = arrayOf(Person.Gender.MALE, Person.Gender.FEMALE)
    private val AGE_GROUPS = arrayOf(
      Person.AgeGroup.YEARS_18_TO_34,
      Person.AgeGroup.YEARS_35_TO_54,
      Person.AgeGroup.YEARS_55_PLUS
    )
    
    const val DEFAULT_BATCH_SIZE = 10000
    const val GENDER_MALE_PROBABILITY = 0.6
  }
  
  /**
   * Generates a flow of synthetic events.
   * 
   * Events are generated in parallel using multiple coroutines for high throughput.
   * Each event has:
   * - A VID randomly selected from the configured range
   * - A timestamp uniformly distributed across the time range
   * - Random demographic attributes (gender and age group)
   */
  fun generateEvents(): Flow<LabeledEvent<TestEvent>> = channelFlow {
    val startTime = timeRange.start
    val endTime = timeRange.endExclusive
    val duration = java.time.Duration.between(startTime, endTime)
    
    logGenerationParameters(startTime, endTime, duration)
    
    val vidArray = createVidArray()
    val durationSeconds = duration.seconds.toDouble()
    
    val numGenerators = calculateOptimalGeneratorCount()
    val eventsPerGenerator = totalEvents / numGenerators
    val remainingEvents = (totalEvents % numGenerators).toInt()
    
    logger.info("Using $numGenerators parallel generators (${Runtime.getRuntime().availableProcessors()} CPU cores)")
    
    launchParallelGenerators(
      numGenerators,
      eventsPerGenerator,
      remainingEvents,
      vidArray,
      startTime,
      durationSeconds
    ).collect { event ->
      send(event)
    }
    
    logger.info("Synthetic event generation completed. Total events: $totalEvents")
  }
  
  private fun logGenerationParameters(startTime: Instant, endTime: Instant, duration: java.time.Duration) {
    println("Generating synthetic events:")
    println("  Start time: $startTime")
    println("  End time: $endTime")
    println("  Duration: ${duration.seconds} seconds")
    println("  Total events to generate: $totalEvents")
    println("  Unique VIDs: $uniqueVids")
    println("  Distribution: Uniform across time range")
  }
  
  private fun createVidArray(): LongArray {
    return LongArray(uniqueVids) { (it + 1).toLong() }
  }
  
  private fun calculateOptimalGeneratorCount(): Int {
    return Runtime.getRuntime().availableProcessors() * 10
  }
  
  private suspend fun launchParallelGenerators(
    numGenerators: Int,
    eventsPerGenerator: Long,
    remainingEvents: Int,
    vidArray: LongArray,
    startTime: Instant,
    durationSeconds: Double
  ): Flow<LabeledEvent<TestEvent>> = channelFlow {
    val generators = (0 until numGenerators).map { generatorId ->
      launch(dispatcher) {
        generateEventsForWorker(
          generatorId,
          eventsPerGenerator,
          remainingEvents,
          vidArray,
          startTime,
          durationSeconds
        ).collect { event ->
          send(event)
        }
      }
    }
    
    generators.forEach { it.join() }
  }
  
  private suspend fun generateEventsForWorker(
    workerId: Int,
    baseEventCount: Long,
    remainingEvents: Int,
    vidArray: LongArray,
    startTime: Instant,
    durationSeconds: Double
  ): Flow<LabeledEvent<TestEvent>> = channelFlow {
    val random = kotlin.random.Random(System.nanoTime() + workerId)
    val eventsToGenerate = baseEventCount + (if (workerId < remainingEvents) 1 else 0)
    var generated = 0L
    
    while (generated < eventsToGenerate) {
      val batchSize = minOf(DEFAULT_BATCH_SIZE, (eventsToGenerate - generated).toInt())
      
      repeat(batchSize) {
        val event = generateSingleEvent(random, vidArray, startTime, durationSeconds)
        send(event)
      }
      
      generated += batchSize
      
      if (generated % 50000 == 0L) {
        logger.info("Worker $workerId: Generated $generated/$eventsToGenerate events")
      }
    }
    
    logger.info("Worker $workerId completed: $eventsToGenerate events")
  }
  
  private fun generateSingleEvent(
    random: kotlin.random.Random,
    vidArray: LongArray,
    startTime: Instant,
    durationSeconds: Double
  ): LabeledEvent<TestEvent> {
    val vid = vidArray[random.nextInt(uniqueVids)]
    val randomSeconds = (random.nextDouble() * durationSeconds).toLong()
    val timestamp = startTime.plusSeconds(randomSeconds)
    
    val person = generatePerson(random)
    val event = TestEvent.newBuilder()
      .setPerson(person)
      .build()
    
    return LabeledEvent(timestamp, vid, event)
  }
  
  private fun generatePerson(random: kotlin.random.Random): Person {
    return Person.newBuilder().apply {
      gender = if (random.nextDouble() < GENDER_MALE_PROBABILITY) {
        Person.Gender.MALE
      } else {
        Person.Gender.FEMALE
      }
      ageGroup = AGE_GROUPS[random.nextInt(AGE_GROUPS.size)]
    }.build()
  }
}