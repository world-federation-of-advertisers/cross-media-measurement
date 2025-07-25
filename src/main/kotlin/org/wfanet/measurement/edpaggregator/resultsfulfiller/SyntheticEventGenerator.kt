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

import com.google.protobuf.DynamicMessage
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.time.LocalDate
import kotlin.random.Random

/**
 * Event source that generates synthetic events for testing and simulation.
 * 
 * This implementation creates synthetic event data based on population
 * specifications and event group configurations, useful for testing
 * and load testing scenarios.
 */
class SyntheticEventGenerator(
  private val startDate: LocalDate,
  private val endDate: LocalDate,
  private val eventsPerDay: Long = 1000,
  private val vidRange: LongRange = 1L..10000L
) : EventSource {
  
  private val random = Random.Default
  
  override suspend fun getEvents(): Flow<LabeledEvent<DynamicMessage>> = flow {
    var currentDate = startDate
    
    while (!currentDate.isAfter(endDate)) {
      // Generate events for current date
      repeat(eventsPerDay.toInt()) {
        val vid = vidRange.random(random)
        
        // Create a simple synthetic event
        // In a real implementation, this would use actual protobuf message types
        // and synthetic data generation based on population specs
        val syntheticEvent = createSyntheticEvent(vid, currentDate)
        
        emit(LabeledEvent(
          event = syntheticEvent,
          vid = vid,
          labels = mapOf(
            "date" to currentDate.toString(),
            "source" to "synthetic"
          )
        ))
      }
      
      currentDate = currentDate.plusDays(1)
    }
  }
  
  override fun getEstimatedEventCount(): Long {
    val days = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate.plusDays(1))
    return days * eventsPerDay
  }
  
  override suspend fun close() {
    // No resources to clean up for synthetic generation
  }
  
  private fun createSyntheticEvent(vid: Long, date: LocalDate): DynamicMessage {
    // This is a placeholder - in real implementation this would create
    // proper DynamicMessage instances based on event templates
    // For now, we'll create a minimal DynamicMessage using the builder pattern
    
    // Create a simple Any message as a placeholder
    val anyMessage = com.google.protobuf.Any.newBuilder()
      .setTypeUrl("type.googleapis.com/synthetic.Event")
      .setValue(com.google.protobuf.ByteString.copyFromUtf8("synthetic_event_$vid"))
      .build()
    
    // Convert to DynamicMessage
    return DynamicMessage.parseFrom(anyMessage.descriptorForType, anyMessage.toByteString())
  }
}