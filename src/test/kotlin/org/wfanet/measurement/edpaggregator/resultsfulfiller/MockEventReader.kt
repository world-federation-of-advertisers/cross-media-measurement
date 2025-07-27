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
import kotlinx.coroutines.flow.flowOf
import java.time.Instant

/**
 * Mock implementation of EventReader for testing purposes.
 * 
 * Provides configurable behavior for testing different scenarios:
 * - Normal event reading with predefined batches
 * - Error scenarios with exceptions
 * - Empty results
 * 
 * @param eventBatches List of event batches to emit. Each list represents a batch of events.
 * @param exception Optional exception to throw when readEvents() is called
 */
class MockEventReader(
  private val eventBatches: List<List<LabeledEvent<DynamicMessage>>> = emptyList(),
  private val exception: Exception? = null
) : EventReader {

  override suspend fun readEvents(): Flow<List<LabeledEvent<DynamicMessage>>> {
    exception?.let { throw it }
    return flowOf(*eventBatches.toTypedArray())
  }
  
  companion object {
    /**
     * Creates a MockEventReader that returns empty results.
     */
    fun empty(): MockEventReader = MockEventReader()
    
    /**
     * Creates a MockEventReader that throws an exception when readEvents() is called.
     */
    fun withException(exception: Exception): MockEventReader = MockEventReader(exception = exception)
    
    /**
     * Creates a MockEventReader with a single batch containing the specified number of mock events.
     */
    fun withEventCount(eventCount: Int): MockEventReader {
      val events = (1..eventCount).map { createMockLabeledEvent("event-$it") }
      return MockEventReader(listOf(events))
    }
    
    /**
     * Creates a MockEventReader with multiple batches.
     */
    fun withBatches(vararg batchSizes: Int): MockEventReader {
      val batches = batchSizes.mapIndexed { batchIndex, size ->
        (1..size).map { eventIndex -> 
          createMockLabeledEvent("batch-$batchIndex-event-$eventIndex") 
        }
      }
      return MockEventReader(batches)
    }
    
    /**
     * Creates a mock LabeledEvent for testing.
     */
    private fun createMockLabeledEvent(id: String): LabeledEvent<DynamicMessage> {
      // Create a minimal DynamicMessage for testing
      val mockMessage = DynamicMessage.getDefaultInstance(
        com.google.protobuf.Empty.getDescriptor()
      ) as DynamicMessage
      
      return LabeledEvent(
        timestamp = Instant.now(),
        vid = id.hashCode().toLong(),
        message = mockMessage,
        eventGroupReferenceId = "test-group-$id"
      )
    }
  }
}

/**
 * Mock implementation of EventReaderFactory for testing purposes.
 * 
 * Allows configuring different EventReader instances for different paths
 * or providing a default EventReader for all requests.
 */
class MockEventReaderFactory(
  private val defaultEventReader: EventReader? = null,
  private val eventReaderMap: Map<Pair<String, String>, EventReader> = emptyMap()
) : EventReaderFactory {

  override fun createEventReader(blobPath: String, metadataPath: String): EventReader {
    return eventReaderMap[Pair(blobPath, metadataPath)] 
      ?: defaultEventReader 
      ?: MockEventReader.empty()
  }
  
  companion object {
    /**
     * Creates a factory that always returns the same EventReader.
     */
    fun withDefault(eventReader: EventReader): MockEventReaderFactory {
      return MockEventReaderFactory(defaultEventReader = eventReader)
    }
    
    /**
     * Creates a factory with path-specific EventReader mappings.
     */
    fun withMappings(mappings: Map<Pair<String, String>, EventReader>): MockEventReaderFactory {
      return MockEventReaderFactory(eventReaderMap = mappings)
    }
  }
}