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
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent

@RunWith(JUnit4::class)
class EventProcessingOrchestratorTest {

  private val testTypeRegistry: TypeRegistry = TypeRegistry.newBuilder()
    .add(TestEvent.getDescriptor())
    .build()

  private fun createTestPopulationSpec(): SyntheticPopulationSpec {
    return SyntheticPopulationSpec.newBuilder().apply {
      vidRangeBuilder.apply {
        start = 1L
        endExclusive = 101L
      }
      eventMessageTypeUrl = "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      addPopulationFields("person.gender")
      addPopulationFields("person.age_group")
      
      // Male sub-population
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = 1L
          endExclusive = 51L
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
      
      // Female sub-population
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = 51L
          endExclusive = 101L
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
            year = 2025
            month = 1
            day = 1
          }
          endExclusiveBuilder.apply {
            year = 2025
            month = 1
            day = 8
          }
        }
        
        // Add frequency spec for male sub-population
        addFrequencySpecs(SyntheticEventGroupSpec.FrequencySpec.newBuilder().apply {
          frequency = 1L
          addVidRangeSpecs(SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.newBuilder().apply {
            vidRangeBuilder.apply {
              start = 1L
              endExclusive = 51L
            }
            samplingRate = 1.0
          }.build())
        }.build())
        
        // Add frequency spec for female sub-population
        addFrequencySpecs(SyntheticEventGroupSpec.FrequencySpec.newBuilder().apply {
          frequency = 1L
          addVidRangeSpecs(SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.newBuilder().apply {
            vidRangeBuilder.apply {
              start = 51L
              endExclusive = 101L
            }
            samplingRate = 1.0
          }.build())
        }.build())
      }.build())
    }.build()
  }

  @Test
  fun `orchestrator runs pipeline with single-threaded configuration`(): Unit = runBlocking {
    val config = PipelineConfiguration(
      startDate = LocalDate.of(2025, 1, 1),
      endDate = LocalDate.of(2025, 1, 7),
      batchSize = 10,
      channelCapacity = 100,
      useParallelPipeline = false,
      parallelBatchSize = 10,
      parallelWorkers = 2,
      threadPoolSize = 4,
      eventSourceType = EventSourceType.SYNTHETIC,
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      zoneId = ZoneId.of("UTC")
    )
    
    val orchestrator = EventProcessingOrchestrator()
    
    // This should complete without throwing
    orchestrator.run(config, testTypeRegistry)
    
    // If we reach here, the test passed
    assertThat(true).isTrue()
  }

  @Test
  fun `orchestrator runs pipeline with parallel configuration`(): Unit = runBlocking {
    val config = PipelineConfiguration(
      startDate = LocalDate.of(2025, 1, 1),
      endDate = LocalDate.of(2025, 1, 7),
      batchSize = 10,
      channelCapacity = 100,
      useParallelPipeline = true,
      parallelBatchSize = 20,
      parallelWorkers = 3,
      threadPoolSize = 6,
      eventSourceType = EventSourceType.SYNTHETIC,
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      zoneId = ZoneId.of("UTC")
    )
    
    val orchestrator = EventProcessingOrchestrator()
    
    // This should complete without throwing
    orchestrator.run(config, testTypeRegistry)
    
    // If we reach here, the test passed
    assertThat(true).isTrue()
  }

  @Test
  fun `orchestrator handles empty event generation`(): Unit = runBlocking {
    // Create specs that won't generate any events
    val emptyPopulationSpec = SyntheticPopulationSpec.newBuilder().apply {
      vidRangeBuilder.apply {
        start = 1L
        endExclusive = 2L // Only one VID
      }
      eventMessageTypeUrl = "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
    }.build()
    
    val emptyEventGroupSpec = SyntheticEventGroupSpec.newBuilder().apply {
      description = "Empty event group spec"
      samplingNonce = 12345L
      
      addDateSpecs(SyntheticEventGroupSpec.DateSpec.newBuilder().apply {
        dateRangeBuilder.apply {
          startBuilder.apply {
            year = 2025
            month = 1
            day = 1
          }
          endExclusiveBuilder.apply {
            year = 2025
            month = 1
            day = 2
          }
        }
        // No frequency specs, so no events will be generated
      }.build())
    }.build()
    
    val config = PipelineConfiguration(
      startDate = LocalDate.of(2025, 1, 1),
      endDate = LocalDate.of(2025, 1, 1),
      batchSize = 5,
      channelCapacity = 50,
      useParallelPipeline = true,
      parallelBatchSize = 10,
      parallelWorkers = 2,
      threadPoolSize = 4,
      eventSourceType = EventSourceType.SYNTHETIC,
      populationSpec = emptyPopulationSpec,
      eventGroupSpec = emptyEventGroupSpec,
      zoneId = ZoneId.of("UTC")
    )
    
    val orchestrator = EventProcessingOrchestrator()
    
    // Should handle empty event generation gracefully
    orchestrator.run(config, testTypeRegistry)
    
    assertThat(true).isTrue()
  }

  @Test
  fun `orchestrator validates configuration`(): Unit = runBlocking {
    val invalidConfig = PipelineConfiguration(
      startDate = LocalDate.of(2025, 1, 7),
      endDate = LocalDate.of(2025, 1, 1), // End before start
      batchSize = 5,
      channelCapacity = 50,
      useParallelPipeline = true,
      parallelBatchSize = 10,
      parallelWorkers = 2,
      threadPoolSize = 4,
      eventSourceType = EventSourceType.SYNTHETIC,
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      zoneId = ZoneId.of("UTC")
    )
    
    val orchestrator = EventProcessingOrchestrator()
    
    try {
      orchestrator.run(invalidConfig, testTypeRegistry)
      assertThat(false).isTrue() // Should not reach here
    } catch (e: IllegalArgumentException) {
      assertThat(e.message).contains("End date must be after or equal to start date")
    }
  }

  @Test
  fun `orchestrator handles different batch sizes correctly`(): Unit = runBlocking {
    val smallBatchConfig = PipelineConfiguration(
      startDate = LocalDate.of(2025, 1, 1),
      endDate = LocalDate.of(2025, 1, 2),
      batchSize = 5,
      channelCapacity = 50,
      useParallelPipeline = true,
      parallelBatchSize = 5,
      parallelWorkers = 2,
      threadPoolSize = 4,
      eventSourceType = EventSourceType.SYNTHETIC,
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      zoneId = ZoneId.of("UTC")
    )
    
    val largeBatchConfig = smallBatchConfig.copy(
      parallelBatchSize = 50,
      batchSize = 50
    )
    
    val orchestrator = EventProcessingOrchestrator()
    
    // Both should complete successfully
    orchestrator.run(smallBatchConfig, testTypeRegistry)
    orchestrator.run(largeBatchConfig, testTypeRegistry)
    
    assertThat(true).isTrue()
  }
}