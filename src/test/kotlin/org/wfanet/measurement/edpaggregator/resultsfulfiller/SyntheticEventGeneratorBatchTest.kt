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
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import java.time.Instant

@RunWith(JUnit4::class)
class SyntheticEventGeneratorBatchTest {

  private fun createTestPopulationSpec(): SyntheticPopulationSpec {
    return SyntheticPopulationSpec.newBuilder().apply {
      vidRangeBuilder.apply {
        start = 1L
        endExclusive = 100L
      }
      eventMessageTypeUrl = "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      
      // Create a simple sub-population
      addSubPopulations(SyntheticPopulationSpec.SubPopulation.newBuilder().apply {
        vidSubRangeBuilder.apply {
          start = 1L
          endExclusive = 101L
        }
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
            day = 2
          }
        }
        
        // Add frequency spec
        addFrequencySpecs(SyntheticEventGroupSpec.FrequencySpec.newBuilder().apply {
          frequency = 1L
          addVidRangeSpecs(SyntheticEventGroupSpec.FrequencySpec.VidRangeSpec.newBuilder().apply {
            vidRangeBuilder.apply {
              start = 1L
              endExclusive = 50L
            }
            samplingRate = 1.0
          }.build())
        }.build())
      }.build())
    }.build()
  }

  @Test
  fun generateEventBatchesBasicTest(): Unit = runBlocking {
    val batchSize = 5
    
    val generator = SyntheticEventGenerator(
      populationSpec = createTestPopulationSpec(),
      eventGroupSpec = createTestEventGroupSpec(),
      batchSize = batchSize
    )

    val batches = generator.generateEventBatches(Dispatchers.Default).toList()
    val allEvents = batches.flatten()
    
    // Should have at least some events
    assertThat(allEvents).isNotEmpty()
    
    // Should have at least 1 batch
    assertThat(batches.size).isAtLeast(1)
    
    // Each batch should be non-empty and not exceed batch size
    batches.forEach { batch ->
      assertThat(batch.size).isAtMost(batchSize)
      assertThat(batch.size).isAtLeast(1)
    }
  }
}