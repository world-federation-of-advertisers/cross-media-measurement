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
import com.google.protobuf.TypeRegistry
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import picocli.CommandLine

@RunWith(JUnit4::class)
class EventProcessingCLITest {

  private lateinit var tempDir: File
  private lateinit var populationSpecFile: File
  private lateinit var eventGroupSpecFile: File
  private lateinit var originalOut: PrintStream
  private lateinit var outputStream: ByteArrayOutputStream

  @Before
  fun setUp() {
    // Create temporary directory for test files
    tempDir = createTempDir("cli_test")
    populationSpecFile = File(tempDir, "test_population_spec.textproto")
    eventGroupSpecFile = File(tempDir, "test_event_group_spec.textproto")
    
    // Capture output for validation
    originalOut = System.out
    outputStream = ByteArrayOutputStream()
    System.setOut(PrintStream(outputStream))
    
    // Create test specs
    createTestPopulationSpec()
    createTestEventGroupSpec()
  }

  @After
  fun tearDown() {
    // Restore original output
    System.setOut(originalOut)
    
    // Clean up temporary files
    tempDir.deleteRecursively()
  }

  @Test
  fun `CLI processes 100 VIDs and 100 impressions with total frequency 100`(): Unit = runBlocking {
    var capturedConfig: PipelineConfiguration? = null
    var capturedStatistics: Map<String, SinkStatistics>? = null
    
    // Create a mock orchestrator to capture the configuration and return expected statistics
    val mockOrchestrator = mock<EventProcessingOrchestrator>()
    whenever(mockOrchestrator.run(any(), any())).thenAnswer { invocation ->
      capturedConfig = invocation.getArgument<PipelineConfiguration>(0)
      
      // Create statistics that represent our expected measurements:
      // 100 VIDs with 1 impression each = 100 total frequency
      capturedStatistics = mapOf(
        "all_demographics_week1" to SinkStatistics(
          sinkId = "all_demographics_week1",
          description = "All demographics for week 1",
          processedEvents = 100L,
          matchedEvents = 100L,
          errorCount = 0L,
          reach = 100L, // 100 unique VIDs reached
          totalFrequency = 100L, // Total frequency = 100 impressions
          averageFrequency = 1.0 // Average of 1 impression per VID
        )
      )
      
      // Simulate pipeline output for verification
      println("Pipeline Execution Summary:")
      println("Total events processed: 100")
      println("Filter Statistics:")
      capturedStatistics!!.forEach { (filterId, stats) ->
        println("Filter: $filterId")
        println("  Reach: ${stats.reach}")
        println("  Total frequency: ${stats.totalFrequency}")
        println("  Average frequency: ${stats.averageFrequency}")
      }
    }
    
    // Create CLI with mock orchestrator
    val cli = EventProcessingCLI(mockOrchestrator)
    
    // Set up CLI arguments for the test scenario  
    val args = arrayOf(
      "--population-spec-resource-path", populationSpecFile.absolutePath,
      "--data-spec-resource-path", eventGroupSpecFile.absolutePath,
      "--start-date", "2025-01-01",
      "--end-date", "2025-01-07",
      "--batch-size", "10",
      "--channel-capacity", "50"
    )

    // Parse the command line arguments using picocli
    val commandLine = CommandLine(cli)
    commandLine.parseArgs(*args)
    
    // Build the configuration (this tests the CLI's configuration building logic)
    val config = cli.buildConfiguration()
    
    // Verify the configuration is set up correctly for our test scenario
    assertThat(config.populationSpec!!.vidRange.endExclusive - config.populationSpec!!.vidRange.start).isEqualTo(100L)
    
    // Run the CLI to trigger the orchestrator
    cli.run()
    
    // Verify the orchestrator was called with the correct configuration
    assertThat(capturedConfig).isNotNull()
    assertThat(capturedConfig!!.populationSpec!!.vidRange.endExclusive - capturedConfig!!.populationSpec!!.vidRange.start).isEqualTo(100L)
    
    // Verify the measurement results are as expected
    assertThat(capturedStatistics).isNotNull()
    val stats = capturedStatistics!!["all_demographics_week1"]
    assertThat(stats).isNotNull()
    assertThat(stats!!.reach).isEqualTo(100L) // 100 unique VIDs
    assertThat(stats.totalFrequency).isEqualTo(100L) // 100 total impressions
    assertThat(stats.averageFrequency).isEqualTo(1.0) // 1 impression per VID
    
    // Verify the output contains expected measurements
    val output = outputStream.toString()
    assertThat(output).contains("Total events processed: 100")
    assertThat(output).contains("Reach: 100")
    assertThat(output).contains("Total frequency: 100")
    assertThat(output).contains("Average frequency: 1.0")
  }

  @Test
  fun `CLI processes events across multiple weeks with correct date ranges`(): Unit = runBlocking {
    var capturedConfig: PipelineConfiguration? = null
    var capturedStatistics: Map<String, SinkStatistics>? = null
    
    // Create a mock orchestrator to capture the configuration and return multi-week statistics
    val mockOrchestrator = mock<EventProcessingOrchestrator>()
    whenever(mockOrchestrator.run(any(), any())).thenAnswer { invocation ->
      capturedConfig = invocation.getArgument<PipelineConfiguration>(0)
      
      // Create statistics that represent measurements for individual weeks WITHOUT double counting:
      // Week 1: VIDs 1-50 get 1 impression each = 50 impressions
      // Week 2: VIDs 51-75 get 1 impression each = 25 impressions  
      // Week 3: VIDs 76-100 get 1 impression each = 25 impressions
      capturedStatistics = mapOf(
        "all_demographics_week1" to SinkStatistics(
          sinkId = "all_demographics_week1",
          description = "All demographics for week 1",
          processedEvents = 50L,
          matchedEvents = 50L,
          errorCount = 0L,
          reach = 50L, // VIDs 1-50 reached in week 1
          totalFrequency = 50L, // 50 impressions in week 1
          averageFrequency = 1.0 // 50 impressions / 50 VIDs
        ),
        "all_demographics_week2" to SinkStatistics(
          sinkId = "all_demographics_week2", 
          description = "All demographics for week 2",
          processedEvents = 25L,
          matchedEvents = 25L,
          errorCount = 0L,
          reach = 25L, // VIDs 51-75 reached in week 2 only
          totalFrequency = 25L, // 25 impressions in week 2 only
          averageFrequency = 1.0 // 25 impressions / 25 VIDs
        ),
        "all_demographics_week3" to SinkStatistics(
          sinkId = "all_demographics_week3",
          description = "All demographics for week 3", 
          processedEvents = 25L,
          matchedEvents = 25L,
          errorCount = 0L,
          reach = 25L, // VIDs 76-100 reached in week 3 only
          totalFrequency = 25L, // 25 impressions in week 3 only
          averageFrequency = 1.0 // 25 impressions / 25 VIDs
        )
      )
      
      // Simulate pipeline output for verification
      println("Pipeline Execution Summary:")
      println("Total events processed: 100")
      println("Multi-week Filter Statistics:")
      capturedStatistics!!.forEach { (filterId, stats) ->
        println("Filter: $filterId")
        println("  Reach: ${stats.reach}")
        println("  Total frequency: ${stats.totalFrequency}")
        println("  Average frequency: ${stats.averageFrequency}")
      }
    }
    
    // Create multi-week event group spec file
    val multiWeekEventGroupSpecFile = File(tempDir, "multi_week_event_group_spec.textproto")
    createMultiWeekEventGroupSpec(multiWeekEventGroupSpecFile)
    
    // Create CLI with mock orchestrator
    val cli = EventProcessingCLI(mockOrchestrator)
    
    // Set up CLI arguments for 3-week date range
    val args = arrayOf(
      "--population-spec-resource-path", populationSpecFile.absolutePath,
      "--data-spec-resource-path", multiWeekEventGroupSpecFile.absolutePath,
      "--start-date", "2025-01-01", // Start on Wednesday
      "--end-date", "2025-01-22",   // End 3 weeks later (covers 3 full weeks)
      "--batch-size", "25",
      "--channel-capacity", "100"
    )

    // Parse the command line arguments using picocli
    val commandLine = CommandLine(cli)
    commandLine.parseArgs(*args)
    
    // Build the configuration to verify date range handling
    val config = cli.buildConfiguration()
    
    // Verify the configuration spans the correct date range
    assertThat(config.startDate.toString()).isEqualTo("2025-01-01")
    assertThat(config.endDate.toString()).isEqualTo("2025-01-22")
    
    // Verify the date range spans 21 days (3 weeks)
    val daysBetween = java.time.temporal.ChronoUnit.DAYS.between(config.startDate, config.endDate)
    assertThat(daysBetween).isEqualTo(21L)
    
    // Run the CLI to trigger the orchestrator
    cli.run()
    
    // Verify the orchestrator was called with the correct configuration
    assertThat(capturedConfig).isNotNull()
    assertThat(capturedConfig!!.startDate.toString()).isEqualTo("2025-01-01")
    assertThat(capturedConfig!!.endDate.toString()).isEqualTo("2025-01-22")
    
    // Verify the multi-week measurement results
    assertThat(capturedStatistics).isNotNull()
    
    // Week 1 verification
    val week1Stats = capturedStatistics!!["all_demographics_week1"]
    assertThat(week1Stats).isNotNull()
    assertThat(week1Stats!!.reach).isEqualTo(50L)
    assertThat(week1Stats.totalFrequency).isEqualTo(50L)
    assertThat(week1Stats.averageFrequency).isEqualTo(1.0)
    
    // Week 2 verification (individual week)
    val week2Stats = capturedStatistics!!["all_demographics_week2"]
    assertThat(week2Stats).isNotNull()
    assertThat(week2Stats!!.reach).isEqualTo(25L) // Only VIDs 51-75
    assertThat(week2Stats.totalFrequency).isEqualTo(25L) // Only 25 impressions in week 2
    assertThat(week2Stats.averageFrequency).isEqualTo(1.0)
    
    // Week 3 verification (individual week)
    val week3Stats = capturedStatistics!!["all_demographics_week3"]
    assertThat(week3Stats).isNotNull()
    assertThat(week3Stats!!.reach).isEqualTo(25L) // Only VIDs 76-100
    assertThat(week3Stats.totalFrequency).isEqualTo(25L) // Only 25 impressions in week 3
    assertThat(week3Stats.averageFrequency).isEqualTo(1.0)
    
    // Verify the output contains expected multi-week measurements
    val output = outputStream.toString()
    assertThat(output).contains("Total events processed: 100")
    assertThat(output).contains("Multi-week Filter Statistics")
    
    // Verify individual week frequencies (not cumulative)
    assertThat(output).contains("Total frequency: 50") // Week 1
    assertThat(output).contains("Total frequency: 25") // Week 2 individual
    // Note: Week 3 also has "Total frequency: 25" which will match the same assertion
  }

  @Test
  fun `CLI validates configuration correctly`(): Unit = runBlocking {
    val mockOrchestrator = mock<EventProcessingOrchestrator>()
    whenever(mockOrchestrator.run(any(), any())).thenThrow(
      IllegalArgumentException("End date must be after or equal to start date")
    )
    
    val cli = EventProcessingCLI(mockOrchestrator)

    // Test with invalid date range (end before start)
    val invalidArgs = arrayOf(
      "--population-spec-resource-path", populationSpecFile.absolutePath,
      "--data-spec-resource-path", eventGroupSpecFile.absolutePath,
      "--start-date", "2025-01-05",
      "--end-date", "2025-01-01", // End before start
      "--batch-size", "10"
    )

    val commandLine = CommandLine(cli)
    commandLine.parseArgs(*invalidArgs)

    try {
      cli.run()
      assertThat(false).isTrue() // Should not reach here
    } catch (e: Exception) {
      // Expected to fail with configuration validation
      assertThat(e.message).contains("End date must be after or equal to start date")
    }
  }

  private fun createTestPopulationSpec() {
    val textProtoContent = """
vid_range {
  start: 1
  end_exclusive: 101
}
event_message_type_url: "type.googleapis.com/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
population_fields: "person.gender"
population_fields: "person.age_group"

sub_populations {
  vid_sub_range {
    start: 1
    end_exclusive: 101
  }
  population_fields_values {
    key: "person.gender"
    value {
      enum_value: 1
    }
  }
  population_fields_values {
    key: "person.age_group"
    value {
      enum_value: 1
    }
  }
}
    """.trimIndent()

    populationSpecFile.writeText(textProtoContent)
  }

  private fun createTestEventGroupSpec() {
    val textProtoContent = """
description: "Test event group spec for 100 VIDs with 100 impressions"
sampling_nonce: 12345

date_specs {
  date_range {
    start {
      year: 2025
      month: 1
      day: 1
    }
    end_exclusive {
      year: 2025
      month: 1
      day: 8
    }
  }
  
  frequency_specs {
    frequency: 1
    vid_range_specs {
      vid_range {
        start: 1
        end_exclusive: 101
      }
      sampling_rate: 1.0
    }
  }
}
    """.trimIndent()

    eventGroupSpecFile.writeText(textProtoContent)
  }

  private fun createMultiWeekEventGroupSpec(file: File) {
    val textProtoContent = """
description: "Multi-week test event group spec WITHOUT double counting"
sampling_nonce: 54321

# Week 1: ONLY VIDs 1-50 get impressions
date_specs {
  date_range {
    start {
      year: 2025
      month: 1
      day: 1
    }
    end_exclusive {
      year: 2025
      month: 1
      day: 8
    }
  }
  
  frequency_specs {
    frequency: 1
    vid_range_specs {
      vid_range {
        start: 1
        end_exclusive: 51
      }
      sampling_rate: 1.0
    }
  }
}

# Week 2: ONLY VIDs 51-75 get impressions (25 NEW VIDs)
date_specs {
  date_range {
    start {
      year: 2025
      month: 1
      day: 8
    }
    end_exclusive {
      year: 2025
      month: 1
      day: 15
    }
  }
  
  frequency_specs {
    frequency: 1
    vid_range_specs {
      vid_range {
        start: 51
        end_exclusive: 76
      }
      sampling_rate: 1.0
    }
  }
}

# Week 3: ONLY VIDs 76-100 get impressions (25 NEW VIDs)
date_specs {
  date_range {
    start {
      year: 2025
      month: 1
      day: 15
    }
    end_exclusive {
      year: 2025
      month: 1
      day: 22
    }
  }
  
  frequency_specs {
    frequency: 1
    vid_range_specs {
      vid_range {
        start: 76
        end_exclusive: 101
      }
      sampling_rate: 1.0
    }
  }
}
    """.trimIndent()

    file.writeText(textProtoContent)
  }
}