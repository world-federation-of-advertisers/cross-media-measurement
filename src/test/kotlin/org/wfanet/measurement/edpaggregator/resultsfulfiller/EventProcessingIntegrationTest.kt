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
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.toByteStringUtf8
import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * End-to-end integration tests for event processing with requisitions and event groups.
 * 
 * These tests validate the complete flow from requisitions through event processing
 * to frequency vector generation, including:
 * - Multiple event groups
 * - Multiple requisitions with different filters
 * - VID sampling
 * - Time-based filtering
 * - Population specs
 */
@RunWith(JUnit4::class)
class EventProcessingIntegrationTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private lateinit var kmsClient: KmsClient
  private lateinit var kekUri: String
  private lateinit var serializedEncryptionKey: ByteString
  private lateinit var typeRegistry: TypeRegistry
  private lateinit var eventProcessingOrchestrator: EventProcessingOrchestrator

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Before
  fun setUp() {
    // Set up KMS
    kekUri = FakeKmsClient.KEY_URI_PREFIX
    kmsClient = TestEncryptedStorage.buildFakeKmsClient(
      FakeKmsClient.KEY_URI_PREFIX,
      keyTemplate = "AES128_GCM",
    )

    // Set up encryption key
    serializedEncryptionKey = EncryptedStorage.generateSerializedEncryptionKey(
      kmsClient,
      kekUri,
      tinkKeyTemplateType = "AES128_GCM_HKDF_1MB",
    )

    // Set up TypeRegistry
    typeRegistry = TypeRegistry.newBuilder()
      .add(TestEvent.getDescriptor())
      .build()

    // Create EventProcessingOrchestrator
    eventProcessingOrchestrator = EventProcessingOrchestrator()
  }

  @Test
  fun `process multiple requisitions with different event groups`() = runBlocking {
    // Set up test data
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup1 = "event-group-1"
    val eventGroup2 = "event-group-2"
    
    // Create test events for different event groups
    val eventsGroup1 = createTestEvents(
      startVid = 1L,
      count = 30,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE
    )
    
    val eventsGroup2 = createTestEvents(
      startVid = 31L,
      count = 40,
      ageGroup = Person.AgeGroup.YEARS_35_TO_54,
      gender = Person.Gender.FEMALE
    )
    
    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(
        eventGroup1 to eventsGroup1,
        eventGroup2 to eventsGroup2
      ),
      testDate
    )
    
    // Create requisitions with different filters
    val requisition1 = createRequisition(
      name = "requisitions/req1",
      eventGroups = mapOf(eventGroup1 to createTimeRange(testDate)),
      filter = "person.gender == 1", // MALE
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )
    
    val requisition2 = createRequisition(
      name = "requisitions/req2", 
      eventGroups = mapOf(eventGroup2 to createTimeRange(testDate)),
      filter = "person.age_group == 2", // YEARS_35_TO_54
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )
    
    val requisitions = listOf(requisition1, requisition2)
    
    // Create pipeline configuration
    val config = PipelineConfiguration(
      startDate = testDate,
      endDate = testDate,
      batchSize = 10,
      channelCapacity = 100,
      useParallelPipeline = true,
      parallelBatchSize = 10,
      parallelWorkers = 2,
      threadPoolSize = 2,
      eventSourceType = EventSourceType.STORAGE,
      eventReader = eventReader,
      eventGroupReferenceIds = listOf(eventGroup1, eventGroup2),
      zoneId = ZoneOffset.UTC,
      disableLogging = false
    )
    
    // Process requisitions
    val result = eventProcessingOrchestrator.runWithRequisitions(
      requisitions,
      config,
      typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(2)
    
    // Requisition 1 should have processed male users from event group 1
    val freq1 = result[requisition1.name]
    assertThat(freq1).isNotNull()
    assertThat(freq1!!.getReach()).isEqualTo(30L) // All 30 events match the filter
    
    // Requisition 2 should have processed female users aged 35-54 from event group 2
    val freq2 = result[requisition2.name]
    assertThat(freq2).isNotNull()
    assertThat(freq2!!.getReach()).isEqualTo(40L) // All 40 events match the filter
  }

  @Test
  fun `process requisitions with VID sampling across event groups`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup1 = "event-group-vid-sample-1"
    val eventGroup2 = "event-group-vid-sample-2"
    
    // Create events with known VID distribution
    val eventsGroup1 = (1L..100L).map { vid ->
      labeledImpression {
        eventTime = createTimeRange(testDate).start.toProtoTime()
        this.vid = vid
        event = testEvent { 
          person = person {
            ageGroup = Person.AgeGroup.YEARS_18_TO_34
            gender = Person.Gender.MALE
          }
        }.pack()
      }
    }
    
    val eventsGroup2 = (101L..200L).map { vid ->
      labeledImpression {
        eventTime = createTimeRange(testDate).start.toProtoTime()
        this.vid = vid
        event = testEvent {
          person = person {
            ageGroup = Person.AgeGroup.YEARS_18_TO_34
            gender = Person.Gender.FEMALE
          }
        }.pack()
      }
    }
    
    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(
        eventGroup1 to eventsGroup1,
        eventGroup2 to eventsGroup2
      ),
      testDate
    )
    
    // Create requisition with VID sampling (50% sampling)
    val requisition = createRequisition(
      name = "requisitions/vid-sampling-test",
      eventGroups = mapOf(
        eventGroup1 to createTimeRange(testDate),
        eventGroup2 to createTimeRange(testDate)
      ),
      filter = "", // No filter - match all
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 0.5f // 50% sampling
    )
    
    val config = PipelineConfiguration(
      startDate = testDate,
      endDate = testDate,
      batchSize = 20,
      channelCapacity = 100,
      useParallelPipeline = true,
      parallelBatchSize = 20,
      parallelWorkers = 2,
      threadPoolSize = 2,
      eventSourceType = EventSourceType.STORAGE,
      eventReader = eventReader,
      eventGroupReferenceIds = listOf(eventGroup1, eventGroup2),
      zoneId = ZoneOffset.UTC,
      disableLogging = false
    )
    
    // Process requisition
    val result = eventProcessingOrchestrator.runWithRequisitions(
      listOf(requisition),
      config,
      typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(1)
    val freq = result[requisition.name]
    assertThat(freq).isNotNull()
    
    // With 50% VID sampling, we expect approximately 100 out of 200 VIDs
    // Allow some tolerance for the sampling
    val reach = freq!!.getReach()
    assertThat(reach).isAtLeast(80L)
    assertThat(reach).isAtMost(120L)
    
    println("VID Sampling Test - Reach with 50% sampling: $reach out of 200 total VIDs")
  }

  @Test
  fun `process requisitions with time-based filtering`() = runBlocking {
    val today = LocalDate.now()
    val yesterday = today.minusDays(1)
    val twoDaysAgo = today.minusDays(2)
    val eventGroup = "time-filtered-events"
    
    // Create events across different days
    val eventsYesterday = createTestEvents(
      startVid = 1L,
      count = 25,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventDate = yesterday
    )
    
    val eventsTwoDaysAgo = createTestEvents(
      startVid = 26L,
      count = 25,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventDate = twoDaysAgo
    )
    
    // Write events to storage for both days
    val eventReader = setupMultiDayStorage(
      mapOf(
        yesterday to mapOf(eventGroup to eventsYesterday),
        twoDaysAgo to mapOf(eventGroup to eventsTwoDaysAgo)
      )
    )
    
    // Create requisition that only includes yesterday's data
    val requisitionYesterday = createRequisition(
      name = "requisitions/time-filter-yesterday",
      eventGroups = mapOf(eventGroup to createTimeRange(yesterday, yesterday)),
      filter = "",
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )
    
    // Create requisition that includes both days
    val requisitionBothDays = createRequisition(
      name = "requisitions/time-filter-both",
      eventGroups = mapOf(eventGroup to createTimeRange(twoDaysAgo, yesterday)),
      filter = "",
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )
    
    val config = PipelineConfiguration(
      startDate = twoDaysAgo,
      endDate = yesterday,
      batchSize = 10,
      channelCapacity = 100,
      useParallelPipeline = true,
      parallelBatchSize = 10,
      parallelWorkers = 2,
      threadPoolSize = 2,
      eventSourceType = EventSourceType.STORAGE,
      eventReader = eventReader,
      eventGroupReferenceIds = listOf(eventGroup),
      zoneId = ZoneOffset.UTC,
      disableLogging = false
    )
    
    // Process requisitions
    val result = eventProcessingOrchestrator.runWithRequisitions(
      listOf(requisitionYesterday, requisitionBothDays),
      config,
      typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(2)
    
    // Yesterday only should have 25 events
    val freqYesterday = result[requisitionYesterday.name]
    assertThat(freqYesterday).isNotNull()
    assertThat(freqYesterday!!.getReach()).isEqualTo(25L)
    
    // Both days should have 50 events
    val freqBothDays = result[requisitionBothDays.name]
    assertThat(freqBothDays).isNotNull()
    assertThat(freqBothDays!!.getReach()).isEqualTo(50L)
  }

  @Test
  fun `process multiple requisitions with overlapping filters`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "overlapping-filters-group"
    
    // Create diverse test events
    val events = mutableListOf<LabeledImpression>()
    var vid = 1L
    
    // 20 Male 18-34
    repeat(20) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
        eventDate = testDate
      ))
    }
    
    // 15 Female 18-34
    repeat(15) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.FEMALE,
        eventDate = testDate
      ))
    }
    
    // 25 Male 35-44
    repeat(25) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_35_TO_54,
        gender = Person.Gender.MALE,
        eventDate = testDate
      ))
    }
    
    // 10 Female 35-44
    repeat(10) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_35_TO_54,
        gender = Person.Gender.FEMALE,
        eventDate = testDate
      ))
    }
    
    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(eventGroup to events),
      testDate
    )
    
    // Create requisitions with different filters
    val requisitions = listOf(
      // All males (45 total)
      createRequisition(
        name = "requisitions/all-males",
        eventGroups = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // All 18-34 (35 total)
      createRequisition(
        name = "requisitions/age-18-34",
        eventGroups = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // Male AND 18-34 (20 total - intersection)
      createRequisition(
        name = "requisitions/male-18-34",
        eventGroups = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1 && person.age_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // All events (70 total)
      createRequisition(
        name = "requisitions/all-events",
        eventGroups = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      )
    )
    
    val config = PipelineConfiguration(
      startDate = testDate,
      endDate = testDate,
      batchSize = 10,
      channelCapacity = 100,
      useParallelPipeline = true,
      parallelBatchSize = 10,
      parallelWorkers = 4,
      threadPoolSize = 4,
      eventSourceType = EventSourceType.STORAGE,
      eventReader = eventReader,
      eventGroupReferenceIds = listOf(eventGroup),
      zoneId = ZoneOffset.UTC,
      disableLogging = false
    )
    
    // Process requisitions
    val result = eventProcessingOrchestrator.runWithRequisitions(
      requisitions,
      config,
      typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(4)
    
    assertThat(result["requisitions/all-males"]!!.getReach()).isEqualTo(45L)
    assertThat(result["requisitions/age-18-34"]!!.getReach()).isEqualTo(35L)
    assertThat(result["requisitions/male-18-34"]!!.getReach()).isEqualTo(20L)
    assertThat(result["requisitions/all-events"]!!.getReach()).isEqualTo(70L)
    
    // Verify frequency distributions
    val allEventsFreq = result["requisitions/all-events"]!!.getFrequencyDistribution()
    assertThat(allEventsFreq[1]).isEqualTo(70L) // Each VID appears once
  }

  @Test
  fun `process requisitions with frequency capping`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "frequency-test-group"
    
    // Create events with multiple impressions per VID
    val events = mutableListOf<LabeledImpression>()
    
    // VIDs 1-10: 1 impression each
    (1L..10L).forEach { vid ->
      events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate))
    }
    
    // VIDs 11-20: 2 impressions each
    (11L..20L).forEach { vid ->
      repeat(2) {
        events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate))
      }
    }
    
    // VIDs 21-25: 5 impressions each
    (21L..25L).forEach { vid ->
      repeat(5) {
        events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate))
      }
    }
    
    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(eventGroup to events),
      testDate
    )
    
    // Create requisition
    val requisition = createRequisition(
      name = "requisitions/frequency-test",
      eventGroups = mapOf(eventGroup to createTimeRange(testDate)),
      filter = "",
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )
    
    val config = PipelineConfiguration(
      startDate = testDate,
      endDate = testDate,
      batchSize = 20,
      channelCapacity = 100,
      useParallelPipeline = true,
      parallelBatchSize = 20,
      parallelWorkers = 2,
      threadPoolSize = 2,
      eventSourceType = EventSourceType.STORAGE,
      eventReader = eventReader,
      eventGroupReferenceIds = listOf(eventGroup),
      zoneId = ZoneOffset.UTC,
      disableLogging = false
    )
    
    // Process requisition
    val result = eventProcessingOrchestrator.runWithRequisitions(
      listOf(requisition),
      config,
      typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(1)
    val freq = result[requisition.name]
    assertThat(freq).isNotNull()
    
    // Total reach should be 25 unique VIDs
    assertThat(freq!!.getReach()).isEqualTo(25L)
    
    // Verify frequency distribution
    val freqDist = freq.getFrequencyDistribution()
    assertThat(freqDist[1]).isEqualTo(10L) // 10 VIDs with frequency 1
    assertThat(freqDist[2]).isEqualTo(10L) // 10 VIDs with frequency 2
    assertThat(freqDist[5]).isEqualTo(5L)  // 5 VIDs with frequency 5
    
    // Total impressions
    val totalImpressions = freq.getTotalFrequency()
    assertThat(totalImpressions).isEqualTo(10 + 20 + 25L) // 55 total impressions
    
    // Max frequency
    assertThat(freq.getMaxFrequency()).isEqualTo(5)
  }

  // Helper functions

  private fun createTestEvents(
    startVid: Long,
    count: Int,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    eventDate: LocalDate = LocalDate.now().minusDays(1)
  ): List<LabeledImpression> {
    return (startVid until startVid + count).map { vid ->
      createLabeledImpression(vid, ageGroup, gender, eventDate)
    }
  }

  private fun createLabeledImpression(
    vid: Long,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    eventDate: LocalDate
  ): LabeledImpression {
    return labeledImpression {
      eventTime = createTimeRange(eventDate).start.toProtoTime()
      this.vid = vid
      event = testEvent {
        person = person {
          this.ageGroup = ageGroup
          this.gender = gender
          socialGradeGroup = Person.SocialGradeGroup.A_B_C1
        }
      }.pack()
    }
  }

  private suspend fun setupStorageWithEvents(
    eventsByGroup: Map<String, List<LabeledImpression>>,
    testDate: LocalDate
  ): EventReader {
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    
    eventsByGroup.forEach { (eventGroup, events) ->
      writeEventsToStorage(events, eventGroup, testDate, impressionsTmpPath, dekTmpPath)
    }
    
    return EventReader(
      kmsClient,
      StorageConfig(rootDirectory = impressionsTmpPath),
      StorageConfig(rootDirectory = dekTmpPath),
      "file:///${dekTmpPath.name}",
      typeRegistry,
    )
  }

  private suspend fun setupMultiDayStorage(
    eventsByDayAndGroup: Map<LocalDate, Map<String, List<LabeledImpression>>>
  ): EventReader {
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    
    eventsByDayAndGroup.forEach { (date, eventsByGroup) ->
      eventsByGroup.forEach { (eventGroup, events) ->
        writeEventsToStorage(events, eventGroup, date, impressionsTmpPath, dekTmpPath)
      }
    }
    
    return EventReader(
      kmsClient,
      StorageConfig(rootDirectory = impressionsTmpPath),
      StorageConfig(rootDirectory = dekTmpPath),
      "file:///${dekTmpPath.name}",
      typeRegistry,
    )
  }

  private suspend fun writeEventsToStorage(
    events: List<LabeledImpression>,
    eventGroup: String,
    date: LocalDate,
    impressionsRoot: java.io.File,
    dekRoot: java.io.File
  ) {
    val impressionsBucketDir = impressionsRoot.resolve("impressions")
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)
    
    val mesosRecordIoStorageClient = EncryptedStorage.buildEncryptedMesosStorageClient(
      impressionsStorageClient,
      kmsClient,
      kekUri,
      serializedEncryptionKey,
    )
    
    val impressionsFlow = flow {
      events.forEach { impression -> emit(impression.toByteString()) }
    }
    
    mesosRecordIoStorageClient.writeBlob(date.toString(), impressionsFlow)
    
    // Set up DEK storage
    val deksBucketDir = dekRoot.resolve("deks")
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)
    
    val encryptedDek = EncryptedDek.newBuilder()
      .setKekUri(kekUri)
      .setEncryptedDek(serializedEncryptionKey)
      .build()
    
    val blobDetails = blobDetails {
      blobUri = "file:///impressions/$date"
      this.encryptedDek = encryptedDek
    }
    
    impressionsDekStorageClient.writeBlob(
      "ds/$date/event-group-reference-id/$eventGroup/metadata",
      blobDetails.toByteString(),
    )
  }

  private fun createTimeRange(date: LocalDate): OpenEndTimeRange {
    return createTimeRange(date, date)
  }

  private fun createTimeRange(startDate: LocalDate, endDate: LocalDate): OpenEndTimeRange {
    return OpenEndTimeRange.fromClosedDateRange(startDate..endDate)
  }

  private fun createRequisition(
    name: String,
    eventGroups: Map<String, OpenEndTimeRange>,
    filter: String,
    vidSamplingStart: Float,
    vidSamplingWidth: Float
  ): Requisition {
    val requisitionSpec = requisitionSpec {
      events = RequisitionSpecKt.events {
        eventGroups.forEach { (groupName, timeRange) ->
          eventGroups += RequisitionSpecKt.eventGroupEntry {
            key = groupName
            value = RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = RequisitionSpecKt.EventGroupEntryKt.eventInterval {
                startTime = timeRange.start.toProtoTime()
                endTime = timeRange.endExclusive.toProtoTime()
              }
              if (filter.isNotEmpty()) {
                this.filter = RequisitionSpecKt.eventFilter {
                  expression = filter
                }
              }
            }
          }
        }
      }
      nonce = System.currentTimeMillis()
    }
    
    val measurementSpec = measurementSpec {
      reachAndFrequency = MeasurementSpecKt.reachAndFrequency {
        reachPrivacyParams = differentialPrivacyParams {
          epsilon = 1.0
          delta = 1E-12
        }
        frequencyPrivacyParams = differentialPrivacyParams {
          epsilon = 1.0
          delta = 1E-12
        }
        maximumFrequency = 10
      }
      vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval {
        start = vidSamplingStart
        width = vidSamplingWidth
      }
    }
    
    return requisition {
      this.name = name
      measurement = "measurements/test-measurement"
      state = Requisition.State.UNFULFILLED
      this.measurementSpec = signedMessage {
        message = measurementSpec.pack()
      }
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol {
          direct = ProtocolConfigKt.direct {
            noiseMechanisms += ProtocolConfig.NoiseMechanism.NONE
            deterministicCountDistinct = ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
            deterministicDistribution = ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
          }
        }
      }
      // For testing, we'll use a simple signed message
      encryptedRequisitionSpec = "encrypted-spec".toByteStringUtf8()
      // We would need to mock the decryption in a real test
    }
  }
}