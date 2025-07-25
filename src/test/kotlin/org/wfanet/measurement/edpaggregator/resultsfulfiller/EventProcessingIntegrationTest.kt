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
import org.junit.After
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
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.identity.externalIdToApiId
import java.security.SecureRandom
import java.nio.file.Paths
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.measurementconsumer.stats.VidSamplingInterval
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
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.CoroutineContext
import com.google.protobuf.DynamicMessage


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
  private val privateEncryptionKey = PRIVATE_ENCRYPTION_KEY
  private lateinit var eventProcessingOrchestrator: EventProcessingOrchestrator
  private val tempDirectories = mutableListOf<java.io.File>()

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @After
  fun tearDown() {
    // Clean up temporary directories
    tempDirectories.forEach { dir ->
      if (dir.exists()) {
        dir.deleteRecursively()
      }
    }
    tempDirectories.clear()
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
    eventProcessingOrchestrator = EventProcessingOrchestrator(privateEncryptionKey)
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
      gender = Person.Gender.MALE,
      eventGroupReferenceId = eventGroup1
    )

    val eventsGroup2 = createTestEvents(
      startVid = 31L,
      count = 40,
      ageGroup = Person.AgeGroup.YEARS_35_TO_54,
      gender = Person.Gender.FEMALE,
      eventGroupReferenceId = eventGroup2
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
      eventGroupsMap = mapOf(eventGroup1 to createTimeRange(testDate)),
      filter = "person.gender == 1", // MALE
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    val requisition2 = createRequisition(
      name = "requisitions/req2",
      eventGroupsMap = mapOf(eventGroup2 to createTimeRange(testDate)),
      filter = "person.age_group == 2", // YEARS_35_TO_54
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    val requisitions = listOf(requisition1, requisition2)

    // Create event source from event reader
    val eventSource = createEventSourceFromReader(eventReader, listOf(eventGroup1, eventGroup2), testDate, testDate)

    // Create pipeline configuration
    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 100,
      threadPoolSize = 2,
      workers = 2,
      eventSource = eventSource,
      populationSpecPath = TEST_DATA_RUNTIME_PATH.toString(),
      vidSamplingInterval = VidSamplingInterval(0.0, 1.0),
      disableLogging = false,
      isSyntheticPopulationSpec = true,
      maxFrequency = 10
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
    assertThat(freq1!!.getArray().sum()).isEqualTo(30)

    // Requisition 2 should have processed female users aged 35-54 from event group 2
    val freq2 = result[requisition2.name]
    assertThat(freq2).isNotNull()
    assertThat(freq2!!.getArray().sum()).isEqualTo(40L) // All 40 events match the filter
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
      eventDate = yesterday,
      eventGroupReferenceId = eventGroup
    )

    val eventsTwoDaysAgo = createTestEvents(
      startVid = 26L,
      count = 25,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventDate = twoDaysAgo,
      eventGroupReferenceId = eventGroup
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
      eventGroupsMap = mapOf(eventGroup to createTimeRange(yesterday, yesterday)),
      filter = "",
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    // Create requisition that includes both days
    val requisitionBothDays = createRequisition(
      name = "requisitions/time-filter-both",
      eventGroupsMap = mapOf(eventGroup to createTimeRange(twoDaysAgo, yesterday)),
      filter = "",
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    // Create event source from event reader
    val eventSource = createEventSourceFromReader(eventReader, listOf(eventGroup), twoDaysAgo, yesterday)

    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 100,
      threadPoolSize = 2,
      workers = 2,
      eventSource = eventSource,
      populationSpecPath = TEST_DATA_RUNTIME_PATH.toString(),
      vidSamplingInterval = VidSamplingInterval(0.0, 1.0),
      disableLogging = false,
      isSyntheticPopulationSpec = true,
      maxFrequency = 10
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
    assertThat(freqYesterday!!.getArray().sum()).isEqualTo(25L)

    // Both days should have 50 events
    val freqBothDays = result[requisitionBothDays.name]
    assertThat(freqBothDays).isNotNull()
    assertThat(freqBothDays!!.getArray().sum()).isEqualTo(50L)
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
        eventDate = testDate,
        eventGroupReferenceId = eventGroup
      ))
    }

    // 15 Female 18-34
    repeat(15) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.FEMALE,
        eventDate = testDate,
        eventGroupReferenceId = eventGroup
      ))
    }

    // 25 Male 35-44
    repeat(25) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_35_TO_54,
        gender = Person.Gender.MALE,
        eventDate = testDate,
        eventGroupReferenceId = eventGroup
      ))
    }

    // 10 Female 35-44
    repeat(10) {
      events.add(createLabeledImpression(
        vid = vid++,
        ageGroup = Person.AgeGroup.YEARS_35_TO_54,
        gender = Person.Gender.FEMALE,
        eventDate = testDate,
        eventGroupReferenceId = eventGroup
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
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // All 18-34 (35 total)
      createRequisition(
        name = "requisitions/age-18-34",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // Male AND 18-34 (20 total - intersection)
      createRequisition(
        name = "requisitions/male-18-34",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1 && person.age_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // All events (70 total)
      createRequisition(
        name = "requisitions/all-events",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      )
    )

    // Create event source from event reader
    val eventSource = createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)

    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 100,
      threadPoolSize = 4,
      workers = 4,
      eventSource = eventSource,
      populationSpecPath = TEST_DATA_RUNTIME_PATH.toString(),
      vidSamplingInterval = VidSamplingInterval(0.0, 1.0),
      disableLogging = false,
      isSyntheticPopulationSpec = true,
      maxFrequency = 10
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

    // Verify that we get the expected reach (frequency distribution not tested here)
    // Each VID appears once, so total reach equals total count
    assertThat(result["requisitions/all-events"]!!.getReach()).isEqualTo(70L)
  }

  @Test
  fun `process requisitions with frequency capping`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "frequency-test-group"

    // Create events with multiple impressions per VID
    val events = mutableListOf<LabeledImpression>()

    // VIDs 1-10: 1 impression each
    (1L..10L).forEach { vid ->
      events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate, eventGroup))
    }

    // VIDs 11-20: 2 impressions each
    (11L..20L).forEach { vid ->
      repeat(2) {
        events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate, eventGroup))
      }
    }

    // VIDs 21-25: 5 impressions each
    (21L..25L).forEach { vid ->
      repeat(5) {
        events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate, eventGroup))
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
      eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
      filter = "",
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    // Create event source from event reader
    val eventSource = createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)

    val config = PipelineConfiguration(
      batchSize = 20,
      channelCapacity = 100,
      threadPoolSize = 2,
      workers = 2,
      eventSource = eventSource,
      populationSpecPath = TEST_DATA_RUNTIME_PATH.toString(),
      vidSamplingInterval = VidSamplingInterval(0.0, 1.0),
      disableLogging = false,
      isSyntheticPopulationSpec = true,
      maxFrequency = 5,
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

    // Total impressions (with frequency capping at 5, should still be 10*1 + 10*2 + 5*5 = 55)
    // since no VID exceeds the cap of 5
    assertThat(freq.getTotalCount()).isEqualTo(55L)

    // Average frequency should be 55/25 = 2.2
    assertThat(freq.getAverageFrequency()).isWithin(0.01).of(2.2)
  }

  // Helper functions

  /**
   * Creates a simple test event source from an EventReader.
   */
  private fun createEventSourceFromReader(
    eventReader: EventReader,
    eventGroups: List<String>,
    startDate: LocalDate,
    endDate: LocalDate
  ): EventSource {
    return TestEventSource(eventReader, eventGroups, startDate, endDate)
  }

  /**
   * EventReader that combines multiple StorageEventReaders for different event groups.
   */
  private class MultiGroupEventReader(
    private val readers: List<EventReader>
  ) : EventReader {
    override suspend fun readEvents(): Flow<List<LabeledEvent<DynamicMessage>>> {
      return flow {
        // Read events from all readers and emit them
        readers.forEach { reader ->
          reader.readEvents().collect { eventList ->
            emit(eventList)
          }
        }
      }
    }
  }

  /**
   * Simple test implementation of EventSource that wraps an EventReader.
   */
  private class TestEventSource(
    private val eventReader: EventReader,
    private val eventGroups: List<String>,
    private val startDate: LocalDate,
    private val endDate: LocalDate
  ) : EventSource {
    override suspend fun generateEventBatches(dispatcher: CoroutineContext): Flow<EventBatch> {
      return flow {
        var batchId = 0L
        // Read events from the eventReader and emit them as EventBatches
        eventReader.readEvents().collect { eventList ->
          emit(EventBatch(eventList, batchId++))
        }
      }
    }
  }

  private fun createTestEvents(
    startVid: Long,
    count: Int,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    eventDate: LocalDate = LocalDate.now().minusDays(1),
    eventGroupReferenceId: String = ""
  ): List<LabeledImpression> {
    return (startVid until startVid + count).map { vid ->
      createLabeledImpression(vid, ageGroup, gender, eventDate, eventGroupReferenceId)
    }
  }

  private fun createLabeledImpression(
    vid: Long,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    eventDate: LocalDate,
    eventGroupReferenceId: String = ""
  ): LabeledImpression {
    return labeledImpression {
      eventTime = createTimeRange(eventDate).start.toProtoTime()
      this.vid = vid
      this.eventGroupReferenceId = eventGroupReferenceId
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
    val timestamp = System.currentTimeMillis()
    val impressionsTmpPath = java.io.File("/tmp/test-impressions-$timestamp")
    val dekTmpPath = java.io.File("/tmp/test-deks-$timestamp")
    impressionsTmpPath.mkdirs()
    dekTmpPath.mkdirs()

    eventsByGroup.forEach { (eventGroup, events) ->
      writeEventsToStorage(events, eventGroup, testDate, impressionsTmpPath, dekTmpPath)
    }

    // Store paths for cleanup
    tempDirectories.add(impressionsTmpPath)
    tempDirectories.add(dekTmpPath)

    // Create readers for all event groups
    val readers = eventsByGroup.keys.map { eventGroup ->
      createStorageEventReader(testDate, eventGroup, impressionsTmpPath, dekTmpPath)
    }

    return MultiGroupEventReader(readers)
  }

  private suspend fun setupMultiDayStorage(
    eventsByDayAndGroup: Map<LocalDate, Map<String, List<LabeledImpression>>>
  ): EventReader {
    val timestamp = System.currentTimeMillis()
    val impressionsTmpPath = java.io.File("/tmp/test-impressions-multiday-$timestamp")
    val dekTmpPath = java.io.File("/tmp/test-deks-multiday-$timestamp")
    impressionsTmpPath.mkdirs()
    dekTmpPath.mkdirs()

    eventsByDayAndGroup.forEach { (date, eventsByGroup) ->
      eventsByGroup.forEach { (eventGroup, events) ->
        writeEventsToStorage(events, eventGroup, date, impressionsTmpPath, dekTmpPath)
      }
    }

    // Store paths for cleanup
    tempDirectories.add(impressionsTmpPath)
    tempDirectories.add(dekTmpPath)

    // Create readers for all date/event group combinations
    val readers = eventsByDayAndGroup.flatMap { (date, eventsByGroup) ->
      eventsByGroup.keys.map { eventGroup ->
        createStorageEventReader(date, eventGroup, impressionsTmpPath, dekTmpPath)
      }
    }

    return MultiGroupEventReader(readers)
  }

  /**
   * Helper function to create StorageEventReader with proper blob and metadata paths.
   * The paths match the pattern used by writeEventsToStorage.
   */
  private fun createStorageEventReader(
    date: LocalDate,
    eventGroup: String,
    impressionsRoot: java.io.File,
    dekRoot: java.io.File
  ): StorageEventReader {
    // Paths must match those used in writeEventsToStorage
    val blobPath = "file:///impressions/$date/$eventGroup"
    val metadataPath = "file:///ds/$date/event-group-reference-id/$eventGroup/metadata"

    return StorageEventReader(
      blobPath = blobPath,
      metadataPath = metadataPath,
      kmsClient = null,  // Disable encryption for tests
      impressionsStorageConfig = StorageConfig(rootDirectory = impressionsRoot.resolve("impressions")),
      impressionDekStorageConfig = StorageConfig(rootDirectory = dekRoot.resolve("deks")),
      typeRegistry = typeRegistry,
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

    // Use non-encrypted storage for tests
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(impressionsStorageClient)

    val impressionsFlow = flow {
      events.forEach { impression -> emit(impression.toByteString()) }
    }

    mesosRecordIoStorageClient.writeBlob("impressions/$date/$eventGroup", impressionsFlow)

    // Set up DEK storage (even though we're not using encryption, the reader expects metadata)
    val deksBucketDir = dekRoot.resolve("deks")
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    // Create empty encrypted DEK since we're not using encryption
    val encryptedDek = EncryptedDek.newBuilder()
      .setKekUri("")
      .setEncryptedDek(ByteString.EMPTY)
      .build()

    val blobDetails = blobDetails {
      blobUri = "file:///impressions/$date/$eventGroup"
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
    eventGroupsMap: Map<String, OpenEndTimeRange>,
    filter: String,
    vidSamplingStart: Float,
    vidSamplingWidth: Float
  ): Requisition {
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
      // Create proper encrypted requisition spec
      val requisitionSpec = requisitionSpec {
        events = RequisitionSpecKt.events {
          eventGroups += eventGroupsMap.map { (eventGroupName, timeRange) ->
            RequisitionSpecKt.eventGroupEntry {
              key = eventGroupName
              value = RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = com.google.type.interval {
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
        measurementPublicKey = MC_PUBLIC_KEY.pack()
        nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
      }

      encryptedRequisitionSpec = encryptRequisitionSpec(
        signRequisitionSpec(requisitionSpec, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY
      )
    }
  }

  // Extension functions for FrequencyVector analysis
  private fun FrequencyVector.getReach(): Long {
    return this.getArray().count { it > 0 }.toLong()
  }

  private fun FrequencyVector.getTotalCount(): Long {
    return this.getArray().sum().toLong()
  }

  private fun FrequencyVector.getAverageFrequency(): Double {
    val reach = getReach()
    return if (reach > 0) getTotalCount().toDouble() / reach else 0.0
  }

  companion object {
    private val SECRET_FILES_PATH = checkNotNull(
      org.wfanet.measurement.common.getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )
    )

    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
        "small_population_spec.textproto"
      )
    private val TEST_DATA_RUNTIME_PATH = org.wfanet.measurement.common.getRuntimePath(TEST_DATA_PATH)!!

    private const val EDP_DISPLAY_NAME = "edp1"
    private const val MEASUREMENT_CONSUMER_ID = "mc"

    private val PRIVATE_ENCRYPTION_KEY = loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")

    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val MC_SIGNING_KEY = loadSigningKey(
      "${MEASUREMENT_CONSUMER_ID}_cs_cert.der",
      "${MEASUREMENT_CONSUMER_ID}_cs_private.der"
    )

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }
  }
}
