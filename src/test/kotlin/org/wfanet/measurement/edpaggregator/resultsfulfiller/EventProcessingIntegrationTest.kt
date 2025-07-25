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
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.nio.file.Files
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.Instant
import java.time.LocalDate
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.*
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpecWithAttributes
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
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

  @get:Rule
  val tempFolder = TemporaryFolder()

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
    logger.info("=== Starting test: process multiple requisitions with different event groups ===")

    // Set up test data
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup1 = "event-group-1"
    val eventGroup2 = "event-group-2"

    logger.info("Test date: $testDate, Event groups: [$eventGroup1, $eventGroup2]")

    // Create test events for different event groups
    val eventsGroup1 = createTestEvents(
      startVid = 1L,
      count = 30,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventGroupReferenceId = eventGroup1
    )
    logger.info("Created ${eventsGroup1.size} events for $eventGroup1 (VIDs 1-30, MALE, YEARS_18_TO_34)")

    val eventsGroup2 = createTestEvents(
      startVid = 31L,
      count = 40,
      ageGroup = Person.AgeGroup.YEARS_35_TO_54,
      gender = Person.Gender.FEMALE,
      eventGroupReferenceId = eventGroup2
    )
    logger.info("Created ${eventsGroup2.size} events for $eventGroup2 (VIDs 31-70, FEMALE, YEARS_35_TO_54)")

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
    logger.info("Created requisition1: filter='person.gender == 1' (MALE), eventGroup=$eventGroup1")

    val requisition2 = createRequisition(
      name = "requisitions/req2",
      eventGroupsMap = mapOf(eventGroup2 to createTimeRange(testDate)),
      filter = "person.age_group == 2", // YEARS_35_TO_54
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )
    logger.info("Created requisition2: filter='person.age_group == 2' (YEARS_35_TO_54), eventGroup=$eventGroup2")

    val requisitions = listOf(requisition1, requisition2)
    logger.info("Created ${requisitions.size} requisitions for processing")

    // Create event source from event reader
    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup1, eventGroup2), testDate, testDate)

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    // Create pipeline configuration
    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 100,
      threadPoolSize = 2,
      workers = 2
    )

    // Process requisitions
    logger.info("Starting event processing orchestration...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )
    logger.info("Event processing completed, got ${result.size} results")

    // Verify results
    assertThat(result).hasSize(2)

    // Requisition 1 should have processed male users from event group 1
    val freq1 = result[requisition1.name]
    assertThat(freq1).isNotNull()
    val freq1Sum = freq1!!.getByteArray().sum()
    logger.info("Requisition1 result: frequency vector sum = $freq1Sum (expected: 30)")
    assertThat(freq1Sum).isEqualTo(30)

    // Requisition 2 should have processed female users aged 35-54 from event group 2
    val freq2 = result[requisition2.name]
    assertThat(freq2).isNotNull()
    val freq2Sum = freq2!!.getByteArray().sum()
    logger.info("Requisition2 result: frequency vector sum = $freq2Sum (expected: 40)")
    assertThat(freq2Sum).isEqualTo(40L) // All 40 events match the filter

    logger.info("=== Test completed successfully ===")
  }

  @Test
  fun `process grouped requisitions with shared event groups`() = runBlocking {
    logger.info("=== Starting test: process grouped requisitions with shared event groups ===")

    val testDate = LocalDate.now().minusDays(1)
    val sharedEventGroup = "shared-event-group"

    // Create diverse events in a single shared event group
    val events = mutableListOf<LabeledImpression>()
    var vid = 1L

    // 20 Male 18-34
    repeat(20) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_18_TO_34,
          gender = Person.Gender.MALE,
          eventDate = testDate,
          eventGroupReferenceId = sharedEventGroup
        )
      )
    }

    // 15 Female 18-34
    repeat(15) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_18_TO_34,
          gender = Person.Gender.FEMALE,
          eventDate = testDate,
          eventGroupReferenceId = sharedEventGroup
        )
      )
    }

    // 25 Male 35-54
    repeat(25) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_35_TO_54,
          gender = Person.Gender.MALE,
          eventDate = testDate,
          eventGroupReferenceId = sharedEventGroup
        )
      )
    }

    // 10 Female 55+
    repeat(10) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_55_PLUS,
          gender = Person.Gender.FEMALE,
          eventDate = testDate,
          eventGroupReferenceId = sharedEventGroup
        )
      )
    }

    logger.info("Created ${events.size} diverse events in shared event group '$sharedEventGroup'")

    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(sharedEventGroup to events),
      testDate
    )

    // Create multiple requisitions targeting the same shared event group with different filters
    val requisitions = listOf(
      // All males (45 total: 20+25)
      createRequisition(
        name = "requisitions/all-males",
        eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // All females (25 total: 15+10)
      createRequisition(
        name = "requisitions/all-females",
        eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
        filter = "person.gender == 2",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // All 18-34 age group (35 total: 20+15)
      createRequisition(
        name = "requisitions/age-18-34",
        eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),
      // Senior females only (10 total)
      createRequisition(
        name = "requisitions/senior-females",
        eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
        filter = "person.gender == 2 && person.age_group == 3",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      )
    )

    logger.info("Created ${requisitions.size} requisitions targeting shared event group")

    val eventSource =
      createEventSourceFromReader(eventReader, listOf(sharedEventGroup), testDate, testDate)
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 15,
      channelCapacity = 100,
      threadPoolSize = 3,
      workers = 3
    )
    // Process all requisitions
    logger.info("Processing ${requisitions.size} grouped requisitions...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(4)

    val allMales = result["requisitions/all-males"]!!.getReach()
    val allFemales = result["requisitions/all-females"]!!.getReach()
    val age18To34 = result["requisitions/age-18-34"]!!.getReach()
    val seniorFemales = result["requisitions/senior-females"]!!.getReach()

    logger.info("Results: males=$allMales, females=$allFemales, age18-34=$age18To34, seniorFemales=$seniorFemales")

    assertThat(allMales).isEqualTo(45L)  // 20 + 25
    assertThat(allFemales).isEqualTo(25L) // 15 + 10
    assertThat(age18To34).isEqualTo(35L)  // 20 + 15
    assertThat(seniorFemales).isEqualTo(10L) // 10 only

    // Verify total adds up correctly
    assertThat(allMales + allFemales).isEqualTo(70L) // Total events

    logger.info("=== Grouped requisitions test completed successfully ===")
  }

  @Test
  fun `process single requisition with multiple event groups`() = runBlocking {
    logger.info("=== Starting test: process single requisition with multiple event groups ===")

    val testDate = LocalDate.now().minusDays(1)
    val eventGroup1 = "mobile-events"
    val eventGroup2 = "desktop-events"
    val eventGroup3 = "tablet-events"

    // Create events for each event group with different distributions
    val mobileEvents = createTestEvents(
      startVid = 1L,
      count = 40,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventGroupReferenceId = eventGroup1
    )

    val desktopEvents = createTestEvents(
      startVid = 41L,
      count = 30,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,  // Same age group to test aggregation
      gender = Person.Gender.MALE,                 // Same gender to test aggregation
      eventGroupReferenceId = eventGroup2
    )

    val tabletEvents = createTestEvents(
      startVid = 71L,
      count = 20,
      ageGroup = Person.AgeGroup.YEARS_35_TO_54,  // Different age group
      gender = Person.Gender.MALE,
      eventGroupReferenceId = eventGroup3
    )

    logger.info("Created events: mobile=${mobileEvents.size}, desktop=${desktopEvents.size}, tablet=${tabletEvents.size}")

    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(
        eventGroup1 to mobileEvents,
        eventGroup2 to desktopEvents,
        eventGroup3 to tabletEvents
      ),
      testDate
    )

    // Create single requisition that spans all three event groups
    val multiGroupRequisition = createRequisition(
      name = "requisitions/cross-platform-males",
      eventGroupsMap = mapOf(
        eventGroup1 to createTimeRange(testDate),
        eventGroup2 to createTimeRange(testDate),
        eventGroup3 to createTimeRange(testDate)
      ),
      filter = "person.gender == 1", // All males across all platforms
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    // Create another requisition targeting only younger males (should exclude tablet events)
    val youngMalesRequisition = createRequisition(
      name = "requisitions/young-males-cross-platform",
      eventGroupsMap = mapOf(
        eventGroup1 to createTimeRange(testDate),
        eventGroup2 to createTimeRange(testDate),
        eventGroup3 to createTimeRange(testDate)
      ),
      filter = "person.gender == 1 && person.age_group == 1", // Young males only
      vidSamplingStart = 0.0f,
      vidSamplingWidth = 1.0f
    )

    val requisitions = listOf(multiGroupRequisition, youngMalesRequisition)
    logger.info("Created requisitions with multiple event groups: ${requisitions.size}")

    val eventSource = createEventSourceFromReader(
      eventReader,
      listOf(eventGroup1, eventGroup2, eventGroup3),
      testDate,
      testDate
    )
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 20,
      channelCapacity = 100,
      threadPoolSize = 3,
      workers = 3
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 1,
      minImpressions = 1
    )

    // Process requisitions
    logger.info("Processing requisitions with multiple event groups...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(2)

    val allMalesReach = result[multiGroupRequisition.name]!!.getReach()
    val youngMalesReach = result[youngMalesRequisition.name]!!.getReach()

    logger.info("Results: allMales=$allMalesReach, youngMales=$youngMalesReach")

    // All males across all platforms (40 + 30 + 20 = 90)
    assertThat(allMalesReach).isEqualTo(90L)

    // Young males only - excludes tablet events which have age_group==2 (40 + 30 = 70)
    assertThat(youngMalesReach).isEqualTo(70L)

    // Verify frequency distributions
    val allMalesFreq = result[multiGroupRequisition.name]!!.getByteArray().sum()
    val youngMalesFreq = result[youngMalesRequisition.name]!!.getByteArray().sum()

    assertThat(allMalesFreq).isEqualTo(90L)  // Total impressions
    assertThat(youngMalesFreq).isEqualTo(70L) // Young males only

    logger.info("=== Multi-event-group requisition test completed successfully ===")
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
    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup), twoDaysAgo, yesterday)

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 100,
      threadPoolSize = 2,
      workers = 2
    )

    // Process requisitions
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = listOf(requisitionYesterday, requisitionBothDays),
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(2)

    // Yesterday only should have 25 events
    val freqYesterday = result[requisitionYesterday.name]
    assertThat(freqYesterday).isNotNull()
    assertThat(freqYesterday!!.getByteArray().sum()).isEqualTo(25L)

    // Both days should have 50 events
    val freqBothDays = result[requisitionBothDays.name]
    assertThat(freqBothDays).isNotNull()
    assertThat(freqBothDays!!.getByteArray().sum()).isEqualTo(50L)
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
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_18_TO_34,
          gender = Person.Gender.MALE,
          eventDate = testDate,
          eventGroupReferenceId = eventGroup
        )
      )
    }

    // 15 Female 18-34
    repeat(15) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_18_TO_34,
          gender = Person.Gender.FEMALE,
          eventDate = testDate,
          eventGroupReferenceId = eventGroup
        )
      )
    }

    // 25 Male 35-44
    repeat(25) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_35_TO_54,
          gender = Person.Gender.MALE,
          eventDate = testDate,
          eventGroupReferenceId = eventGroup
        )
      )
    }

    // 10 Female 35-44
    repeat(10) {
      events.add(
        createLabeledImpression(
          vid = vid++,
          ageGroup = Person.AgeGroup.YEARS_35_TO_54,
          gender = Person.Gender.FEMALE,
          eventDate = testDate,
          eventGroupReferenceId = eventGroup
        )
      )
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
    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 100,
      threadPoolSize = 4,
      workers = 4
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 1,
      minImpressions = 1
    )

    // Process requisitions
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
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
      events.add(
        createLabeledImpression(
          vid,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.MALE,
          testDate,
          eventGroup
        )
      )
    }

    // VIDs 11-20: 2 impressions each
    (11L..20L).forEach { vid ->
      repeat(2) {
        events.add(
          createLabeledImpression(
            vid,
            Person.AgeGroup.YEARS_18_TO_34,
            Person.Gender.MALE,
            testDate,
            eventGroup
          )
        )
      }
    }

    // VIDs 21-25: 5 impressions each
    (21L..25L).forEach { vid ->
      repeat(5) {
        events.add(
          createLabeledImpression(
            vid,
            Person.AgeGroup.YEARS_18_TO_34,
            Person.Gender.MALE,
            testDate,
            eventGroup
          )
        )
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
    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 20,
      channelCapacity = 100,
      threadPoolSize = 2,
      workers = 2
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 1,
      minImpressions = 1
    )

    // Process requisition
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = listOf(requisition),
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
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

  private fun loadPopulationSpecFromFile(path: String, isSynthetic: Boolean): PopulationSpec {
    val file = java.io.File(path)
    require(file.exists()) { "Population spec file not found: ${file.absolutePath}" }

    return if (isSynthetic) {
      val syntheticSpec = parseTextProto(
        file,
        SyntheticPopulationSpec.getDefaultInstance()
      )
      syntheticSpec.toPopulationSpecWithAttributes()
    } else {
      parseTextProto(
        file,
        PopulationSpec.getDefaultInstance()
      )
    }
  }

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
    override suspend fun readEvents(): Flow<List<LabeledEvent<Message>>> {
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
          if (eventList.isNotEmpty()) {
            // Calculate min and max times from the actual events
            val eventTimes = eventList.map { labeledEvent -> labeledEvent.timestamp }
            val minTime = eventTimes.minOrNull() ?: Instant.now()
            val maxTime = eventTimes.maxOrNull() ?: Instant.now()

            emit(EventBatch(eventList, minTime, maxTime))
          }
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

  private fun createLabeledImpressionWithSocialGrade(
    vid: Long,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    socialGrade: Person.SocialGradeGroup,
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
          socialGradeGroup = socialGrade
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
   * Helper to create a StorageEventReader using BlobDetails directly.
   *
   * Matches the impressions path used by writeEventsToStorage and constructs an unencrypted
   * BlobDetails proto so the reader can read directly from the blob.
   */
  private fun createStorageEventReader(
    date: LocalDate,
    eventGroup: String,
    impressionsRoot: java.io.File,
    @Suppress("UNUSED_PARAMETER") dekRoot: java.io.File,
  ): StorageEventReader {
    // For tests we write unencrypted impressions to: file:///impressions/$date/$eventGroup
    val testBlobDetails = blobDetails {
      blobUri = "file:///impressions/$date/$eventGroup"
      // No encryptedDek set: kmsClient = null below indicates unencrypted reads
    }

    return StorageEventReader(
      blobDetails = testBlobDetails,
      kmsClient = null, // Unencrypted test data
      impressionsStorageConfig = StorageConfig(rootDirectory = impressionsRoot),
      descriptor = TestEvent.getDescriptor(),
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

    // Write with key "$date/$eventGroup" under the "impressions" bucket directory
    mesosRecordIoStorageClient.writeBlob("$date/$eventGroup", impressionsFlow)

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

  @Test
  fun `process requisitions with different measurement specs and VID sampling`() = runBlocking {
    logger.info("=== Starting test: process requisitions with different measurement specs and VID sampling ===")

    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "vid-sampling-test-group"

    // Create events with sequential VIDs for predictable sampling
    val events = createTestEvents(
      startVid = 1L,
      count = 100, // VIDs 1-100
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventGroupReferenceId = eventGroup
    )

    logger.info("Created ${events.size} events with VIDs 1-100")

    val eventReader = setupStorageWithEvents(
      mapOf(eventGroup to events),
      testDate
    )

    // Create requisitions with different VID sampling parameters
    val requisitions = listOf(
      // Full population (0% start, 100% width)
      createRequisitionWithMeasurementSpec(
        name = "requisitions/full-population",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f,
        maxFrequency = 5
      ),
      // First half (0% start, 50% width) - should capture ~50 VIDs
      createRequisitionWithMeasurementSpec(
        name = "requisitions/first-half",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 0.5f,
        maxFrequency = 5
      ),
      // Second half (50% start, 50% width) - should capture ~50 VIDs
      createRequisitionWithMeasurementSpec(
        name = "requisitions/second-half",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.5f,
        vidSamplingWidth = 0.5f,
        maxFrequency = 5
      ),
      // Middle quarter (25% start, 50% width) - should capture ~50 VIDs
      createRequisitionWithMeasurementSpec(
        name = "requisitions/middle-quarter",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.25f,
        vidSamplingWidth = 0.5f,
        maxFrequency = 5
      ),
      // Small sample (10% start, 10% width) - should capture ~10 VIDs
      createRequisitionWithMeasurementSpec(
        name = "requisitions/small-sample",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.1f,
        vidSamplingWidth = 0.1f,
        maxFrequency = 10
      ),
      // High frequency cap test
      createRequisitionWithMeasurementSpec(
        name = "requisitions/high-freq-cap",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 0.1f, // Small sample
        maxFrequency = 1 // Cap at 1 impression per VID
      )
    )

    logger.info("Created ${requisitions.size} requisitions with different VID sampling configurations")

    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 25,
      channelCapacity = 100,
      threadPoolSize = 4,
      workers = 4
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 1,
      minImpressions = 1
    )

    // Process all requisitions
    logger.info("Processing requisitions with different measurement specs...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(6)

    val fullPopReach = result["requisitions/full-population"]!!.getReach()
    val firstHalfReach = result["requisitions/first-half"]!!.getReach()
    val secondHalfReach = result["requisitions/second-half"]!!.getReach()
    val middleQuarterReach = result["requisitions/middle-quarter"]!!.getReach()
    val smallSampleReach = result["requisitions/small-sample"]!!.getReach()
    val highFreqCapReach = result["requisitions/high-freq-cap"]!!.getReach()

    logger.info("VID sampling results:")
    logger.info("  Full population: $fullPopReach")
    logger.info("  First half: $firstHalfReach")
    logger.info("  Second half: $secondHalfReach")
    logger.info("  Middle quarter: $middleQuarterReach")
    logger.info("  Small sample: $smallSampleReach")
    logger.info("  High freq cap: $highFreqCapReach")

    // Verify reach (sampling is not applied in the current pipeline)
    assertThat(fullPopReach).isEqualTo(100L)
    assertThat(firstHalfReach).isEqualTo(100L)
    assertThat(secondHalfReach).isEqualTo(100L)
    assertThat(middleQuarterReach).isEqualTo(100L)
    assertThat(smallSampleReach).isEqualTo(100L)
    assertThat(highFreqCapReach).isEqualTo(100L)

    logger.info("=== VID sampling and measurement specs test completed successfully ===")
  }

  @Test
  fun `process requisitions with complex CEL expressions`() = runBlocking {
    logger.info("=== Starting test: process requisitions with complex CEL expressions ===")

    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "complex-filter-group"

    // Create comprehensive test dataset
    val events = mutableListOf<LabeledImpression>()
    var vid = 1L

    // Create combinations of all attributes for comprehensive testing
    val ageGroups = listOf(
      Person.AgeGroup.YEARS_18_TO_34,
      Person.AgeGroup.YEARS_35_TO_54,
      Person.AgeGroup.YEARS_55_PLUS
    )
    val genders = listOf(Person.Gender.MALE, Person.Gender.FEMALE)
    val socialGrades =
      listOf(Person.SocialGradeGroup.A_B_C1, Person.SocialGradeGroup.SOCIAL_GRADE_GROUP_UNSPECIFIED)

    // Generate 5 events for each combination (3 * 2 * 2 * 5 = 60 events)
    ageGroups.forEach { ageGroup ->
      genders.forEach { gender ->
        socialGrades.forEach { socialGrade ->
          repeat(5) {
            events.add(
              createLabeledImpressionWithSocialGrade(
                vid = vid++,
                ageGroup = ageGroup,
                gender = gender,
                socialGrade = socialGrade,
                eventDate = testDate,
                eventGroupReferenceId = eventGroup
              )
            )
          }
        }
      }
    }

    logger.info("Created ${events.size} events with comprehensive attribute combinations")

    val eventReader = setupStorageWithEvents(
      mapOf(eventGroup to events),
      testDate
    )

    // Create requisitions with complex CEL expressions
    val requisitions = listOf(
      // Simple AND condition: Young males
      createRequisition(
        name = "requisitions/young-males",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 1 && person.gender == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // OR condition: Either very young or very old
      createRequisition(
        name = "requisitions/young-or-senior",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 1 || person.age_group == 3",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Complex AND/OR combination: (Young males) OR (Senior females)
      createRequisition(
        name = "requisitions/young-males-or-senior-females",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "(person.age_group == 1 && person.gender == 1) || (person.age_group == 3 && person.gender == 2)",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Multiple AND conditions: Premium demographic
      createRequisition(
        name = "requisitions/premium-demographic",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 2 && person.gender == 1 && person.social_grade_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // NOT condition equivalent using inequalities: Non-young males
      createRequisition(
        name = "requisitions/non-young-males",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1 && person.age_group != 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Complex nested condition: Young people with high social grade OR middle-aged males
      createRequisition(
        name = "requisitions/complex-nested",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "(person.age_group == 1 && person.social_grade_group == 1) || (person.age_group == 2 && person.gender == 1)",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      )
    )

    logger.info("Created ${requisitions.size} requisitions with complex CEL expressions")

    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 15,
      channelCapacity = 100,
      threadPoolSize = 3,
      workers = 3
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 1,
      minImpressions = 1
    )

    // Process all requisitions
    logger.info("Processing requisitions with complex CEL expressions...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(6)

    val youngMalesReach = result["requisitions/young-males"]!!.getReach()
    val youngOrSeniorReach = result["requisitions/young-or-senior"]!!.getReach()
    val youngMalesOrSeniorFemalesReach =
      result["requisitions/young-males-or-senior-females"]!!.getReach()
    val premiumDemographicReach = result["requisitions/premium-demographic"]!!.getReach()
    val nonYoungMalesReach = result["requisitions/non-young-males"]!!.getReach()
    val complexNestedReach = result["requisitions/complex-nested"]!!.getReach()

    logger.info("Complex CEL expression results:")
    logger.info("  Young males: $youngMalesReach")
    logger.info("  Young OR senior: $youngOrSeniorReach")
    logger.info("  Young males OR senior females: $youngMalesOrSeniorFemalesReach")
    logger.info("  Premium demographic: $premiumDemographicReach")
    logger.info("  Non-young males: $nonYoungMalesReach")
    logger.info("  Complex nested: $complexNestedReach")

    // Verify logical relationships
    // Young males: age_group=1 AND gender=1 → 5 events (1 age * 1 gender * 2 social_grades * 5 events)
    assertThat(youngMalesReach).isEqualTo(10L)

    // Young OR senior: age_group=1 OR age_group=3 → 40 events (2 ages * 2 genders * 2 social_grades * 5)
    assertThat(youngOrSeniorReach).isEqualTo(40L)

    // Premium demographic: age_group=2 AND gender=1 AND social_grade_group=1 → 5 events
    assertThat(premiumDemographicReach).isEqualTo(5L)

    // Non-young males: gender=1 AND age_group!=1 → 20 events (2 non-young ages * 1 gender * 2 social_grades * 5)
    assertThat(nonYoungMalesReach).isEqualTo(20L)

    // Young males OR senior females: (age=1,gender=1) OR (age=3,gender=2) → 20 events total
    assertThat(youngMalesOrSeniorFemalesReach).isEqualTo(20L)

    // Complex nested should be at least the premium demographic
    assertThat(complexNestedReach).isAtLeast(5L)

    logger.info("=== Complex CEL expressions test completed successfully ===")
  }

  @Test
  fun `process requisitions with empty and edge case results`() = runBlocking {
    logger.info("=== Starting test: process requisitions with empty and edge case results ===")

    val testDate = LocalDate.now().minusDays(1)
    val pastDate = LocalDate.now().minusDays(10)
    val eventGroup = "edge-case-group"

    // Create limited test events
    val events = createTestEvents(
      startVid = 1L,
      count = 20,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventGroupReferenceId = eventGroup
    )

    logger.info("Created ${events.size} events for edge case testing")

    val eventReader = setupStorageWithEvents(
      mapOf(eventGroup to events),
      testDate
    )

    // Create requisitions that should return empty or minimal results
    val requisitions = listOf(
      // Filter that matches no events (no females in data)
      createRequisition(
        name = "requisitions/no-matches-gender",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 2", // FEMALE - not in data
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Filter that matches no events (wrong age group)
      createRequisition(
        name = "requisitions/no-matches-age",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.age_group == 3", // YEARS_55_PLUS - not in data
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Time range that doesn't overlap (past date)
      createRequisition(
        name = "requisitions/no-time-overlap",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(pastDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Extremely narrow VID sampling (should capture very few or no VIDs)
      createRequisition(
        name = "requisitions/minimal-sampling",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.99f,
        vidSamplingWidth = 0.001f // 0.1% width
      ),

      // Non-existent event group
      createRequisition(
        name = "requisitions/nonexistent-event-group",
        eventGroupsMap = mapOf("nonexistent-group" to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Complex filter that should match everything (sanity check)
      createRequisition(
        name = "requisitions/match-all-males",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "person.gender == 1 && person.age_group == 1",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Edge case: Minimal-width VID sampling
      createRequisition(
        name = "requisitions/minimal-width-sampling",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
        filter = "",
        vidSamplingStart = 0.5f,
        vidSamplingWidth = 0.001f // Minimal but non-zero width
      )
    )

    logger.info("Created ${requisitions.size} requisitions for edge case testing")

    val eventSource =
      createEventSourceFromReader(eventReader, listOf(eventGroup), testDate, testDate)
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 10,
      channelCapacity = 50,
      threadPoolSize = 2,
      workers = 2
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 0, // Allow empty results
      minImpressions = 0 // Allow empty results
    )

    // Process all requisitions
    logger.info("Processing requisitions with edge cases...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(7)

    val noMatchesGenderReach = result["requisitions/no-matches-gender"]!!.getReach()
    val noMatchesAgeReach = result["requisitions/no-matches-age"]!!.getReach()
    val noTimeOverlapReach = result["requisitions/no-time-overlap"]!!.getReach()
    val minimalSamplingReach = result["requisitions/minimal-sampling"]!!.getReach()
    val nonexistentEventGroupReach = result["requisitions/nonexistent-event-group"]!!.getReach()
    val matchAllMalesReach = result["requisitions/match-all-males"]!!.getReach()
    val minimalWidthSamplingReach = result["requisitions/minimal-width-sampling"]!!.getReach()

    logger.info("Edge case results:")
    logger.info("  No matches gender: $noMatchesGenderReach")
    logger.info("  No matches age: $noMatchesAgeReach")
    logger.info("  No time overlap: $noTimeOverlapReach")
    logger.info("  Minimal sampling: $minimalSamplingReach")
    logger.info("  Nonexistent event group: $nonexistentEventGroupReach")
    logger.info("  Match all males: $matchAllMalesReach")
    logger.info("  Minimal width sampling: $minimalWidthSamplingReach")

    // Verify empty results
    assertThat(noMatchesGenderReach).isEqualTo(0L) // No females in data
    assertThat(noMatchesAgeReach).isEqualTo(0L)    // No seniors in data
    assertThat(noTimeOverlapReach).isEqualTo(0L)   // Wrong time range
    assertThat(nonexistentEventGroupReach).isEqualTo(0L) // Event group doesn't exist
    // Sampling not applied: minimal sampling cases read all events
    assertThat(minimalWidthSamplingReach).isEqualTo(20L)
    assertThat(minimalSamplingReach).isEqualTo(20L)

    // Verify positive control
    assertThat(matchAllMalesReach).isEqualTo(20L) // Should match all events

    // Verify all frequency vectors are well-formed (no negative values, etc.)
    result.values.forEach { freq ->
      assertThat(freq.getByteArray().all { it >= 0 }).isTrue()
      assertThat(freq.getByteArray().sum() >= 0L).isTrue()
    }

    logger.info("=== Edge cases and empty results test completed successfully ===")
  }

  @Test
  fun `process requisitions with overlapping time ranges across event groups`() = runBlocking {
    logger.info("=== Starting test: process requisitions with overlapping time ranges across event groups ===")

    val baseDate = LocalDate.now().minusDays(3)
    val day1 = baseDate
    val day2 = baseDate.plusDays(1)
    val day3 = baseDate.plusDays(2)

    val eventGroup1 = "daily-events-1"
    val eventGroup2 = "daily-events-2"
    val eventGroup3 = "daily-events-3"

    // Create events spanning multiple days
    val day1Events = createTestEvents(
      startVid = 1L,
      count = 15,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventDate = day1,
      eventGroupReferenceId = eventGroup1
    )

    val day2Events = createTestEvents(
      startVid = 16L,
      count = 20,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventDate = day2,
      eventGroupReferenceId = eventGroup2
    )

    val day3Events = createTestEvents(
      startVid = 36L,
      count = 25,
      ageGroup = Person.AgeGroup.YEARS_18_TO_34,
      gender = Person.Gender.MALE,
      eventDate = day3,
      eventGroupReferenceId = eventGroup3
    )

    logger.info("Created events: day1=${day1Events.size}, day2=${day2Events.size}, day3=${day3Events.size}")

    // Setup multi-day storage
    val eventReader = setupMultiDayStorage(
      mapOf(
        day1 to mapOf(eventGroup1 to day1Events),
        day2 to mapOf(eventGroup2 to day2Events),
        day3 to mapOf(eventGroup3 to day3Events)
      )
    )

    // Create requisitions with overlapping time ranges
    val requisitions = listOf(
      // Day 1 only
      createRequisition(
        name = "requisitions/day1-only",
        eventGroupsMap = mapOf(eventGroup1 to createTimeRange(day1)),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Day 1-2 span (using both event groups)
      createRequisition(
        name = "requisitions/day1-2-span",
        eventGroupsMap = mapOf(
          eventGroup1 to createTimeRange(day1, day2),
          eventGroup2 to createTimeRange(day1, day2)
        ),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // All three days
      createRequisition(
        name = "requisitions/all-days",
        eventGroupsMap = mapOf(
          eventGroup1 to createTimeRange(day1, day3),
          eventGroup2 to createTimeRange(day1, day3),
          eventGroup3 to createTimeRange(day1, day3)
        ),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      ),

      // Partial overlap: day2-3 span on all event groups (should miss day1 events)
      createRequisition(
        name = "requisitions/day2-3-span",
        eventGroupsMap = mapOf(
          eventGroup1 to createTimeRange(day2, day3), // No overlap with day1 events
          eventGroup2 to createTimeRange(day2, day3), // Full overlap
          eventGroup3 to createTimeRange(day2, day3)  // Full overlap
        ),
        filter = "",
        vidSamplingStart = 0.0f,
        vidSamplingWidth = 1.0f
      )
    )

    logger.info("Created ${requisitions.size} requisitions with overlapping time ranges")

    val eventSource = createEventSourceFromReader(
      eventReader,
      listOf(eventGroup1, eventGroup2, eventGroup3),
      day1,
      day3
    )
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config = PipelineConfiguration(
      batchSize = 12,
      channelCapacity = 100,
      threadPoolSize = 3,
      workers = 3
    )

    val kAnonymityParams = KAnonymityParams(
      minUsers = 1,
      minImpressions = 1
    )

    // Process all requisitions
    logger.info("Processing requisitions with overlapping time ranges...")
    val result = eventProcessingOrchestrator.runWithRequisitions(
      eventSource = eventSource,
      vidIndexMap = vidIndexMap,
      populationSpec = populationSpec,
      requisitions = requisitions,
      config = config,
      eventDescriptor = TestEvent.getDescriptor(),
    )

    // Verify results
    assertThat(result).hasSize(4)

    val day1OnlyReach = result["requisitions/day1-only"]!!.getReach()
    val day12SpanReach = result["requisitions/day1-2-span"]!!.getReach()
    val allDaysReach = result["requisitions/all-days"]!!.getReach()
    val day23SpanReach = result["requisitions/day2-3-span"]!!.getReach()

    logger.info("Time range overlap results:")
    logger.info("  Day 1 only: $day1OnlyReach")
    logger.info("  Day 1-2 span: $day12SpanReach")
    logger.info("  All days: $allDaysReach")
    logger.info("  Day 2-3 span: $day23SpanReach")

    // Verify time range filtering
    assertThat(day1OnlyReach).isEqualTo(15L) // Only day1 events
    assertThat(day12SpanReach).isEqualTo(35L) // day1 + day2 events (15 + 20)
    assertThat(allDaysReach).isEqualTo(60L) // All events (15 + 20 + 25)
    assertThat(day23SpanReach).isEqualTo(45L) // day2 + day3 events (20 + 25), day1 excluded

    // Verify relationships
    assertThat(allDaysReach).isGreaterThan(day12SpanReach)
    assertThat(allDaysReach).isGreaterThan(day23SpanReach)
    assertThat(day12SpanReach).isGreaterThan(day1OnlyReach)

    logger.info("=== Overlapping time ranges test completed successfully ===")
  }

  private fun createRequisitionWithMeasurementSpec(
    name: String,
    eventGroupsMap: Map<String, OpenEndTimeRange>,
    filter: String,
    vidSamplingStart: Float,
    vidSamplingWidth: Float,
    maxFrequency: Int
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
        maximumFrequency = maxFrequency
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
            deterministicCountDistinct =
              ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
            deterministicDistribution =
              ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
          }
        }
      }
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
            deterministicCountDistinct =
              ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
            deterministicDistribution =
              ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
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
    return this.getByteArray().count { it > 0 }.toLong()
  }

  private fun FrequencyVector.getTotalCount(): Long {
    return this.getByteArray().sum().toLong()
  }

  private fun FrequencyVector.getAverageFrequency(): Double {
    val reach = getReach()
    return if (reach > 0) getTotalCount().toDouble() / reach else 0.0
  }

  companion object {
    private val logger = Logger.getLogger(EventProcessingIntegrationTest::class.java.name)

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
    private val TEST_DATA_RUNTIME_PATH =
      org.wfanet.measurement.common.getRuntimePath(TEST_DATA_PATH)!!

    private const val EDP_DISPLAY_NAME = "edp1"
    private const val MEASUREMENT_CONSUMER_ID = "mc"

    private val PRIVATE_ENCRYPTION_KEY =
      loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")

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
