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
import java.nio.file.Files
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.testing.TestEncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * End-to-end integration tests for event processing with requisitions and event groups.
 *
 * These tests validate the complete flow from requisitions through event processing to frequency
 * vector generation, including:
 * - Multiple event groups
 * - Multiple requisitions with different filters
 * - Time-based filtering
 * - Population specs
 */
@RunWith(JUnit4::class)
class EventProcessingIntegrationTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "/fake-key"
  private lateinit var kmsClient: KmsClient
  private lateinit var serializedEncryptionKey: ByteString
  private val privateEncryptionKey = PRIVATE_ENCRYPTION_KEY
  private lateinit var eventProcessingOrchestrator: EventProcessingOrchestrator<Message>

  @Before
  fun initEventProcessingOrchestrator() {
    // Set up KMS
    kmsClient = TestEncryptedStorage.buildFakeKmsClient(kekUri, keyTemplate = "AES128_GCM")

    // Set up encryption key
    serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(
        kmsClient,
        kekUri,
        tinkKeyTemplateType = "AES128_GCM_HKDF_1MB",
      )

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
    val eventsGroup1 =
      createTestEvents(
        startVid = 1L,
        count = 30,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
      )

    val eventsGroup2 =
      createTestEvents(
        startVid = 31L,
        count = 40,
        ageGroup = Person.AgeGroup.YEARS_35_TO_54,
        gender = Person.Gender.FEMALE,
      )

    // Write events to storage and create event source
    val eventSource =
      setupStorageWithEvents(
        mapOf(eventGroup1 to eventsGroup1, eventGroup2 to eventsGroup2),
        testDate,
      )

    // Create requisitions with different filters
    val requisition1 =
      createRequisition(
        name = "requisitions/req1",
        eventGroupsMap = mapOf(eventGroup1 to createTimeRange(testDate)),
        filter = "person.gender == 1", // MALE
      )

    val requisition2 =
      createRequisition(
        name = "requisitions/req2",
        eventGroupsMap = mapOf(eventGroup2 to createTimeRange(testDate)),
        filter = "person.age_group == 2", // YEARS_35_TO_54
      )

    val requisitions = listOf(requisition1, requisition2)

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    // Create pipeline configuration
    val config =
      PipelineConfiguration(batchSize = 10, channelCapacity = 100, threadPoolSize = 2, workers = 2)

    // Process requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
        config = config,
        eventDescriptor = TestEvent.getDescriptor(),
      )

    // Verify results
    assertThat(result).hasSize(2)

    // Requisition 1 should have processed male users from event group 1
    val freq1 = result[requisition1.name]
    assertThat(freq1).isNotNull()
    val freq1Sum = freq1!!.getByteArray().sum()
    assertThat(freq1Sum).isEqualTo(30)

    // Requisition 2 should have processed female users aged 35-54 from event group 2
    val freq2 = result[requisition2.name]
    assertThat(freq2).isNotNull()
    val freq2Sum = freq2!!.getByteArray().sum()
    assertThat(freq2Sum).isEqualTo(40L) // All 40 events match the filter
  }

  @Test
  fun `process grouped requisitions with shared event groups`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val sharedEventGroup = "shared-event-group"

    // Create diverse events in a single shared event group
    val events =
      buildList<LabeledImpression> {
        var vid = 1L

        // 20 Male 18-34
        repeat(20) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_18_TO_34,
              gender = Person.Gender.MALE,
              eventDate = testDate,
            )
          )
        }

        // 15 Female 18-34
        repeat(15) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_18_TO_34,
              gender = Person.Gender.FEMALE,
              eventDate = testDate,
            )
          )
        }

        // 25 Male 35-54
        repeat(25) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_35_TO_54,
              gender = Person.Gender.MALE,
              eventDate = testDate,
            )
          )
        }

        // 10 Female 55+
        repeat(10) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_55_PLUS,
              gender = Person.Gender.FEMALE,
              eventDate = testDate,
            )
          )
        }
      }

    // Write events to storage
    val eventSource = setupStorageWithEvents(mapOf(sharedEventGroup to events), testDate)

    // Create multiple requisitions targeting the same shared event group with different filters
    val requisitions =
      listOf(
        // All males (45 total: 20+25)
        createRequisition(
          name = "requisitions/all-males",
          eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
          filter = "person.gender == 1",
        ),
        // All females (25 total: 15+10)
        createRequisition(
          name = "requisitions/all-females",
          eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
          filter = "person.gender == 2",
        ),
        // All 18-34 age group (35 total: 20+15)
        createRequisition(
          name = "requisitions/age-18-34",
          eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
          filter = "person.age_group == 1",
        ),
        // Senior females only (10 total)
        createRequisition(
          name = "requisitions/senior-females",
          eventGroupsMap = mapOf(sharedEventGroup to createTimeRange(testDate)),
          filter = "person.gender == 2 && person.age_group == 3",
        ),
      )

    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 15, channelCapacity = 100, threadPoolSize = 3, workers = 3)
    // Process all requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
        config = config,
        eventDescriptor = TestEvent.getDescriptor(),
      )

    // Verify results
    assertThat(result).hasSize(4)

    val allMales = result["requisitions/all-males"]!!.getReach()
    val allFemales = result["requisitions/all-females"]!!.getReach()
    val age18To34 = result["requisitions/age-18-34"]!!.getReach()
    val seniorFemales = result["requisitions/senior-females"]!!.getReach()

    assertThat(allMales).isEqualTo(45L) // 20 + 25
    assertThat(allFemales).isEqualTo(25L) // 15 + 10
    assertThat(age18To34).isEqualTo(35L) // 20 + 15
    assertThat(seniorFemales).isEqualTo(10L) // 10 only

    // Verify total adds up correctly
    assertThat(allMales + allFemales).isEqualTo(70L) // Total events
  }

  @Test
  fun `process single requisition with multiple event groups`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup1 = "mobile-events"
    val eventGroup2 = "desktop-events"
    val eventGroup3 = "tablet-events"

    // Create events for each event group with different distributions
    val mobileEvents =
      createTestEvents(
        startVid = 1L,
        count = 40,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
      )

    val desktopEvents =
      createTestEvents(
        startVid = 41L,
        count = 30,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34, // Same age group to test aggregation
        gender = Person.Gender.MALE, // Same gender to test aggregation
      )

    val tabletEvents =
      createTestEvents(
        startVid = 71L,
        count = 20,
        ageGroup = Person.AgeGroup.YEARS_35_TO_54, // Different age group
        gender = Person.Gender.MALE,
      )

    // Write events to storage
    val eventSource =
      setupStorageWithEvents(
        mapOf(
          eventGroup1 to mobileEvents,
          eventGroup2 to desktopEvents,
          eventGroup3 to tabletEvents,
        ),
        testDate,
      )

    // Create single requisition that spans all three event groups
    val multiGroupRequisition =
      createRequisition(
        name = "requisitions/cross-platform-males",
        eventGroupsMap =
          mapOf(
            eventGroup1 to createTimeRange(testDate),
            eventGroup2 to createTimeRange(testDate),
            eventGroup3 to createTimeRange(testDate),
          ),
        filter = "person.gender == 1", // All males across all platforms
      )

    // Create another requisition targeting only younger males (should exclude tablet events)
    val youngMalesRequisition =
      createRequisition(
        name = "requisitions/young-males-cross-platform",
        eventGroupsMap =
          mapOf(
            eventGroup1 to createTimeRange(testDate),
            eventGroup2 to createTimeRange(testDate),
            eventGroup3 to createTimeRange(testDate),
          ),
        filter = "person.gender == 1 && person.age_group == 1", // Young males only
      )

    val requisitions = listOf(multiGroupRequisition, youngMalesRequisition)
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 20, channelCapacity = 100, threadPoolSize = 3, workers = 3)

    // Process requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
        config = config,
        eventDescriptor = TestEvent.getDescriptor(),
      )

    // Verify results
    assertThat(result).hasSize(2)

    val allMalesReach = result[multiGroupRequisition.name]!!.getReach()
    val youngMalesReach = result[youngMalesRequisition.name]!!.getReach()

    // All males across all platforms (40 + 30 + 20 = 90)
    assertThat(allMalesReach).isEqualTo(90L)

    // Young males only - excludes tablet events which have age_group==2 (40 + 30 = 70)
    assertThat(youngMalesReach).isEqualTo(70L)

    // Verify frequency distributions
    val allMalesFreq = result[multiGroupRequisition.name]!!.getByteArray().sum()
    val youngMalesFreq = result[youngMalesRequisition.name]!!.getByteArray().sum()

    assertThat(allMalesFreq).isEqualTo(90L) // Total impressions
    assertThat(youngMalesFreq).isEqualTo(70L) // Young males only
  }

  @Test
  fun `process requisitions with time-based filtering`() = runBlocking {
    val today = LocalDate.now()
    val yesterday = today.minusDays(1)
    val twoDaysAgo = today.minusDays(2)
    val eventGroup = "time-filtered-events"

    // Create events across different days
    val eventsYesterday =
      createTestEvents(
        startVid = 1L,
        count = 25,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
        eventDate = yesterday,
      )

    val eventsTwoDaysAgo =
      createTestEvents(
        startVid = 26L,
        count = 25,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
        eventDate = twoDaysAgo,
      )

    // Write events to storage for both days
    val eventSource =
      setupMultiDayStorage(
        mapOf(
          yesterday to mapOf(eventGroup to eventsYesterday),
          twoDaysAgo to mapOf(eventGroup to eventsTwoDaysAgo),
        )
      )

    // Create requisition that only includes yesterday's data
    val requisitionYesterday =
      createRequisition(
        name = "requisitions/time-filter-yesterday",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(yesterday, yesterday)),
        filter = "",
      )

    // Create requisition that includes both days
    val requisitionBothDays =
      createRequisition(
        name = "requisitions/time-filter-both",
        eventGroupsMap = mapOf(eventGroup to createTimeRange(twoDaysAgo, yesterday)),
        filter = "",
      )

    // Create event source from event reader

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 10, channelCapacity = 100, threadPoolSize = 2, workers = 2)

    // Process requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = listOf(requisitionYesterday, requisitionBothDays),
        eventGroupReferenceIdMap =
          createEventGroupReferenceIdMap(listOf(requisitionYesterday, requisitionBothDays)),
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
    val events =
      buildList<LabeledImpression> {
        var vid = 1L

        // 20 Male 18-34
        repeat(20) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_18_TO_34,
              gender = Person.Gender.MALE,
              eventDate = testDate,
            )
          )
        }

        // 15 Female 18-34
        repeat(15) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_18_TO_34,
              gender = Person.Gender.FEMALE,
              eventDate = testDate,
            )
          )
        }

        // 25 Male 35-44
        repeat(25) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_35_TO_54,
              gender = Person.Gender.MALE,
              eventDate = testDate,
            )
          )
        }

        // 10 Female 35-44
        repeat(10) {
          add(
            createLabeledImpression(
              vid = vid++,
              ageGroup = Person.AgeGroup.YEARS_35_TO_54,
              gender = Person.Gender.FEMALE,
              eventDate = testDate,
            )
          )
        }
      }

    // Write events to storage
    val eventSource = setupStorageWithEvents(mapOf(eventGroup to events), testDate)

    // Create requisitions with different filters
    val requisitions =
      listOf(
        // All males (45 total)
        createRequisition(
          name = "requisitions/all-males",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.gender == 1",
        ),
        // All 18-34 (35 total)
        createRequisition(
          name = "requisitions/age-18-34",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.age_group == 1",
        ),
        // Male AND 18-34 (20 total - intersection)
        createRequisition(
          name = "requisitions/male-18-34",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.gender == 1 && person.age_group == 1",
        ),
        // All events (70 total)
        createRequisition(
          name = "requisitions/all-events",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "",
        ),
      )

    // Create event source from event reader

    // Load population spec
    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 10, channelCapacity = 100, threadPoolSize = 4, workers = 4)

    // Process requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
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
  fun `process requisitions with complex CEL expressions`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "complex-filter-group"

    // Create comprehensive test dataset
    val events =
      buildList<LabeledImpression> {
        var vid = 1L

        // Create combinations of all attributes for comprehensive testing
        val ageGroups =
          listOf(
            Person.AgeGroup.YEARS_18_TO_34,
            Person.AgeGroup.YEARS_35_TO_54,
            Person.AgeGroup.YEARS_55_PLUS,
          )
        val genders = listOf(Person.Gender.MALE, Person.Gender.FEMALE)
        val socialGrades =
          listOf(
            Person.SocialGradeGroup.A_B_C1,
            Person.SocialGradeGroup.SOCIAL_GRADE_GROUP_UNSPECIFIED,
          )

        // Generate 5 events for each combination (3 * 2 * 2 * 5 = 60 events)
        ageGroups.forEach { ageGroup ->
          genders.forEach { gender ->
            socialGrades.forEach { socialGrade ->
              repeat(5) {
                add(
                  createLabeledImpressionWithSocialGrade(
                    vid = vid++,
                    ageGroup = ageGroup,
                    gender = gender,
                    socialGrade = socialGrade,
                    eventDate = testDate,
                  )
                )
              }
            }
          }
        }
      }

    val eventSource = setupStorageWithEvents(mapOf(eventGroup to events), testDate)

    // Create requisitions with complex CEL expressions
    val requisitions =
      listOf(
        // Simple AND condition: Young males
        createRequisition(
          name = "requisitions/young-males",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.age_group == 1 && person.gender == 1",
        ),

        // OR condition: Either very young or very old
        createRequisition(
          name = "requisitions/young-or-senior",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.age_group == 1 || person.age_group == 3",
        ),

        // Complex AND/OR combination: (Young males) OR (Senior females)
        createRequisition(
          name = "requisitions/young-males-or-senior-females",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter =
            "(person.age_group == 1 && person.gender == 1) || (person.age_group == 3 && person.gender == 2)",
        ),

        // Multiple AND conditions: Premium demographic
        createRequisition(
          name = "requisitions/premium-demographic",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.age_group == 2 && person.gender == 1 && person.social_grade_group == 1",
        ),

        // NOT condition equivalent using inequalities: Non-young males
        createRequisition(
          name = "requisitions/non-young-males",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.gender == 1 && person.age_group != 1",
        ),

        // Complex nested condition: Young people with high social grade OR middle-aged males
        createRequisition(
          name = "requisitions/complex-nested",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter =
            "(person.age_group == 1 && person.social_grade_group == 1) || (person.age_group == 2 && person.gender == 1)",
        ),
      )

    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 15, channelCapacity = 100, threadPoolSize = 3, workers = 3)

    // Process all requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
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

    // Verify logical relationships
    // Young males: age_group=1 AND gender=1 → 5 events (1 age * 1 gender * 2 social_grades * 5
    // events)
    assertThat(youngMalesReach).isEqualTo(10L)

    // Young OR senior: age_group=1 OR age_group=3 → 40 events (2 ages * 2 genders * 2 social_grades
    // * 5)
    assertThat(youngOrSeniorReach).isEqualTo(40L)

    // Premium demographic: age_group=2 AND gender=1 AND social_grade_group=1 → 5 events
    assertThat(premiumDemographicReach).isEqualTo(5L)

    // Non-young males: gender=1 AND age_group!=1 → 20 events (2 non-young ages * 1 gender * 2
    // social_grades * 5)
    assertThat(nonYoungMalesReach).isEqualTo(20L)

    // Young males OR senior females: (age=1,gender=1) OR (age=3,gender=2) → 20 events total
    assertThat(youngMalesOrSeniorFemalesReach).isEqualTo(20L)

    // Complex nested should be at least the premium demographic
    assertThat(complexNestedReach).isAtLeast(5L)
  }

  @Test
  fun `process requisitions with empty and edge case results`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val pastDate = LocalDate.now().minusDays(10)
    val eventGroup = "edge-case-group"

    // Create limited test events
    val events =
      createTestEvents(
        startVid = 1L,
        count = 20,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
      )

    val eventSource = setupStorageWithEvents(mapOf(eventGroup to events), testDate)

    // Create requisitions that should return empty or minimal results
    val requisitions =
      listOf(
        // Filter that matches no events (no females in data)
        createRequisition(
          name = "requisitions/no-matches-gender",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.gender == 2", // FEMALE - not in data
        ),

        // Filter that matches no events (wrong age group)
        createRequisition(
          name = "requisitions/no-matches-age",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.age_group == 3", // YEARS_55_PLUS - not in data
        ),

        // Time range that doesn't overlap (past date)
        createRequisition(
          name = "requisitions/no-time-overlap",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(pastDate)),
          filter = "",
        ),

        // Non-existent event group
        createRequisition(
          name = "requisitions/nonexistent-event-group",
          eventGroupsMap = mapOf("nonexistent-group" to createTimeRange(testDate)),
          filter = "",
        ),

        // Complex filter that should match everything (sanity check)
        createRequisition(
          name = "requisitions/match-all-males",
          eventGroupsMap = mapOf(eventGroup to createTimeRange(testDate)),
          filter = "person.gender == 1 && person.age_group == 1",
        ),
      )

    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 10, channelCapacity = 50, threadPoolSize = 2, workers = 2)

    // Process all requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
        config = config,
        eventDescriptor = TestEvent.getDescriptor(),
      )

    // Verify results
    assertThat(result).hasSize(5)

    val noMatchesGenderReach = result["requisitions/no-matches-gender"]!!.getReach()
    val noMatchesAgeReach = result["requisitions/no-matches-age"]!!.getReach()
    val noTimeOverlapReach = result["requisitions/no-time-overlap"]!!.getReach()
    val nonexistentEventGroupReach = result["requisitions/nonexistent-event-group"]!!.getReach()
    val matchAllMalesReach = result["requisitions/match-all-males"]!!.getReach()

    // Verify empty results
    assertThat(noMatchesGenderReach).isEqualTo(0L) // No females in data
    assertThat(noMatchesAgeReach).isEqualTo(0L) // No seniors in data
    assertThat(noTimeOverlapReach).isEqualTo(0L) // Wrong time range
    assertThat(nonexistentEventGroupReach).isEqualTo(0L) // Event group doesn't exist

    // Verify positive control
    assertThat(matchAllMalesReach).isEqualTo(20L) // Should match all events

    // Verify all frequency vectors are well-formed (no negative values, etc.)
    result.values.forEach { freq ->
      assertThat(freq.getByteArray().all { it >= 0 }).isTrue()
      assertThat(freq.getByteArray().sum() >= 0L).isTrue()
    }
  }

  @Test
  fun `process requisitions with overlapping time ranges across event groups`() = runBlocking {
    val baseDate = LocalDate.now().minusDays(3)
    val day1 = baseDate
    val day2 = baseDate.plusDays(1)
    val day3 = baseDate.plusDays(2)

    val eventGroup1 = "daily-events-1"
    val eventGroup2 = "daily-events-2"
    val eventGroup3 = "daily-events-3"

    // Create events spanning multiple days
    val day1Events =
      createTestEvents(
        startVid = 1L,
        count = 15,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
        eventDate = day1,
      )

    val day2Events =
      createTestEvents(
        startVid = 16L,
        count = 20,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
        eventDate = day2,
      )

    val day3Events =
      createTestEvents(
        startVid = 36L,
        count = 25,
        ageGroup = Person.AgeGroup.YEARS_18_TO_34,
        gender = Person.Gender.MALE,
        eventDate = day3,
      )

    // Setup multi-day storage
    val eventSource =
      setupMultiDayStorage(
        mapOf(
          day1 to mapOf(eventGroup1 to day1Events),
          day2 to mapOf(eventGroup2 to day2Events),
          day3 to mapOf(eventGroup3 to day3Events),
        )
      )

    // Create requisitions with overlapping time ranges
    val requisitions =
      listOf(
        // Day 1 only
        createRequisition(
          name = "requisitions/day1-only",
          eventGroupsMap = mapOf(eventGroup1 to createTimeRange(day1)),
          filter = "",
        ),

        // Day 1-2 span (using both event groups)
        createRequisition(
          name = "requisitions/day1-2-span",
          eventGroupsMap =
            mapOf(
              eventGroup1 to createTimeRange(day1, day2),
              eventGroup2 to createTimeRange(day1, day2),
            ),
          filter = "",
        ),

        // All three days
        createRequisition(
          name = "requisitions/all-days",
          eventGroupsMap =
            mapOf(
              eventGroup1 to createTimeRange(day1, day3),
              eventGroup2 to createTimeRange(day1, day3),
              eventGroup3 to createTimeRange(day1, day3),
            ),
          filter = "",
        ),

        // Partial overlap: day2-3 span on all event groups (should miss day1 events)
        createRequisition(
          name = "requisitions/day2-3-span",
          eventGroupsMap =
            mapOf(
              eventGroup1 to createTimeRange(day2, day3), // No overlap with day1 events
              eventGroup2 to createTimeRange(day2, day3), // Full overlap
              eventGroup3 to createTimeRange(day2, day3), // Full overlap
            ),
          filter = "",
        ),
      )

    val populationSpec = loadPopulationSpecFromFile(TEST_DATA_RUNTIME_PATH.toString(), true)
    val vidIndexMap = InMemoryVidIndexMap.build(populationSpec)

    val config =
      PipelineConfiguration(batchSize = 12, channelCapacity = 100, threadPoolSize = 3, workers = 3)

    // Process all requisitions
    val result =
      eventProcessingOrchestrator.run(
        eventSource = eventSource,
        vidIndexMap = vidIndexMap,
        populationSpec = populationSpec,
        requisitions = requisitions,
        eventGroupReferenceIdMap = createEventGroupReferenceIdMap(requisitions),
        config = config,
        eventDescriptor = TestEvent.getDescriptor(),
      )

    // Verify results
    assertThat(result).hasSize(4)

    val day1OnlyReach = result["requisitions/day1-only"]!!.getReach()
    val day12SpanReach = result["requisitions/day1-2-span"]!!.getReach()
    val allDaysReach = result["requisitions/all-days"]!!.getReach()
    val day23SpanReach = result["requisitions/day2-3-span"]!!.getReach()

    // Verify time range filtering
    assertThat(day1OnlyReach).isEqualTo(15L) // Only day1 events
    assertThat(day12SpanReach).isEqualTo(35L) // day1 + day2 events (15 + 20)
    assertThat(allDaysReach).isEqualTo(60L) // All events (15 + 20 + 25)
    assertThat(day23SpanReach).isEqualTo(45L) // day2 + day3 events (20 + 25), day1 excluded

    // Verify relationships
    assertThat(allDaysReach).isGreaterThan(day12SpanReach)
    assertThat(allDaysReach).isGreaterThan(day23SpanReach)
    assertThat(day12SpanReach).isGreaterThan(day1OnlyReach)
  }

  // Helper functions

  private fun loadPopulationSpecFromFile(path: String, isSynthetic: Boolean): PopulationSpec {
    val file = java.io.File(path)
    require(file.exists()) { "Population spec file not found: ${file.absolutePath}" }

    return parseTextProto(file, PopulationSpec.getDefaultInstance())
  }

  /** Creates a simple test event source from an EventReader. */
  private fun createEventSourceFromReader(
    eventReader: EventReader<Message>,
    eventGroups: List<String>,
    startDate: LocalDate,
    endDate: LocalDate,
    eventGroupReferenceId: String,
  ): EventSource<Message> {
    return TestEventSource(eventReader, eventGroups, startDate, endDate, eventGroupReferenceId)
  }

  /** Creates an event source for a single event group. */
  private suspend fun setupSingleEventGroupStorage(
    events: List<LabeledImpression>,
    eventGroupReferenceId: String,
    testDate: LocalDate,
  ): EventSource<Message> {
    val eventReader = setupStorageWithEvents(mapOf(eventGroupReferenceId to events), testDate)
    return eventReader // MultiGroupEventSource can be used as EventSource directly
  }

  /** EventSource that combines multiple EventReaders for different event groups. */
  /** Combined event reader that reads from multiple EventReaders sequentially */
  private class CombinedEventReader(private val readers: List<EventReader<Message>>) :
    EventReader<Message> {
    override suspend fun readEvents(): Flow<List<LabeledEvent<Message>>> {
      return flow {
        readers.forEach { reader ->
          reader.readEvents().collect { events ->
            if (events.isNotEmpty()) {
              emit(events)
            }
          }
        }
      }
    }
  }

  private class MultiGroupEventSource(
    private val readerMap: Map<String, EventReader<Message>>,
    private val eventGroups: List<String>,
    private val startDate: LocalDate,
    private val endDate: LocalDate,
  ) : EventSource<Message> {
    override fun generateEventBatches(): Flow<EventBatch<Message>> {
      return flow {
        // Read events from all readers and emit them with correct eventGroupReferenceId
        readerMap.forEach { (eventGroupReferenceId, reader) ->
          reader.readEvents().collect { eventList ->
            if (eventList.isNotEmpty()) {
              // Calculate min and max times from the actual events
              val eventTimes = eventList.map { labeledEvent -> labeledEvent.timestamp }
              val minTime = eventTimes.minOrNull() ?: Instant.now()
              val maxTime = eventTimes.maxOrNull() ?: Instant.now()

              emit(
                EventBatch(
                  eventList,
                  minTime,
                  maxTime,
                  eventGroupReferenceId = eventGroupReferenceId,
                )
              )
            }
          }
        }
      }
    }
  }

  /** Simple test implementation of EventSource that wraps an EventReader. */
  private class TestEventSource(
    private val eventReader: EventReader<Message>,
    private val eventGroups: List<String>,
    private val startDate: LocalDate,
    private val endDate: LocalDate,
    private val eventGroupReferenceId: String,
  ) : EventSource<Message> {
    override fun generateEventBatches(): Flow<EventBatch<Message>> {
      return flow {
        // Read events from the eventReader and emit them as EventBatches
        eventReader.readEvents().collect { eventList ->
          if (eventList.isNotEmpty()) {
            // Calculate min and max times from the actual events
            val eventTimes = eventList.map { labeledEvent -> labeledEvent.timestamp }
            val minTime = eventTimes.minOrNull() ?: Instant.now()
            val maxTime = eventTimes.maxOrNull() ?: Instant.now()

            emit(
              EventBatch(eventList, minTime, maxTime, eventGroupReferenceId = eventGroupReferenceId)
            )
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
  ): List<LabeledImpression> {
    return (startVid until startVid + count).map { vid ->
      createLabeledImpression(vid, ageGroup, gender, eventDate)
    }
  }

  private fun createLabeledImpression(
    vid: Long,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    eventDate: LocalDate,
  ): LabeledImpression {
    return labeledImpression {
      eventTime = createTimeRange(eventDate).start.toProtoTime()
      this.vid = vid
      event =
        testEvent {
            person = person {
              this.ageGroup = ageGroup
              this.gender = gender
              socialGradeGroup = Person.SocialGradeGroup.A_B_C1
            }
          }
          .pack()
    }
  }

  private fun createLabeledImpressionWithSocialGrade(
    vid: Long,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
    socialGrade: Person.SocialGradeGroup,
    eventDate: LocalDate,
  ): LabeledImpression {
    return labeledImpression {
      eventTime = createTimeRange(eventDate).start.toProtoTime()
      this.vid = vid
      event =
        testEvent {
            person = person {
              this.ageGroup = ageGroup
              this.gender = gender
              socialGradeGroup = socialGrade
            }
          }
          .pack()
    }
  }

  private suspend fun setupStorageWithEvents(
    eventsByGroup: Map<String, List<LabeledImpression>>,
    testDate: LocalDate,
  ): MultiGroupEventSource {
    val impressionsTmpPath = tempFolder.newFolder("test-impressions")
    val dekTmpPath = tempFolder.newFolder("test-deks")

    eventsByGroup.forEach { (eventGroup, events) ->
      writeEventsToStorage(events, eventGroup, testDate, impressionsTmpPath, dekTmpPath)
    }

    // Create readers map for all event groups
    val readerMap =
      eventsByGroup.keys.associateWith { eventGroup ->
        createStorageEventReader(testDate, eventGroup, impressionsTmpPath, dekTmpPath)
      }

    return MultiGroupEventSource(readerMap, eventsByGroup.keys.toList(), testDate, testDate)
  }

  private suspend fun setupMultiDayStorage(
    eventsByDayAndGroup: Map<LocalDate, Map<String, List<LabeledImpression>>>
  ): MultiGroupEventSource {
    val impressionsTmpPath = tempFolder.newFolder("test-impressions-multiday")
    val dekTmpPath = tempFolder.newFolder("test-deks-multiday")

    eventsByDayAndGroup.forEach { (date, eventsByGroup) ->
      eventsByGroup.forEach { (eventGroup, events) ->
        writeEventsToStorage(events, eventGroup, date, impressionsTmpPath, dekTmpPath)
      }
    }

    // Create readers for all date/event group combinations
    val readers =
      eventsByDayAndGroup.flatMap { (date, eventsByGroup) ->
        eventsByGroup.keys.map { eventGroup ->
          createStorageEventReader(date, eventGroup, impressionsTmpPath, dekTmpPath)
        }
      }

    // Create readers map grouped by event group (combining readers for same event group across
    // dates)
    val readerMap = mutableMapOf<String, EventReader<Message>>()
    val allEventGroups = eventsByDayAndGroup.values.flatMap { it.keys }.distinct()

    allEventGroups.forEach { eventGroup ->
      // Find all readers for this event group across all dates
      val readersForGroup = mutableListOf<EventReader<Message>>()
      eventsByDayAndGroup.forEach { (date, eventsByGroup) ->
        if (eventsByGroup.containsKey(eventGroup)) {
          val reader = createStorageEventReader(date, eventGroup, impressionsTmpPath, dekTmpPath)
          readersForGroup.add(reader)
        }
      }

      // Create a combined reader that reads from all readers for this event group
      if (readersForGroup.size == 1) {
        readerMap[eventGroup] = readersForGroup[0]
      } else {
        readerMap[eventGroup] = CombinedEventReader(readersForGroup)
      }
    }

    val startDate = eventsByDayAndGroup.keys.minOrNull() ?: LocalDate.now()
    val endDate = eventsByDayAndGroup.keys.maxOrNull() ?: LocalDate.now()
    return MultiGroupEventSource(readerMap, readerMap.keys.toList(), startDate, endDate)
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
  ): EventReader<Message> {
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
    dekRoot: java.io.File,
  ) {
    val impressionsBucketDir = impressionsRoot.resolve("impressions")
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)

    // Use non-encrypted storage for tests
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(impressionsStorageClient)

    val impressionsFlow = flow { events.forEach { impression -> emit(impression.toByteString()) } }

    // Write with key "$date/$eventGroup" under the "impressions" bucket directory
    mesosRecordIoStorageClient.writeBlob("$date/$eventGroup", impressionsFlow)

    // Set up DEK storage (even though we're not using encryption, the reader expects metadata)
    val deksBucketDir = dekRoot.resolve("deks")
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val blobDetails = blobDetails { blobUri = "file:///impressions/$date/$eventGroup" }

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

  private fun createEventGroupReferenceIdMap(requisitions: List<Requisition>): Map<String, String> {
    val eventGroupReferenceIdMap = mutableMapOf<String, String>()
    for (requisition in requisitions) {
      val signedRequisitionSpec =
        decryptRequisitionSpec(requisition.encryptedRequisitionSpec, PRIVATE_ENCRYPTION_KEY)
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      for (eventGroup in requisitionSpec.events.eventGroupsList) {
        // Extract event group reference ID from the event group resource name
        // In test data, the event group resource name IS the reference ID
        eventGroupReferenceIdMap[eventGroup.key] = eventGroup.key
      }
    }
    return eventGroupReferenceIdMap
  }

  private fun createRequisition(
    name: String,
    eventGroupsMap: Map<String, OpenEndTimeRange>,
    filter: String,
  ): Requisition {
    val measurementSpec = measurementSpec {
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
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
    }

    return requisition {
      this.name = name
      measurement = "measurements/test-measurement"
      state = Requisition.State.UNFULFILLED
      this.measurementSpec = signedMessage { message = measurementSpec.pack() }
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
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
        events =
          RequisitionSpecKt.events {
            eventGroups +=
              eventGroupsMap.map { (eventGroupName, timeRange) ->
                RequisitionSpecKt.eventGroupEntry {
                  key = eventGroupName
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      collectionInterval =
                        com.google.type.interval {
                          startTime = timeRange.start.toProtoTime()
                          endTime = timeRange.endExclusive.toProtoTime()
                        }
                      if (filter.isNotEmpty()) {
                        this.filter = RequisitionSpecKt.eventFilter { expression = filter }
                      }
                    }
                }
              }
          }
        measurementPublicKey = MC_PUBLIC_KEY.pack()
        nonce = SecureRandom.getInstance("SHA1PRNG").nextLong()
      }

      encryptedRequisitionSpec =
        encryptRequisitionSpec(
          signRequisitionSpec(requisitionSpec, MC_SIGNING_KEY),
          DATA_PROVIDER_PUBLIC_KEY,
        )
    }
  }

  // Extension functions for StripedByteFrequencyVector analysis
  private fun StripedByteFrequencyVector.getReach(): Long {
    return this.getByteArray().count { it > 0 }.toLong()
  }

  private fun StripedByteFrequencyVector.getTotalCount(): Long {
    return this.getByteArray().sum().toLong()
  }

  private fun StripedByteFrequencyVector.getAverageFrequency(): Double {
    val reach = getReach()
    return if (reach > 0) getTotalCount().toDouble() / reach else 0.0
  }

  companion object {
    private val SECRET_FILES_PATH =
      checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "resultsfulfiller",
        "testing",
        "small_population_spec.textproto",
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

    private val MC_SIGNING_KEY =
      loadSigningKey(
        "${MEASUREMENT_CONSUMER_ID}_cs_cert.der",
        "${MEASUREMENT_CONSUMER_ID}_cs_private.der",
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

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
