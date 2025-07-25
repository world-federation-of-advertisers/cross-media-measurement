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
import com.google.type.Interval
import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha.VidIndexMap
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
 * Simplified integration tests for event processing pipeline.
 * Tests the core pipeline functionality without requisition complexities.
 */
@RunWith(JUnit4::class)
class EventProcessingIntegrationSimpleTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private lateinit var kmsClient: KmsClient
  private lateinit var kekUri: String
  private lateinit var serializedEncryptionKey: ByteString
  private lateinit var typeRegistry: TypeRegistry

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
  }

  @Test
  fun `pipeline processes events from multiple event groups with filters`() = runBlocking {
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
    
    // Create storage event source
    val storageEventSource = StorageEventSource(
      eventReader = eventReader,
      dateRange = testDate..testDate,
      eventGroupReferenceIds = listOf(eventGroup1, eventGroup2),
      batchSize = 10
    )
    
    // Create VID index map
    val vidIndexMap = mock<VidIndexMap> {
      on { get(any()) } doAnswer { invocation ->
        val vid = invocation.arguments[0] as Long
        if (vid in 1L..100L) {
          (vid - 1).toInt()  // Map VID to index
        } else {
          -1  // Invalid VID
        }
      }
      on { size } doReturn 200L
    }
    
    // Create pipeline
    val pipeline = ParallelBatchedPipeline(
      batchSize = 10,
      workers = 2,
      dispatcher = Dispatchers.Default
    )
    
    // Create filters
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000 - 86400))
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000))
      .build()
    
    val filters = listOf(
      // Filter for males from event group 1
      FilterConfiguration(
        filterSpec = FilterSpec(
          celExpression = "person.gender == 1",
          collectionInterval = timeInterval,
          vidSamplingStart = 0L,
          vidSamplingWidth = 1000000L,
          eventGroupReferenceId = eventGroup1
        ),
        requisitionNames = setOf("filter1")
      ),
      // Filter for females from event group 2
      FilterConfiguration(
        filterSpec = FilterSpec(
          celExpression = "person.gender == 2",
          collectionInterval = timeInterval,
          vidSamplingStart = 0L,
          vidSamplingWidth = 1000000L,
          eventGroupReferenceId = eventGroup2
        ),
        requisitionNames = setOf("filter2")
      )
    )
    
    // Generate event batches from storage
    val eventBatchFlow = storageEventSource.generateEventBatches(Dispatchers.Default)
    
    // Convert Any to DynamicMessage flow
    val dynamicMessageEventBatchFlow = eventBatchFlow.map { batch ->
      batch.map { event ->
        if (event.message is com.google.protobuf.Any) {
          val message = event.message as com.google.protobuf.Any
          val descriptor = typeRegistry.getDescriptorForTypeUrl(message.typeUrl)
            ?: error("Unknown message type: ${message.typeUrl}")
          val dynamicMessage = com.google.protobuf.DynamicMessage.parseFrom(descriptor, message.value)
          LabeledEvent(
            timestamp = event.timestamp,
            vid = event.vid,
            message = dynamicMessage,
            eventGroupReferenceId = event.eventGroupReferenceId
          )
        } else {
          event as LabeledEvent<com.google.protobuf.DynamicMessage>
        }
      }
    }
    
    // Process events through pipeline
    val result = pipeline.processEventBatches(
      eventBatchFlow = dynamicMessageEventBatchFlow,
      vidIndexMap = vidIndexMap,
      filters = filters,
      typeRegistry = typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(2)
    
    // Filter 1 should have processed male users from event group 1
    val freq1 = result[filters[0].filterSpec]
    assertThat(freq1).isNotNull()
    assertThat(freq1!!.getReach()).isEqualTo(30L)
    
    // Filter 2 should have processed female users from event group 2
    val freq2 = result[filters[1].filterSpec]
    assertThat(freq2).isNotNull()
    assertThat(freq2!!.getReach()).isEqualTo(40L)
    
    // Verify frequency distributions
    assertThat(freq1.getFrequencyDistribution()[1]).isEqualTo(30L)
    assertThat(freq2.getFrequencyDistribution()[1]).isEqualTo(40L)
  }

  @Test
  fun `pipeline handles VID sampling correctly`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "vid-sampling-group"
    
    // Create 100 events with sequential VIDs
    val events = (1L..100L).map { vid ->
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
    
    // Write events to storage
    val eventReader = setupStorageWithEvents(
      mapOf(eventGroup to events),
      testDate
    )
    
    // Create storage event source
    val storageEventSource = StorageEventSource(
      eventReader = eventReader,
      dateRange = testDate..testDate,
      eventGroupReferenceIds = listOf(eventGroup),
      batchSize = 20
    )
    
    // Create VID index map with full range
    val vidIndexMap = mock<VidIndexMap> {
      on { get(any()) } doAnswer { invocation ->
        val vid = invocation.arguments[0] as Long
        if (vid in 1L..100L) {
          (vid - 1).toInt()  // Map VID to index
        } else {
          -1  // Invalid VID
        }
      }
      on { size } doReturn 200L
    }
    
    // Create pipeline
    val pipeline = ParallelBatchedPipeline(
      batchSize = 20,
      workers = 2,
      dispatcher = Dispatchers.Default
    )
    
    // Create filters with different VID sampling ranges
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000 - 86400))
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000))
      .build()
    
    val filters = listOf(
      // First 50 VIDs
      FilterConfiguration(
        filterSpec = FilterSpec(
          celExpression = "",
          collectionInterval = timeInterval,
          vidSamplingStart = 1L,
          vidSamplingWidth = 50L,
          eventGroupReferenceId = eventGroup
        ),
        requisitionNames = setOf("first50")
      ),
      // VIDs 51-100
      FilterConfiguration(
        filterSpec = FilterSpec(
          celExpression = "",
          collectionInterval = timeInterval,
          vidSamplingStart = 51L,
          vidSamplingWidth = 50L,
          eventGroupReferenceId = eventGroup
        ),
        requisitionNames = setOf("last50")
      ),
      // All VIDs
      FilterConfiguration(
        filterSpec = FilterSpec(
          celExpression = "",
          collectionInterval = timeInterval,
          vidSamplingStart = 1L,
          vidSamplingWidth = 100L,
          eventGroupReferenceId = eventGroup
        ),
        requisitionNames = setOf("all")
      )
    )
    
    // Process events
    val eventBatchFlow = storageEventSource.generateEventBatches(Dispatchers.Default)
    val dynamicMessageEventBatchFlow = eventBatchFlow.map { batch ->
      batch.map { event ->
        if (event.message is com.google.protobuf.Any) {
          val message = event.message as com.google.protobuf.Any
          val descriptor = typeRegistry.getDescriptorForTypeUrl(message.typeUrl)
            ?: error("Unknown message type: ${message.typeUrl}")
          val dynamicMessage = com.google.protobuf.DynamicMessage.parseFrom(descriptor, message.value)
          LabeledEvent(
            timestamp = event.timestamp,
            vid = event.vid,
            message = dynamicMessage,
            eventGroupReferenceId = event.eventGroupReferenceId
          )
        } else {
          event as LabeledEvent<com.google.protobuf.DynamicMessage>
        }
      }
    }
    
    val result = pipeline.processEventBatches(
      eventBatchFlow = dynamicMessageEventBatchFlow,
      vidIndexMap = vidIndexMap,
      filters = filters,
      typeRegistry = typeRegistry
    )
    
    // Verify results
    assertThat(result).hasSize(3)
    
    val freq1 = result[filters[0].filterSpec]!!
    val freq2 = result[filters[1].filterSpec]!!
    val freq3 = result[filters[2].filterSpec]!!
    
    assertThat(freq1.getReach()).isEqualTo(50L)
    assertThat(freq2.getReach()).isEqualTo(50L)
    assertThat(freq3.getReach()).isEqualTo(100L)
    
    // Verify VID ranges
    val vids1 = freq1.getVids()
    val vids2 = freq2.getVids()
    val vids3 = freq3.getVids()
    
    assertThat(vids1.min()).isEqualTo(1L)
    assertThat(vids1.max()).isEqualTo(50L)
    
    assertThat(vids2.min()).isEqualTo(51L)
    assertThat(vids2.max()).isEqualTo(100L)
    
    assertThat(vids3.min()).isEqualTo(1L)
    assertThat(vids3.max()).isEqualTo(100L)
  }

  @Test
  fun `pipeline handles frequency counting correctly`() = runBlocking {
    val testDate = LocalDate.now().minusDays(1)
    val eventGroup = "frequency-group"
    
    // Create events with different frequency patterns
    val events = mutableListOf<LabeledImpression>()
    
    // VIDs 1-10: 1 impression each
    (1L..10L).forEach { vid ->
      events.add(createLabeledImpression(vid, Person.AgeGroup.YEARS_18_TO_34, Person.Gender.MALE, testDate))
    }
    
    // VIDs 11-20: 3 impressions each
    (11L..20L).forEach { vid ->
      repeat(3) {
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
    
    // Create storage event source
    val storageEventSource = StorageEventSource(
      eventReader = eventReader,
      dateRange = testDate..testDate,
      eventGroupReferenceIds = listOf(eventGroup),
      batchSize = 20
    )
    
    // Create VID index map
    val vidIndexMap = mock<VidIndexMap> {
      on { get(any()) } doAnswer { invocation ->
        val vid = invocation.arguments[0] as Long
        if (vid in 1L..30L) {
          (vid - 1).toInt()  // Map VID to index
        } else {
          -1  // Invalid VID
        }
      }
      on { size } doReturn 100L
    }
    
    // Create pipeline
    val pipeline = ParallelBatchedPipeline(
      batchSize = 20,
      workers = 1,
      dispatcher = Dispatchers.Default
    )
    
    // Create filter
    val timeInterval = Interval.newBuilder()
      .setStartTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000 - 86400))
      .setEndTime(com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(System.currentTimeMillis() / 1000))
      .build()
    
    val filter = FilterConfiguration(
      filterSpec = FilterSpec(
        celExpression = "",
        collectionInterval = timeInterval,
        vidSamplingStart = 0L,
        vidSamplingWidth = 1000000L,
        eventGroupReferenceId = eventGroup
      ),
      requisitionNames = setOf("frequency-test")
    )
    
    // Process events
    val eventBatchFlow = storageEventSource.generateEventBatches(Dispatchers.Default)
    val dynamicMessageEventBatchFlow = eventBatchFlow.map { batch ->
      batch.map { event ->
        if (event.message is com.google.protobuf.Any) {
          val message = event.message as com.google.protobuf.Any
          val descriptor = typeRegistry.getDescriptorForTypeUrl(message.typeUrl)
            ?: error("Unknown message type: ${message.typeUrl}")
          val dynamicMessage = com.google.protobuf.DynamicMessage.parseFrom(descriptor, message.value)
          LabeledEvent(
            timestamp = event.timestamp,
            vid = event.vid,
            message = dynamicMessage,
            eventGroupReferenceId = event.eventGroupReferenceId
          )
        } else {
          event as LabeledEvent<com.google.protobuf.DynamicMessage>
        }
      }
    }
    
    val result = pipeline.processEventBatches(
      eventBatchFlow = dynamicMessageEventBatchFlow,
      vidIndexMap = vidIndexMap,
      filters = listOf(filter),
      typeRegistry = typeRegistry
    )
    
    // Verify results
    val freq = result[filter.filterSpec]!!
    
    // Total reach should be 25 unique VIDs
    assertThat(freq.getReach()).isEqualTo(25L)
    
    // Verify frequency distribution
    val freqDist = freq.getFrequencyDistribution()
    assertThat(freqDist[1]).isEqualTo(10L) // 10 VIDs with frequency 1
    assertThat(freqDist[3]).isEqualTo(10L) // 10 VIDs with frequency 3
    assertThat(freqDist[5]).isEqualTo(5L)  // 5 VIDs with frequency 5
    
    // Total impressions
    val totalImpressions = freq.getTotalFrequency()
    assertThat(totalImpressions).isEqualTo(10 + 30 + 25L) // 65 total impressions
    
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
      IMPRESSIONS_DEK_FILE_URI_PREFIX,
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
    val impressionsBucketDir = impressionsRoot.resolve(IMPRESSIONS_BUCKET)
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
    val deksBucketDir = dekRoot.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)
    
    val encryptedDek = EncryptedDek.newBuilder()
      .setKekUri(kekUri)
      .setEncryptedDek(serializedEncryptionKey)
      .build()
    
    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$date"
      this.encryptedDek = encryptedDek
    }
    
    impressionsDekStorageClient.writeBlob(
      "ds/$date/event-group-reference-id/$eventGroup/metadata",
      blobDetails.toByteString(),
    )
  }

  private fun createTimeRange(date: LocalDate): OpenEndTimeRange {
    return OpenEndTimeRange.fromClosedDateRange(date..date)
  }

  companion object {
    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET"
    private const val IMPRESSIONS_DEK_BUCKET = "impression-dek-bucket"
    private const val IMPRESSIONS_DEK_FILE_URI_PREFIX = "file:///$IMPRESSIONS_DEK_BUCKET"
  }
}