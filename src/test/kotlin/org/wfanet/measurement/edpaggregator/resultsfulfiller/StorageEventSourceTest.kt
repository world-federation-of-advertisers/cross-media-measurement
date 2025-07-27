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
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import com.google.protobuf.timestamp
import com.google.type.interval
import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.*
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class StorageEventSourceTest {

  @get:Rule
  val tmp = TemporaryFolder()

  private val modelLine = "test-model-line"
  private val impressionsBlobDetailsUriPrefix = "file:///meta-bucket"
  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  private fun createKmsSetup(): Triple<FakeKmsClient, String, ByteString> {
    val kmsClient = FakeKmsClient()
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

    val aeadKeyTemplate = KeyTemplates.get("AES128_GCM_HKDF_1MB")
    val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
    val serializedEncryptionKey = ByteString.copyFrom(
      TinkProtoKeysetFormat.serializeEncryptedKeyset(
        keyEncryptionHandle,
        kmsClient.getAead(kekUri),
        byteArrayOf()
      )
    )

    return Triple(kmsClient, kekUri, serializedEncryptionKey)
  }

  private fun createImpressionService(metadataTmpPath: File): StorageImpressionMetadataService {
    return StorageImpressionMetadataService(
      impressionsMetadataStorageConfig = StorageConfig(rootDirectory = metadataTmpPath),
      impressionsBlobDetailsUriPrefix = impressionsBlobDetailsUriPrefix,
      zoneIdForDates = ZoneId.of("UTC"),
    )
  }

  /**
   * Helper function to create impression files and write BlobDetails metadata.
   */
  private suspend fun createImpressionFilesForDate(
    impressionsTmpPath: File,
    metadataTmpPath: File,
    date: LocalDate,
    eventGroupRef: String,
    kmsClient: FakeKmsClient,
    kekUri: String,
    serializedEncryptionKey: ByteString,
    modelLine: String = this.modelLine,
    impressionCount: Int = 5
  ) {
    // Create impressions bucket
    val impressionsBucketDir = File(impressionsTmpPath, "impressions")
    impressionsBucketDir.mkdirs()
    val impressionsStorageClient =
      SelectedStorageClient("file:///impressions/$date/$eventGroupRef", impressionsTmpPath)

    val aeadStorageClient =
      impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

    // Create test impressions
    val impressions = (1..impressionCount).map { i ->
      LabeledImpression.newBuilder()
        .setVid(i.toLong())
        .setEventTime(date.atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime())
        .setEvent(TEST_EVENT.pack())
        .setEventGroupReferenceId(eventGroupRef)
        .build()
    }

    // Write impressions to storage
    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }
    mesosRecordIoStorageClient.writeBlob("$date/$eventGroupRef", impressionsFlow)

    // Write metadata
    val metadataBucketDir = File(metadataTmpPath, "meta-bucket")
    metadataBucketDir.mkdirs()
    val metadataFs = FileSystemStorageClient(metadataBucketDir)
    val key = "ds/$date/model-line/$modelLine/event-group-reference-id/$eventGroupRef/metadata"
    val encryptedDek = encryptedDek {
      this.kekUri = kekUri
      this.encryptedDek = serializedEncryptionKey
    }
    val blobDetailsBytes = blobDetails {
      blobUri = "file:///impressions/$date/$eventGroupRef"
      this.encryptedDek = encryptedDek
    }.toByteString()
    metadataFs.writeBlob(key, blobDetailsBytes)
  }

  private fun createEventGroupDetails(
    eventGroupReferenceId: String,
    startDate: LocalDate,
    endDate: LocalDate,
    zoneId: ZoneId,
  ): GroupedRequisitions.EventGroupDetails {
    val startInstant = startDate.atStartOfDay(zoneId).toInstant()
    val endInstant = endDate.atStartOfDay(zoneId).toInstant()

    return eventGroupDetails {
      this.eventGroupReferenceId = eventGroupReferenceId
      collectionIntervals += interval {
        startTime = timestamp {
          seconds = startInstant.epochSecond
          nanos = startInstant.nano
        }
        endTime = timestamp {
          seconds = endInstant.epochSecond
          nanos = endInstant.nano
        }
      }
    }
  }

  private fun createEventGroupDetailsWithMultipleIntervals(
    eventGroupReferenceId: String,
    intervals: List<Pair<LocalDate, LocalDate>>,
  ): GroupedRequisitions.EventGroupDetails {
    return eventGroupDetails {
      this.eventGroupReferenceId = eventGroupReferenceId

      intervals.forEach { (startDate, endDate) ->
        val startInstant = startDate.atStartOfDay(ZoneId.of("UTC")).toInstant()
        val endInstant = endDate.atStartOfDay(ZoneId.of("UTC")).toInstant()

        collectionIntervals += interval {
          startTime = timestamp {
            seconds = startInstant.epochSecond
            nanos = startInstant.nano
          }
          endTime = timestamp {
            seconds = endInstant.epochSecond
            nanos = endInstant.nano
          }
        }
      }
    }
  }

  @Test
  fun `generateEventBatches handles empty event group list`(): Unit = runBlocking {
    val eventGroupDetailsList = emptyList<GroupedRequisitions.EventGroupDetails>()

    val impressionService = createImpressionService(tmp.root)
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventGroupDetailsList = eventGroupDetailsList,
        modelLine = modelLine,
        kmsClient = null,
        impressionsStorageConfig = StorageConfig(rootDirectory = tmp.root),
        descriptor = Empty.getDescriptor(),
        batchSize = 1000,
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).isEmpty()
  }

  @Test
  fun `generateEventBatches fails when metadata is missing`(): Unit = runBlocking {
    // Set up bucket but don't write any metadata files
    val bucketName = "meta-bucket"
    val bucketDir = File(tmp.root, bucketName)
    bucketDir.mkdirs()

    val eventGroupDetailsList =
      listOf(
        createEventGroupDetails(
          "event-group-1",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        )
      )


    val impressionService = createImpressionService(tmp.root)
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventGroupDetailsList = eventGroupDetailsList,
        modelLine = modelLine,
        kmsClient = null,
        impressionsStorageConfig = StorageConfig(rootDirectory = tmp.root),
        descriptor = Empty.getDescriptor(),
        batchSize = 1000,
      )

    // The entire operation should fail when metadata reading fails
    assertFailsWith<ImpressionReadException> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }

  @Test
  fun `generateEventBatches deduplicates EventReaders for overlapping intervals`(): Unit =
    runBlocking {
      // Set up separate directories for impressions and metadata
      val impressionsTmpPath = Files.createTempDirectory(null).toFile()
      val metadataTmpPath = tmp.root
      val eventGroupRef = "event-group-1"

      val (kmsClient, kekUri, serializedEncryptionKey) = createKmsSetup()

      // Create impression files and metadata for Jan 1, 2, and 3
      for (date in listOf(
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 1, 2),
        LocalDate.of(2025, 1, 3)
      )) {
        createImpressionFilesForDate(
          impressionsTmpPath,
          metadataTmpPath,
          date,
          eventGroupRef,
          kmsClient,
          kekUri,
          serializedEncryptionKey
        )
      }

      // Create an event group with overlapping intervals
      // Interval 1: Jan 1-3 (exclusive end: Jan 1, 2), Interval 2: Jan 2-4 (exclusive end: Jan 2,
      // 3)
      // Total dates without dedup: 4, unique dates with dedup: 3 (Jan 1, 2, 3)
      val eventGroupDetailsList =
        listOf(
          createEventGroupDetailsWithMultipleIntervals(
            eventGroupRef,
            listOf(
              Pair(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 3)), // Jan 1, 2 (exclusive end)
              Pair(
                LocalDate.of(2025, 1, 2),
                LocalDate.of(2025, 1, 4),
              ), // Jan 2, 3 (exclusive end, overlap on 2)
            ),
          )
        )

      val impressionService = createImpressionService(metadataTmpPath)
      val eventSource =
        StorageEventSource(
          impressionMetadataService = impressionService,
          eventGroupDetailsList = eventGroupDetailsList,
          modelLine = modelLine,
          kmsClient = kmsClient,
          impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
          descriptor = TestEvent.getDescriptor(),
          batchSize = 1000,
        )

      val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

      // Verify the total number of events emitted
      val totalEvents = batches.flatMap { it.events }.size
      // With deduplication: 3 unique dates × 5 events per date = 15 events
      assertThat(totalEvents).isEqualTo(15)

      // Verify we have exactly 3 batches (one per unique date)
      assertThat(batches).hasSize(3)

      // Verify each batch contains the expected number of events
      assertThat(batches.all { it.events.size == 5 }).isTrue()
    }

  @Test
  fun `generateEventBatches handles EventReader exception`(): Unit = runBlocking {
    // Set up separate directories but don't create impression files to cause failure
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = tmp.root
    val eventGroupRef = "group-1"

    // Create metadata but not impression files to trigger failure in StorageEventReader
    val metadataBucketDir = File(metadataTmpPath, "meta-bucket")
    metadataBucketDir.mkdirs()
    val metadataFs = FileSystemStorageClient(metadataBucketDir)

    // Create metadata for both dates (Jan 1 and Jan 2 are in the range)
    for (date in listOf(LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2))) {
      val key = "ds/$date/model-line/$modelLine/event-group-reference-id/$eventGroupRef/metadata"
      val blobDetailsBytes = blobDetails {
        blobUri = "file:///impressions/$date/$eventGroupRef"
        encryptedDek = EncryptedDek.getDefaultInstance()
      }.toByteString()
      metadataFs.writeBlob(key, blobDetailsBytes)
    }

    // Note: Can't inject failing reader without factory pattern, but missing impression files will cause failure
    val eventGroupDetails =
      createEventGroupDetails(
        eventGroupRef,
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 1, 3),
        ZoneId.of("UTC"),
      )

    val impressionService = createImpressionService(metadataTmpPath)
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventGroupDetailsList = listOf(eventGroupDetails),
        modelLine = modelLine,
        kmsClient = null,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        descriptor = TestEvent.getDescriptor(),
        batchSize = 1000,
      )

    assertFailsWith<Exception> {
      eventSource.generateEventBatches(Dispatchers.Unconfined).toList()
    }
  }


  @Test
  fun `generateEventBatches handles multiple event groups`(): Unit = runBlocking {
    // Set up separate directories for impressions and metadata
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = tmp.root

    val (kmsClient, kekUri, serializedEncryptionKey) = createKmsSetup()

    // Create impression files and metadata for both groups on both dates
    createImpressionFilesForDate(
      impressionsTmpPath,
      metadataTmpPath,
      LocalDate.of(2025, 1, 1),
      "group-1",
      kmsClient,
      kekUri,
      serializedEncryptionKey
    )
    createImpressionFilesForDate(
      impressionsTmpPath,
      metadataTmpPath,
      LocalDate.of(2025, 1, 1),
      "group-2",
      kmsClient,
      kekUri,
      serializedEncryptionKey
    )
    createImpressionFilesForDate(
      impressionsTmpPath,
      metadataTmpPath,
      LocalDate.of(2025, 1, 2),
      "group-1",
      kmsClient,
      kekUri,
      serializedEncryptionKey
    )
    createImpressionFilesForDate(
      impressionsTmpPath,
      metadataTmpPath,
      LocalDate.of(2025, 1, 2),
      "group-2",
      kmsClient,
      kekUri,
      serializedEncryptionKey
    )

    val eventGroupDetails =
      listOf(
        createEventGroupDetails(
          "group-1",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        ),
        createEventGroupDetails(
          "group-2",
          LocalDate.of(2025, 1, 1),
          LocalDate.of(2025, 1, 3),
          ZoneId.of("UTC"),
        ),
      )

    val impressionService = createImpressionService(metadataTmpPath)
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventGroupDetailsList = eventGroupDetails,
        modelLine = modelLine,
        kmsClient = kmsClient,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        descriptor = TestEvent.getDescriptor(),
        batchSize = 1000,
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(4) // Two event groups × two dates
    // Verify total events: 4 files × 5 events per file = 20 events
    val totalEvents = batches.flatMap { it.events }.size
    assertThat(totalEvents).isEqualTo(20)
  }

  @Test
  fun `generateEventBatches processes large date ranges`(): Unit = runBlocking {
    // Set up separate directories for impressions and metadata
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val metadataTmpPath = tmp.root
    val eventGroupRef = "group-1"

    val (kmsClient, kekUri, serializedEncryptionKey) = createKmsSetup()

    // Create impression files and metadata for 90 days (Jan 1 - Mar 31)
    for (day in 1..31) {
      createImpressionFilesForDate(
        impressionsTmpPath,
        metadataTmpPath,
        LocalDate.of(2025, 1, day),
        eventGroupRef,
        kmsClient,
        kekUri,
        serializedEncryptionKey
      )
    }
    for (day in 1..28) {
      createImpressionFilesForDate(
        impressionsTmpPath,
        metadataTmpPath,
        LocalDate.of(2025, 2, day),
        eventGroupRef,
        kmsClient,
        kekUri,
        serializedEncryptionKey
      )
    }
    for (day in 1..31) {
      createImpressionFilesForDate(
        impressionsTmpPath,
        metadataTmpPath,
        LocalDate.of(2025, 3, day),
        eventGroupRef,
        kmsClient,
        kekUri,
        serializedEncryptionKey
      )
    }

    // Test with a 90-day range (Jan 1 - Mar 31)
    val eventGroupDetails =
      createEventGroupDetails(
        eventGroupRef,
        LocalDate.of(2025, 1, 1),
        LocalDate.of(2025, 4, 1),
        ZoneId.of("UTC"),
      )


    val impressionService = createImpressionService(metadataTmpPath)
    val eventSource =
      StorageEventSource(
        impressionMetadataService = impressionService,
        eventGroupDetailsList = listOf(eventGroupDetails),
        modelLine = modelLine,
        kmsClient = kmsClient,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        descriptor = TestEvent.getDescriptor(),
        batchSize = 1000,
      )

    val batches = eventSource.generateEventBatches(Dispatchers.Unconfined).toList()

    assertThat(batches).hasSize(90) // Jan 1 - Mar 31 = 90 days = 90 batches
    assertThat(batches.all { it.events.size == 5 }).isTrue() // Each batch has 5 events
  }

  companion object {
    private val TEST_EVENT = testEvent {
      person = person {
        ageGroup =
          org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.AgeGroup.YEARS_18_TO_34
        gender = org.wfanet.measurement.api.v2alpha.event_templates.testing.Person.Gender.MALE
      }
    }
  }
}
