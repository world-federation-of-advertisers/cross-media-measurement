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
import java.nio.file.Files
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
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
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class EventReaderBatchedTest {

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
    kmsClient =
      TestEncryptedStorage.buildFakeKmsClient(
        FakeKmsClient.KEY_URI_PREFIX,
        keyTemplate = "AES128_GCM",
      )

    // Set up encryption key
    serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(
        kmsClient,
        kekUri,
        tinkKeyTemplateType = "AES128_GCM_HKDF_1MB",
      )

    // Set up TypeRegistry with test event types
    typeRegistry = TypeRegistry.newBuilder()
      .add(TEST_EVENT.descriptorForType)
      .build()
  }

  @Test
  fun `getLabeledEventsBatched returns events in correctly sized batches`() = runBlocking {
    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)

    // Setup encrypted mesos client
    val mesosRecordIoStorageClient =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        impressionsStorageClient,
        kmsClient,
        kekUri,
        serializedEncryptionKey,
      )

    // Create test impressions
    val impressionCount = 1000
    val impressions =
      List(impressionCount) { index ->
        labeledImpression {
          eventTime = TIME_RANGE.start.toProtoTime()
          vid = index.toLong()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(DS.toString(), impressionsFlow)

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    val deksBucketDir = dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      this.encryptedDek = encryptedDek
    }

    impressionsDekStorageClient.writeBlob(
      "ds/$DS/event-group-reference-id/$EVENT_GROUP_REFERENCE_ID/metadata",
      blobDetails.toByteString(),
    )

    // Create EventReader with batch size
    val batchSize = 100
    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = dekTmpPath),
        IMPRESSIONS_DEK_FILE_URI_PREFIX,
        typeRegistry,
        batchSize
      )

    // Get labeled events in batches
    val batches = eventReader.getLabeledEventsBatched(DS, EVENT_GROUP_REFERENCE_ID, batchSize).toList()

    // Verify the batches
    assertThat(batches).hasSize(10) // 1000 events / 100 batch size = 10 batches
    
    // Verify each batch size
    for (batch in batches) {
      assertThat(batch).hasSize(batchSize)
    }
    
    // Verify all events are present and in order
    val allEvents = batches.flatten()
    assertThat(allEvents).hasSize(impressionCount)
    for (i in 0 until impressionCount) {
      assertThat(allEvents[i].vid).isEqualTo(i.toLong())
    }
  }

  @Test
  fun `getLabeledEventsBatched handles non-exact batch size multiple`() = runBlocking {
    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)

    // Setup encrypted mesos client
    val mesosRecordIoStorageClient =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        impressionsStorageClient,
        kmsClient,
        kekUri,
        serializedEncryptionKey,
      )

    // Create 253 impressions (not an exact multiple of batch size 100)
    val impressionCount = 253
    val batchSize = 100
    val impressions =
      List(impressionCount) { index ->
        labeledImpression {
          eventTime = TIME_RANGE.start.toProtoTime()
          vid = index.toLong()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(DS.toString(), impressionsFlow)

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    val deksBucketDir = dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      this.encryptedDek = encryptedDek
    }

    impressionsDekStorageClient.writeBlob(
      "ds/$DS/event-group-reference-id/$EVENT_GROUP_REFERENCE_ID/metadata",
      blobDetails.toByteString(),
    )

    // Create EventReader
    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = dekTmpPath),
        IMPRESSIONS_DEK_FILE_URI_PREFIX,
        typeRegistry
      )

    // Get labeled events in batches
    val batches = eventReader.getLabeledEventsBatched(DS, EVENT_GROUP_REFERENCE_ID, batchSize).toList()

    // Verify we get 3 batches (2 full + 1 partial)
    assertThat(batches).hasSize(3)
    assertThat(batches[0]).hasSize(100)
    assertThat(batches[1]).hasSize(100)
    assertThat(batches[2]).hasSize(53) // remainder
  }

  @Test
  fun `getLabeledEventsBatched handles edge case with exact batch size multiple`() = runBlocking {
    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)

    // Setup encrypted mesos client
    val mesosRecordIoStorageClient =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        impressionsStorageClient,
        kmsClient,
        kekUri,
        serializedEncryptionKey,
      )

    // Create exactly 200 impressions (exact multiple of batch size 50)
    val impressionCount = 200
    val batchSize = 50
    val impressions =
      List(impressionCount) { index ->
        labeledImpression {
          eventTime = TIME_RANGE.start.toProtoTime()
          vid = index.toLong()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(DS.toString(), impressionsFlow)

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    val deksBucketDir = dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      this.encryptedDek = encryptedDek
    }

    impressionsDekStorageClient.writeBlob(
      "ds/$DS/event-group-reference-id/$EVENT_GROUP_REFERENCE_ID/metadata",
      blobDetails.toByteString(),
    )

    // Create EventReader
    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = dekTmpPath),
        IMPRESSIONS_DEK_FILE_URI_PREFIX,
        typeRegistry
      )

    // Get labeled events in batches
    val batches = eventReader.getLabeledEventsBatched(DS, EVENT_GROUP_REFERENCE_ID, batchSize).toList()

    // Verify we get exactly 4 full batches
    assertThat(batches).hasSize(4)
    batches.forEach { batch ->
      assertThat(batch).hasSize(batchSize)
    }
  }

  @Test
  fun `getLabeledEventsBatched handles small number of events`() = runBlocking {
    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())
    val impressionsStorageClient = FileSystemStorageClient(impressionsBucketDir)

    // Setup encrypted mesos client
    val mesosRecordIoStorageClient =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        impressionsStorageClient,
        kmsClient,
        kekUri,
        serializedEncryptionKey,
      )

    // Create only 10 impressions (less than batch size)
    val impressionCount = 10
    val batchSize = 100
    val impressions =
      List(impressionCount) { index ->
        labeledImpression {
          eventTime = TIME_RANGE.start.toProtoTime()
          vid = index.toLong()
          event = TEST_EVENT.pack()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(DS.toString(), impressionsFlow)

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    val deksBucketDir = dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      this.encryptedDek = encryptedDek
    }

    impressionsDekStorageClient.writeBlob(
      "ds/$DS/event-group-reference-id/$EVENT_GROUP_REFERENCE_ID/metadata",
      blobDetails.toByteString(),
    )

    // Create EventReader
    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = dekTmpPath),
        IMPRESSIONS_DEK_FILE_URI_PREFIX,
        typeRegistry
      )

    // Get labeled events in batches
    val batches = eventReader.getLabeledEventsBatched(DS, EVENT_GROUP_REFERENCE_ID, batchSize).toList()

    // Verify we get exactly 1 batch with all events
    assertThat(batches).hasSize(1)
    assertThat(batches[0]).hasSize(impressionCount)
  }

  @Test
  fun `getLabeledEventsBatched throws exception if impressions blob not found`() {
    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())

    // Create the impressions DEK store
    val dekTmpPath = Files.createTempDirectory(null).toFile()
    val deksBucketDir = dekTmpPath.resolve(IMPRESSIONS_DEK_BUCKET)
    Files.createDirectories(deksBucketDir.toPath())
    val impressionsDekStorageClient = FileSystemStorageClient(deksBucketDir)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      this.encryptedDek = encryptedDek
    }

    runBlocking {
      impressionsDekStorageClient.writeBlob(
        "ds/$DS/event-group-reference-id/$EVENT_GROUP_REFERENCE_ID/metadata",
        blobDetails.toByteString(),
      )
    }

    // Create EventReader
    val eventReader =
      EventReader(
        kmsClient,
        StorageConfig(rootDirectory = impressionsTmpPath),
        StorageConfig(rootDirectory = dekTmpPath),
        IMPRESSIONS_DEK_FILE_URI_PREFIX,
        typeRegistry
      )
    
    // Get labeled events should throw exception
    assertFailsWith<ImpressionReadException> {
      runBlocking { eventReader.getLabeledEventsBatched(DS, EVENT_GROUP_REFERENCE_ID).toList() }
    }
  }

  companion object {
    private val ZONE_ID = ZoneId.of("America/New_York")
    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    private val DS = LocalDate.ofInstant(TIME_RANGE.start, ZONE_ID)

    private val PERSON = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val TEST_EVENT = testEvent { person = PERSON }

    private const val EVENT_GROUP_REFERENCE_ID = "some-event-group-reference-id"

    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET"

    private const val IMPRESSIONS_DEK_BUCKET = "impression-dek-bucket"
    private const val IMPRESSIONS_DEK_FILE_URI_PREFIX = "file:///$IMPRESSIONS_DEK_BUCKET"
  }
}