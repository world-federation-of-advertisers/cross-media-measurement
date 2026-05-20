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
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class EventReaderTest {

  @get:Rule val tempFolder = TemporaryFolder()

  private lateinit var kmsClient: KmsClient
  private lateinit var kekUri: String
  private lateinit var serializedEncryptionKey: ByteString
  private lateinit var encryptedDek: EncryptedDek

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

    encryptedDek = encryptedDek {
      this.kekUri = this@EventReaderTest.kekUri
      typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      ciphertext = serializedEncryptionKey
    }
  }

  @Test
  fun `readEvents returns labeled events and correct impression count`() = runBlocking {
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
        encryptedDek,
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
    val encryptedDek = encryptedDek {
      this.kekUri = this@EventReaderTest.kekUri
      typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      ciphertext = serializedEncryptionKey
    }

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      this.encryptedDek = encryptedDek
    }

    // Create EventReader with paths
    val eventReader =
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        descriptor = TestEvent.getDescriptor(),
      )

    // Read events
    val result = eventReader.readEvents().toList()

    // Verify the result
    val flattenedResult = result.flatten()
    assertThat(flattenedResult).hasSize(impressionCount)
    for (i in 0 until impressionCount) {
      assertThat(flattenedResult[i].vid).isEqualTo(i.toLong())
    }
  }

  @Test
  fun `readEvents returns labeled events for unencrypted storage`() = runBlocking {
    // Create impressions storage client (unencrypted)
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())
    val storageClient = FileSystemStorageClient(impressionsBucketDir)
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)

    // Create test impressions
    val impressionCount = 250
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

    // Write impressions to storage without encryption
    mesosRecordIoStorageClient.writeBlob(DS.toString(), impressionsFlow)

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS"
      // encryptedDek intentionally omitted for unencrypted read
    }

    // Create EventReader with kmsClient = null to indicate unencrypted data
    val eventReader =
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = null,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        descriptor = TestEvent.getDescriptor(),
      )

    // Read events
    val result = eventReader.readEvents().toList()

    // Verify the result
    val flattenedResult = result.flatten()
    assertThat(flattenedResult).hasSize(impressionCount)
    for (i in 0 until impressionCount) {
      assertThat(flattenedResult[i].vid).isEqualTo(i.toLong())
    }
  }

  @Test
  fun `readEvents throws exception if impressions blob not found`() {
    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    val impressionsBucketDir = impressionsTmpPath.resolve(IMPRESSIONS_BUCKET)
    Files.createDirectories(impressionsBucketDir.toPath())

    val encryptedDek = encryptedDek {
      this.kekUri = this@EventReaderTest.kekUri
      typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      ciphertext = serializedEncryptionKey
    }

    val blobDetails = blobDetails {
      blobUri = "$IMPRESSIONS_FILE_URI/$DS" // Non-existent in this test
      this.encryptedDek = encryptedDek
    }

    // Create EventReader with paths
    val eventReader =
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = StorageConfig(rootDirectory = impressionsTmpPath),
        descriptor = TestEvent.getDescriptor(),
      )
    // Read events
    assertFailsWith<ImpressionReadException> { runBlocking { eventReader.readEvents().toList() } }
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
  }
}
