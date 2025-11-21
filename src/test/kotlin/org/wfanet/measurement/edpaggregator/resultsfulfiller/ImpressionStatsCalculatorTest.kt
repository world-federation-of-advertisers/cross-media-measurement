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
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.interval
import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

private val TEST_EVENT: Any = Any.getDefaultInstance()

@RunWith(JUnit4::class)
class ImpressionStatsCalculatorTest {

  @get:Rule val tmp = TemporaryFolder()

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"

  @Before
  fun setupTink() {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  @Test
  fun `counts records and distinct vids across blobs`() = runBlocking {
    val (kmsClient, serializedEncryptionKey) = createKmsSetup()
    val storageConfig = StorageConfig(rootDirectory = tmp.root)

    val metadataUriA =
      writeEncryptedImpressions(
        impressionsBucket = "impressions",
        metadataBucket = "meta-bucket",
        blobKey = "ds/2025-01-01/event-group/eg-1/impressions",
        metadataKey = "ds/2025-01-01/event-group/eg-1/metadata",
        vids = listOf(1L, 2L, 3L),
        kmsClient = kmsClient,
        serializedEncryptionKey = serializedEncryptionKey,
      )

    val metadataUriB =
      writeEncryptedImpressions(
        impressionsBucket = "impressions",
        metadataBucket = "meta-bucket",
        blobKey = "ds/2025-01-02/event-group/eg-2/impressions",
        metadataKey = "ds/2025-01-02/event-group/eg-2/metadata",
        vids = listOf(3L, 4L, 5L),
        kmsClient = kmsClient,
        serializedEncryptionKey = serializedEncryptionKey,
      )

    val calculator = ImpressionStatsCalculator(storageConfig, kmsClient)

    val result = calculator.compute(listOf(metadataUriA, metadataUriB))

    assertThat(result.totalRecords).isEqualTo(6)
    assertThat(result.distinctVids).isEqualTo(5)
    assertThat(result.blobStats).hasSize(2)
    assertThat(result.blobStats[0].recordCount).isEqualTo(3)
    assertThat(result.blobStats[1].recordCount).isEqualTo(3)
  }

  private fun createKmsSetup(): Pair<FakeKmsClient, ByteString> {
    val kmsClient = FakeKmsClient()
    val kekHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kekHandle.getPrimitive(Aead::class.java))

    val dataKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM_HKDF_1MB"))
    val serializedEncryptionKey =
      ByteString.copyFrom(
        TinkProtoKeysetFormat.serializeEncryptedKeyset(
          dataKeyHandle,
          kmsClient.getAead(kekUri),
          byteArrayOf(),
        )
      )

    return Pair(kmsClient, serializedEncryptionKey)
  }

  private suspend fun writeEncryptedImpressions(
    impressionsBucket: String,
    metadataBucket: String,
    blobKey: String,
    metadataKey: String,
    vids: List<Long>,
    kmsClient: FakeKmsClient,
    serializedEncryptionKey: ByteString,
  ): String {
    val impressionsUri = "file:///$impressionsBucket/$blobKey"
    File(tmp.root, impressionsBucket).mkdirs()
    val impressionsClient = SelectedStorageClient(impressionsUri, tmp.root)
    val aeadStorageClient =
      impressionsClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
    val impressionsStorage = MesosRecordIoStorageClient(aeadStorageClient)

    val baseInstant = Instant.EPOCH.atZone(ZoneOffset.UTC).toInstant()
    val impressionFlow =
      flow {
        for (vid in vids) {
          emit(
            labeledImpression {
              this.vid = vid
              eventTime = timestamp { seconds = baseInstant.epochSecond }
              event = TEST_EVENT
            }
              .toByteString()
          )
        }
      }

    impressionsStorage.writeBlob(blobKey, impressionFlow)

    val metadataDir = File(tmp.root, metadataBucket)
    metadataDir.mkdirs()

    val metadataClient = FileSystemStorageClient(metadataDir)
    val blobDetails =
      blobDetails {
        blobUri = impressionsUri
        encryptedDek = encryptedDek {
          this.kekUri = this@ImpressionStatsCalculatorTest.kekUri
          ciphertext = serializedEncryptionKey
          typeUrl = "type.googleapis.com/google.crypto.tink.Keyset"
          protobufFormat = EncryptedDek.ProtobufFormat.BINARY
        }
        eventGroupReferenceId = metadataKey
        modelLine = "model-line"
        interval = interval {
          startTime = timestamp { seconds = baseInstant.epochSecond }
          endTime = timestamp { seconds = baseInstant.plusSeconds(86400).epochSecond }
        }
      }

    metadataClient.writeBlob(metadataKey, blobDetails.toByteString())
    return "file:///$metadataBucket/$metadataKey"
  }
}
