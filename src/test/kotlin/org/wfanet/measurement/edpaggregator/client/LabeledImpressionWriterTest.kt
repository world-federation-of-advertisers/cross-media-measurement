// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.client

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.Any
import com.google.type.interval
import java.nio.file.Files
import java.time.Instant
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.storage.SelectedStorageClient

class LabeledImpressionWriterTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `write produces decryptable RecordIO and matching sidecar`() = runBlocking {
    val kmsClient = buildKmsClient()
    val writer = LabeledImpressionWriter.forFileSystem(kmsClient, temporaryFolder.root)
    val impressions =
      listOf(
        impression(vid = 101L, entityKeys = listOf("campaign" to "campaign-1")),
        impression(vid = 102L, entityKeys = listOf("ad" to "ad-1", "campaign" to "campaign-1")),
      )

    val details =
      writer.write(
        impressions = flowOf(*impressions.toTypedArray()),
        blobUri = BLOB_URI,
        metadataUri = METADATA_URI,
        kekUri = KEK_URI,
        modelLine = MODEL_LINE,
        interval = INTERVAL,
      )

    val storedDetails = readBlobDetails(METADATA_URI)
    assertThat(storedDetails).isEqualTo(details)
    assertThat(storedDetails.blobUri).isEqualTo(BLOB_URI)
    assertThat(storedDetails.modelLine).isEqualTo(MODEL_LINE)
    assertThat(storedDetails.eventGroupReferenceId).isEmpty()
    assertThat(storedDetails.entityKeysList.map { it.entityType to it.entityIdsList })
      .containsExactly("ad" to listOf("ad-1"), "campaign" to listOf("campaign-1"))
      .inOrder()

    val blobUri = SelectedStorageClient.parseBlobUri(BLOB_URI)
    val encryptedStorage =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        storageClient = SelectedStorageClient(blobUri, temporaryFolder.root),
        kmsClient = kmsClient,
        kekUri = KEK_URI,
        encryptedDek = storedDetails.encryptedDek,
      )
    val storedImpressions =
      encryptedStorage.getBlob(blobUri.key)!!.read().toList().map(LabeledImpression::parseFrom)
    assertThat(storedImpressions).containsExactlyElementsIn(impressions).inOrder()

    val copiedBlobUri = SelectedStorageClient.parseBlobUri(COPIED_BLOB_URI)
    Files.copy(
      temporaryFolder.root.toPath().resolve(blobUri.bucket).resolve(blobUri.key),
      temporaryFolder.root.toPath().resolve(copiedBlobUri.bucket).resolve(copiedBlobUri.key),
    )
    val storageWithDifferentAssociatedData =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        storageClient = SelectedStorageClient(copiedBlobUri, temporaryFolder.root),
        kmsClient = kmsClient,
        kekUri = KEK_URI,
        encryptedDek = storedDetails.encryptedDek,
      )
    assertFails { storageWithDifferentAssociatedData.getBlob(copiedBlobUri.key)!!.read().toList() }
    Unit
  }

  @Test
  fun `write generates a fresh data encryption key for each blob`() = runBlocking {
    val writer = LabeledImpressionWriter.forFileSystem(buildKmsClient(), temporaryFolder.root)
    val impression = impression(vid = 101L, entityKeys = listOf("campaign" to "campaign-1"))

    val firstDetails =
      writer.write(
        impressions = flowOf(impression),
        blobUri = BLOB_URI,
        metadataUri = METADATA_URI,
        kekUri = KEK_URI,
        modelLine = MODEL_LINE,
        interval = INTERVAL,
      )
    val secondDetails =
      writer.write(
        impressions = flowOf(impression),
        blobUri = SECOND_BLOB_URI,
        metadataUri = SECOND_METADATA_URI,
        kekUri = KEK_URI,
        modelLine = MODEL_LINE,
        interval = INTERVAL,
      )

    assertThat(secondDetails.encryptedDek.ciphertext)
      .isNotEqualTo(firstDetails.encryptedDek.ciphertext)
    assertThat(readBlobDetails(SECOND_METADATA_URI)).isEqualTo(secondDetails)
  }

  @Test
  fun `write rejects a metadata object in another bucket before creating either object`() =
    runBlocking {
      val writer = LabeledImpressionWriter.forFileSystem(buildKmsClient(), temporaryFolder.root)
      val metadataUri = "file:///another-bucket/metadata.binpb"

      val exception =
        assertFailsWith<IllegalArgumentException> {
          writer.write(
            impressions = flowOf(impression(vid = 101L, entityKeys = listOf("ad" to "ad-1"))),
            blobUri = BLOB_URI,
            metadataUri = metadataUri,
            kekUri = KEK_URI,
            modelLine = MODEL_LINE,
            interval = INTERVAL,
          )
        }

      assertThat(exception).hasMessageThat().contains("must be in the same bucket")
      assertThat(getBlob(BLOB_URI)).isNull()
      assertThat(getBlob(metadataUri)).isNull()
    }

  @Test
  fun `write does not publish metadata when an impression is invalid`() = runBlocking {
    val writer = LabeledImpressionWriter.forFileSystem(buildKmsClient(), temporaryFolder.root)
    val invalidImpression =
      impression(vid = 101L, entityKeys = listOf("campaign" to "campaign-1"))
        .toBuilder()
        .clearEntityKeys()
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> {
        writer.write(
          impressions = flowOf(invalidImpression),
          blobUri = BLOB_URI,
          metadataUri = METADATA_URI,
          kekUri = KEK_URI,
          modelLine = MODEL_LINE,
          interval = INTERVAL,
        )
      }

    assertThat(exception).hasMessageThat().contains("at least one entity key")
    assertThat(getBlob(METADATA_URI)).isNull()
  }

  @Test
  fun `write does not publish metadata for an empty flow`() = runBlocking {
    val writer = LabeledImpressionWriter.forFileSystem(buildKmsClient(), temporaryFolder.root)

    val exception =
      assertFailsWith<IllegalStateException> {
        writer.write(
          impressions = emptyFlow(),
          blobUri = BLOB_URI,
          metadataUri = METADATA_URI,
          kekUri = KEK_URI,
          modelLine = MODEL_LINE,
          interval = INTERVAL,
        )
      }

    assertThat(exception).hasMessageThat().contains("must not be empty")
    assertThat(getBlob(METADATA_URI)).isNull()
  }

  @Test
  fun `write rejects an invalid interval before creating either object`() = runBlocking {
    val writer = LabeledImpressionWriter.forFileSystem(buildKmsClient(), temporaryFolder.root)
    val invalidInterval = interval {
      startTime = Instant.parse("2026-01-15T00:00:00Z").toProtoTime()
      endTime = Instant.parse("2026-01-15T00:00:00Z").toProtoTime()
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        writer.write(
          impressions = flowOf(impression(vid = 101L, entityKeys = listOf("ad" to "ad-1"))),
          blobUri = BLOB_URI,
          metadataUri = METADATA_URI,
          kekUri = KEK_URI,
          modelLine = MODEL_LINE,
          interval = invalidInterval,
        )
      }

    assertThat(exception).hasMessageThat().contains("start time must be before end time")
    assertThat(getBlob(BLOB_URI)).isNull()
    assertThat(getBlob(METADATA_URI)).isNull()
  }

  private suspend fun readBlobDetails(metadataUri: String): BlobDetails {
    val uri = SelectedStorageClient.parseBlobUri(metadataUri)
    val blob = SelectedStorageClient(uri, temporaryFolder.root).getBlob(uri.key)!!
    return BlobDetails.parseFrom(blob.read().flatten())
  }

  private suspend fun getBlob(uri: String) =
    SelectedStorageClient(uri, temporaryFolder.root)
      .getBlob(SelectedStorageClient.parseBlobUri(uri).key)

  private fun buildKmsClient(): FakeKmsClient {
    AeadConfig.register()
    val kmsClient = FakeKmsClient()
    val keysetHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(
      KEK_URI,
      keysetHandle.getPrimitive(RegistryConfiguration.get(), Aead::class.java),
    )
    return kmsClient
  }

  private fun impression(vid: Long, entityKeys: List<Pair<String, String>>): LabeledImpression =
    labeledImpression {
      this.vid = vid
      eventTime = Instant.parse("2026-01-15T12:00:00Z").toProtoTime()
      event = Any.pack(Instant.parse("2026-01-15T12:00:00Z").toProtoTime())
      this.entityKeys +=
        entityKeys.map { (type, id) ->
          entityKey {
            entityType = type
            entityId = id
          }
        }
    }

  companion object {
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "writer-test"
    private const val BLOB_URI = "file:///operator-bucket/2026-01-15/impressions.enc.recordio"
    private const val METADATA_URI = "file:///operator-bucket/2026-01-15/metadata.binpb"
    private const val COPIED_BLOB_URI =
      "file:///operator-bucket/2026-01-15/copied-impressions.enc.recordio"
    private const val SECOND_BLOB_URI =
      "file:///operator-bucket/2026-01-15/impressions-2.enc.recordio"
    private const val SECOND_METADATA_URI = "file:///operator-bucket/2026-01-15/metadata-2.binpb"
    private const val MODEL_LINE =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
    private val INTERVAL = interval {
      startTime = Instant.parse("2026-01-15T00:00:00Z").toProtoTime()
      endTime = Instant.parse("2026-01-16T00:00:00Z").toProtoTime()
    }
  }
}
