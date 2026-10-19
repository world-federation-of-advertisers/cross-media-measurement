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
import com.google.protobuf.Any
import com.google.type.interval
import java.time.Instant
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
        impression(
          vid = 102L,
          entityKeys = listOf("ad" to "ad-1", "campaign" to "campaign-1"),
        ),
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

    val storedDetails = readBlobDetails()
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
  }

  @Test
  fun `write rejects a metadata object in another bucket`() {
    val writer =
      LabeledImpressionWriter.forFileSystem(buildKmsClient(), temporaryFolder.root)

    val exception =
      kotlin.test.assertFailsWith<IllegalArgumentException> {
        runBlocking {
          writer.write(
            impressions = flowOf(impression(vid = 101L, entityKeys = listOf("ad" to "ad-1"))),
            blobUri = BLOB_URI,
            metadataUri = "file:///another-bucket/metadata.binpb",
            kekUri = KEK_URI,
            modelLine = MODEL_LINE,
            interval = INTERVAL,
          )
        }
      }

    assertThat(exception).hasMessageThat().contains("must be in the same bucket")
  }

  private suspend fun readBlobDetails(): BlobDetails {
    val uri = SelectedStorageClient.parseBlobUri(METADATA_URI)
    val blob = SelectedStorageClient(uri, temporaryFolder.root).getBlob(uri.key)!!
    return BlobDetails.parseFrom(blob.read().flatten())
  }

  private fun buildKmsClient(): FakeKmsClient {
    val kmsClient = FakeKmsClient()
    val keysetHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(KEK_URI, keysetHandle.getPrimitive(Aead::class.java))
    return kmsClient
  }

  private fun impression(
    vid: Long,
    entityKeys: List<Pair<String, String>>,
  ): LabeledImpression = labeledImpression {
    this.vid = vid
    eventTime = Instant.parse("2026-01-15T12:00:00Z").toProtoTime()
    event = Any.pack(Instant.parse("2026-01-15T12:00:00Z").toProtoTime())
    this.entityKeys += entityKeys.map { (type, id) ->
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
    private const val MODEL_LINE =
      "modelProviders/model-provider-1/modelSuites/model-suite-1/modelLines/model-line-1"
    private val INTERVAL = interval {
      startTime = Instant.parse("2026-01-15T00:00:00Z").toProtoTime()
      endTime = Instant.parse("2026-01-16T00:00:00Z").toProtoTime()
    }
  }
}
