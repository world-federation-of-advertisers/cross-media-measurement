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

package org.wfanet.measurement.loadtest.edpaggregator.testing

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class ImpressionWriterTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `writeLabeledImpressionData writes labeled impressions by date`() {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsClient = run {
      val client = FakeKmsClient()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      client
    }
    tempFolder.root.resolve("some-impression-bucket").mkdirs()
    tempFolder.root.resolve("some-metadata-bucket").mkdirs()
    val impressionWriter =
      ImpressionsWriter(
        "some-event-group-path",
        "some-event-group-path",
        kekUri,
        kmsClient,
        "some-impression-bucket",
        "some-metadata-bucket",
        tempFolder.root,
        "file:///",
      )
    val events: Sequence<LabeledEventDateShard<TestEvent>> =
      sequenceOf(
        LabeledEventDateShard(
          LocalDate.parse("2020-01-01"),
          sequenceOf(
            LabeledEvent(
              vid = 1,
              message = TestEvent.getDefaultInstance(),
              timestamp = LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
            ),
            LabeledEvent(
              vid = 1,
              message = TestEvent.getDefaultInstance(),
              timestamp = LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
            ),
            LabeledEvent(
              vid = 1,
              message = TestEvent.getDefaultInstance(),
              timestamp = LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
            ),
          ),
        ),
        LabeledEventDateShard(
          LocalDate.parse("2020-01-02"),
          sequenceOf(
            LabeledEvent(
              vid = 1,
              message = TestEvent.getDefaultInstance(),
              timestamp = LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
            ),
            LabeledEvent(
              vid = 1,
              message = TestEvent.getDefaultInstance(),
              timestamp = LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
            ),
            LabeledEvent(
              vid = 1,
              message = TestEvent.getDefaultInstance(),
              timestamp = LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
            ),
          ),
        ),
      )
    runBlocking {
      impressionWriter.writeLabeledImpressionData(events, "some-model-line", "edp/edp-test")
    }
    val storageClient = FileSystemStorageClient(tempFolder.root)
    runBlocking {
      listOf("2020-01-01", "2020-01-02").forEach { date ->
        val blobDetails =
          BlobDetails.parseFrom(
            storageClient
              .getBlob(
                "some-metadata-bucket/edp/edp-test/ds/$date/some-event-group-path/metadata.binpb"
              )!!
              .read()
              .flatten()
          )
        assertThat(blobDetails.blobUri)
          .isEqualTo(
            "file:///some-impression-bucket/edp/edp-test/ds/$date/some-event-group-path/impressions"
          )
        val encryptedDek = blobDetails.encryptedDek
        assertThat(encryptedDek.kekUri).isEqualTo(kekUri)
        val serializedEncryptionKey = encryptedDek.ciphertext

        val selectedStorageClient = SelectedStorageClient(blobDetails.blobUri, tempFolder.root)
        val decryptionClient =
          selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)
        val impressions =
          MesosRecordIoStorageClient(decryptionClient)
            .getBlob("edp/edp-test/ds/$date/some-event-group-path/impressions")!!
            .read()
            .toList()
        assertThat(impressions.size).isEqualTo(3)
        impressions.forEach { it: ByteString ->
          val event = LabeledImpression.parseFrom(it)
          assertThat(event.vid).isEqualTo(1)
          assertThat(event.event.unpack(TestEvent::class.java))
            .isEqualTo(TestEvent.getDefaultInstance())
          assertThat(event.eventTime)
            .isEqualTo(
              LocalDate.parse(date).atStartOfDay(ZoneId.of("UTC")).toInstant().toProtoTime()
            )
        }
      }
    }
  }

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }
  }
}
