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
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

@RunWith(JUnit4::class)
class ImpressionWriterTest {

  @Rule @JvmField val tempFolder = TemporaryFolder()

  @Test
  fun `writeLabeledImpressionData writes labeled impressions by date`() {
    val modelLineName = "modelProviders/provider1/modelSuites/suite1/modelLines/some-model-line"
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
    val events: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
      sequenceOf(
        EntityKeyedLabeledEventDateShard(
          LocalDate.parse("2020-01-01"),
          sequenceOf(
            EntityKeysWithLabeledEvents(
              entityKeys = emptyList(),
              labeledEvents =
                sequenceOf(
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                ),
            )
          ),
        ),
        EntityKeyedLabeledEventDateShard(
          LocalDate.parse("2020-01-02"),
          sequenceOf(
            EntityKeysWithLabeledEvents(
              entityKeys = emptyList(),
              labeledEvents =
                sequenceOf(
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                ),
            )
          ),
        ),
      )
    runBlocking {
      impressionWriter.writeLabeledImpressionData(events, modelLineName, "edp/edp-test")
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
        assertThat(blobDetails.modelLine).isEqualTo(modelLineName)
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

  @Test
  fun `writeLabeledImpressionData requires model line resource name`() {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsClient = run {
      val client = FakeKmsClient()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      client
    }
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
    val events =
      sequenceOf(
        EntityKeyedLabeledEventDateShard(
          LocalDate.parse("2020-01-01"),
          sequenceOf(
            EntityKeysWithLabeledEvents(
              entityKeys = emptyList(),
              labeledEvents =
                sequenceOf(
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  )
                ),
            )
          ),
        )
      )

    assertFailsWith<IllegalArgumentException> {
      runBlocking {
        impressionWriter.writeLabeledImpressionData(events, "some-model-line", "edp/edp-test")
      }
    }
  }

  @Test
  fun `writeLabeledImpressionData with flatOutputBasePath writes to flat layout`() {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsClient = run {
      val client = FakeKmsClient()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      client
    }
    val bucket = "flat-bucket"
    tempFolder.root.resolve(bucket).mkdirs()
    val impressionWriter =
      ImpressionsWriter(
        "some-event-group-ref",
        "some-event-group-path",
        kekUri,
        kmsClient,
        bucket,
        bucket,
        tempFolder.root,
        "file:///",
      )
    val events: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
      sequenceOf(
        EntityKeyedLabeledEventDateShard(
          LocalDate.parse("2020-01-01"),
          sequenceOf(
            EntityKeysWithLabeledEvents(
              entityKeys = emptyList(),
              labeledEvents =
                sequenceOf(
                  LabeledEvent(
                    vid = 1,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                  LabeledEvent(
                    vid = 2,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-01").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  ),
                ),
            )
          ),
        ),
        EntityKeyedLabeledEventDateShard(
          LocalDate.parse("2020-01-02"),
          sequenceOf(
            EntityKeysWithLabeledEvents(
              entityKeys = emptyList(),
              labeledEvents =
                sequenceOf(
                  LabeledEvent(
                    vid = 3,
                    message = TestEvent.getDefaultInstance(),
                    timestamp =
                      LocalDate.parse("2020-01-02").atStartOfDay(ZoneId.of("UTC")).toInstant(),
                  )
                ),
            )
          ),
        ),
      )

    val modelLineName = "modelProviders/provider1/modelSuites/suite1/modelLines/some-model-line"
    val flatBase = "my/flat/base"
    runBlocking {
      impressionWriter.writeLabeledImpressionData(
        events,
        modelLineName,
        flatOutputBasePath = flatBase,
      )
    }

    val storageClient = FileSystemStorageClient(tempFolder.root)
    runBlocking {
      listOf("2020-01-01" to 2, "2020-01-02" to 1).forEach { (date, expectedCount) ->
        val metadataKey = "$bucket/$flatBase/$date/metadata.binpb"
        val blobDetails =
          BlobDetails.parseFrom(storageClient.getBlob(metadataKey)!!.read().flatten())

        assertThat(blobDetails.blobUri).isEqualTo("file:///$bucket/$flatBase/$date/impressions")

        val encryptedDek = blobDetails.encryptedDek
        assertThat(encryptedDek.kekUri).isEqualTo(kekUri)

        val selectedStorageClient = SelectedStorageClient(blobDetails.blobUri, tempFolder.root)
        val decryptionClient =
          selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, encryptedDek.ciphertext)
        val impressions =
          MesosRecordIoStorageClient(decryptionClient)
            .getBlob("$flatBase/$date/impressions")!!
            .read()
            .toList()
        assertThat(impressions).hasSize(expectedCount)
        impressions.forEach { bytes: ByteString ->
          val event = LabeledImpression.parseFrom(bytes)
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

  @Test
  fun `writeLabeledImpressionData stamps per-group EntityKeys within a single blob`() {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsClient = run {
      val client = FakeKmsClient()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
      client
    }
    tempFolder.root.resolve("multi-impression-bucket").mkdirs()
    tempFolder.root.resolve("multi-metadata-bucket").mkdirs()
    val impressionWriter =
      ImpressionsWriter(
        "some-event-group-ref",
        "some-event-group-path",
        kekUri,
        kmsClient,
        "multi-impression-bucket",
        "multi-metadata-bucket",
        tempFolder.root,
        "file:///",
      )

    val date = LocalDate.parse("2020-01-01")
    val timestamp = date.atStartOfDay(ZoneId.of("UTC")).toInstant()
    val groupAEntityKeys = listOf(EntityKey("type-a", "id-a"), EntityKey("type-a", "id-a2"))
    val groupBEntityKeys = listOf(EntityKey("type-b", "id-b"))
    val events: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
      sequenceOf(
        EntityKeyedLabeledEventDateShard(
          date,
          sequenceOf(
            EntityKeysWithLabeledEvents(
              entityKeys = groupAEntityKeys,
              labeledEvents =
                sequenceOf(
                  LabeledEvent(timestamp, vid = 10, message = TestEvent.getDefaultInstance()),
                  LabeledEvent(timestamp, vid = 11, message = TestEvent.getDefaultInstance()),
                ),
            ),
            EntityKeysWithLabeledEvents(
              entityKeys = groupBEntityKeys,
              labeledEvents =
                sequenceOf(
                  LabeledEvent(timestamp, vid = 20, message = TestEvent.getDefaultInstance())
                ),
            ),
          ),
        )
      )

    val modelLineName = "modelProviders/provider1/modelSuites/suite1/modelLines/some-model-line"
    runBlocking {
      impressionWriter.writeLabeledImpressionData(events, modelLineName, "edp/edp-test")
    }

    val storageClient = FileSystemStorageClient(tempFolder.root)
    runBlocking {
      val blobDetails =
        BlobDetails.parseFrom(
          storageClient
            .getBlob(
              "multi-metadata-bucket/edp/edp-test/ds/2020-01-01/some-event-group-path/metadata.binpb"
            )!!
            .read()
            .flatten()
        )
      val selectedStorageClient = SelectedStorageClient(blobDetails.blobUri, tempFolder.root)
      val decryptionClient =
        selectedStorageClient.withEnvelopeEncryption(
          kmsClient,
          kekUri,
          blobDetails.encryptedDek.ciphertext,
        )
      val impressions =
        MesosRecordIoStorageClient(decryptionClient)
          .getBlob("edp/edp-test/ds/2020-01-01/some-event-group-path/impressions")!!
          .read()
          .toList()
          .map { LabeledImpression.parseFrom(it) }
      assertThat(impressions).hasSize(3)

      val impressionsByEntityKeys: Map<List<Pair<String, String>>, List<LabeledImpression>> =
        impressions.groupBy { impression ->
          impression.entityKeysList.map { it.entityType to it.entityId }
        }
      val groupAKey: List<Pair<String, String>> =
        groupAEntityKeys.map { it.entityType to it.entityId }
      val groupBKey: List<Pair<String, String>> =
        groupBEntityKeys.map { it.entityType to it.entityId }
      assertThat(impressionsByEntityKeys.keys).containsExactly(groupAKey, groupBKey)

      val groupAImpressions: List<LabeledImpression> = impressionsByEntityKeys.getValue(groupAKey)
      assertThat(groupAImpressions.map { it.vid }).containsExactly(10L, 11L).inOrder()
      groupAImpressions.forEach { impression ->
        assertThat(impression.event.unpack(TestEvent::class.java))
          .isEqualTo(TestEvent.getDefaultInstance())
        assertThat(impression.eventTime).isEqualTo(timestamp.toProtoTime())
      }

      val groupBImpressions: List<LabeledImpression> = impressionsByEntityKeys.getValue(groupBKey)
      assertThat(groupBImpressions.map { it.vid }).containsExactly(20L)
      groupBImpressions.forEach { impression ->
        assertThat(impression.event.unpack(TestEvent::class.java))
          .isEqualTo(TestEvent.getDefaultInstance())
        assertThat(impression.eventTime).isEqualTo(timestamp.toProtoTime())
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
