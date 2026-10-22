/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.aead.AesGcmParameters
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.storage.ParquetEncryptionConfig
import org.wfanet.measurement.storage.ParquetStorageClient
import org.wfanet.measurement.storage.ParquetValue

class RawImpressionsWriterTest {
  @get:Rule val tempFolder = TemporaryFolder()

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "raw-impression-key"
  private val kmsClient =
    FakeKmsClient().apply {
      val keyset =
        KeysetHandle.generateNew(
          AesGcmParameters.builder()
            .setKeySizeBytes(16)
            .setIvSizeBytes(12)
            .setTagSizeBytes(16)
            .build()
        )
      setAead(kekUri, keyset.getPrimitive(RegistryConfiguration.get(), Aead::class.java))
    }

  @Test
  fun `writeRawImpressions preserves all hierarchy columns per impression`(): Unit = runBlocking {
    val keysByImpression = listOf(keys("1"), keys("2").reversed())
    val writer = newWriter(RawImpressionColumns.ENTITY_KEY_COLUMNS)
    val blobKeys = writer.writeRawImpressions(shards(keysByImpression), "raw") { emptyMap() }

    assertThat(blobKeys).containsExactly("raw/$EVENT_DATE/stream")
    val blob = checkNotNull(newReader().getBlob(blobKeys.single()))
    val rows = blob.readRows().toList()
    assertThat(rows).hasSize(2)
    for ((index, row) in rows.withIndex()) {
      val suffix = (index + 1).toString()
      assertThat(
          row
            .filterKeys { it != "event_id" && it != "event_time_usec" }
            .mapValues { it.value.stringValue }
        )
        .containsExactly(
          "person_id",
          "person-$suffix",
          "campaign_id",
          "campaign-$suffix",
          "adset_id",
          "adset-$suffix",
          "ad_id",
          "ad-$suffix",
          "account_id",
          "account-$suffix",
          "creative_id",
          "creative-$suffix",
        )
      assertThat(row.values.map { it.kindCase }.count { it == ParquetValue.KindCase.STRING_VALUE })
        .isEqualTo(7)
      assertThat(row.getValue("event_time_usec").int64Value)
        .isEqualTo(EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toEpochSecond() * 1_000_000L)
    }
    assertThat(rows.map { it.getValue("event_id").stringValue }.toSet()).hasSize(2)
    assertThat(blob.readKeyValueMetadata()).containsEntry("event_date", EVENT_DATE.toString())
  }

  @Test
  fun `writeRawImpressions supports person-only legacy input`(): Unit = runBlocking {
    val writer = newWriter(mapOf("person" to "person_id"))
    val blobKeys =
      writer.writeRawImpressions(shards(listOf(listOf(EntityKey("person", "p-1")))), "raw") {
        emptyMap()
      }

    val row = checkNotNull(newReader().getBlob(blobKeys.single())).readRows().toList().single()
    assertThat(row.keys).containsExactly("event_id", "event_time_usec", "person_id")
    assertThat(row.getValue("person_id").stringValue).isEqualTo("p-1")
  }

  @Test
  fun `writeRawImpressions retains person and creative keys`(): Unit = runBlocking {
    val writer = newWriter(mapOf("person" to "person_id", "creative-id" to "creative_id"))
    val blobKeys =
      writer.writeRawImpressions(
        shards(listOf(listOf(EntityKey("person", "p-1"), EntityKey("creative-id", "c-1")))),
        "raw",
      ) {
        emptyMap()
      }

    val row = checkNotNull(newReader().getBlob(blobKeys.single())).readRows().toList().single()
    assertThat(row.keys).containsExactly("event_id", "event_time_usec", "person_id", "creative_id")
    assertThat(row.getValue("creative_id").stringValue).isEqualTo("c-1")
  }

  @Test
  fun `writeRawImpressions rejects a missing required entity key`(): Unit = runBlocking {
    val writer = newWriter(mapOf("campaign" to "campaign_id"))
    val exception =
      assertFailsWith<IllegalArgumentException> {
        writer.writeRawImpressions(shards(listOf(listOf(EntityKey("person", "p-1")))), "raw") {
          emptyMap()
        }
      }
    assertThat(exception.message).contains("campaign")
  }

  @Test
  fun `writeRawImpressions rejects an empty entity ID`(): Unit = runBlocking {
    val writer = newWriter(mapOf("campaign" to "campaign_id"))
    assertFailsWith<IllegalArgumentException> {
      writer.writeRawImpressions(shards(listOf(listOf(EntityKey("campaign", "")))), "raw") {
        emptyMap()
      }
    }
  }

  @Test
  fun `writeRawImpressions rejects multiple IDs of the same entity type`(): Unit = runBlocking {
    val writer = newWriter(mapOf("campaign" to "campaign_id"))
    assertFailsWith<IllegalArgumentException> {
      writer.writeRawImpressions(
        shards(listOf(listOf(EntityKey("campaign", "c-1"), EntityKey("campaign", "c-2")))),
        "raw",
      ) {
        emptyMap()
      }
    }
  }

  @Test
  fun `writeRawImpressions rejects duplicate keys`(): Unit = runBlocking {
    val writer = newWriter(mapOf("campaign" to "campaign_id"))
    assertFailsWith<IllegalArgumentException> {
      writer.writeRawImpressions(
        shards(listOf(listOf(EntityKey("campaign", "c-1"), EntityKey("campaign", "c-1")))),
        "raw",
      ) {
        emptyMap()
      }
    }
  }

  @Test
  fun `RawImpressionsWriter rejects shared entity-key columns`() {
    assertFailsWith<IllegalArgumentException> {
      newWriter(mapOf("campaign" to "entity_id", "ad-id" to "entity_id"))
    }
  }

  @Test
  fun `writeRawImpressions skips empty input`(): Unit = runBlocking {
    assertThat(
        newWriter(RawImpressionColumns.ENTITY_KEY_COLUMNS).writeRawImpressions(
          emptySequence<EntityKeyedLabeledEventDateShard<TestEvent>>(),
          "raw",
        ) {
          emptyMap()
        }
      )
      .isEmpty()
  }

  private fun newWriter(columns: Map<String, String>) =
    RawImpressionsWriter(
      blobKeyId = "stream",
      kekUri = kekUri,
      kmsClient = kmsClient,
      storageConfiguration = Configuration(),
      rootPath = Path(tempFolder.root.toURI()),
      requiredEntityKeyColumns = columns,
    )

  private fun newReader() =
    ParquetStorageClient(
      Configuration(),
      Path(tempFolder.root.toURI()),
      encryptionConfig = ParquetEncryptionConfig(kmsProvider = { kmsClient }),
    )

  private fun shards(
    keysByImpression: List<List<EntityKey>>
  ): Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
    sequenceOf(
      EntityKeyedLabeledEventDateShard(
        localDate = EVENT_DATE,
        entityKeysWithLabeledEvents =
          keysByImpression.asSequence().map { keys ->
            EntityKeysWithLabeledEvents(
              entityKeys = keys,
              labeledEvents =
                sequenceOf(
                  LabeledEvent(
                    vid = 1L,
                    message = TestEvent.getDefaultInstance(),
                    timestamp = EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toInstant(),
                  )
                ),
            )
          },
      )
    )

  private fun keys(suffix: String): List<EntityKey> =
    listOf(
      EntityKey("person", "person-$suffix"),
      EntityKey("campaign", "campaign-$suffix"),
      EntityKey("adset-id", "adset-$suffix"),
      EntityKey("ad-id", "ad-$suffix"),
      EntityKey("account-id", "account-$suffix"),
      EntityKey("creative-id", "creative-$suffix"),
    )

  companion object {
    init {
      AeadConfig.register()
    }

    private val EVENT_DATE = LocalDate.parse("2020-01-01")
  }
}
