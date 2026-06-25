/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.DigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.VirtualPersonActivity

@RunWith(JUnit4::class)
class VidLabelingSinkTest {
  @get:Rule val tempFolder = TemporaryFolder()

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "vid-labeling-key"
  private val kmsClient =
    FakeKmsClient().apply {
      val handle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
      setAead(kekUri, handle.getPrimitive(Aead::class.java))
    }
  private val outputStorageParams =
    VidLabelerParams.StorageParams.newBuilder()
      .setGcsProjectId("test-project")
      .setImpressionsBlobPrefix("file:///labeled")
      .build()

  private fun sink(contexts: List<ModelLineContext>) =
    VidLabelingSink(
      inputBlobUri = "file:///raw/file-1.parquet",
      modelLineContexts = contexts,
      impressionConverter = FakeImpressionConverter(),
      eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID,
      encryptKmsClient = kmsClient,
      encryptKekUri = kekUri,
      outputStorageParams = outputStorageParams,
      storageConfig = StorageConfig(rootDirectory = tempFolder.root),
    )

  private fun context(activeWindow: ActiveWindow) =
    ModelLineContext(
      modelLine = MODEL_LINE,
      activeWindow = activeWindow,
      assigner = FixedVidAssigner(VID),
      config = VidLabelerParams.ModelLineConfig.getDefaultInstance(),
    )

  @Test
  fun `commit writes in-window labeled impressions and skips out-of-window`() =
    runBlocking<Unit> {
      tempFolder.root.resolve("labeled").mkdirs()
      val sink = sink(listOf(context(ActiveWindow(startMicros = 1_000L, endMicros = 2_000L))))

      sink.processBatch(
        listOf(
          rawEvent(eventTimeMicros = 1_000L, eventGroup = "eg1", idByte = 1), // in [1000, 2000)
          rawEvent(eventTimeMicros = 1_999L, eventGroup = "eg1", idByte = 2), // in
          rawEvent(eventTimeMicros = 2_000L, eventGroup = "eg1", idByte = 3), // out (end exclusive)
          rawEvent(eventTimeMicros = 500L, eventGroup = "eg1", idByte = 4), // out (before start)
        )
      )
      sink.commit()
      sink.close()

      val blobDetails = readSoleBlobDetails()
      assertThat(blobDetails.modelLine).isEqualTo(MODEL_LINE)
      assertThat(blobDetails.eventGroupReferenceId).isEqualTo("eg1")

      val impressions = readImpressions(blobDetails)
      assertThat(impressions).hasSize(2)
      assertThat(impressions.map { it.vid }.toSet()).containsExactly(VID)
      assertThat(impressions.map { it.eventGroupReferenceId }.toSet()).containsExactly("eg1")

      // Each impression carries its source row's entity keys verbatim (idBytes 1 and 2 survive).
      assertThat(impressions.map { it.entityKeysList.toSet() })
        .containsExactly(
          setOf(
            LabeledImpressionKt.entityKey {
              entityType = "household"
              entityId = "hh-1"
            },
            LabeledImpressionKt.entityKey {
              entityType = "person"
              entityId = "p-shared"
            },
          ),
          setOf(
            LabeledImpressionKt.entityKey {
              entityType = "household"
              entityId = "hh-2"
            },
            LabeledImpressionKt.entityKey {
              entityType = "person"
              entityId = "p-shared"
            },
          ),
        )

      // BlobDetails.entity_keys is the deduplicated union grouped by entity_type: one group per
      // type, household ids unioned, and the shared person id collapsed to a single entry.
      assertThat(blobDetails.entityKeysList.map { it.entityType })
        .containsExactly("household", "person")
      val unionByType = blobDetails.entityKeysList.associate { it.entityType to it.entityIdsList }
      assertThat(unionByType.getValue("household")).containsExactly("hh-1", "hh-2")
      assertThat(unionByType.getValue("person")).containsExactly("p-shared")
    }

  @Test
  fun `close without commit publishes no output`() =
    runBlocking<Unit> {
      tempFolder.root.resolve("labeled").mkdirs()
      val sink = sink(listOf(context(ActiveWindow(startMicros = 1_000L, endMicros = 2_000L))))

      sink.processBatch(listOf(rawEvent(eventTimeMicros = 1_000L, eventGroup = "eg1", idByte = 1)))
      sink.close() // no commit()

      val files = tempFolder.root.walkTopDown().filter { it.isFile }.toList()
      assertThat(files).isEmpty()
    }

  @Test
  fun `processBatch rejects an impression with empty entity keys`() =
    runBlocking<Unit> {
      tempFolder.root.resolve("labeled").mkdirs()
      // A converter that drops entity keys must fail loudly: an empty `entityKeys` would otherwise
      // silently strip both LabeledImpression.entity_keys and the BlobDetails.entity_keys union.
      // ConvertedImpression's init guard rejects it, so the failure propagates out of processBatch
      // before any labeled impression or blob details are emitted.
      val sink =
        VidLabelingSink(
          inputBlobUri = "file:///raw/file-1.parquet",
          modelLineContexts =
            listOf(context(ActiveWindow(startMicros = 1_000L, endMicros = 2_000L))),
          impressionConverter =
            ImpressionConverter { event, _, eventGroupReferenceId ->
              ConvertedImpression(
                labelerInput = LabelerInput.getDefaultInstance(),
                eventTimeMicros = event.row.getValue(EVENT_TIME_COLUMN).int64Value,
                eventGroupReferenceId = eventGroupReferenceId,
                event = Any.getDefaultInstance(),
                entityKeys = emptyList(),
              )
            },
          eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID,
          encryptKmsClient = kmsClient,
          encryptKekUri = kekUri,
          outputStorageParams = outputStorageParams,
          storageConfig = StorageConfig(rootDirectory = tempFolder.root),
        )

      assertFailsWith<IllegalArgumentException> {
        sink.processBatch(
          listOf(rawEvent(eventTimeMicros = 1_000L, eventGroup = "eg1", idByte = 1))
        )
      }
    }

  /** Reads back the labeled impressions from an output blob via its [BlobDetails]. */
  private suspend fun readImpressions(blobDetails: BlobDetails): List<LabeledImpression> {
    val parsed = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)
    val decryptionClient =
      SelectedStorageClient(parsed, tempFolder.root, null)
        .withEnvelopeEncryption(kmsClient, kekUri, blobDetails.encryptedDek.ciphertext)
    return MesosRecordIoStorageClient(decryptionClient).getBlob(parsed.key)!!.read().toList().map {
      LabeledImpression.parseFrom(it)
    }
  }

  /** Finds and parses the single `*.metadata.binpb` blob written under [tempFolder]. */
  private fun readSoleBlobDetails(): BlobDetails {
    val metadataFile =
      tempFolder.root.walkTopDown().single { it.isFile && it.name.endsWith(".metadata.binpb") }
    return BlobDetails.parseFrom(metadataFile.readBytes())
  }

  /**
   * Projects the test rows: reads event time + event group from fixed columns, and tags each
   * impression with a per-row `household` key plus a shared `person` key (so the per-blob union
   * exercises both multi-type grouping and cross-impression deduplication).
   */
  private class FakeImpressionConverter : ImpressionConverter {
    override fun convert(
      event: ParquetDigestedEvent,
      config: VidLabelerParams.ModelLineConfig,
      eventGroupReferenceId: String,
    ): ConvertedImpression =
      ConvertedImpression(
        labelerInput = LabelerInput.getDefaultInstance(),
        eventTimeMicros = event.row.getValue(EVENT_TIME_COLUMN).int64Value,
        eventGroupReferenceId = eventGroupReferenceId,
        event = Any.getDefaultInstance(),
        entityKeys =
          listOf(
            LabeledImpressionKt.entityKey {
              entityType = "household"
              entityId = "hh-${event.digest.high}"
            },
            LabeledImpressionKt.entityKey {
              entityType = "person"
              entityId = "p-shared"
            },
          ),
      )
  }

  /** Assigns a fixed VID regardless of input. */
  private class FixedVidAssigner(private val vid: Long) : VidAssigner {
    override fun assign(input: LabelerInput): LabelerOutput =
      LabelerOutput.newBuilder()
        .addPeople(VirtualPersonActivity.newBuilder().setVirtualPersonId(vid).build())
        .build()
  }

  private fun rawEvent(
    eventTimeMicros: Long,
    eventGroup: String,
    idByte: Int,
  ): ParquetDigestedEvent =
    DigestedEvent(
      row =
        mapOf(
          EVENT_TIME_COLUMN to ParquetValue.newBuilder().setInt64Value(eventTimeMicros).build(),
          EVENT_GROUP_COLUMN to ParquetValue.newBuilder().setStringValue(eventGroup).build(),
        ),
      digest = EventIdDigest(high = idByte.toLong(), low = idByte),
    )

  companion object {
    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val VID = 42L
    private const val EVENT_TIME_COLUMN = "event_time_micros"
    private const val EVENT_GROUP_COLUMN = "event_group"
    // The per-file event group reference id the reader reads from the Parquet footer and the sink
    // passes to the converter. The test rows also carry it in EVENT_GROUP_COLUMN so the rows match.
    private const val EVENT_GROUP_REFERENCE_ID = "eg1"
  }
}
