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

import com.google.crypto.tink.KmsClient
import com.google.protobuf.timestamp
import com.google.type.interval
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.launch
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.entityKeyGroup
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Per-input-file [RawImpressionSource.BlobSink] that labels one raw-impression file's
 * shard-filtered events and writes the encrypted labeled output for that file.
 *
 * One instance is minted per input blob by [VidLabeler]. [processBatch] is called concurrently by
 * the reader's CPU pool, so labeling happens in parallel and the labeled records are accumulated
 * into a thread-safe [ConcurrentHashMap] (per-key atomic, so concurrent batches contend only on the
 * same output group); [commit] then writes one encrypted output blob per `(model line, event
 * group)`, each with its own DEK. Output blob keys are derived deterministically from the input
 * blob URI so a retried input file overwrites its previous output rather than duplicating it (the
 * reader's per-blob guarantee requires idempotent output naming).
 *
 * @param inputBlobUri URI of the raw-impression file this sink consumes.
 * @param modelLineContexts model lines to label with, each with its [ActiveWindow] and
 *   [VidAssigner].
 * @param impressionConverter converts a Parquet row into a [ConvertedImpression] (schema seam).
 * @param encryptKmsClient encrypt/decrypt KMS client for the labeled output.
 * @param encryptKekUri KEK URI for generating per-output DEKs.
 * @param outputStorageParams GCS project + blob prefix for labeled output.
 * @param storageConfig storage configuration built from [outputStorageParams].
 */
class VidLabelingSink(
  private val inputBlobUri: String,
  private val modelLineContexts: List<ModelLineContext>,
  private val impressionConverter: ImpressionConverter,
  private val encryptKmsClient: KmsClient,
  private val encryptKekUri: String,
  private val outputStorageParams: VidLabelerParams.StorageParams,
  private val storageConfig: StorageConfig,
) : RawImpressionSource.BlobSink {

  private val labeledByGroup = ConcurrentHashMap<OutputGroupKey, MutableList<LabeledImpression>>()

  override suspend fun processBatch(events: List<ParquetDigestedEvent>) {
    // Label first (CPU work, and the canonical Labeler is stateless), then accumulate into the
    // concurrent map, since processBatch is invoked concurrently for this blob.
    val produced = ArrayList<Pair<OutputGroupKey, LabeledImpression>>()
    for (digestedEvent in events) {
      for (context in modelLineContexts) {
        val converted = impressionConverter.convert(digestedEvent, context.config) ?: continue
        if (!context.activeWindow.contains(converted.eventTimeMicros)) continue

        // Memoized path: attach the impression's pre-computed rank(s) (keyed by its EventIdDigest)
        // so the model's RankedPopulationNode leaf derives a collision-free VID via Feistel. All of
        // the fingerprint's per-subpool ranks are attached (a fingerprint can route to several
        // subpools across impressions); the leaf selects the one matching its own pool_offset. No
        // match (overflow / unseen) leaves the input untouched and the leaf falls back to hashing.
        val rankAssignments = context.rankIndex?.lookup(digestedEvent.digest).orEmpty()
        val labelerInput =
          if (rankAssignments.isEmpty()) {
            converted.labelerInput
          } else {
            converted.labelerInput.toBuilder().addAllRankAssignments(rankAssignments).build()
          }

        val output = context.assigner.assign(labelerInput)
        if (output.peopleCount == 0) continue

        val labeled = labeledImpression {
          eventTime = timestamp {
            seconds = Math.floorDiv(converted.eventTimeMicros, MICROS_PER_SECOND)
            nanos =
              (Math.floorMod(converted.eventTimeMicros, MICROS_PER_SECOND) * NANOS_PER_MICRO)
                .toInt()
          }
          vid = output.getPeople(0).virtualPersonId
          event = converted.event
          eventGroupReferenceId = converted.eventGroupReferenceId
          entityKeys += converted.entityKeys
        }
        produced.add(OutputGroupKey(context.modelLine, converted.eventGroupReferenceId) to labeled)
      }
    }

    for ((key, impression) in produced) {
      // computeIfAbsent is atomic per key, so concurrent processBatch calls accumulating into
      // different groups never block each other; only same-group adds serialize on the inner
      // synchronized list.
      labeledByGroup
        .computeIfAbsent(key) { Collections.synchronizedList(mutableListOf()) }
        .add(impression)
    }
  }

  override suspend fun commit() {
    coroutineScope {
      for ((key, impressions) in labeledByGroup) {
        launch { writeGroup(key, impressions) }
      }
    }
  }

  override suspend fun close() {
    // No output stream is held open between calls — writeGroup() opens and closes its own output
    // within commit(). close() therefore never publishes partial output and is safe to run alone
    // after a failed processBatch (commit() will not have run).
  }

  /** Encrypts [impressions] under a fresh DEK and writes the labeled-impression blob + metadata. */
  private suspend fun writeGroup(key: OutputGroupKey, impressions: List<LabeledImpression>) {
    val serializedEncryptionKey =
      EncryptedStorage.generateSerializedEncryptionKey(
        encryptKmsClient,
        encryptKekUri,
        TINK_KEY_TEMPLATE,
      )
    val outputEncryptedDek = encryptedDek {
      kekUri = encryptKekUri
      ciphertext = serializedEncryptionKey
      protobufFormat = EncryptedDek.ProtobufFormat.BINARY
      typeUrl = TINK_KEYSET_TYPE_URL
    }

    val blobKey = outputBlobKey(key)
    val outputBlobUri = "${outputStorageParams.impressionsBlobPrefix}/$blobKey"
    val outputStorageClient =
      SelectedStorageClient(
        SelectedStorageClient.parseBlobUri(outputBlobUri),
        storageConfig.rootDirectory,
        storageConfig.projectId,
      )
    val aeadStorageClient =
      outputStorageClient.withEnvelopeEncryption(
        encryptKmsClient,
        encryptKekUri,
        serializedEncryptionKey,
      )
    // TODO(world-federation-of-advertisers/cross-media-measurement#3999): Add ifGenerationMatch
    // (write-if-absent) to prevent overwrite races on Pub/Sub redelivery.
    MesosRecordIoStorageClient(aeadStorageClient)
      .writeBlob(blobKey, impressions.map { it.toByteString() }.asFlow())

    val earliest = impressions.minByOrNull { it.eventTime.epochNanos }!!.eventTime
    val latest = impressions.maxByOrNull { it.eventTime.epochNanos }!!.eventTime
    val blobEntityKeys =
      impressions
        .flatMap { it.entityKeysList }
        .groupBy { it.entityType }
        .map { (type, keys) ->
          entityKeyGroup {
            entityType = type
            entityIds += keys.map { it.entityId }.distinct()
          }
        }
    val details = blobDetails {
      blobUri = outputBlobUri
      encryptedDek = outputEncryptedDek
      eventGroupReferenceId = key.eventGroupReferenceId
      modelLine = key.modelLine
      interval = interval {
        startTime = earliest
        endTime = latest
      }
      entityKeys += blobEntityKeys
    }

    val metadataKey = "$blobKey.metadata.binpb"
    val metadataUri = "${outputStorageParams.impressionsBlobPrefix}/$metadataKey"
    // TODO(world-federation-of-advertisers/cross-media-measurement#3999): Add ifGenerationMatch
    // (write-if-absent) to prevent overwrite races on Pub/Sub redelivery. Same exposure as the
    // labeled-impressions write above: the metadata key is deterministic, so concurrent VMs
    // labeling the same input file would race here too.
    SelectedStorageClient(
        SelectedStorageClient.parseBlobUri(metadataUri),
        storageConfig.rootDirectory,
        storageConfig.projectId,
      )
      .writeBlob(metadataKey, details.toByteString())

    logger.info(
      "Wrote ${impressions.size} labeled impressions for ${key.modelLine}/" +
        "${key.eventGroupReferenceId} to $outputBlobUri"
    )
  }

  /**
   * Deterministic output blob key for [key] under this input file, so a retried input file
   * overwrites its previous output instead of duplicating it.
   */
  private fun outputBlobKey(key: OutputGroupKey): String {
    val digest =
      MessageDigest.getInstance("SHA-256")
        .digest(
          "$inputBlobUri|${key.modelLine}|${key.eventGroupReferenceId}".toByteArray(Charsets.UTF_8)
        )
    return "labeled-impressions/" + digest.joinToString("") { "%02x".format(it) }
  }

  private val com.google.protobuf.Timestamp.epochNanos: Long
    get() = seconds * NANOS_PER_SECOND + nanos

  companion object {
    private val logger = Logger.getLogger(VidLabelingSink::class.java.name)

    private const val MICROS_PER_SECOND = 1_000_000L
    private const val NANOS_PER_MICRO = 1_000L
    private const val NANOS_PER_SECOND = 1_000_000_000L
    private const val TINK_KEY_TEMPLATE = "AES128_GCM_HKDF_1MB"
    private const val TINK_KEYSET_TYPE_URL = "type.googleapis.com/google.crypto.tink.Keyset"
  }
}

/**
 * A model line resolved to everything [VidLabelingSink] needs to label with it.
 *
 * @property modelLine model line resource name.
 * @property activeWindow the model line's active interval, for event-time filtering.
 * @property assigner the [VidAssigner] bound to this model line's compiled model.
 * @property config the model line's field-mapping configuration.
 * @property rankIndex the memoized rank index for this model line, or `null` for the non-memoized
 *   (hash-only) path.
 */
data class ModelLineContext(
  val modelLine: String,
  val activeWindow: ActiveWindow,
  val assigner: VidAssigner,
  val config: VidLabelerParams.ModelLineConfig,
  val rankIndex: MemoizedRankIndex? = null,
)

/** Identifies one labeled-output blob: a `(model line, event group)` pair within an input file. */
private data class OutputGroupKey(val modelLine: String, val eventGroupReferenceId: String)
