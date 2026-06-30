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
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.interval
import io.opentelemetry.api.common.Attributes
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
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
import org.wfanet.virtualpeople.common.copy

/**
 * Per-input-file [RawImpressionSource.BlobSink] that labels one raw-impression file's
 * shard-filtered events and writes the encrypted labeled output for that file.
 *
 * One instance is minted per input blob by [VidLabeler]. [processBatch] is called concurrently by
 * the reader's CPU pool, so labeling happens in parallel; each labeled record is streamed into its
 * output group's [GroupWriter], which drains a bounded channel straight into a single streaming
 * `writeBlob`. The full labeled output is never accumulated in memory — channel backpressure bounds
 * the heap regardless of input size, which is what lets a single large input file (tens of GB of
 * labeled output, amplified by co-viewing) be processed without OOM. There is one encrypted output
 * blob per model line, each with its own DEK.
 *
 * Output blob keys are derived deterministically from the input blob URI so a retried input file
 * overwrites its previous output rather than duplicating it (the reader's per-blob guarantee
 * requires idempotent output naming).
 *
 * **Failure semantics.** [commit] writes each group's `.metadata.binpb` sidecar only after that
 * group's data blob has been fully written. This ordering is load-bearing: consumers
 * (`DataAvailabilitySync`, then the ResultsFulfiller) discover impressions blobs *only* through
 * their metadata sidecars, never by listing the impressions directory. So if labeling fails partway
 * through, the partially written data blob has no sidecar, is invisible to every consumer, and is
 * overwritten by the complete blob on retry (deterministic key). [close] simply cancels the open
 * writers; no partial output can leak into a measurement.
 *
 * **Concurrency contract.** The [RawImpressionSource.BlobSink] protocol guarantees [commit] and
 * [close] run only after every [processBatch] call for this blob has returned. That quiescence is
 * what makes the weakly-consistent [writers] (`ConcurrentHashMap`) iteration in [commit] safe: no
 * [processBatch] can add a new writer while [commit] is iterating.
 *
 * **KMS fan-out.** Each output group performs its own KMS round trips (DEK generation + envelope
 * bind) when its writer starts, with no cross-group cap. At the current operating scale — one input
 * file at a time, a handful of model lines — this stays well within KMS quota. A consumer that fans
 * this sink out at higher concurrency (e.g. many input files in parallel) must bound the aggregate
 * KMS concurrency itself; the batched-file path that needs this adds the cap (see #4072).
 *
 * @param inputBlobUri URI of the raw-impression file this sink consumes.
 * @param modelLineContexts model lines to label with, each with its [ActiveWindow] and
 *   [VidAssigner].
 * @param impressionConverter converts a Parquet row into a [ConvertedImpression] (schema seam).
 * @param encryptKmsClient encrypt/decrypt KMS client for the labeled output.
 * @param encryptKekUri KEK URI for generating per-output DEKs.
 * @param outputStorageParams GCS project + blob prefix for labeled output.
 * @param storageConfig storage configuration built from [outputStorageParams].
 * @param dataProvider resource name of the `DataProvider`, used for metric attribution.
 * @param metrics OpenTelemetry instruments for labeling rate, drops, and errors.
 * @param encryptionKeySemaphore bounds concurrent per-group KMS key-setup work. It must be **shared
 *   across all sinks of one WorkItem** (one model line generates one output group per file, and the
 *   non-memoized path bundles many model lines, so a WorkItem fans out `nModelLines * nFiles`
 *   groups — each doing a KMS roundtrip to wrap its output DEK). Because every file in a WorkItem
 *   belongs to the same `DataProvider` and therefore wraps against the same KEK, an unbounded
 *   fan-out would burst that one KEK past Cloud KMS's per-project rate limits. [VidLabeler] owns
 *   the per-WorkItem instance (one `VidLabeler.label()` call per WorkItem) and passes it to every
 *   sink; the permit covers only the short key-setup phase, never the long streaming write, so it
 *   can never stall a started blob stream.
 */
class VidLabelingSink(
  private val inputBlobUri: String,
  private val modelLineContexts: List<ModelLineContext>,
  private val impressionConverter: ImpressionConverter,
  private val encryptKmsClient: KmsClient,
  private val encryptKekUri: String,
  private val outputStorageParams: VidLabelerParams.StorageParams,
  private val storageConfig: StorageConfig,
  private val dataProvider: String,
  private val metrics: VidLabelerMetrics,
  private val encryptionKeySemaphore: Semaphore,
) : RawImpressionSource.BlobSink {

  /**
   * Sink-owned scope for the per-group writer coroutines.
   *
   * Its lifecycle matches this sink: writers are launched lazily as groups first appear in
   * [processBatch], awaited in [commit], and torn down in [close]. A [SupervisorJob] keeps one
   * group's write failure from cancelling its siblings before [commit] can observe it via
   * [awaitAll].
   */
  private val writerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

  /** One streaming writer per model-line output group, created on the group's first record. */
  private val writers = ConcurrentHashMap<OutputGroupKey, GroupWriter>()

  override suspend fun processBatch(events: List<ParquetDigestedEvent>) {
    // Label first (CPU work, and the canonical Labeler is stateless), then stream each produced
    // record into its group's writer. processBatch is invoked concurrently for this blob.
    val produced = ArrayList<GroupedImpression>()
    for (digestedEvent in events) {
      for (context in modelLineContexts) {
        try {
          val converted = impressionConverter.convert(digestedEvent, context.config)
          if (converted == null) {
            metrics.impressionsDroppedCounter.add(
              1,
              dropAttributes(context.modelLine, VidLabelerMetrics.DROP_REASON_CONVERTER_SKIP),
            )
            continue
          }
          if (!context.activeWindow.contains(Timestamps.toMicros(converted.eventTime))) {
            metrics.impressionsDroppedCounter.add(
              1,
              dropAttributes(context.modelLine, VidLabelerMetrics.DROP_REASON_OUTSIDE_WINDOW),
            )
            continue
          }

          // Memoized path: attach the impression's pre-computed rank(s) (keyed by its
          // EventIdDigest)
          // so the model's RankedPopulationNode leaf derives a collision-free VID via Feistel. All
          // of
          // the fingerprint's per-subpool ranks are attached (a fingerprint can route to several
          // subpools across impressions); the leaf selects the one matching its own pool_offset. No
          // match (overflow / unseen) leaves the input untouched and the leaf falls back to
          // hashing.
          val ranks = context.rankIndex?.lookup(digestedEvent.digest).orEmpty()
          val labelerInput =
            if (ranks.isEmpty()) {
              converted.labelerInput
            } else {
              converted.labelerInput.copy { rankAssignments += ranks }
            }
          // TODO(world-federation-of-advertisers/cross-media-measurement#4073): Once
          // virtual-people-common#75 (memoized_rank_fallback signal) and
          // virtual-people-core-serving#89 (RankedPopulationNode hash fallback) are merged, count
          // VirtualPersonActivity.memoized_rank_fallback across output.peopleList (coviewing-safe)
          // into a per-(dataProvider, modelLine) counter here to surface silent degradation to
          // hash-based VID assignment when subpools saturate.

          val output = context.assigner.assign(labelerInput)
          if (output.peopleCount == 0) {
            metrics.impressionsDroppedCounter.add(
              1,
              dropAttributes(context.modelLine, VidLabelerMetrics.DROP_REASON_NO_ASSIGNMENT),
            )
            continue
          }

          val key = OutputGroupKey(context.modelLine)
          // A LabelerOutput can assign multiple virtual people to a single impression; emit one
          // LabeledImpression per assigned VID rather than only the first.
          for (person in output.peopleList) {
            produced.add(
              GroupedImpression(
                key,
                labeledImpression {
                  eventTime = converted.eventTime
                  vid = person.virtualPersonId
                  event = converted.event
                  eventGroupReferenceId = converted.eventGroupReferenceId
                  entityKeys += converted.entityKeys
                },
              )
            )
          }
          metrics.impressionsLabeledCounter.add(
            output.peopleList.size.toLong(),
            labelAttributes(context.modelLine),
          )
        } catch (e: CancellationException) {
          // Cooperative cancellation is not a labeling error; rethrow without inflating the metric.
          throw e
        } catch (e: Exception) {
          metrics.labelingErrorsCounter.add(1, labelAttributes(context.modelLine))
          throw e
        }
      }
    }

    for ((key, impression) in produced) {
      // computeIfAbsent is atomic per key, so concurrent processBatch calls for different groups
      // never block each other; send() then suspends on the group's channel if it is full,
      // applying backpressure that bounds the heap.
      writers.computeIfAbsent(key) { GroupWriter(it) }.send(impression)
    }
  }

  override suspend fun commit() {
    // Signal end-of-input to every writer, then await them so each data blob is fully written
    // before we publish any metadata sidecar. Writing the sidecar strictly after the data blob
    // succeeds is the invariant that makes a failed/partial data blob inert (see the class KDoc):
    // consumers discover impressions only via the sidecar. awaitAll surfaces the first writer
    // failure, in which case no sidecars are written and the work item is retried.
    for (writer in writers.values) {
      writer.finish()
    }
    writers.values.map { it.deferred }.awaitAll()
    for (writer in writers.values) {
      writer.writeMetadata()
    }
  }

  override suspend fun close() {
    // Abort any still-open writers. On the failure path commit() has not run, so cancelling here
    // tears down the open writeBlob streams. Any partially written data blob is harmless: it has no
    // metadata sidecar, so no consumer can discover it, and a retry overwrites it (deterministic
    // key).
    writerScope.cancel()
  }

  /**
   * Streams one model-line output group's labeled impressions into a single encrypted blob.
   *
   * [send] is called by the concurrent [processBatch] invocations; a dedicated coroutine
   * ([deferred]) drains the bounded [channel] straight into [MesosRecordIoStorageClient.writeBlob],
   * so the records are never all held in memory at once. The per-blob aggregates needed for the
   * metadata sidecar ([earliest], [latest], [entityIdsByType], [eventGroupReferenceId]) are
   * accumulated by that single draining coroutine as each record passes through, so no
   * synchronization is needed and [commit] reads them safely after awaiting [deferred].
   */
  private inner class GroupWriter(private val key: OutputGroupKey) {
    private val channel = Channel<LabeledImpression>(WRITER_CHANNEL_CAPACITY)

    private var earliest: Timestamp? = null
    private var latest: Timestamp? = null
    private val entityIdsByType = LinkedHashMap<String, LinkedHashSet<String>>()
    // The event group reference id for this file's impressions (one per input file). Captured while
    // streaming and written to the sidecar so DataAvailabilitySync can register this blob's
    // ImpressionMetadata (create requires it) and the results fulfiller can locate it for
    // reference-id event groups. Mirrors the out-of-band impressions' sidecar.
    private var eventGroupReferenceId: String = ""

    private val blobKey = outputBlobKey(key)
    private val outputBlobUri = "${outputStorageParams.impressionsBlobPrefix}/$blobKey"
    private lateinit var outputEncryptedDek: EncryptedDek

    /** The draining/writing coroutine. Started eagerly so the blob is opened as records arrive. */
    val deferred: Deferred<Unit> = writerScope.async { runWriter() }

    suspend fun send(impression: LabeledImpression) = channel.send(impression)

    /** Signals end-of-input so the draining coroutine can finalize the blob. */
    fun finish() {
      channel.close()
    }

    private suspend fun runWriter() {
      // Generate-and-wrap the output DEK (a KMS roundtrip) and build the envelope-encrypting
      // client under [encryptionKeySemaphore] so the WorkItem's group fan-out cannot burst the
      // shared KEK past Cloud KMS rate limits. The permit guards only this short setup, not the
      // streaming write below, so holding it can never stall another group's blob stream.
      val aeadStorageClient =
        encryptionKeySemaphore.withPermit {
          val serializedEncryptionKey =
            EncryptedStorage.generateSerializedEncryptionKey(
              encryptKmsClient,
              encryptKekUri,
              TINK_KEY_TEMPLATE,
            )
          outputEncryptedDek = encryptedDek {
            kekUri = encryptKekUri
            ciphertext = serializedEncryptionKey
            protobufFormat = EncryptedDek.ProtobufFormat.BINARY
            typeUrl = TINK_KEYSET_TYPE_URL
          }
          SelectedStorageClient(
              SelectedStorageClient.parseBlobUri(outputBlobUri),
              storageConfig.rootDirectory,
              storageConfig.projectId,
            )
            .withEnvelopeEncryption(encryptKmsClient, encryptKekUri, serializedEncryptionKey)
        }
      try {
        // TODO(world-federation-of-advertisers/cross-media-measurement#3999): Add ifGenerationMatch
        // (write-if-absent) to prevent overwrite races on Pub/Sub redelivery.
        MesosRecordIoStorageClient(aeadStorageClient)
          .writeBlob(
            blobKey,
            channel.consumeAsFlow().map { impression ->
              updateAggregates(impression)
              impression.toByteString()
            },
          )
      } catch (e: CancellationException) {
        // Writer scope cancelled (e.g. via close() on the failure path); not a labeling error.
        throw e
      } catch (e: Exception) {
        metrics.labelingErrorsCounter.add(1, labelAttributes(key.modelLine))
        throw e
      }
      metrics.blobsWrittenCounter.add(1, labelAttributes(key.modelLine))
      logger.info("Wrote labeled impressions for ${key.modelLine} to $outputBlobUri")
    }

    private fun updateAggregates(impression: LabeledImpression) {
      val eventTime = impression.eventTime
      val currentEarliest = earliest
      if (currentEarliest == null || eventTime.epochNanos < currentEarliest.epochNanos) {
        earliest = eventTime
      }
      val currentLatest = latest
      if (currentLatest == null || eventTime.epochNanos > currentLatest.epochNanos) {
        latest = eventTime
      }
      for (entityKey in impression.entityKeysList) {
        entityIdsByType.getOrPut(entityKey.entityType) { LinkedHashSet() }.add(entityKey.entityId)
      }
      if (eventGroupReferenceId.isEmpty() && impression.eventGroupReferenceId.isNotEmpty()) {
        eventGroupReferenceId = impression.eventGroupReferenceId
      }
    }

    /** Writes the `.metadata.binpb` sidecar from the aggregates collected while streaming. */
    suspend fun writeMetadata() {
      val details = blobDetails {
        blobUri = outputBlobUri
        encryptedDek = outputEncryptedDek
        modelLine = key.modelLine
        eventGroupReferenceId = this@GroupWriter.eventGroupReferenceId
        interval = interval {
          startTime = checkNotNull(earliest) { "No impressions written for ${key.modelLine}" }
          endTime = checkNotNull(latest) { "No impressions written for ${key.modelLine}" }
        }
        entityKeys +=
          entityIdsByType.map { (type, ids) ->
            entityKeyGroup {
              entityType = type
              entityIds += ids
            }
          }
      }

      val metadataKey = "$blobKey.metadata.binpb"
      val metadataUri = "${outputStorageParams.impressionsBlobPrefix}/$metadataKey"
      // TODO(world-federation-of-advertisers/cross-media-measurement#3999): Add ifGenerationMatch
      // (write-if-absent) to prevent overwrite races on Pub/Sub redelivery. Same exposure as the
      // labeled-impressions write: the metadata key is deterministic, so concurrent VMs labeling
      // the same input file would race here too.
      SelectedStorageClient(
          SelectedStorageClient.parseBlobUri(metadataUri),
          storageConfig.rootDirectory,
          storageConfig.projectId,
        )
        .writeBlob(metadataKey, details.toByteString())
    }
  }

  private fun labelAttributes(modelLine: String): Attributes =
    Attributes.of(
      VidLabelerMetrics.DATA_PROVIDER_KEY,
      dataProvider,
      VidLabelerMetrics.MODEL_LINE_KEY,
      modelLine,
    )

  private fun dropAttributes(modelLine: String, reason: String): Attributes =
    Attributes.builder()
      .put(VidLabelerMetrics.DATA_PROVIDER_KEY, dataProvider)
      .put(VidLabelerMetrics.MODEL_LINE_KEY, modelLine)
      .put(VidLabelerMetrics.DROP_REASON_KEY, reason)
      .build()

  /**
   * Deterministic output blob key for [key] under this input file, so a retried input file
   * overwrites its previous output instead of duplicating it.
   */
  private fun outputBlobKey(key: OutputGroupKey): String {
    val digest =
      MessageDigest.getInstance("SHA-256")
        .digest("$inputBlobUri|${key.modelLine}".toByteArray(Charsets.UTF_8))
    return "labeled-impressions/" + digest.joinToString("") { "%02x".format(it) }
  }

  private val Timestamp.epochNanos: Long
    get() = seconds * NANOS_PER_SECOND + nanos

  companion object {
    private val logger = Logger.getLogger(VidLabelingSink::class.java.name)

    /**
     * Default permit count for the per-WorkItem [encryptionKeySemaphore]: the maximum number of
     * output-group DEKs wrapped against a `DataProvider`'s KEK concurrently across all of a
     * WorkItem's files. Sized to comfortably stay under Cloud KMS per-project request quotas while
     * keeping key setup off the critical path. Mirrors
     * `MemoizedRankIndex.DEFAULT_READER_PARALLELISM` on the read side.
     */
    const val DEFAULT_ENCRYPTION_KEY_PARALLELISM = 16
    private const val NANOS_PER_SECOND = 1_000_000_000L
    private const val TINK_KEY_TEMPLATE = "AES128_GCM_HKDF_1MB"
    private const val TINK_KEYSET_TYPE_URL = "type.googleapis.com/google.crypto.tink.Keyset"

    /**
     * Bounded capacity of each output group's writer channel. Caps in-flight labeled records per
     * group so the heap stays bounded (capacity × groups × record size) while still decoupling the
     * concurrent labeling producers from the single I/O-bound writer.
     */
    private const val WRITER_CHANNEL_CAPACITY = 256
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

/**
 * Identifies one labeled-output blob within an input file: one blob per model line. New writers
 * group by model line and carry the per-blob entity-key union on `BlobDetails.entity_keys` rather
 * than splitting output by the legacy `event_group_reference_id`.
 */
private data class OutputGroupKey(val modelLine: String)

/** A labeled impression paired with the output group it belongs to, for channel hand-off. */
private data class GroupedImpression(val key: OutputGroupKey, val impression: LabeledImpression)
