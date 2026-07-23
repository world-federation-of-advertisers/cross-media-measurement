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

/**
 * Storage-based event source implementation for reading events from blob storage.
 *
 * This file contains the [StorageEventSource] class which provides parallel event reading
 * capabilities for the EDP aggregator results fulfiller system.
 */
package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import java.io.IOException
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey

/** Result of processing a single EventReader. */
data class EventReaderResult(val batchCount: Int, val eventCount: Int)

/** An [EventReader] paired with its resolved [EventGroupIdentifier] and blob URI. */
data class ResolvedEventReader(
  val reader: EventReader<Message>,
  val eventGroupIdentifier: EventGroupIdentifier,
  val blobUri: String,
)

/**
 * Account identifier and location/region extracted from a KMS KEK URI.
 *
 * For GCP KMS URIs, [accountId] is the GCP project ID and [location] is the Cloud KMS location. For
 * AWS KMS URIs, [accountId] is the AWS account ID and [location] is the AWS region. For test URIs
 * (fake-kms://), both fields are empty strings.
 */
data class KmsKeyLocation(val accountId: String, val location: String)

/** Component for tracking progress during event processing. */
class ProgressTracker(private val totalEventReaders: Int) : AutoCloseable {
  private var processedReaders = 0L
  private var totalEventsRead = 0L
  private var totalBatchesSent = 0L
  private var closed = false

  fun updateProgress(eventCount: Int, batchCount: Int) {
    synchronized(this) {
      check(!closed) { "ProgressTracker has been closed and cannot be reused" }

      processedReaders++
      totalEventsRead += eventCount
      totalBatchesSent += batchCount

      // Log progress every 10% or at least every 10 readers
      if (
        processedReaders % maxOf(1L, totalEventReaders.toLong() / 10) == 0L ||
          processedReaders == totalEventReaders.toLong()
      ) {
        val progressPercent = (processedReaders * 100) / totalEventReaders
        logger.info(
          "Progress: $progressPercent% ($processedReaders/$totalEventReaders EventReaders) - Total events: $totalEventsRead, Batches sent: $totalBatchesSent"
        )
      }
    }
  }

  override fun close() {
    synchronized(this) {
      if (!closed) {
        closed = true
        logger.info(
          "Completed storage event generation - Total events: $totalEventsRead, Total batches: $totalBatchesSent"
        )
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(ProgressTracker::class.java.name)
  }
}

/**
 * Event source that builds readers from an impression metadata facade.
 *
 * Resolves data sources via [ImpressionMetadataService] for each configured event group interval,
 * then creates [EventReader]s and streams batches in parallel. This class is agnostic to storage
 * layout and transport.
 *
 * Performance characteristics:
 * - Parallelism: one coroutine per resolved data source
 * - Memory: bounded by batch size and number of concurrent readers
 *
 * @property impressionDataSourceProvider facade providing data sources for intervals.
 * @property eventGroupDetailsList event groups with their collection intervals.
 * @property modelLine model line to use for fetching impressions
 * @property kmsClient KMS client for decryption operations
 * @property impressionsStorageConfig storage configuration for reading impressions
 * @property descriptor protobuf descriptor for parsing events
 * @property batchSize batch size for event reading
 * @property readConcurrency maximum number of blobs read + decrypted concurrently. Bounds the
 *   outbound Cloud Storage / Cloud KMS fan-out so one work item cannot exhaust Cloud NAT ports or
 *   overwhelm KMS.
 * @property readMaxAttempts maximum total attempts (including the first) for opening a single blob
 *   (build storage client + DEK unwrap + open) before giving up. Only failures that occur before
 *   any batch has been emitted are retried; once streaming has started, mid-stream read failures
 *   are left to the storage client's own retry budget.
 * @property readRetryBackoff exponential backoff applied between blob-open retries.
 * @property eventReaderFactory builds the [EventReader] for a blob. Defaults to a
 *   [StorageEventReader]; injectable for testing.
 */
class StorageEventSource(
  private val impressionDataSourceProvider: ImpressionDataSourceProvider,
  private val eventGroupDetailsList: List<EventGroupDetails>,
  private val modelLine: String,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val descriptor: Descriptors.Descriptor,
  private val batchSize: Int,
  private val readConcurrency: Int = DEFAULT_READ_CONCURRENCY,
  private val readMaxAttempts: Int = DEFAULT_READ_MAX_ATTEMPTS,
  private val readRetryBackoff: ExponentialBackoff = ExponentialBackoff(),
  private val eventReaderFactory: (BlobDetails) -> EventReader<Message> = { blobDetails ->
    StorageEventReader(blobDetails, kmsClient, impressionsStorageConfig, descriptor, batchSize)
  },
) : EventSource<Message> {
  init {
    require(readConcurrency > 0) { "readConcurrency must be positive" }
    require(readMaxAttempts >= 1) { "readMaxAttempts must be at least 1" }
  }

  /** Cache for unique impression data sources to avoid duplicate API calls. */
  private var cachedImpressionDataSources: List<ImpressionDataSource>? = null

  private val useEntityKeyStrategy: Boolean = run {
    require(eventGroupDetailsList.isNotEmpty()) { "eventGroupDetailsList must not be empty" }
    val allEntityKey = eventGroupDetailsList.all { hasEntityKey(it) }
    val noneEntityKey = eventGroupDetailsList.none { hasEntityKey(it) }
    require(allEntityKey || noneEntityKey) { "Cannot mix entity-key and reference-id event groups" }
    allEntityKey
  }

  /**
   * Generates batches of events by reading from storage in parallel.
   *
   * This method creates an [EventReader] for each combination of event group and date within the
   * specified collection intervals. All readers operate concurrently to maximize throughput.
   *
   * ## Implementation Details
   * 1. **Expansion**: Expands each event group's collection intervals into individual dates
   * 2. **Reader creation**: Creates one [EventReader] per date/event group combination
   * 3. **Parallel execution**: Launches all readers concurrently using the provided dispatcher
   * 4. **Streaming**: Results are streamed back as they become available via a channel flow
   *
   * ## Error Handling
   *
   * If any individual [EventReader] fails:
   * - The entire operation fails fast
   * - All other concurrent readers are cancelled
   * - The original exception is propagated to the caller
   *
   * ## Progress Tracking
   *
   * Progress is logged at:
   * - Every 10% of completion
   * - Upon full completion with total events and batches processed
   *
   * @return A [Flow] that emits [EventBatch]es as they are read from storage
   */
  override fun generateEventBatches(): Flow<EventBatch<Message>> {
    logger.info("Starting storage-based event generation with batching")

    return channelFlow {
      val eventReaders: List<ResolvedEventReader> = createEventReaders()
      ProgressTracker(eventReaders.size).use { progressTracker ->
        logger.info(
          "Processing ${eventReaders.size} EventReaders across ${eventGroupDetailsList.size} event groups"
        )
        // Launch one coroutine per EventReader, but bound how many read + decrypt concurrently via
        // a
        // semaphore so the outbound Cloud Storage / Cloud KMS fan-out cannot exhaust Cloud NAT
        // ports
        // or overwhelm KMS (the ~64-wide unbounded churn that caused the production egress storm).
        val readSemaphore = Semaphore(readConcurrency)
        coroutineScope {
          eventReaders.forEach { resolvedReader ->
            launch {
              readSemaphore.withPermit {
                val result = processEventReader(resolvedReader) { eventBatch -> send(eventBatch) }
                progressTracker.updateProgress(result.eventCount, result.batchCount)
              }
            }
          }
          logger.info(
            "Launched ${eventReaders.size} EventReader processing coroutines (readConcurrency=$readConcurrency)"
          )
        }
      }
    }
  }

  /** Returns true if [details] has an entity key with a non-empty entity ID. */
  private fun hasEntityKey(details: EventGroupDetails): Boolean {
    return details.hasEntityKey() && details.entityKey.entityId.isNotEmpty()
  }

  /** Collects all impression data sources and deduplicates by blob URI. */
  private suspend fun getUniqueImpressionDataSources(): List<ImpressionDataSource> {
    cachedImpressionDataSources?.let {
      return it
    }

    val allSources =
      eventGroupDetailsList.flatMap { details ->
        logger.info("EventGroup details: $details")
        val selector =
          if (useEntityKeyStrategy) {
            ImpressionQuerySelector.ByEntityKey(
              entityKey {
                entityType = details.entityKey.entityType
                entityId = details.entityKey.entityId
              }
            )
          } else {
            ImpressionQuerySelector.ByEventGroupReferenceId(details.eventGroupReferenceId)
          }
        details.collectionIntervalsList.flatMap { interval ->
          logger.info("EventGroup collection interval: $interval")
          impressionDataSourceProvider.listImpressionDataSources(modelLine, selector, interval)
        }
      }
    val result = allSources.distinctBy { it.blobDetails.blobUri }
    cachedImpressionDataSources = result
    return result
  }

  private suspend fun createEventReaders(): List<ResolvedEventReader> {
    logger.info("Creating event readers... ")
    val uniqueSources = getUniqueImpressionDataSources()

    return uniqueSources.map { source ->
      val eventGroupIdentifier =
        if (useEntityKeyStrategy) {
          EventGroupIdentifier.ByEntityKeys(source.blobDetails.entityKeysList)
        } else {
          EventGroupIdentifier.ByReferenceId(source.blobDetails.eventGroupReferenceId)
        }
      ResolvedEventReader(
        reader = eventReaderFactory(source.blobDetails),
        eventGroupIdentifier = eventGroupIdentifier,
        blobUri = source.blobDetails.blobUri,
      )
    }
  }

  /** Processes events from a single EventReader and returns batch and event counts. */
  private suspend fun processEventReader(
    resolvedReader: ResolvedEventReader,
    sendEventBatch: suspend (EventBatch<Message>) -> Unit,
  ): EventReaderResult {
    logger.fine("Reading events from ${resolvedReader.blobUri}")

    // Stream the blob: emit each batch downstream as it is read, so peak memory stays at ~one
    // batch per in-flight reader rather than a whole buffered blob. The blob open + DEK unwrap
    // happen before the first batch is emitted, so retrying is safe (no double-count into the
    // accumulating sinks) ONLY while nothing has been emitted yet -- exactly the transient
    // KMS-endpoint / GCS-connect failure that nothing else retries. Once streaming has started, a
    // mid-stream read failure is left to the storage client's own retry budget, not retried here.
    var attempt = 0
    while (true) {
      attempt++
      var batchCount = 0
      var eventCount = 0
      var emitted = false
      try {
        resolvedReader.reader.readEvents().collect { events ->
          val eventBatch =
            EventBatch(
              events = events,
              minTime = events.minOf { it.timestamp },
              maxTime = events.maxOf { it.timestamp },
              eventGroupIdentifier = resolvedReader.eventGroupIdentifier,
            )
          sendEventBatch(eventBatch)
          emitted = true
          batchCount++
          eventCount += events.size
        }
        logger.fine("Read $eventCount events in $batchCount batches for ${resolvedReader.blobUri}")
        return EventReaderResult(batchCount, eventCount)
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        if (emitted || attempt >= readMaxAttempts || !isRetryableReadFailure(e)) {
          throw e
        }
        logger.warning {
          "Transient failure opening ${resolvedReader.blobUri} on attempt $attempt of " +
            "$readMaxAttempts (${e.message}); retrying"
        }
        delay(readRetryBackoff.durationForAttempt(attempt).toMillis())
      }
    }
  }

  /**
   * Whether a blob open/DEK-unwrap failure is safe to retry.
   *
   * This is only consulted for failures that occur before any batch has been emitted, so a retry
   * cannot double-count into the accumulating sinks; retryability is therefore purely a question of
   * whether the failure is a transient egress blip.
   *
   * The dominant production failures surface as an [IOException] somewhere in the cause chain: a
   * GCS read wraps a `SocketException` ("Broken pipe") or `SSLHandshakeException` ("Remote host
   * terminated the handshake"), and a Cloud KMS `GeneralSecurityException` ("decryption failed")
   * wraps an [IOException] ("Error requesting access token") from the token endpoint.
   *
   * [InvalidProtocolBufferException] is excluded even though it is an [IOException]: a blob that
   * fails to parse is permanently corrupt data, so it must fail fast rather than burn the retry
   * budget. Other non-transient failures (a missing blob, a malformed URI, a genuine crypto error)
   * are simply not matched.
   */
  private fun isRetryableReadFailure(t: Throwable): Boolean {
    return causalChain(t).any { cause ->
      cause is IOException && cause !is InvalidProtocolBufferException
    }
  }

  /** Lazily walks [t]'s cause chain (including itself), guarding against cycles. */
  private fun causalChain(t: Throwable): Sequence<Throwable> = sequence {
    val seen = mutableSetOf<Throwable>()
    var current: Throwable? = t
    while (current != null && seen.add(current)) {
      yield(current)
      current = current.cause
    }
  }

  /**
   * Gets the KEK URI from the impression data sources for TrusTee protocol encryption.
   *
   * The TrusTee protocol encrypts output data using a Key Encryption Key (KEK) from KMS. We obtain
   * the KEK URI from the impression metadata because the EDP's impression data was encrypted using
   * a KEK on their keyring, and the TrusTee output should use a key on the same keyring (either the
   * same key or a remapped key via kekUriToKeyNameMap). Supports both GCP KMS and AWS KMS URIs.
   *
   * Returns the KEK URI from the most recent data source (sorted by interval end time in descending
   * order). All data sources for the same EDP must use KEK URIs with the same project/account and
   * location/region; mixing different projects or accounts for the same EDP is not supported.
   *
   * Returns null if no data sources are available, in which case a non-encrypted empty sketch will
   * be fulfilled.
   *
   * @throws IllegalArgumentException if KEK URIs have different project/account or location/region
   */
  suspend fun getKekUri(): String? {
    val uniqueSources = getUniqueImpressionDataSources()
    if (uniqueSources.isEmpty()) return null

    // Validate all KEK URIs have the same project ID and location
    val kekUris = uniqueSources.map { it.blobDetails.encryptedDek.kekUri }
    validateKekUrisConsistency(kekUris)

    // Sort by interval end time in descending order and return the first KEK URI
    val sortedSources = uniqueSources.sortedByDescending { it.interval.endTime.seconds }
    return sortedSources.first().blobDetails.encryptedDek.kekUri
  }

  /**
   * Validates that all KEK URIs have the same project/account and location/region.
   *
   * Supported URI formats:
   * - GCP: gcp-kms://projects/{PROJECT}/locations/{LOCATION}/keyRings/{RING}/cryptoKeys/{KEY}
   * - AWS: aws-kms://arn:aws:kms:{REGION}:{ACCOUNT}:key/{KEY_ID}
   */
  private fun validateKekUrisConsistency(kekUris: List<String>) {
    if (kekUris.size <= 1) return

    val firstProjectAndLocation = extractProjectAndLocation(kekUris.first())
    for (kekUri in kekUris.drop(1)) {
      val projectAndLocation = extractProjectAndLocation(kekUri)
      require(projectAndLocation == firstProjectAndLocation) {
        "All KEK URIs must have the same project/account and location/region. " +
          "Expected: $firstProjectAndLocation, Found: $projectAndLocation in URI: $kekUri"
      }
    }
  }

  /** Extracts the [KmsKeyLocation] from a KEK URI. */
  private fun extractProjectAndLocation(kekUri: String): KmsKeyLocation {
    if (kekUri.startsWith("fake-kms://")) {
      return KmsKeyLocation("", "")
    }

    val gcpMatch = KmsConstants.GCP_KMS_KEY_URI_REGEX.matchEntire(kekUri)
    if (gcpMatch != null) {
      return KmsKeyLocation(gcpMatch.groupValues[1], gcpMatch.groupValues[2])
    }

    val awsMatch = KmsConstants.AWS_KMS_KEY_URI_REGEX.matchEntire(kekUri)
    if (awsMatch != null) {
      return KmsKeyLocation(awsMatch.groupValues[2], awsMatch.groupValues[1])
    }

    throw IllegalArgumentException("Unsupported KMS KEK URI format: $kekUri")
  }

  companion object {
    private val logger = Logger.getLogger(StorageEventSource::class.java.name)

    /** Default maximum total attempts for reading a single blob (first attempt + retries). */
    const val DEFAULT_READ_MAX_ATTEMPTS = 4

    /**
     * Default bound on concurrent per-blob storage read + DEK decrypt operations.
     *
     * Chosen conservatively: high enough to keep same-region GCS busy, low enough to avoid the
     * unbounded connection churn (~64-wide) that exhausted Cloud NAT ports and hammered KMS in
     * production.
     */
    const val DEFAULT_READ_CONCURRENCY = 16
  }
}
