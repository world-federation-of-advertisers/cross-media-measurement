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

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import com.google.type.Interval
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails

/** Per-blob statistics when scanning impression data. */
data class BlobImpressionStats(
  val metadataUri: String,
  val blobUri: String,
  val recordCount: Long,
  /** Number of distinct VIDs contributed by this blob relative to earlier blobs. */
  val newDistinctVids: Long,
)

/** Aggregated statistics across all processed impression blobs. */
data class FilteredImpressionStats(
  val celExpression: String,
  val totalRecords: Long,
  val distinctVids: Long,
  val blobStats: List<BlobImpressionStats>,
)

/** Aggregated statistics across all processed impression blobs. */
data class ImpressionStats(val filterStats: List<FilteredImpressionStats>)

/**
 * Scans impression blobs described by [BlobDetails] metadata and computes record/VID counts.
 *
 * The calculator reads impression records, applies filtering using [FilterProcessor], and tallies:
 * - Total records across all blobs after filtering
 * - Distinct VIDs across all blobs after filtering
 *
 * @param storageConfig storage configuration for accessing metadata and impression blobs.
 * @param kmsClient KMS client used to decrypt impression blobs. If null, blobs are read as
 *   plaintext.
 * @param eventDescriptor descriptor for the impression event message contained in
 *   `LabeledImpression.event`.
 * @param celExpressions CEL expressions used to filter events before counting. An empty list is
 *   treated as a single no-op filter for backwards compatibility.
 * @param collectionIntervalOverride optional interval to use for filtering. If null, a match-all
 *   interval is used for stats.
 */
class ImpressionStatsCalculator(
  private val storageConfig: StorageConfig,
  private val kmsClient: KmsClient?,
  private val eventDescriptor: Descriptors.Descriptor,
  private val celExpressions: List<String>,
  private val collectionIntervalOverride: Interval? = null,
) {

  /**
   * Container for per-filter aggregates. Equals/hashCode use reference identity to avoid issues
   * from mutating internal state.
   */
  private class FilterAccumulator(val id: Int, val celExpression: String) {
    val globalVids: LongOpenHashSet = LongOpenHashSet()
    val vidsByInterval: MutableMap<Interval, LongOpenHashSet> = mutableMapOf()
    val blobStats: MutableList<BlobImpressionStats> = mutableListOf()
    var totalRecords: Long = 0
  }

  /**
   * Computes statistics for the provided metadata URIs.
   *
   * @param metadataUris list of BlobDetails metadata URIs
   * @throws IllegalArgumentException if no metadata URIs are provided
   */
  suspend fun compute(metadataUris: Collection<String>): ImpressionStats {
    require(metadataUris.isNotEmpty()) { "At least one metadata URI is required" }

    logger.info("Scanning ${metadataUris.size} metadata blobs for impression stats")

    val filters: List<FilterAccumulator> =
      celExpressions.ifEmpty { listOf("") }.mapIndexed { index, expr ->
        FilterAccumulator(index, expr)
      }

    for (metadataUri in metadataUris) {
      val blobDetails = BlobDetailsLoader.load(metadataUri, storageConfig)
      scanBlob(metadataUri, blobDetails, filters)
    }

    val filterStats =
      filters.map { accumulator ->
        FilteredImpressionStats(
          celExpression = accumulator.celExpression,
          totalRecords = accumulator.totalRecords,
          distinctVids = accumulator.globalVids.size.toLong(),
          blobStats = accumulator.blobStats,
        )
      }
    return ImpressionStats(filterStats = filterStats)
  }

  private suspend fun scanBlob(
    metadataUri: String,
    blobDetails: BlobDetails,
    filters: List<FilterAccumulator>,
  ) {
    require(blobDetails.blobUri.isNotBlank()) {
      "BlobDetails at $metadataUri missing blob_uri"
    }

    if (filters.isEmpty()) {
      return
    }

    val blobInterval = blobDetails.interval

    val processors: Map<FilterAccumulator, FilterProcessor<Message>> =
      filters.associateWith { createFilterProcessor(blobDetails, it.celExpression) }
    val eventReader =
      StorageEventReader(blobDetails, kmsClient, storageConfig, eventDescriptor)

    logger.info("Counting impressions in ${blobDetails.blobUri}")

    // Get or create the VID set for this interval
    val intervalVidsByFilter = filters.associateWith { filter ->
      filter.vidsByInterval.getOrPut(blobInterval) { LongOpenHashSet() }
    }

    val startingDistinctByFilter =
      intervalVidsByFilter.mapValues { it.value.size.toLong() }.toMutableMap()
    val recordCountByFilter = filters.associateWith { 0L }.toMutableMap()

    try {
      eventReader.readEvents().collect { events ->
        if (events.isEmpty()) {
          return@collect
        }
        val eventBatch =
          EventBatch(
            events = events,
            minTime = events.minOf { it.timestamp },
            maxTime = events.maxOf { it.timestamp },
            eventGroupReferenceId = blobDetails.eventGroupReferenceId,
          )
        for ((accumulator, processor) in processors) {
          val filteredBatch = processor.processBatch(eventBatch)
          if (filteredBatch.events.isEmpty()) {
            continue
          }
          val updatedCount = recordCountByFilter.getValue(accumulator) + filteredBatch.events.size
          recordCountByFilter[accumulator] = updatedCount
          // Add VIDs to both the interval-specific set and the global set
          val intervalVids = intervalVidsByFilter.getValue(accumulator)
          filteredBatch.events.forEach { event ->
            intervalVids.add(event.vid)
            accumulator.globalVids.add(event.vid)
          }
        }
      }
    } catch (e: InvalidProtocolBufferException) {
      throw ImpressionReadException(
        blobDetails.blobUri,
        ImpressionReadException.Code.INVALID_FORMAT,
        e.message,
      )
    }

    for (filter in filters) {
      val recordCount = recordCountByFilter.getValue(filter)
      val intervalVids = intervalVidsByFilter.getValue(filter)
      val newDistinct = intervalVids.size.toLong() - startingDistinctByFilter.getValue(filter)
      if (recordCount > 0 || newDistinct > 0) {
        filter.blobStats +=
          BlobImpressionStats(
            metadataUri = metadataUri,
            blobUri = blobDetails.blobUri,
            recordCount = recordCount,
            newDistinctVids = newDistinct,
          )
        filter.totalRecords += recordCount
      }
      logger.info(
        "Finished ${blobDetails.blobUri} for filter '${filter.celExpression}': records=$recordCount, new distinct VIDs=$newDistinct"
      )
    }
  }

  private fun createFilterProcessor(
    blobDetails: BlobDetails,
    celExpression: String,
  ): FilterProcessor<Message> {
    val interval =
      collectionIntervalOverride ?: matchAllInterval

    val filterSpec =
      FilterSpec(
        celExpression = celExpression,
        collectionInterval = interval,
        eventGroupReferenceIds = listOf(blobDetails.eventGroupReferenceId),
      )

    return FilterProcessor<Message>(filterSpec, eventDescriptor)
  }

  companion object {
    private val logger = Logger.getLogger(ImpressionStatsCalculator::class.java.name)

    private const val MAX_TIMESTAMP_SECONDS = 253402300799L // 9999-12-31T23:59:59Z
    private val matchAllInterval: Interval =
      Interval.newBuilder()
        .setStartTime(Timestamp.newBuilder().setSeconds(0).build())
        .setEndTime(Timestamp.newBuilder().setSeconds(MAX_TIMESTAMP_SECONDS).build())
        .build()
  }
}
