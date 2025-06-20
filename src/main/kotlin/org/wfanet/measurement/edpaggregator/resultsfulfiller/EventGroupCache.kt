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

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.Weigher
import com.google.crypto.tink.KmsClient
import java.time.LocalDate
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Cache for event group impression data to avoid repeatedly downloading the same data from storage.
 *
 * This class uses Guava's LoadingCache, which is a cache that knows how to load values automatically
 * when they are requested but not present. Key features:
 * - Automatic loading: When getLabeledImpressions() is called with a key not in the cache,
 *   the cache automatically calls the load() method to fetch the data from storage
 * - Thread-safe: Multiple concurrent requests for the same key will result in only one load operation
 * - Memory-bounded: The cache uses a weigher to estimate memory usage and evicts entries when
 *   the maximum weight is exceeded, using LRU (Least Recently Used) eviction
 * - Time-based expiration: Entries expire after a configurable time period
 *
 * Memory estimation for LabeledImpression:
 * Based on the proto definition:
 * - event_time (Timestamp): ~12 bytes (8 bytes seconds + 4 bytes nanos)
 * - vid (int64): 8 bytes
 * - event (Any): ~300 bytes average (100-500 bytes typical range)
 * - event_group_reference_id (string): ~80 bytes average
 * - Proto overhead + Java object overhead: ~100 bytes
 * Total: ~500 bytes per impression
 *
 * For a 280GB machine with 200GB allocated for cache, this allows caching ~400 million impressions.
 *
 * @param kmsClient The KMS client for encryption operations
 * @param impressionsStorageConfig Configuration for impressions storage
 * @param impressionDekStorageConfig Configuration for impression DEK storage
 * @param labeledImpressionsDekPrefix Prefix for labeled impressions DEK
 * @param cacheMaxMemoryMB Maximum memory usage in megabytes (default: 200GB for 280GB machines)
 * @param cacheExpireAfterMinutes Cache expiration time in minutes (default: 60)
 */
class EventGroupCache(
  private val kmsClient: KmsClient,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionDekStorageConfig: StorageConfig,
  private val labeledImpressionsDekPrefix: String,
  private val cacheMaxMemoryMB: Long = 200_000, // 200GB default for 280GB machines
  private val cacheExpireAfterMinutes: Long = 60,
) {
  /** Cache key for event group lookups */
  private data class EventGroupKey(val ds: LocalDate, val eventGroupReferenceId: String)

  companion object {
    /**
     * Estimated size of a single LabeledImpression in bytes.
     * Based on proto definition analysis:
     * - event_time: 12 bytes
     * - vid: 8 bytes
     * - event: 300 bytes (average, varies 100-500)
     * - event_group_reference_id: 80 bytes
     * - Proto + Java overhead: 100 bytes
     * Total: 500 bytes
     */
    private const val ESTIMATED_IMPRESSION_SIZE_BYTES = 500

    /**
     * Safety factor to account for GC overhead and prevent OOM.
     * We use 80% of the configured max memory to leave headroom.
     */
    private const val MEMORY_SAFETY_FACTOR = 0.8

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }

  /**
   * LoadingCache for labeled impressions.
   *
   * How it works:
   * 1. When get() is called, the cache first checks if the key exists
   * 2. If the key exists and hasn't expired, it returns the cached value immediately
   * 3. If the key doesn't exist or has expired, it calls the load() method in the CacheLoader
   * 4. The load() method fetches data from storage (expensive operation)
   * 5. The fetched data is stored in the cache and returned
   * 6. Subsequent calls with the same key return the cached data without hitting storage
   *
   * Memory management:
   * - Uses a Weigher to calculate the memory footprint of each entry
   * - Weight is calculated as: number of impressions × estimated size per impression
   * - When total weight exceeds maximumWeight, least recently used entries are evicted
   * - The safety factor ensures we don't use all allocated memory, leaving room for GC
   */
  private val impressionsCache: LoadingCache<EventGroupKey, List<LabeledImpression>> =
    CacheBuilder.newBuilder()
      .maximumWeight((cacheMaxMemoryMB * 1024 * 1024 * MEMORY_SAFETY_FACTOR).toLong())
      .weigher(
        Weigher<EventGroupKey, List<LabeledImpression>> { _, impressions ->
          // Calculate weight based on number of impressions
          // Each impression is estimated at ESTIMATED_IMPRESSION_SIZE_BYTES
          impressions.size * ESTIMATED_IMPRESSION_SIZE_BYTES
        }
      )
      .expireAfterWrite(cacheExpireAfterMinutes, TimeUnit.MINUTES)
      .recordStats() // Enable statistics collection for monitoring
      .build(
        object : CacheLoader<EventGroupKey, List<LabeledImpression>>() {
          override fun load(key: EventGroupKey): List<LabeledImpression> {
            // This method is called automatically when a key is not in the cache
            // It runs in a blocking context because CacheLoader is synchronous
            return runBlocking {
              val blobDetails = fetchBlobDetails(key.ds, key.eventGroupReferenceId)
              logger.info("~~~~~~~~~~~~~~~~~~~ fetching impressions")
              val test = fetchLabeledImpressions(blobDetails).toList()
              logger.info("~~~~~~~~~~~~~~~~~~~ fetching impressions3: $test")
              test.toList()
            }
          }
        }
      )

  /**
   * Gets labeled impressions for a given date and event group ID.
   *
   * This method uses caching to avoid repeated storage reads and parsing.
   * On cache hit: Returns immediately with cached data (microseconds)
   * On cache miss: Fetches from storage, caches, and returns (seconds)
   *
   * @param ds The date for the labeled impressions
   * @param eventGroupReferenceId The ID of the event group
   * @return A flow of labeled impressions
   */
  suspend fun getLabeledImpressions(ds: LocalDate, eventGroupReferenceId: String): Flow<LabeledImpression> {
    val key = EventGroupKey(ds, eventGroupReferenceId)
    // get() will either return cached data or trigger a load from storage
    val impressions = impressionsCache.get(key)
    // Convert the cached list back to a Flow for consistency with the API
    return flow {
      impressions.forEach { emit(it) }
    }
  }

  /**
   * Invalidates all cached entries, forcing fresh reads from storage on next access.
   * Use this when you know the underlying data has changed globally.
   */
  fun invalidateAll() {
    impressionsCache.invalidateAll()
  }

  /**
   * Invalidates cached entries for a specific event group.
   * Use this when you know a specific event group's data has changed.
   *
   * @param ds The date for the labeled impressions
   * @param eventGroupId The ID of the event group
   */
  fun invalidate(ds: LocalDate, eventGroupId: String) {
    val key = EventGroupKey(ds, eventGroupId)
    impressionsCache.invalidate(key)
  }

  /**
   * Fetches blob details from storage.
   * This method is not cached as blob details are small and quick to fetch.
   *
   * @param ds The date of the encrypted dek
   * @param eventGroupId The ID of the event group
   * @return The blob details with the DEK
   */
  private suspend fun fetchBlobDetails(ds: LocalDate, eventGroupReferenceId: String): BlobDetails {
    val dekBlobKey = "ds/$ds/event-group-reference-id/$eventGroupReferenceId/metadata"
    val dekBlobUri = "$labeledImpressionsDekPrefix/$dekBlobKey"

    val storageClientUri = SelectedStorageClient.parseBlobUri(dekBlobUri)
    val impressionsDekStorageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionDekStorageConfig.rootDirectory,
        impressionDekStorageConfig.projectId,
      )
    // Get EncryptedDek message from storage using the blobKey made up of the ds and eventGroupId
    logger.info("Reading blob $dekBlobKey")
    val blob =
      impressionsDekStorageClient.getBlob(dekBlobKey)
        ?: throw ImpressionReadException(dekBlobKey, ImpressionReadException.Code.BLOB_NOT_FOUND)

    val full = blob.read().flatten()
    try {
      logger.info("Read blob size: ${full.size()} bytes")
      logger.info("~~~~~~~~~~~~~ blob: $full")
      logger.info("Base64 blob contents: ${Base64.getEncoder().encodeToString(full.toByteArray())}")
    } catch (e: Exception){
      logger.severe(e.message)
    }
    return BlobDetails.parseFrom(full)
//    return BlobDetails.parseFrom(blob.read().flatten())
  }

  /**
   * Fetches labeled impressions from storage using blob details.
   * This is the expensive operation that we want to cache.
   *
   * @param blobDetails The blob details with the DEK
   * @return A flow of labeled impressions
   */
  private suspend fun fetchLabeledImpressions(blobDetails: BlobDetails): Flow<LabeledImpression> {
    val storageClientUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)
    logger.info("~~~~~~~ fun fetchLabeledImpressions")
    val encryptedDek = blobDetails.encryptedDek
    val selectedStorageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionsStorageConfig.rootDirectory,
        impressionsStorageConfig.projectId,
      )

    val impressionsStorage =
      EncryptedStorage.buildEncryptedMesosStorageClient(
        selectedStorageClient,
        kekUri = encryptedDek.kekUri,
        kmsClient = kmsClient,
        serializedEncryptionKey = encryptedDek.encryptedDek,
      )

    val impressionBlob =
      impressionsStorage.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )
    logger.info("~~~~~~~ fun fetchLabeledImpressions 2")

//    return impressionBlob.read().map { impressionByteString ->
//      LabeledImpression.parseFrom(impressionByteString)
//        ?: throw ImpressionReadException(
//          storageClientUri.key,
//          ImpressionReadException.Code.INVALID_FORMAT,
//        )
//    }
    var failedCount = 0

    return impressionBlob.read().map { impressionByteString ->
      try {
        LabeledImpression.parseFrom(impressionByteString)
      } catch (e: Exception) {
        failedCount++
        logger.info("~~~~~~~~~~~~~~~~~~~~~` IMPRESSION FAILED: $failedCount $e")
        throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.INVALID_FORMAT,
        )
      }
    }.also {
      if (failedCount > 0) {
        logger.info { "$failedCount impressions failed to parse" }
      }
    }
  }

  /**
   * Gets cache statistics for monitoring purposes.
   *
   * Useful statistics include:
   * - hitCount: Number of times get() returned a cached value
   * - missCount: Number of times get() had to load a value
   * - loadSuccessCount: Number of successful loads
   * - loadExceptionCount: Number of failed loads
   * - totalLoadTime: Total time spent loading values
   * - evictionCount: Number of entries evicted due to size constraints
   *
   * @return Cache statistics including size, memory usage estimate, and performance metrics
   */
  fun getCacheStats(): Map<String, Any> {
    val stats = impressionsCache.stats()
    val currentSize = impressionsCache.size()

    // Calculate total impressions across all cached entries
    var totalImpressions = 0L
    impressionsCache.asMap().values.forEach { list ->
      totalImpressions += list.size
    }

    val estimatedMemoryMB = (totalImpressions * ESTIMATED_IMPRESSION_SIZE_BYTES) / (1024.0 * 1024.0)
    val maxCapacityImpressions = (cacheMaxMemoryMB * 1024 * 1024 * MEMORY_SAFETY_FACTOR / ESTIMATED_IMPRESSION_SIZE_BYTES).toLong()

    return mapOf(
      "currentEntries" to currentSize,
      "totalImpressionsCached" to totalImpressions,
      "estimatedMemoryMB" to "%.2f".format(estimatedMemoryMB),
      "maxMemoryMB" to cacheMaxMemoryMB,
      "maxCapacityImpressions" to maxCapacityImpressions,
      "utilizationPercent" to "%.2f%%".format((estimatedMemoryMB / cacheMaxMemoryMB) * 100),
      "hitRate" to "%.2f%%".format(stats.hitRate() * 100),
      "missRate" to "%.2f%%".format(stats.missRate() * 100),
      "evictionCount" to stats.evictionCount(),
      "loadCount" to stats.loadCount(),
      "totalLoadTimeMs" to stats.totalLoadTime() / 1_000_000, // Convert nanos to millis
      "averageLoadPenaltyMs" to stats.averageLoadPenalty() / 1_000_000
    )
  }
}
