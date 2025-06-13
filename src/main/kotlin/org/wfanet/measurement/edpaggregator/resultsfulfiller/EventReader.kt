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
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression

/**
 * Reads labeled impressions from storage with caching support.
 *
 * This class uses EventGroupCache to provide efficient access to labeled impressions
 * while avoiding repeated downloads of the same data from storage.
 *
 * @param kmsClient The KMS client for encryption operations
 * @param impressionsStorageConfig Configuration for impressions storage
 * @param impressionDekStorageConfig Configuration for impression DEK storage
 * @param labeledImpressionsDekPrefix Prefix for labeled impressions DEK
 * @param cacheMaxMemoryMB Maximum memory for cache in MB (default: 200GB)
 * @param cacheExpireAfterMinutes Cache expiration time in minutes (default: 60)
 */
class EventReader(
  kmsClient: KmsClient,
  impressionsStorageConfig: StorageConfig,
  impressionDekStorageConfig: StorageConfig,
  labeledImpressionsDekPrefix: String,
  cacheMaxMemoryMB: Long = 200_000,
  cacheExpireAfterMinutes: Long = 60,
) {
  private val eventGroupCache = EventGroupCache(
    kmsClient = kmsClient,
    impressionsStorageConfig = impressionsStorageConfig,
    impressionDekStorageConfig = impressionDekStorageConfig,
    labeledImpressionsDekPrefix = labeledImpressionsDekPrefix,
    cacheMaxMemoryMB = cacheMaxMemoryMB,
    cacheExpireAfterMinutes = cacheExpireAfterMinutes,
  )
  /**
   * Retrieves a flow of labeled impressions for a given ds and event group ID.
   * 
   * This method uses the EventGroupCache to avoid repeated storage reads.
   * First call for a given (ds, eventGroupId) will fetch from storage and cache.
   * Subsequent calls will return cached data until expiration.
   *
   * @param ds The ds for the labeled impressions
   * @param eventGroupId The ID of the event group
   * @return A flow of labeled impressions
   */
  suspend fun getLabeledImpressions(ds: LocalDate, eventGroupId: String): Flow<LabeledImpression> {
    return eventGroupCache.getLabeledImpressions(ds, eventGroupId)
  }

  /**
   * Gets cache statistics for monitoring cache performance.
   * 
   * @return Map containing cache statistics like hit rate, memory usage, etc.
   */
  fun getCacheStats(): Map<String, Any> {
    return eventGroupCache.getCacheStats()
  }

  /**
   * Invalidates all cached entries, forcing fresh reads from storage on next access.
   */
  fun invalidateCache() {
    eventGroupCache.invalidateAll()
  }

  /**
   * Invalidates cached entries for a specific event group.
   * 
   * @param ds The date for the labeled impressions
   * @param eventGroupId The ID of the event group
   */
  fun invalidateCache(ds: LocalDate, eventGroupId: String) {
    eventGroupCache.invalidate(ds, eventGroupId)
  }
}
